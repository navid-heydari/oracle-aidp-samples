"""``verify``: static checks on a migration's output. PASS / REVIEW / FAIL.

FAIL means the artifact is broken as written (a cell that does not parse, a
credential literal, a job that points at a notebook this migration did not
produce, a dependency cycle, an invalid cron). REVIEW means it is well formed
but carries findings or unfilled work orders. Nothing here runs Spark.
"""
from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .fallback.validate import _SECRET_PATTERNS
from .migrate import IN_PROGRESS
from .naming import JOB_NAME
from .notebook import cell_text
from .report import Report

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

RUN_IF = {"ALL_SUCCESS", "ALL_FAILED", "ALL_DONE", "AT_LEAST_ONE_SUCCESS", "AT_LEAST_ONE_FAILED",
          "NONE_FAILED"}
_QUARTZ_FIELD = re.compile(r"^[0-9A-Z*?/,#L\-W]+$")


@dataclass
class Check:
    artifact: str
    grade: str = "PASS"            # PASS | REVIEW | FAIL
    problems: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    def fail(self, msg):
        self.grade = "FAIL"
        self.problems.append(msg)

    def review(self, msg):
        if self.grade == "PASS":
            self.grade = "REVIEW"
        self.notes.append(msg)


def quartz_problem(expr: str) -> str:
    fields = expr.split()
    if len(fields) not in (6, 7):
        return f"{expr!r} has {len(fields)} fields; Quartz needs 6 or 7"
    if any(not _QUARTZ_FIELD.match(f.upper()) for f in fields):
        return f"{expr!r} has a field Quartz does not accept"
    dom, dow = fields[3], fields[5]
    if (dom == "?") == (dow == "?"):
        return f"{expr!r}: exactly one of day-of-month and day-of-week must be '?'"
    return ""


def check_notebook(path: Path, out_dir: Path) -> Check:
    c = Check(str(path.relative_to(out_dir)))
    try:
        nb = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        c.fail(f"not valid JSON: {exc}")
        return c
    if nb.get("nbformat") != 4:
        c.fail("not an nbformat 4 notebook")
    tagged = False
    pending = assisted = 0
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        text = cell_text(cell)
        if "parameters" in (cell.get("metadata") or {}).get("tags", []):
            tagged = True
        body = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith(("%", "!")))
        try:
            ast.parse(body)
        except SyntaxError as exc:
            c.fail(f"cell {i} does not parse: {exc.msg} (line {exc.lineno})")
        for pattern, what in _SECRET_PATTERNS:
            if pattern.search(text):
                c.fail(f"cell {i} contains {what}")
        pending += text.count("# REVIEW REQUIRED [FB_")
        assisted += text.count("# LLM-ASSISTED -- REVIEW REQUIRED [FB_") // 2
    if not tagged:
        c.review("no cell tagged 'parameters': job task parameters cannot be read")
    if pending:
        c.review(f"{pending} unconverted step(s) that raise until filled")
    if assisted:
        c.review(f"{assisted} LLM-written step(s) to read before cutover")
    return c


def _cycle(tasks: list) -> bool:
    deps = {t["taskKey"]: [d["taskKey"] for d in t.get("dependsOn", [])] for t in tasks}
    state = {}

    def visit(k):
        if state.get(k) == 1:
            return True
        if state.get(k) == 2:
            return False
        state[k] = 1
        if any(visit(d) for d in deps.get(k, []) if d in deps):
            return True
        state[k] = 2
        return False
    return any(visit(k) for k in deps)


def check_job(path: Path, out_dir: Path, notebook_paths: set) -> Check:
    c = Check(str(path.relative_to(out_dir)))
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        c.fail(f"not valid JSON: {exc}")
        return c
    if not JOB_NAME.fullmatch(body.get("name", "")):
        c.fail(f"job name {body.get('name')!r} is not a name AIDP accepts")
    tasks = body.get("tasks") or []
    if not tasks:
        c.fail("job has no tasks")
    keys = [t.get("taskKey") for t in tasks]
    if len(set(keys)) != len(keys):
        c.fail("duplicate taskKey")
    for t in tasks:
        if t.get("type") != "NOTEBOOK_TASK":
            c.fail(f"task {t.get('taskKey')}: type {t.get('type')} is not NOTEBOOK_TASK")
        if t.get("notebookPath") not in notebook_paths:
            c.fail(f"task {t.get('taskKey')} runs {t.get('notebookPath')}, which this migration "
                   f"did not produce")
        if t.get("runIf") not in RUN_IF:
            c.fail(f"task {t.get('taskKey')}: runIf {t.get('runIf')} is not known")
        for d in t.get("dependsOn", []):
            if d.get("taskKey") not in keys:
                c.fail(f"task {t.get('taskKey')} depends on missing task {d.get('taskKey')}")
        if isinstance(t.get("parameters"), dict):
            c.fail(f"task {t.get('taskKey')}: parameters must be a list of {{name, value}}")
    if _cycle(tasks):
        c.fail("task dependencies form a cycle")
    sched = body.get("schedule")
    if sched:
        problem = quartz_problem(sched.get("quartzCronExpression", ""))
        if problem:
            c.fail(problem)
        tz = sched.get("timezoneId")
        if ZoneInfo is not None:
            try:
                ZoneInfo(tz or "")
            except Exception:
                c.fail(f"timezone {tz!r} is not an IANA zone")
        if sched.get("pauseStatus") != "PAUSED":
            c.review("schedule is not PAUSED: the job fires as soon as it is published")
    review = path.with_name(path.name.replace(".job.json", ".review.md"))
    if review.is_file() and "**review**" in review.read_text(encoding="utf-8") or \
            review.is_file() and "**manual**" in review.read_text(encoding="utf-8"):
        c.review(f"read {review.name}")
    return c


def verify(out_dir) -> list:
    out_dir = Path(out_dir)
    checks = []
    top = Check("report.json")
    if not (out_dir / "report.json").is_file():
        top.fail("no report.json; run migrate")
        return [top]
    if (out_dir / IN_PROGRESS).exists():
        top.fail("an interrupted migration: re-run migrate")
    report = Report.load(out_dir)
    notebook_paths = {r.extra.get("remote_path") for r in report.results
                      if r.kind in ("notebook", "ddl", "reconcile")}
    checks.append(top)
    for r in report.results:
        if r.kind in ("notebook", "ddl", "reconcile"):
            for a in r.artifacts:
                if a.endswith(".ipynb"):
                    p = out_dir / a
                    if not p.is_file():
                        c = Check(a)
                        c.fail("listed in report.json but missing")
                        checks.append(c)
                        continue
                    c = check_notebook(p, out_dir)
                    if r.status in ("manual", "failed"):
                        c.review(f"report status {r.status}")
                    checks.append(c)
        elif r.kind == "job" and r.artifacts:
            p = out_dir / r.artifacts[0]
            if p.is_file():
                c = check_job(p, out_dir, notebook_paths)
                if r.status == "manual":
                    c.review("parts of this pipeline are not in the job (manual)")
                checks.append(c)
    return checks
