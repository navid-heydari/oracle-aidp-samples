"""Splice validated fallback answers into their notebooks and update the report."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..notebook import cell_text, read_ipynb, write_ipynb
from ..report import Finding, Report, status_for
from .validate import extract_code, validate
from .workorder import load_orders, response_path


@dataclass
class Outcome:
    id: str
    state: str          # pending | rejected | declined | applied
    notebook: str = ""
    errors: list = field(default_factory=list)
    assumptions: list = field(default_factory=list)


def _splice(nb: dict, fid: str, code: str, banner: str) -> bool:
    start, end = f"# >>> {fid}", f"# <<< {fid}"
    for cell in nb.get("cells", []):
        text = cell_text(cell)
        if start not in text or end not in text:
            continue
        before, rest = text.split(start, 1)
        _, after = rest.split(end, 1)
        before = before.replace(f"# REVIEW REQUIRED [{fid}]:",
                                f"# LLM-ASSISTED -- REVIEW REQUIRED [{fid}]:")
        new = f"{before}{start}\n{banner}\n{code.rstrip()}\n{end}{after}"
        lines = new.splitlines(keepends=True)
        cell["source"] = lines
        meta = cell.setdefault("metadata", {}).setdefault("ocidi2aidp", {})
        meta["fallback_status"] = "llm_assisted"
        return True
    return False


def apply(out_dir, *, author: str = "session") -> list:
    out_dir = Path(out_dir)
    report = Report.load(out_dir)
    by_id = {fid: r for r in report.results for fid in r.fallbacks}
    outcomes = []
    for order in load_orders(out_dir):
        fid = order["id"]
        resp = response_path(out_dir, fid)
        result = by_id.get(fid)
        if result is None:
            outcomes.append(Outcome(fid, "rejected", errors=["no report row lists this work "
                                                              "order; re-run migrate"]))
            continue
        nb_path = out_dir / result.artifacts[0]
        if not resp.is_file():
            outcomes.append(Outcome(fid, "pending", str(nb_path)))
            continue
        verdict = validate(extract_code(resp.read_text(encoding="utf-8")), order)
        if not verdict.ok:
            outcomes.append(Outcome(fid, "rejected", str(nb_path), verdict.errors))
            continue
        node = order["node"]["label"]
        result.findings = [f for f in result.findings
                           if not (f.code == "FB01_LLM" and f.node == node)]
        result.findings = [f for f in result.findings if not (f.code.startswith("FB0")
                                                              and f.node == node
                                                              and fid in f.message)]
        if verdict.declined:
            result.findings.append(Finding("FB03_DECLINED", f"{fid}: the fallback author "
                                           f"declined -- {order['reason']}; rebuild by hand",
                                           "manual", node))
            outcomes.append(Outcome(fid, "declined", str(nb_path)))
        else:
            nb = read_ipynb(nb_path)
            banner = (f"# LLM-ASSISTED -- REVIEW REQUIRED [{fid}]: written by {author} for: "
                      f"{order['reason'][:200]}")
            if not _splice(nb, fid, verdict.code, banner):
                result.findings.append(Finding("FB01_LLM", order["reason"], "fallback", node))
                outcomes.append(Outcome(fid, "rejected", str(nb_path),
                                        ["the stub markers are not in the notebook; re-run "
                                         "migrate, then apply again"]))
                continue
            write_ipynb(nb_path, nb)
            notes = "; ".join(verdict.assumptions) or "none stated"
            result.findings.append(Finding("FB02_LLM_ASSISTED", f"{fid}: LLM-written code "
                                           f"({author}) passed validation -- read it before "
                                           f"cutover. Assumptions: {notes}", "review", node))
            done = result.extra.setdefault("llm_assisted", [])
            if fid not in done:
                done.append(fid)
            outcomes.append(Outcome(fid, "applied", str(nb_path),
                                    assumptions=verdict.assumptions))
        status = status_for(result.findings)
        if status in ("ok", "needs_review") and result.extra.get("llm_assisted"):
            status = "llm_assisted"
        result.status = status
    report.write(out_dir)
    return outcomes
