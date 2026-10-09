"""``migrate``: snapshot -> notebooks, jobs, DDL, work orders, report. Offline.

What gets migrated:

* Every Integration / Data Loader / SQL / REST / OCI Data Flow task becomes a
  notebook. Where a task is published in an application, the **published**
  copy is compiled -- that is what runs in production, and it can differ from
  the design-time copy in the project.
* Every Pipeline task becomes a multi-task job.
* Every published non-pipeline task, and every design-time task no pipeline
  references, gets a single-task job so it can still be run on its own.
* A task schedule on a published task becomes that job's (PAUSED) schedule.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional

from . import __version__
from .compile import artifacts
from .compile.dataflow import FlowCompiler
from .compile.expressions import Udf
from .compile.pipeline import PipelineCompiler, TaskArtifact
from .compile.schedule import convert as convert_schedule
from .compile.tasks import TaskNotebook
from .di import expr_text
from .naming import file_stem, job_name, task_key, unique
from .report import Finding, ObjectResult, Report, status_for
from .snapshot import Snapshot

FLOW_TASKS = ("INTEGRATION_TASK", "DATA_LOADER_TASK")
OTHER_TASKS = ("SQL_TASK", "REST_TASK", "OCI_DATAFLOW_TASK")
IN_PROGRESS = ".migrate-in-progress"


class MigrationError(Exception):
    pass


class Migration:
    def __init__(self, snapshot: Snapshot, out_dir: Path, config, *, strict: bool = False,
                 only: Optional[set] = None):
        self.snap = snapshot
        self.out = Path(out_dir)
        self.config = config
        self.strict = strict
        self.only = {o.upper() for o in only} if only else None
        self.report = Report(__version__, str(snapshot.root), config.to_json())
        self.artifacts: dict = {}       # task identity -> TaskArtifact
        self.results: dict = {}         # task identity -> ObjectResult (notebook)
        self.job_names: set = set()
        self.nb_paths: set = set()
        self.ddl: list = []
        self.seeds: list = []
        self.fallbacks: list = []
        self.udfs = self._udfs()

    # ------------------------------------------------------------ helpers
    @staticmethod
    def identity(task: dict) -> str:
        return (task.get("identifier") or task.get("name") or task.get("key") or "").upper()

    def _udfs(self) -> dict:
        out = {}
        libs = {k: v.get("name") for k, v in self.snap.all("function_libraries").items()}
        for u in self.snap.all("user_defined_functions").values():
            sig = (u.get("signatures") or [{}])[0]
            args = [a.get("name") for a in sig.get("arguments") or [] if a.get("name")]
            body = expr_text(u.get("expr"))
            if not (u.get("name") and body):
                continue
            udf = Udf(u["name"], args, body)
            out[u["name"].upper()] = udf
            lib = libs.get((u.get("parentRef") or {}).get("parent"))
            if lib:
                out[f"{lib}.{u['name']}".upper()] = udf
        return out

    def _tasks(self) -> list:
        """Task objects to migrate: published copies first, then design-only ones."""
        chosen, order = {}, []
        for po in self.snap.published_tasks():
            ident = self.identity(po)
            if ident and ident not in chosen:
                chosen[ident] = po
                order.append(ident)
        for t in self.snap.all("tasks").values():
            ident = self.identity(t)
            if ident and ident not in chosen:
                chosen[ident] = t
                order.append(ident)
        out = [chosen[i] for i in order]
        if self.only:
            out = [t for t in out if self.identity(t) in self.only or
                   (t.get("name") or "").upper() in self.only]
        return out

    def _design_copy(self, task: dict) -> dict:
        return self.snap.find("tasks", task.get("identifier")) or \
            self.snap.find("tasks", task.get("name")) or task

    def _paths(self, task: dict) -> tuple[str, Path]:
        design = self._design_copy(task)
        folders = [file_stem(n) for n in self.snap.folder_path(design)]
        stem = file_stem(task.get("name") or task.get("identifier") or task.get("key"))
        rel = "/".join(folders + [stem])
        candidate = rel
        n = 2
        while candidate in self.nb_paths:
            candidate = f"{rel}_{n}"
            n += 1
        self.nb_paths.add(candidate)
        remote = f"{self.config.aidp.notebook_root.rstrip('/')}/{candidate}.ipynb"
        return remote, self.out / "notebooks" / f"{candidate}.ipynb"

    @staticmethod
    def _bindings(task: dict) -> dict:
        out = {}
        for name, value in ((task.get("configProviderDelegate") or {}).get("bindings") or {}).items():
            if isinstance(value, dict) and "simpleValue" in value:
                out[name] = value["simpleValue"]
        return out

    # ---------------------------------------------------------- notebooks
    def compile_task(self, task: dict) -> TaskArtifact:
        ident = self.identity(task)
        if ident in self.artifacts:
            return self.artifacts[ident]
        kind = task.get("modelType", "")
        name = task.get("name") or task.get("identifier") or task.get("key")
        if kind == "PIPELINE_TASK":
            pipeline = task.get("pipeline") or {}
            if not pipeline.get("nodes"):
                pipeline = self.snap.find("pipelines", pipeline) or pipeline
            art = TaskArtifact(pipeline=pipeline, name=name)
            self.artifacts[ident] = art
            return art
        if kind not in FLOW_TASKS + OTHER_TASKS:
            art = TaskArtifact(name=name, error=f"task type {kind or '(none)'} is not supported")
            self.artifacts[ident] = art
            return art
        remote, local = self._paths(task)
        result = ObjectResult("notebook", name, kind, task.get("key", ""),
                              artifacts=[str(local.relative_to(self.out))],
                              extra={"remote_path": remote,
                                     "published": bool(task.get("_applicationKey")),
                                     "application": task.get("_applicationKey", "")})
        if not task.get("_applicationKey"):
            result.findings.append(Finding("MG01_DESIGN_ONLY", "not published in any application "
                                           "in the snapshot; compiled from the design-time copy",
                                           "info"))
        try:
            if kind in FLOW_TASKS:
                nb = self._compile_flow(task, name, remote, result)
            else:
                tn = TaskNotebook(task, snapshot=self.snap, config=self.config,
                                  notebook_path=remote)
                nb = tn.build()
                result.findings += tn.findings
                self._add_fallbacks(tn.fallbacks, result)
            nb.write(local)
        except Exception as exc:
            if self.strict:
                raise
            result.findings.append(Finding("MG99_ERROR", f"{type(exc).__name__}: {exc}", "manual"))
            result.status = "failed"
            self.report.add(result)
            art = TaskArtifact(name=name, error=f"compilation failed: {exc}")
            self.artifacts[ident] = art
            return art
        result.status = status_for(result.findings)
        self.report.add(result)
        self.results[ident] = result
        art = TaskArtifact(notebook_path=remote, name=name)
        self.artifacts[ident] = art
        return art

    def _compile_flow(self, task: dict, name: str, remote: str, result: ObjectResult):
        flow = task.get("dataFlow") or {}
        if not flow.get("nodes"):
            flow = self.snap.find("data_flows", flow) or flow
        if not flow.get("nodes"):
            raise ValueError("the task's data flow is not in the snapshot")
        kind = task.get("modelType")
        if kind == "DATA_LOADER_TASK" and task.get("isSingleLoad") is False:
            result.findings.append(Finding("DL01_MULTI", "multi-entity data loader: each entity "
                                           "is a source/target pair in the flow; check that every "
                                           "entity is present", "review"))
        overrides = self._bindings(task)
        if any(str(k).upper().startswith("OCI_DF_") for k in overrides):
            result.findings.append(Finding("MG02_OCI_DF_RUNTIME", "the task ran on OCI Data Flow; "
                                           "pool/shape settings are replaced by the AIDP job "
                                           "cluster", "info"))
        fc = FlowCompiler(flow, snapshot=self.snap, config=self.config, name=name,
                          key=task.get("key", ""), kind=kind, overrides=overrides, udfs=self.udfs,
                          wm_task=name, notebook_path=remote, strict=self.strict)
        res = fc.compile()
        result.findings += res.findings
        self._add_fallbacks(res.fallbacks, result)
        self.ddl += res.ddl
        for t in res.targets:
            t["object"] = name
            self.report.targets.append(t)
        for s in res.sources:
            if s.to_json() not in self.report.sources:
                self.report.sources.append(s.to_json())
        for wm in res.watermarks:
            self.seeds.append(self._seed(task, name, wm))
        if "SYS_LAST_LOAD_DATE" in fc.used_params:
            self.seeds.append(self._seed(task, name, {"source": "SYS.LAST_LOAD_DATE",
                                                      "column": "SYS.LAST_LOAD_DATE"}))
        if res.failed:
            result.findings.append(Finding("MG03_FLOW", "data flow could not be compiled", "manual"))
        return res.notebook

    def _seed(self, task: dict, name: str, wm: dict) -> dict:
        run = None
        app = task.get("_applicationKey")
        if app:
            run = self.snap.last_success_run(app, task.get("key", ""))
        value, basis = None, "no successful run in the snapshot"
        if run and run.get("startTimeMillis"):
            value = artifacts.millis_to_iso(run["startTimeMillis"])
            basis = f"start of OCI-DI run {run.get('key', '?')}"
        return {"task": name, "source": wm["source"], "column": wm["column"], "value": value,
                "basis": basis}

    def _add_fallbacks(self, orders: list, result: ObjectResult) -> None:
        for o in orders:
            self.fallbacks.append(o)
            result.fallbacks.append(o["id"])

    # --------------------------------------------------------------- jobs
    def _job(self, name: str, source: dict, tasks: list, findings: list, *, kind: str) -> None:
        jname = unique(job_name(name, self.config.job_prefix), self.job_names)
        body = {"name": jname,
                "description": f"Migrated from OCI Data Integration {kind} {name} by ocidi2aidp "
                               f"{__version__}.",
                "path": "jobs", "maxConcurrentRuns": 1, "tasks": tasks}
        by_path = {r.extra.get("remote_path"): r for r in self.report.results if r.kind == "notebook"}
        for t in tasks:
            nb = by_path.get(t["notebookPath"])
            if nb is not None and nb.status not in ("ok", "needs_review"):
                findings.append(Finding("JB03_NOTEBOOK_STATUS", f"task {t['taskKey']} runs notebook "
                                        f"{nb.name}, which is {nb.status}; it raises at its first "
                                        f"unconverted step until that is resolved", "review"))
        if self.config.aidp.cluster_key:
            for t in tasks:
                t["cluster"] = {"clusterKey": self.config.aidp.cluster_key}
        else:
            findings.append(Finding("JB01_NO_CLUSTER", "no cluster key configured: AIDP will not "
                                    "create this job until publish is given --cluster-key "
                                    "(tasks[].cluster is required)", "info"))
        sched, sched_findings = self._schedule(source, tasks)
        findings += sched_findings
        if sched:
            body["schedule"] = sched
        rel = f"jobs/{jname}.job.json"
        (self.out / "jobs").mkdir(parents=True, exist_ok=True)
        (self.out / rel).write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
        notes = self.out / f"jobs/{jname}.review.md"
        notes.write_text(f"# Job {jname}\n\nFrom OCI-DI {kind} `{name}`.\n\n"
                         + ("\n".join(f"- **{f.severity}** {f.render()}" for f in findings)
                            or "Nothing to review.") + "\n", encoding="utf-8")
        result = ObjectResult("job", jname, kind, source.get("key", ""),
                              artifacts=[rel, f"jobs/{jname}.review.md"], findings=findings,
                              extra={"notebooks": [t["notebookPath"] for t in tasks],
                                     "scheduled": bool(sched)})
        result.status = status_for(findings)
        self.report.add(result)

    def _schedule(self, task: dict, tasks: list):
        findings, chosen = [], None
        if not task.get("_applicationKey"):
            return None, findings
        schedules = self.snap.task_schedules_for(task.get("key", ""))
        if not schedules:
            findings.append(Finding("SC00_NONE", "no task schedule in the snapshot; the job runs "
                                    "only when started", "info"))
            return None, findings
        for ts in schedules:
            sched = ts.get("scheduleRef") or {}
            if not sched.get("frequencyDetails"):
                sched = self.snap.find("schedules", sched) or sched
            res = convert_schedule(sched, default_timezone=self.config.default_timezone)
            findings += res.findings
            label = f"{ts.get('name')} ({res.described})"
            if chosen is not None:
                findings.append(Finding("SC07_MULTIPLE", f"task schedule {label} not attached: "
                                        f"an AIDP job has one schedule -- create another job for "
                                        f"it", "review"))
                continue
            if res.schedule is None:
                continue
            chosen = res.schedule
            state = "enabled" if ts.get("isEnabled", True) else "DISABLED in OCI-DI"
            findings.append(Finding("SC04_SCHEDULE", f"{label} -> `{chosen['quartzCronExpression']}`"
                                    f" {chosen['timezoneId']}, created PAUSED ({state}); unpause "
                                    f"after cutover", "review"))
            if ts.get("startTimeMillis") or ts.get("endTimeMillis"):
                findings.append(Finding("SC08_WINDOW", f"{ts.get('name')}: start/end window is not "
                                        f"carried over (AIDP schedules have no window)", "review"))
            retries = int(ts.get("retryAttempts") or 0)
            if retries:
                for t in tasks:
                    t.setdefault("maxRetries", retries)
            if ts.get("isConcurrentAllowed"):
                findings.append(Finding("SC09_CONCURRENT", "OCI-DI allowed concurrent runs; the job "
                                        "allows one (maxConcurrentRuns 1)", "info"))
            if self._bindings(ts):
                findings.append(Finding("SC10_SCHEDULE_PARAMS", f"{ts.get('name')} overrides "
                                        f"parameters {sorted(self._bindings(ts))}; carried as "
                                        f"task parameters", "info"))
                for t in tasks:
                    t["parameters"] = [{"name": k, "value": str(v)}
                                       for k, v in self._bindings(ts).items()]
        return chosen, findings

    # ----------------------------------------------------------------- run
    def run(self) -> Report:
        if self.out.exists() and any(self.out.iterdir()):
            for sub in ("notebooks", "jobs", "fallback", "ddl", "reconcile", "watermarks"):
                if (self.out / sub).exists():
                    responses = list((self.out / sub).glob("*.py")) if sub == "fallback" else []
                    keep = {p.name: p.read_text(encoding="utf-8") for p in responses}
                    shutil.rmtree(self.out / sub)
                    if keep:
                        (self.out / sub).mkdir(parents=True)
                        for n, text in keep.items():
                            (self.out / sub / n).write_text(text, encoding="utf-8")
        self.out.mkdir(parents=True, exist_ok=True)
        marker = self.out / IN_PROGRESS
        marker.write_text("migrate started; report.json is stale until this file is gone\n")
        tasks = self._tasks()
        pipeline_refs = set()
        for t in tasks:
            if t.get("modelType") == "PIPELINE_TASK":
                pl = t.get("pipeline") or {}
                for node in pl.get("nodes") or []:
                    ref = (node.get("operator") or {}).get("task") or {}
                    for k in ("identifier", "name"):
                        if ref.get(k):
                            pipeline_refs.add(ref[k].upper())
        for t in tasks:
            self.compile_task(t)
        for t in tasks:
            kind = t.get("modelType")
            name = t.get("name") or t.get("identifier")
            if kind == "PIPELINE_TASK":
                art = self.compile_task(t)
                pc = PipelineCompiler(art.pipeline or {}, self._resolver, name=name)
                res = pc.compile()
                if res.tasks:
                    self._job(name, t, res.tasks, res.findings, kind="PIPELINE_TASK")
                else:
                    self.report.add(ObjectResult("job", name, kind, t.get("key", ""),
                                                 status="manual", findings=res.findings + [
                                                     Finding("JB02_EMPTY", "no task of this "
                                                             "pipeline could be migrated",
                                                             "manual")]))
                continue
            art = self.compile_task(t)
            standalone = bool(t.get("_applicationKey")) or self.identity(t) not in pipeline_refs
            if art.error or not standalone:
                continue
            self._job(name, t, [{"taskKey": task_key(name), "type": "NOTEBOOK_TASK",
                                 "runIf": "ALL_SUCCESS", "notebookPath": art.notebook_path,
                                 "dependsOn": []}], [], kind=kind)
        self._write_shared()
        self.report.write(self.out)
        marker.unlink()
        return self.report

    def _resolver(self, ref: dict) -> TaskArtifact:
        task = None
        for kind in ("published_objects", "tasks"):
            for attr in ("identifier", "name", "key"):
                if ref.get(attr):
                    task = self.snap.find(kind, ref[attr])
                    if task:
                        break
            if task:
                break
        if task is None:
            if ref.get("modelType") in FLOW_TASKS + OTHER_TASKS and (ref.get("dataFlow") or
                                                                    ref.get("script")):
                task = ref
            else:
                return TaskArtifact(error=f"task {ref.get('name') or ref.get('key')} is not in "
                                          f"the snapshot (cross-workspace or deleted?)")
        return self.compile_task(task)

    def _write_shared(self) -> None:
        tables, ddl_findings = artifacts.merge_ddl(self.ddl)
        (self.out / "ddl").mkdir(parents=True, exist_ok=True)
        (self.out / "ddl/setup.sql").write_text(
            artifacts.setup_sql(tables, self.config.target.control_schema), encoding="utf-8")
        artifacts.setup_notebook(tables, self.config).write(self.out / "ddl/00_setup.ipynb")
        setup = ObjectResult("ddl", "00_setup", "SETUP", "", artifacts=[
            "ddl/00_setup.ipynb", "ddl/setup.sql"], findings=ddl_findings,
            extra={"remote_path": f"{self.config.aidp.notebook_root.rstrip('/')}/_setup/00_setup.ipynb",
                   "tables": [f"{t['schema']}.{t['table']}" for t in tables]})
        setup.status = status_for(ddl_findings)
        self.report.add(setup)
        (self.out / "watermarks").mkdir(parents=True, exist_ok=True)
        (self.out / "watermarks/seed.sql").write_text(
            artifacts.seed_sql(self.seeds, self.config.target.control_schema), encoding="utf-8")
        if self.report.targets:
            artifacts.reconcile_notebook(self.report.targets, self.config).write(
                self.out / "reconcile/reconcile.ipynb")
            self.report.add(ObjectResult(
                "reconcile", "reconcile", "RECONCILE", "", artifacts=["reconcile/reconcile.ipynb"],
                extra={"remote_path": f"{self.config.aidp.notebook_root.rstrip('/')}/_reconcile/"
                                      f"reconcile.ipynb"}))
        (self.out / "sources.md").write_text(artifacts.sources_markdown(self.report.sources),
                                             encoding="utf-8")
        (self.out / "fallback").mkdir(parents=True, exist_ok=True)
        for order in self.fallbacks:
            (self.out / f"fallback/{order['id']}.json").write_text(
                json.dumps(order, indent=2, default=str) + "\n", encoding="utf-8")


def migrate(snapshot_path, out_dir, config, *, strict=False, only=None) -> Report:
    snap = Snapshot.load(snapshot_path)
    return Migration(snap, Path(out_dir), config, strict=strict, only=only).run()
