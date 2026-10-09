"""``publish``: upload a finished migration into an AIDP workspace.

Rules (each one learned the hard way by a sibling migrator):

* **Dry run by default.** Without ``apply`` nothing is sent.
* **Never overwrite.** A notebook path or job name that already exists is
  skipped. ``notebook update-content`` overwrites silently, so the check is
  made here, against a listing of the target folder.
* **Notebooks first, jobs second**, and a job is refused if any notebook it
  runs was not uploaded by this migration (in this publish, or an earlier
  one recorded in ``publish.json``) -- it would otherwise run a notebook
  nobody vouched for, or one that is not there.
* **A job needs a cluster.** AIDP rejects a task without one (live,
  2026-10-09), so without ``--cluster-key`` notebooks upload and jobs wait.
* **A prefix per person**, optional: it becomes a folder under the notebook
  root and the front of every job name, so two people can publish the same
  migration into one workspace.
* Jobs are created exactly as ``migrate`` wrote them: schedules PAUSED.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from ..migrate import IN_PROGRESS
from ..naming import JOB_NAME
from ..report import Report
from .client import AidpClient, AidpError


NO_CLUSTER = ("no --cluster-key: AIDP rejects a job task without a cluster (400 "
              "tasks[i].cluster must not be null); re-run publish with --cluster-key <USER "
              "cluster in this workspace> -- uploaded notebooks are skipped, not re-sent")


class PublishError(Exception):
    pass


@dataclass
class PublishPlan:
    notebooks: list = field(default_factory=list)   # {"local", "remote", "status", "name"}
    jobs: list = field(default_factory=list)        # {"file", "name", "body", "notebooks"}
    warnings: list = field(default_factory=list)


@dataclass
class PublishLog:
    uploaded: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    created_jobs: list = field(default_factory=list)
    refused_jobs: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    previously_uploaded: list = field(default_factory=list)

    def to_json(self) -> dict:
        return asdict(self)


def _with_prefix(remote: str, root: str, prefix: str) -> str:
    if not prefix:
        return remote
    root = root.rstrip("/")
    if remote.startswith(root + "/"):
        return f"{root}/{prefix}/{remote[len(root) + 1:]}"
    return remote


def plan_publish(out_dir, config, *, prefix: str = "") -> PublishPlan:
    out_dir = Path(out_dir)
    if not (out_dir / "report.json").is_file():
        raise PublishError(f"{out_dir} has no report.json; run migrate first")
    if (out_dir / IN_PROGRESS).exists():
        raise PublishError(f"{out_dir} holds an interrupted migration; re-run migrate")
    if prefix and not JOB_NAME.fullmatch(prefix):
        raise PublishError(f"prefix {prefix!r} must be a letter then letters, digits or "
                           f"underscores (it starts every job name)")
    report = Report.load(out_dir)
    root = config.aidp.notebook_root
    plan = PublishPlan()
    remap = {}
    order = {"ddl": 0, "notebook": 1, "reconcile": 2}
    for r in sorted(report.results, key=lambda r: order.get(r.kind, 9)):
        if r.kind not in order:
            continue
        local = next((out_dir / a for a in r.artifacts if a.endswith(".ipynb")), None)
        remote = r.extra.get("remote_path")
        if local is None or not remote or not local.is_file():
            if r.status != "failed":
                plan.warnings.append(f"{r.kind} {r.name}: no notebook file to upload")
            continue
        new_remote = _with_prefix(remote, root, prefix)
        remap[remote] = new_remote
        plan.notebooks.append({"local": str(local), "remote": new_remote, "status": r.status,
                               "name": r.name})
        if r.status in ("manual", "fallback_pending"):
            plan.warnings.append(f"notebook {r.name} is {r.status}: it raises at its first "
                                 f"unconverted step")
    for r in report.results:
        if r.kind != "job":
            continue
        path = out_dir / r.artifacts[0] if r.artifacts else None
        if path is None or not path.is_file():
            continue
        body = json.loads(path.read_text(encoding="utf-8"))
        if prefix:
            body["name"] = f"{prefix}_{body['name']}"
        for t in body.get("tasks", []):
            t["notebookPath"] = remap.get(t["notebookPath"], t["notebookPath"])
        plan.jobs.append({"file": str(path), "name": body["name"], "body": body,
                          "notebooks": [t["notebookPath"] for t in body.get("tasks", [])],
                          "status": r.status})
    return plan


def publish(out_dir, config, *, client: AidpClient = None, workspace_key: str = "",
            cluster_key: str = "", prefix: str = "", apply: bool = False) -> tuple:
    plan = plan_publish(out_dir, config, prefix=prefix)
    log = PublishLog()
    if not apply:
        return plan, log
    if client is None or not workspace_key:
        raise PublishError("publish --apply needs an AIDP client and a workspace key "
                           "(run provision first)")
    if cluster_key:
        keys = {c.get("key") for c in client.clusters(workspace_key)}
        if cluster_key not in keys:
            raise PublishError(f"cluster {cluster_key} is not one of this workspace's clusters "
                               f"({', '.join(sorted(k for k in keys if k)) or 'none'})")
    listed: dict = {}

    def exists(remote: str) -> bool:
        parent, _, name = remote.rpartition("/")
        if parent not in listed:
            listed[parent] = client.names_in(workspace_key, parent)
        return name in listed[parent]

    made: set = set()

    def ensure_folders(remote: str) -> None:
        rel = client.object_path(remote.rpartition("/")[0])
        parts = [p for p in rel.split("/") if p]
        for i in range(1, len(parts) + 1):
            folder = "/Workspace/" + "/".join(parts[:i])
            if folder in made:
                continue
            client.mkdir(workspace_key, folder)
            made.add(folder)

    # Notebooks an earlier publish of this same migration uploaded are ours: a
    # job may run them even though this run skipped them as already present.
    previous = Path(out_dir) / "publish.json"
    ours = set()
    if previous.is_file():
        try:
            prev = json.loads(previous.read_text(encoding="utf-8"))
            ours = set(prev.get("uploaded", [])) | set(prev.get("previously_uploaded", []))
        except ValueError:
            ours = set()
    log.previously_uploaded = sorted(ours)
    uploaded = set()
    for nb in plan.notebooks:
        remote = nb["remote"]
        try:
            if exists(remote):
                log.skipped.append({"notebook": remote, "why": "already exists (not overwritten)"})
                continue
            ensure_folders(remote)
            client.put_notebook(workspace_key, remote,
                                json.loads(Path(nb["local"]).read_text(encoding="utf-8")))
            uploaded.add(remote)
            log.uploaded.append(remote)
        except AidpError as exc:
            log.errors.append({"notebook": remote, "error": str(exc)})
    if not cluster_key:
        # AIDP refuses a job task without a cluster (live, 2026-10-09:
        # 400 InvalidParameter "tasks[0].cluster must not be null"), so a job is not
        # even attempted without one. Notebooks are uploaded either way.
        for job in plan.jobs:
            log.refused_jobs.append({"job": job["name"], "why": NO_CLUSTER})
        _write_log(out_dir, log)
        return plan, log
    existing_jobs = {j.get("name") for j in client.jobs(workspace_key)}
    for job in plan.jobs:
        name = job["name"]
        if name in existing_jobs:
            log.skipped.append({"job": name, "why": "a job with this name exists"})
            continue
        missing = [p for p in job["notebooks"] if p not in uploaded and p not in ours]
        if missing:
            log.refused_jobs.append({"job": name, "why": "notebooks not uploaded by this "
                                                         "migration: " + ", ".join(missing)})
            continue
        body = json.loads(json.dumps(job["body"]))
        if cluster_key:
            for t in body.get("tasks", []):
                t["cluster"] = {"clusterKey": cluster_key}
        try:
            key = client.create_job(workspace_key, body)
            log.created_jobs.append({"job": name, "key": key})
        except AidpError as exc:
            log.errors.append({"job": name, "error": str(exc)})
    _write_log(out_dir, log)
    return plan, log


def _write_log(out_dir, log: PublishLog) -> None:
    (Path(out_dir) / "publish.json").write_text(json.dumps(log.to_json(), indent=2) + "\n",
                                                encoding="utf-8")
