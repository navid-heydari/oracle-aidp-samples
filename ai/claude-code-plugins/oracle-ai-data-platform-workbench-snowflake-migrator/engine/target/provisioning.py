"""Provision the migration environment inside AIDP. I/O injected as `call`.

The one-time setup the prod flow needs, in dependency order:

  1. the WORKSPACE (name translated by `naming.translate_name`, so a name the
     API might reject never reaches it);
  2. the migration CLUSTER (default `migration-assets`, small default config —
     sizing is a later, explicit decision);
  3. cluster LIBRARIES, when the fallback paths need any (PyPI/Maven; a
     restart follows, as the doc requires);
  4. the workspace folder `backup-snowflake-migration/` with the
     data-migration SCRIPTS and the migration PLAN artifacts;
  5. four parametrised JOBS (discover / structure / copy / reconcile) wired
     to those scripts — created paused-by-absence-of-schedule: running one is
     always a human's call.

The EXTERNAL catalog itself is NOT registered here — that is the existing
`snowmig.py catalog` stage, one writer per concern.

Discipline is the house discipline: look first, create only if absent, poll
the read-back with a bounded backoff, and record per step
`{step, action, verified, detail}`. Dry run by default; nothing reaches AIDP
without `execute=True`. Every underlying REST shape is the DOCUMENTED
20260430 contract (see provision_api.py) and remains ⚠️ unverified live.
"""
from __future__ import annotations

import json
import os
import pathlib
import tempfile
import time
from typing import Callable

from .naming import translate_name
from .stage_notebooks import STAGES, build_stage_notebook
from .provision_api import (
    build_cluster_body, build_job_body,
    build_library_items, build_provision_command, build_workspace_body,
)

__all__ = ["JOB_SPECS", "SCRIPTS_FOLDER", "PLAN_FOLDER", "REPORTS_FOLDER",
           "BACKUP_FOLDER", "ProvisionTransportError",
           "make_provision_call", "provision", "render_provision",
           "async_operation_key", "connection_test_outcome"]


class ProvisionTransportError(RuntimeError):
    """A provisioning call failed. Named so the CLI can report it as a
    message with the partial result intact, rather than a traceback that
    loses the record of what was already created."""

# Workspace-object paths are RELATIVE (a leading slash is a live 400).
_ROOT = "backup-snowflake-migration"
SCRIPTS_FOLDER = f"{_ROOT}/scripts"
PLAN_FOLDER = f"{_ROOT}/plan"
# What the SCRIPTS receive as --reports-dir: /Workspace is the live-verified
# mount of the workspace tree on cluster filesystems (probed on a real run).
REPORTS_FOLDER = f"/Workspace/{_ROOT}/reports"
# Runbook S6 backs the discovery manifest up here, dated, BEFORE any later
# stage reads it, and S9 backs the full plan up here before scope is reduced.
# Provisioning used to create scripts/, plan/ and reports/ only, so the first
# thing that tried to write a backup found no folder to write it into.
BACKUP_FOLDER = f"{_ROOT}/backup"

# One job per NOTEBOOK. The names mirror the stage flags (sans `--`) and are
# written into the notebook's own PARAMS cell, because job parameters reach a
# notebook neither as argv nor as environment (probed live).
#
# There is no longer a driver wrapper. Each stage is a single self-contained
# `.ipynb` -- parameters, helpers and logic in one object -- so the code a
# user opens in the console is exactly the code the job runs. AIDP types a
# workspace object by extension (`.py` -> FILE, `.ipynb` -> NOTEBOOK) and a
# job task needs a NOTEBOOK, so `.ipynb` is not a preference here.
#
# `source-mode` defaults to `connector`: reading Snowflake directly from the
# cluster is the path proven end to end (a table read and a pushdown), and it
# needs no successful catalog crawl. Pass `--source-mode external-catalog`
# plus `--external-catalog` when the crawl works.
JOB_SPECS: tuple[dict, ...] = (
    {"name": "snowmig_00_discover", "notebook": "00_discover_snowflake.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "reports-dir")},
    {"name": "snowmig_01_structure", "notebook": "01_create_structure.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "target-catalog", "reports-dir")},
    {"name": "snowmig_02_copy_schema", "notebook": "02_copy_schema.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "target-catalog", "schema", "reports-dir")},
    {"name": "snowmig_03_reconcile", "notebook": "03_reconcile.ipynb",
     "parameters": ("target-catalog", "reports-dir")},
)

# The shared helpers are INLINED into each generated notebook (see
# target/stage_notebooks.py), so nothing is uploaded beside them and no
# notebook depends on a module sitting on the /Workspace mount.


def make_provision_call(platform_ocid: str, *, backend: str = "oci_raw",
                        run_process=None) -> Callable[..., dict]:
    """A `call(operation, **kwargs) -> dict` over the documented API."""
    import subprocess

    from .runner import _printable
    from .executor import collect_pages, parse_cli_envelope

    def _run(cmd):
        return subprocess.run(cmd, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")

    runner = run_process or _run

    def _once(operation: str, kwargs: dict) -> tuple[list[dict], dict]:
        """One request: its rows and its response headers (lower-cased)."""
        spooled = None
        # File CONTENT never goes through this transport at all: uploads use
        # the `workspace-object` surface, which takes a local path. A body
        # carrying `content` would mean someone reintroduced the Jupyter
        # contents path, so refuse rather than spool it into argv.
        body = kwargs.get("body")
        if isinstance(body, dict) and "content" in body:
            raise ValueError(
                "this transport does not carry file content; upload through "
                "the workspace-object operations (upload_ws_file), which take "
                "a local path")
        try:
            cmd = build_provision_command(backend, operation, platform_ocid,
                                          **kwargs)
            print("  $ " + " ".join(_printable(c) for c in cmd))
            proc = runner(cmd)
            if proc.returncode != 0:
                raise ProvisionTransportError(
                    f"{operation} failed (exit {proc.returncode}): "
                    f"{(proc.stderr or proc.stdout or '')[:300]}")
            # The aidp CLI's literal "Response:" prefix is stripped by the
            # parser; the headers come back with the rows because a list
            # endpoint names its next page in one of them.
            rows, headers = parse_cli_envelope(proc.stdout or "")
        finally:
            if spooled:
                try:
                    os.unlink(spooled)
                except OSError:
                    pass
        if headers.get("opc-next-page") and cmd[0] == "aidp":
            # The workspace-object listing rides the aidp CLI, whose paging
            # flags are undocumented. Page one handed back as the whole would
            # read every object past it as absent; refuse and say why.
            raise ProvisionTransportError(
                f"{operation}: the aidp CLI answered with a next-page token, "
                f"so this listing is only its first page and the rest cannot "
                f"be requested through that CLI; the listing is incomplete "
                f"and was not used.")
        return rows, headers

    def call(operation: str, **kwargs) -> dict:
        if operation.startswith("list_"):
            # A collection may span pages: `opc-next-page` is followed until
            # the server stops sending one, so jobs.in_flight_runs and every
            # look-first check see the whole collection.
            def fetch(page):
                rows, headers = _once(operation, {**kwargs, "page": page}
                                      if page else kwargs)
                return rows, headers.get("opc-next-page")

            try:
                items = collect_pages(fetch, operation)
            except ProvisionTransportError:
                raise
            except RuntimeError as exc:
                raise ProvisionTransportError(str(exc)) from exc
            return {"items": items}
        rows, headers = _once(operation, kwargs)
        row = rows[0] if rows else {}
        # An async action answers 202 with an EMPTY body and its operation
        # key in a response header. The parser cannot put a header into a
        # row, so the transport keeps them beside it, under `_headers`, for
        # async_operation_key to read.
        if headers and isinstance(row, dict):
            row = dict(row, _headers=headers)
        return row

    return call


# Where the async operation key of a 202 may ride. `aidp-async-operation-key`
# is the header live-verified on the validated deployment; the documented
# testConnection contract names `oidl-async-operation-key` and
# `datalake-async-operation-key`; `opc-work-request-id` is the OCI-wide
# convention. All four are read, headers first, case-insensitively.
_ASYNC_KEY_HEADERS = ("aidp-async-operation-key",
                      "datalake-async-operation-key",
                      "oidl-async-operation-key", "opc-work-request-id")
_ASYNC_TERMINAL = ("SUCCEEDED", "SUCCESS", "FAILED", "CANCELED", "CANCELLED")


def async_operation_key(payload: dict) -> str | None:
    """The async operation key a 202 carried, wherever the envelope put it.

    `oci raw-request` prints `{"data": <body>, "headers": {...}, "status"}`.
    For an empty body the key rides in a HEADER, which make_provision_call
    keeps under `_headers`; when the parser returned the whole envelope (a
    null body) the headers sit under `headers`; a body may also carry it as
    `key`, at the top level or under `data`. Reading it at the top level of
    the parsed row only -- as the catalog stage once did -- found nothing in
    any of these shapes, so the poll never ran and every test reported
    PENDING.
    """
    if not isinstance(payload, dict):
        return None
    for headers in (payload.get("_headers"), payload.get("headers")):
        if isinstance(headers, dict):
            lowered = {str(k).lower(): v for k, v in headers.items()}
            for name in _ASYNC_KEY_HEADERS:
                if lowered.get(name):
                    return str(lowered[name])
    for name in _ASYNC_KEY_HEADERS:
        if payload.get(name):
            return str(payload[name])
    data = payload.get("data")
    if isinstance(data, dict) and data.get("key"):
        return str(data["key"])
    if payload.get("key"):
        return str(payload["key"])
    return None


def connection_test_outcome(call: Callable[..., dict], probe: dict, *,
                            delays: tuple[float, ...] = (5.0, 10.0, 15.0,
                                                         20.0, 30.0),
                            sleep: Callable[[float], None] | None = None
                            ) -> dict:
    """The verdict of a testConnection POST, read back through
    `GET /asyncOperations/{key}` with a bounded backoff.

    {requested, status, operation_key, error?, note?}. PENDING means one
    thing: the key was found and the operation had not ended when the poll
    budget ran out. A 202 whose envelope carries no key is reported as
    exactly that -- the verdict cannot be read -- and a poll that fails is
    UNREADABLE with its error. None of these is a pass.
    """
    sleep = sleep or time.sleep
    key = async_operation_key(probe)
    outcome: dict = {"requested": True, "status": "PENDING",
                     "operation_key": key}
    if not key:
        outcome["note"] = (
            "the API accepted the test request but its envelope carried no "
            "async operation key (in a header or the body), so the verdict "
            "cannot be read; PENDING is not a pass")
        return outcome
    last = "PENDING"
    for delay in delays:
        sleep(delay)
        try:
            op = call("get_async_operation", key=key)
        except Exception as exc:
            outcome["status"] = "UNREADABLE"
            outcome["error"] = (f"GET asyncOperations/{key}: "
                                f"{str(exc)[:200]}")
            return outcome
        last = str(op.get("status") or op.get("lifecycleState")
                   or "PENDING").upper()
        if last in _ASYNC_TERMINAL:
            outcome["status"] = last
            if op.get("errorCode") or op.get("errorMessage"):
                outcome["error"] = (f'{op.get("errorCode")}: '
                                    f'{op.get("errorMessage")}')
            return outcome
    outcome["note"] = (
        f"the operation still reported {last} after {len(delays)} polls "
        f"over {sum(delays):g}s, so the verdict was not readable within "
        f"the budget; PENDING is not a pass -- re-check operation {key}")
    return outcome


def _match(items: list[dict], display_name: str) -> dict | None:
    wanted = display_name.strip().lower()
    for item in items or []:
        name = str(item.get("displayName") or item.get("name")
                   or item.get("key") or "").lower()
        if name == wanted:
            return item
    return None


def _is_conflict(exc: Exception) -> bool:
    """A 409 "ongoing operation": the workspace is still settling after its
    own POST returned. Retried with the bounded backoff, as catalog_deploy
    does for a table posted into a settling schema."""
    text = str(exc)
    return "409" in text or "ongoing" in text.lower()


def _active(item: dict) -> bool:
    """ACTIVE -- or carrying no lifecycleState at all, since an absent field
    is not evidence of settling."""
    return str(item.get("lifecycleState") or "ACTIVE").upper() == "ACTIVE"


def _poll(list_fn, display_name: str, delays: tuple[float, ...], *,
          require_active: bool = False) -> dict | None:
    """The item once it is visible (and ACTIVE, when asked for); None when it
    never appeared. With `require_active`, an item that appeared but was
    still settling when the budget ran out is returned as last seen, so the
    caller can tell "never visible" from "visible, not yet ACTIVE"."""
    last = None
    for attempt in range(len(delays) + 1):
        try:
            found = _match(list_fn().get("items") or [], display_name)
        except Exception:
            found = None
        if found is not None:
            last = found
            if not require_active or _active(found):
                return found
        if attempt < len(delays):
            time.sleep(delays[attempt])
    return last


def _key(item: dict, fallback: str) -> str:
    return str(item.get("key") or item.get("id") or fallback)


def _pypi_from_requirements(path: pathlib.Path | None) -> list[str]:
    if path is None or not path.is_file():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def provision(*, call: Callable[..., dict] | None, workspace_name: str,
              cluster_name: str = "migration-assets",
              scripts: list[pathlib.Path],
              plan_files: list[pathlib.Path] = (),
              requirements: pathlib.Path | None = None,
              maven: list[str] = (),
              external_catalog: str | None = None,
              target_catalog: str | None = None,
              source_mode: str = "connector",
              source_config: pathlib.Path | None = None,
              warehouse_clusters: list[dict] = (),
              execute: bool = False,
              delays: tuple[float, ...] = (3.0, 5.0, 10.0, 15.0),
              subnet_id: str | None = None,
              reuse_existing: bool = False) -> dict:
    """Provision this migration's own environment inside AIDP.

    `reuse_existing=False` is the default and the rule: a migration creates
    its own workspace, cluster and jobs so that everything it touches can be
    identified, audited and torn down as a unit. A name already in use is a
    COLLISION -- reported, and the run stops so the user can choose another
    name. Adopting a stranger's workspace silently makes the blast radius of
    the migration unknowable.
    """
    ws_name = translate_name(workspace_name, kind="workspace")
    cl_name = translate_name(cluster_name, kind="cluster")
    pypi = _pypi_from_requirements(requirements)
    # One cluster per Snowflake warehouse, named after it. Sizing is NOT
    # carried over: the user asked for same-name clusters on the AIDP default
    # config, and the `compute` stage's proposal stays a proposal until
    # somebody decides on it.
    warehouse_targets = []
    for wh in warehouse_clusters or ():
        source_name = str(wh.get("name") or wh.get("warehouse") or "").strip()
        if not source_name:
            continue
        translated = translate_name(source_name, kind="cluster")
        warehouse_targets.append(
            {"warehouse": source_name, "name": translated.name,
             "renamed": translated.changed, "notes": translated.notes,
             "source_size": wh.get("size")})

    out: dict = {
        "dry_run": not execute,
        "workspace": {"requested": workspace_name, "name": ws_name.name,
                      "renamed": ws_name.changed, "notes": ws_name.notes},
        "cluster": {"requested": cluster_name, "name": cl_name.name,
                    "renamed": cl_name.changed, "notes": cl_name.notes},
        "warehouse_clusters": warehouse_targets,
        "scripts_folder": SCRIPTS_FOLDER, "plan_folder": PLAN_FOLDER,
        "reports_folder": REPORTS_FOLDER,
        "libraries": {"pypi": pypi, "maven": list(maven)},
        "external_catalog": external_catalog,
        "target_catalog": target_catalog,
        "source_mode": source_mode,
        "steps": [],
    }

    def step(name: str, action: str, verified: bool | None,
             detail: str = "") -> None:
        out["steps"].append({"step": name, "action": action,
                             "verified": verified, "detail": detail})

    if not execute:
        step("workspace", "would ensure", None, ws_name.name)
        step("cluster", "would ensure", None, cl_name.name)
        for target in warehouse_targets:
            step("warehouse-cluster", "would ensure", None,
                 f'{target["warehouse"]} -> {target["name"]} '
                 f'(AIDP default config; source size '
                 f'{target["source_size"] or "unknown"} NOT carried over)')
        if pypi or maven:
            step("libraries", "would install + restart", None,
                 ", ".join(pypi + list(maven)))
        # PREVIEW WHAT EXECUTE ACTUALLY UPLOADS: one generated `.ipynb` per
        # stage, never the `.py` under engine/dataplane/. Those are the
        # canonical SOURCES and they stay on the operator's machine -- AIDP
        # types a workspace object by extension, so a `.py` would land as a
        # FILE and no job could run it. Previewing the source names advertised
        # an upload that never happens, which is the one thing a dry run may
        # not do.
        for spec in JOB_SPECS:
            step("upload", "would upload", None,
                 f'{spec["notebook"]} (generated) -> '
                 f'{SCRIPTS_FOLDER}/{spec["notebook"]}')
        for path in plan_files:
            step("upload", "would upload", None,
                 f"{path.name} -> {PLAN_FOLDER}/{path.name}")
        for spec in JOB_SPECS:
            step("job", "would create", None, spec["name"])
        return out

    if call is None:
        raise ValueError("execute=True requires a transport callable")

    # 1 · workspace: look, create if absent, poll until visible AND ACTIVE --
    found = _match(call("list_workspaces").get("items") or [], ws_name.name)
    ws_created = False
    if found is None:
        call("create_workspace",
             body=build_workspace_body(ws_name.name,
                                       description="snowflake-migrator "
                                                   "migration workspace",
                                       subnet_id=subnet_id))
        ws_created = True
        # A workspace reports ACTIVE seconds after its POST returns, and a
        # cluster created inside that window is a 409. So wait for ACTIVE,
        # not just for the name to appear; a slow ACTIVE is recorded, not a
        # stop, because the cluster POST below retries on the 409 anyway.
        found = _poll(lambda: call("list_workspaces"), ws_name.name, delays,
                      require_active=True)
        settling = found is not None and not _active(found)
        step("workspace", "create_requested" if found is None else "created",
             found is not None,
             f'{ws_name.name}: visible, but lifecycleState='
             f'{found.get("lifecycleState")} after the poll budget; the '
             f'cluster POST is retried on 409 while it settles'
             if settling else ws_name.name)
    elif reuse_existing:
        step("workspace", "reused", True, _key(found, ws_name.name))
    else:
        step("workspace", "name_taken", False, ws_name.name)
        step("halt", "stopped", False,
             f"a workspace named {ws_name.name!r} already exists and this "
             f"migration does not reuse what it did not create. Choose "
             f"another --workspace-name, or pass --reuse-existing if you "
             f"really mean to migrate into someone else's workspace. If a "
             f"previous run of THIS migration created it (its PROVISION.md "
             f"lists the workspace step), --reuse-existing is the intended "
             f"resume, not a rule violation.")
        return out
    ws_key = _key(found or {}, ws_name.name)
    out["workspace"]["key"] = ws_key
    if found is None:
        step("halt", "stopped", False,
             "the workspace never became visible; nothing else was attempted")
        return out

    # 2 · cluster ------------------------------------------------------------
    # From here on the workspace EXISTS, so nothing below may raise out of
    # this function: an exception would lose the record of it, and the next
    # run would halt on name_taken and call the operator's own workspace
    # "someone else's". A failure is a recorded step plus a halt that says
    # how to resume.
    resume = (
        f"workspace {ws_name.name!r} (key {ws_key}) WAS created by this run "
        f"and is recorded above. Re-run the same command with "
        f"--reuse-existing to continue into it; do not pick a new "
        f"--workspace-name, or this one is orphaned."
        if ws_created else
        f"workspace {ws_name.name!r} (key {ws_key}) is the one being reused; "
        f"fix the cause above and re-run the same command.")
    try:
        found = _match(
            call("list_clusters", workspace=ws_key).get("items") or [],
            cl_name.name)
    except Exception as exc:
        step("cluster", "failed", False, f"list_clusters: {str(exc)[:200]}")
        step("halt", "stopped", False, resume)
        return out
    if found is None:
        # A cluster POSTed before the workspace reports ACTIVE is a 409
        # "ongoing operation". Retried with the bounded backoff, each retry
        # on the record; anything else fails the step and halts with the
        # record intact.
        for attempt in range(len(delays) + 1):
            try:
                call("create_cluster", workspace=ws_key,
                     body=build_cluster_body(cl_name.name))
                break
            except Exception as exc:
                if _is_conflict(exc) and attempt < len(delays):
                    step("cluster", "retried", None,
                         f"attempt {attempt + 1}: 409/ongoing operation on "
                         f"workspace {ws_key}; waiting {delays[attempt]:g}s")
                    time.sleep(delays[attempt])
                    continue
                step("cluster", "failed", False,
                     f"create_cluster: {str(exc)[:200]}")
                step("halt", "stopped", False, resume)
                return out
        found = _poll(lambda: call("list_clusters", workspace=ws_key),
                      cl_name.name, delays)
        step("cluster", "create_requested" if found is None else "created",
             found is not None, cl_name.name)
    elif reuse_existing:
        step("cluster", "reused", True, _key(found, cl_name.name))
    else:
        step("cluster", "name_taken", False, cl_name.name)
        step("halt", "stopped", False,
             f"a cluster named {cl_name.name!r} already exists in this "
             f"workspace and this migration does not reuse what it did not "
             f"create. Choose another --cluster-name, or pass "
             f"--reuse-existing.")
        return out
    if found is None:
        # Falling through would bake the DISPLAY NAME into four job bodies as
        # a clusterKey, and they would be "created" and unrunnable.
        step("halt", "stopped", False,
             "the cluster never became visible, so its key is unknown; jobs "
             "would be created bound to an invalid cluster. Nothing else was "
             "attempted")
        return out
    cluster_key = _key(found, cl_name.name)
    out["cluster"]["key"] = cluster_key

    # 2b · one cluster per Snowflake warehouse, same name, default config ----
    # These are the customer's own compute, not the migration's: a failure on
    # one is recorded and the rest continue, and NOTHING here changes the
    # migration cluster the jobs are bound to.
    for target in warehouse_targets:
        existing = _match(
            call("list_clusters", workspace=ws_key).get("items") or [],
            target["name"])
        if existing is not None:
            target["key"] = _key(existing, target["name"])
            step("warehouse-cluster",
                 "reused" if reuse_existing else "name_taken",
                 bool(reuse_existing),
                 f'{target["warehouse"]} -> {target["name"]}'
                 + ("" if reuse_existing else
                    ": a cluster of that name already exists and was left "
                    "untouched; it is not this migration's"))
            continue
        try:
            call("create_cluster", workspace=ws_key,
                 body=build_cluster_body(target["name"]))
        except Exception as exc:
            step("warehouse-cluster", "failed", False,
                 f'{target["warehouse"]} -> {target["name"]}: '
                 f'{str(exc)[:200]}')
            continue
        seen = _poll(lambda: call("list_clusters", workspace=ws_key),
                     target["name"], delays)
        target["key"] = _key(seen or {}, target["name"]) if seen else None
        step("warehouse-cluster",
             "created" if seen else "create_requested", seen is not None,
             f'{target["warehouse"]} -> {target["name"]}'
             + ("" if seen else " — accepted, but it never became visible"))

    # 3 · libraries (only when a fallback needs them) ------------------------
    if pypi or maven:
        # The library item shape is the one field family still inferred
        # (assumption B12), so a failure here is expected-possible and must
        # not cost the record of the workspace and cluster above.
        try:
            call("install_libraries", workspace=ws_key, cluster=cluster_key,
                 body=build_library_items(pypi=pypi, maven=list(maven)))
            call("restart_cluster", workspace=ws_key, cluster=cluster_key)
        except Exception as exc:
            step("libraries", "failed", False,
                 f"{len(pypi) + len(maven)} requested; the install or restart "
                 f"was rejected ({str(exc)[:160]}). The cluster is unchanged "
                 f"as far as this run knows")
        else:
            try:
                listed = call("list_libraries", workspace=ws_key,
                              cluster=cluster_key).get("items") or []
                step("libraries", "install_requested + restart", None,
                     f"{len(pypi) + len(maven)} requested; the server lists "
                     f"{len(listed)} — confirm after the restart settles")
            except Exception as exc:
                step("libraries", "install_requested + restart", None,
                     f"library list unreadable ({str(exc)[:120]}); "
                     f"NOT confirmed, not assumed")

    # 4 · the folder tree, then scripts and plan artifacts -------------------
    # Created through the workspace-object surface (live-verified); the
    # Jupyter contents API returned 200-then-unreadable on a real build.
    for folder in (_ROOT, SCRIPTS_FOLDER, PLAN_FOLDER, f"{_ROOT}/reports",
                   BACKUP_FOLDER):
        try:
            call("create_ws_folder", workspace=ws_key, path=folder)
        except Exception as exc:
            # A pre-existing folder may reject the create; existence is
            # decided by the per-file listing below, so record and continue.
            step("folder", "create_failed_or_exists", None,
                 f"{folder}: {str(exc)[:120]}")

    for folder, files in ((PLAN_FOLDER, plan_files),):
        for path in files:
            remote = f"{folder}/{path.name}"
            try:
                call("upload_ws_file", workspace=ws_key, path=remote,
                     local_path=str(path))
                items = call("list_ws_objects", workspace=ws_key,
                             path=folder).get("items") or []
                found = any(
                    str(i.get("path") or "").endswith("/" + path.name)
                    or i.get("displayName") == path.name for i in items)
                step("upload", "uploaded" if found else "upload_requested",
                     found,
                     remote if found else f"{remote}: not visible in listing")
            except Exception as exc:
                step("upload", "failed", False,
                     f"{remote}: {str(exc)[:200]}")

    # 5 · stage notebooks + jobs ---------------------------------------------
    # Job `parameters` reach the notebook neither as argv nor as environment
    # (probed live), so this run's coordinates are written into each stage
    # notebook's own PARAMS cell -- visible and editable in the console,
    # regenerated here when the defaults change.
    defaults = {"reports-dir": REPORTS_FOLDER, "source-mode": source_mode}
    if external_catalog:
        defaults["source-catalog"] = external_catalog
    if target_catalog:
        defaults["target-catalog"] = target_catalog
    if source_config is not None:
        # The scripts read the credential from this file ON THE MOUNT, so the
        # path they receive is the /Workspace one, not the local one.
        defaults["source-config"] = \
            f"{REPORTS_FOLDER.rsplit('/', 1)[0]}/plan/{source_config.name}"
    existing = call("list_jobs", workspace=ws_key).get("items") or []
    stages_by_notebook = {st.notebook_name: st for st in STAGES}
    for spec in JOB_SPECS:
        stage = stages_by_notebook.get(spec["notebook"])
        if stage is None:
            step("notebook", "failed", False,
                 f'{spec["notebook"]}: no stage definition. The job was NOT '
                 f'created rather than pointed at a notebook that does not '
                 f'exist.')
            continue
        notebook_path = f'{SCRIPTS_FOLDER}/{spec["notebook"]}'
        try:
            # Built here, with this run's coordinates already in PARAMS, so
            # the notebook on the workspace is ready to run unedited.
            nb = build_stage_notebook(stage, overrides=defaults)
            fd, local = tempfile.mkstemp(prefix="snowmig_stage_",
                                         suffix=".ipynb")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(nb, fh, indent=1)
            try:
                call("upload_ws_file", workspace=ws_key, path=notebook_path,
                     local_path=local, object_type="NOTEBOOK")
            finally:
                os.unlink(local)
            # Read it back, like every other upload: a 2xx is not the claim.
            listed = call("list_ws_objects", workspace=ws_key,
                          path=SCRIPTS_FOLDER).get("items") or []
            name = notebook_path.rsplit("/", 1)[-1]
            seen = any(str(i.get("path") or "").endswith("/" + name)
                       or i.get("displayName") == name for i in listed)
            step("notebook", "uploaded" if seen else "upload_requested", seen,
                 notebook_path if seen
                 else f"{notebook_path}: not visible in the listing")
            if not seen:
                continue
        except Exception as exc:
            step("notebook", "failed", False,
                 f"{notebook_path}: {str(exc)[:200]}")
            continue

        if _match(existing, spec["name"]) is not None:
            if reuse_existing:
                step("job", "reused", True,
                     f'{spec["name"]} (stage notebook refreshed)')
            else:
                step("job", "name_taken", False,
                     f'{spec["name"]} already exists and was NOT adopted; '
                     f'its stage notebook was refreshed but the job itself '
                     f'is not this migration\'s. Rename or --reuse-existing.')
            continue
        body = build_job_body(spec["name"], notebook_path=notebook_path,
                              cluster_key=cluster_key)
        try:
            call("create_job", workspace=ws_key, body=body)
            found = _poll(lambda: call("list_jobs", workspace=ws_key),
                          spec["name"], delays)
            step("job", "created" if found else "create_requested",
                 found is not None, spec["name"])
        except Exception as exc:
            step("job", "failed", False, f'{spec["name"]}: {str(exc)[:200]}')

    return out


def render_provision(res: dict) -> str:
    lines = ["# Provisioning — the migration environment inside AIDP", ""]
    if res["dry_run"]:
        lines += ["**DRY RUN — nothing was created.** Re-run with `--execute` "
                  "after reviewing the plan below.", ""]
    lines += [
        "⚠️ Every REST shape used here follows the documented 20260430 "
        "contract and is **not yet live-verified** by this plugin; two field "
        "families (library items, per-task job fields) are inferred and "
        "called out in `provision_api.py`.",
        "",
        f'Workspace: `{res["workspace"]["name"]}`'
        + (f' (translated from `{res["workspace"]["requested"]}` — '
           + "; ".join(res["workspace"]["notes"]) + ")"
           if res["workspace"]["renamed"] else ""),
        f'Cluster: `{res["cluster"]["name"]}`',
        f'Scripts: `{res["scripts_folder"]}` · Plan: `{res["plan_folder"]}` · '
        f'Reports: `{res["reports_folder"]}`',
        ""]

    if res.get("warehouse_clusters"):
        lines += ["## Snowflake warehouses → AIDP compute clusters", "",
                  "Same name, **AIDP default config**. The source size is "
                  "reported and deliberately NOT carried over: a Snowflake "
                  "warehouse size is not a Spark shape, and "
                  "`COMPUTE_PROPOSAL.md` keeps that a decision rather than a "
                  "default.", "",
                  "| Warehouse | Source size | AIDP cluster | Renamed |",
                  "|---|---|---|---|"]
        for target in res["warehouse_clusters"]:
            note = ("; ".join(target.get("notes") or [])
                    if target.get("renamed") else "no")
            lines.append(f'| `{target["warehouse"]}` | '
                         f'{target.get("source_size") or "unknown"} | '
                         f'`{target["name"]}` | {note} |')
        lines.append("")

    lines += [
        "| Step | Action | Verified | Detail |", "|---|---|---|---|"]
    for s in res["steps"]:
        verified = {True: "yes", False: "**no**", None: "—"}[s["verified"]]
        lines.append(f'| {s["step"]} | {s["action"]} | {verified} | '
                     f'{s["detail"]} |')
    lines += [
        "",
        "Pending is pending: `create_requested` means the API accepted the "
        "request and the object never became visible within the poll budget "
        "— check the console before proceeding.",
        "",
        "Next: run the `snowmig_00_discover` job (or the script by hand), "
        "then structure, then copy schema-by-schema, then reconcile. Every "
        "run is a human's call; no schedule was created.",
        ""]
    return "\n".join(lines)
