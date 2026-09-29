"""The stage board: what is supposed to run, what has run, what it found.

Read the run before executing it. This module makes no decisions and touches
no environment -- it reads the artifacts already in `--out-dir` and reports
the shape of the pipeline against them.

Two rules it holds to, both learned the hard way elsewhere in this plugin:

  * A stage that could not look is FLAGGED, never shown as clean. "0
    exposures" and "we could not read the policy references" are opposite
    findings and must not render the same.
  * Four stages write to AIDP, and the board says which: `provision`,
    `catalog` and `deploy`, each a dry run without `--execute`, and `run`,
    which has no dry run -- it starts an in-AIDP job that creates tables or
    copies rows. The one further write is `smoke --write-probe --execute`:
    one probe schema, created and removed; `--write-probe` alone is a dry
    run. `notebook --upload` sends nothing -- a dry run without `--execute`,
    refused with it (GAPS.md 13).
"""
from __future__ import annotations

import datetime
import json
import pathlib
import re

from plan.smoke import smoke_verdict

__all__ = ["RUNS_ON", "STAGES", "UTILITY_COMMANDS", "build_stage_board", "phase_report",
           "stage_for"]

# THE ordered pipeline. Every other view of a run -- the board, the phase
# report, the diagram, the token roll-up, "what can run now" -- reads this,
# so none of them can disagree about what the phases are.
#
#   command  the CLI subcommand that runs it (`run` for an AIDP workflow)
#   phase    setup / discovery / planning / target / reporting
#   runbook  the runbook step(s) it implements, or "-" for a support stage
#   requires  groups of prerequisites; every group must have ONE of its
#            stages complete before this one is unblocked
#   alternative_to  another stage that produces the same result; having
#            done either satisfies both, so the board never stalls on the
#            path that was not taken
#   job / job_prefix  the AIDP job a `run` stage starts, or the prefix of a
#            job per schema; its artifact is then a glob, one file per job
# Where a phase's work actually executes. Traced from each command's
# transport: the catalog API is a control-plane call and uses no cluster;
# `deploy --transport sql` runs on the configured aidp.cluster_id; the
# workflows run on the migration cluster S2 provisions.
RUNS_ON = {
    "local": "operator machine (offline)",
    "local_snowflake": "operator machine -> Snowflake (read-only)",
    "local_both": "operator machine -> Snowflake + AIDP control plane",
    "control_plane": "AIDP control-plane API (no cluster)",
    "control_plane_or_configured": (
        "AIDP control-plane API; --transport sql uses the configured "
        "aidp.cluster_id"),
    "migration_cluster": "migration cluster (provisioned at S2)",
}

STAGES: tuple[dict, ...] = (
    # Optional only in the sense that a run can skip it; skipping it is how
    # a wrong host or role costs hours later.
    {"stage": "preflight", "requires": [], "runs_on": RUNS_ON["local_both"], "command": "preflight", "phase": "setup",
     "runbook": "-", "needs": "a connection config", "writes": False,
     "optional": True, "artifact": "preflight.json",
     "purpose": "read the connection config back to the user, field by "
                "field, and test both ends before anything else runs"},
    {"stage": "assess", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "assess", "phase": "discovery",
     "runbook": "S7 (views)", "alternative_to": "ingest", "needs": "Snowflake", "writes": False,
     "artifact": "inventory.json",
     "purpose": "inventory tables and views, plus a census of everything that "
                "is not one"},
    # Optional: the in-AIDP path (S6 -> S7) instead of a laptop assess.
    {"stage": "ingest", "requires": [['discover-workflow']], "runs_on": RUNS_ON["local"], "command": "ingest", "phase": "discovery",
     "runbook": "S7", "alternative_to": "assess", "needs": "the S6 discovery manifest", "writes": False,
     "optional": True, "artifact": "ingest_result.json",
     "purpose": "bridge the in-AIDP discovery manifest into inventory.json "
                "with the same type mapper assess uses"},
    {"stage": "deps", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "deps", "phase": "discovery", "runbook": "-",
     "needs": "Snowflake", "writes": False,
     "artifact": "dependencies.json",
     "purpose": "lineage, so views land after their base tables"},
    {"stage": "maintenance", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "maintenance", "phase": "discovery",
     "runbook": "-", "needs": "Snowflake", "writes": False,
     "artifact": "maintenance.json",
     "purpose": "clustering, retention and churn — who inherits OPTIMIZE/VACUUM"},
    {"stage": "security", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "security", "phase": "discovery",
     "runbook": "-", "needs": "Snowflake", "writes": False,
     "artifact": "security.json",
     "purpose": "masking/row-access/aggregation/projection policies, tag "
                "attachments, secure views, grants — what arrives "
                "unprotected"},
    {"stage": "compute", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "compute", "phase": "discovery",
     "runbook": "S12", "needs": "Snowflake", "writes": False,
     "artifact": "compute.json",
     "purpose": "warehouse-to-cluster sizing proposal"},
    # Optional: it feeds `plan` when run, but `plan` runs without it, so the
    # board must not stall on it as "next".
    {"stage": "data-options", "requires": [], "runs_on": RUNS_ON["local"], "command": "data-options", "phase": "planning",
     "runbook": "S11", "needs": "nothing (offline)", "writes": False,
     "optional": True,
     "artifact": "data_options.json",
     "purpose": "the data-movement architecture options — presented, never chosen"},
    {"stage": "plan", "requires": [['assess', 'ingest'], ['deps']], "runs_on": RUNS_ON["local"], "command": "plan", "phase": "planning",
     "runbook": "S7-S9", "needs": "nothing (offline)", "writes": False,
     "artifact": "plan.json",
     "purpose": "what can migrate, in what order, to which target name"},
    {"stage": "ddl", "requires": [['plan']], "runs_on": RUNS_ON["local"], "command": "ddl", "phase": "planning", "runbook": "S7",
     "needs": "nothing (offline)", "writes": False,
     "artifact": "ddl_plan.json",
     "purpose": "the statements/bodies that would create the structure"},
    {"stage": "smoke", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "smoke", "phase": "target", "runbook": "-",
     "needs": "Snowflake + AIDP", "writes": False,
     "artifact": "smoke.json",
     "purpose": "connectivity and permissions on both ends"},
    # Optional: required for the in-AIDP data path, not for a structure-only
    # clone, so the board must not stall on it as "next".
    {"stage": "provision", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "provision", "phase": "setup",
     "runbook": "S1 S2 S5", "needs": "AIDP", "writes": True, "optional": True,
     "artifact": "provision_result.json",
     "purpose": "workspace, migration-assets cluster, the backup-snowflake-"
                "migration/ folder with scripts + plan, and the four "
                "migration jobs. Dry-run unless --execute"},
    {"stage": "catalog", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "catalog", "phase": "setup",
     "runbook": "S3 S4", "needs": "AIDP", "writes": True,
     "artifact": "catalog_result.json",
     "purpose": "register the target catalog — EXTERNAL/SNOWFLAKE by default, "
                "a read-only pointer that copies nothing. Dry-run unless "
                "--execute"},
    {"stage": "discover-workflow", "requires": [['provision']], "runs_on": RUNS_ON["migration_cluster"], "command": "run", "job": "snowmig_00_discover",
     "phase": "discovery", "runbook": "S6", "needs": "AIDP (provisioned)",
     "writes": False, "optional": True,
     "artifact": "run_snowmig_00_discover.json",
     "purpose": "discovery as an AIDP workflow: two INFORMATION_SCHEMA "
                "queries, a manifest written in the workspace"},
    {"stage": "structure-workflow", "requires": [['ddl'], ['provision']], "runs_on": RUNS_ON["migration_cluster"], "command": "run",
     "job": "snowmig_01_structure", "phase": "target", "runbook": "S10",
     "needs": "AIDP (provisioned) + an approved ddl_plan.json",
     "writes": True, "optional": True, "alternative_to": "deploy",
     "artifact": "run_snowmig_01_structure.json",
     "purpose": "create schemas, empty tables and views by workflow, from "
                "the approved plan on the workspace"},
    {"stage": "deploy", "requires": [['ddl'], ['catalog']], "runs_on": RUNS_ON["control_plane_or_configured"], "command": "deploy", "phase": "target",
     "runbook": "S10 (catalog API)", "needs": "AIDP", "writes": True,
     "alternative_to": "structure-workflow",
     "artifact": "deploy_result.json",
     "purpose": "create schemas, tables and views in a STANDARD catalog. "
                "Refuses an EXTERNAL target. Dry-run unless --execute"},
    # Registered by provision and NEVER run by the migrator: moving rows is
    # the customer's later decision. Once a plan is pushed there is one job
    # per schema, snowmig_02_copy_<schema> (target.provisioning's
    # COPY_JOB_PREFIX), and no generic snowmig_02_copy_schema job; each run
    # writes run_<job>.json, so the stage is every artifact the prefix names.
    {"stage": "copy-workflow", "requires": [['structure-workflow', 'deploy'], ['data-options']], "runs_on": RUNS_ON["migration_cluster"], "command": "run", "job": "snowmig_02_copy_schema",
     "job_prefix": "snowmig_02_copy_",
     "phase": "target", "runbook": "S11", "needs": "an architecture decision",
     "writes": True, "optional": True,
     "artifact": "run_snowmig_02_copy_*.json",
     "purpose": "copy one schema's rows, one job per schema "
                "(snowmig_02_copy_<schema>). Registered, never run by the "
                "migrator"},
    {"stage": "reconcile-workflow", "requires": [['copy-workflow']], "runs_on": RUNS_ON["migration_cluster"], "command": "run",
     "job": "snowmig_03_reconcile", "phase": "target", "runbook": "S11",
     "needs": "a copy that ran", "writes": False, "optional": True,
     "artifact": "run_snowmig_03_reconcile.json",
     "purpose": "compare source and target counts after a copy"},
    {"stage": "notebook", "requires": [['ddl']], "runs_on": RUNS_ON["local"], "command": "notebook", "phase": "target",
     "runbook": "-", "needs": "nothing (offline)", "writes": False,
     "artifact": "NOTEBOOK.md",
     "purpose": "the clone as an executable AIDP notebook"},
    {"stage": "summary", "requires": [['plan']], "runs_on": RUNS_ON["local"], "command": "summary", "phase": "reporting",
     "runbook": "S9 S12", "needs": "nothing (offline)", "writes": False,
     "artifact": "SUMMARY.md",
     "purpose": "per-object roll-up: rows, risk, migration status, the "
                "translation map and the token cost"},
    {"stage": "publish", "requires": [['summary']], "runs_on": RUNS_ON["control_plane"],
     "command": "publish", "phase": "reporting", "runbook": "-",
     "needs": "AIDP (the migration workspace)", "writes": True,
     "optional": True, "artifact": "publish_result.json",
     "purpose": "copy the finished report, inputs and outputs, into the "
                "workspace's reports/final-<UTC> folder, each file read back. "
                "Dry-run unless --execute"},
    {"stage": "tokens", "requires": [], "runs_on": RUNS_ON["local"], "command": "tokens", "phase": "reporting",
     "runbook": "-", "needs": "nothing (local files)", "writes": False,
     "optional": True, "artifact": "tokens.json",
     "purpose": "LLM token usage per stage and phase"},
    # Last: the compute is released once the structure exists and the run's
    # record is written. Stop (reversible) unless delete is asked for.
    {"stage": "teardown", "requires": [['structure-workflow', 'deploy']],
     "runs_on": RUNS_ON["control_plane"], "command": "teardown",
     "phase": "teardown", "runbook": "-", "needs": "AIDP (provisioned)",
     "writes": True, "optional": True, "artifact": "teardown_result.json",
     "purpose": "terminate the clusters this migration allocated (stop by "
                "default, delete when asked); the workspace, catalogs and "
                "jobs are kept. Dry-run unless --execute"},
)

# CLI commands that are tools, not steps of a migration. Every other command
# must be a phase above -- a test holds that.
UTILITY_COMMANDS = ("stages", "demo", "databases", "catalogs", "clean",
                    "build-notebooks", "init-config", "fetch")

_UNKNOWN = "could not be determined"


def _artifact_paths(out_dir: pathlib.Path, name: str) -> list[pathlib.Path]:
    """The files a stage's artifact names: one, or every match of a glob."""
    if "*" in name:
        return sorted(p for p in out_dir.glob(name) if p.is_file())
    path = out_dir / name
    return [path] if path.exists() else []


def _load(out_dir: pathlib.Path, name: str):
    if "*" in name:
        # One artifact per invocation target (run_<job>.json): the stage has
        # run if any exists, and each is reported. A record that does not
        # name its job is named by its file, never left as "?".
        paths = _artifact_paths(out_dir, name)
        if not paths:
            return None
        many = []
        for p in paths:
            run = _load(out_dir, p.name)
            if isinstance(run, dict) and not run.get("job"):
                run = {**run, "job": run.get("job_key")
                       or p.stem.removeprefix("run_")}
            many.append(run)
        return {"_many": many}
    path = out_dir / name
    if not path.exists():
        return None
    if path.suffix != ".json":
        return {"_exists": True}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"_unreadable": True}


def _finding(stage: str, data: dict) -> tuple[str, bool]:
    """(what it found, needs attention)."""
    if data.get("_unreadable"):
        return ("artifact present but unreadable", True)
    if data.get("_exists"):
        return ("written", False)

    if stage == "assess":
        counts = data.get("counts_by_type") or {}
        census = data.get("census") or {}
        text = (f'{data.get("object_count", "?")} object(s) '
                f'({", ".join(f"{v} {k.lower()}" for k, v in sorted(counts.items())) or "none"})')
        if census:
            text += f'; {census.get("total", 0)} non-table/view object(s) that cannot migrate'
        notes = data.get("extraction_notes") or []
        if notes:
            return (text + f'; **{len(notes)} scope(s) unreadable**', True)
        return (text, False)

    if stage == "deps":
        # `cycles` lives in plan.json, never here. What dependencies.json
        # does say is WHERE the graph came from: account_usage is the full
        # graph; parsed_ddl is partial (view DDL only); account_usage_empty
        # and account_usage+parsed_ddl mean ACCOUNT_USAGE was readable but
        # lagged behind the DDL for some or all views, whose DDL was parsed
        # instead; not_extracted is "did not look" and must not read as
        # clean. A value the board does not know is flagged, not guessed at.
        source = data.get("source_used")
        edges = len(data.get("edges") or [])
        if not source:
            # Both producers write the key. Without it, how the graph was got
            # is unknown, and "view DDL only" would be a guess about it.
            return (f'lineage source not recorded; {edges} edge(s) -- '
                    '**completeness unknown**', True)
        text = f'lineage from {source}; {edges} edge(s)'
        if source == "not_extracted":
            return (text + " -- **NOT extracted; view order unchecked**", True)
        unresolved = len(data.get("unresolved_references") or [])
        tail = f', {unresolved} unresolved reference(s)' if unresolved else ''
        if source == "account_usage":
            return (text, False)
        if source == "parsed_ddl":
            return (text + " -- **partial graph (view DDL only)**" + tail, True)
        if source in ("account_usage+parsed_ddl", "account_usage_empty"):
            # The producer writes a per-run warning naming the lagged views
            # and the ones still unordered; surface it rather than restate
            # a weaker version.
            missing = len(data.get("views_without_account_usage_edge") or [])
            note = data.get("warning") or (
                f"{missing} view(s) had no ACCOUNT_USAGE edge; their DDL was "
                "parsed (OBJECT_DEPENDENCIES lags DDL up to ~3 h) -- re-run "
                "deps before relying on the wave order")
            return (text + f" -- **{note}**" + tail, True)
        return (text + " -- **provenance not recognised; completeness unknown**",
                True)

    if stage == "maintenance":
        flagged = data.get("objects_with_signals")
        readable = (data.get("account_usage") or {}).get("readable", True)
        text = f'{flagged} of {len(data.get("tables") or [])} table(s) need a maintenance decision'
        if not readable:
            return (text + "; **ACCOUNT_USAGE unreadable — churn NOT measured**",
                    True)
        return (text, bool(flagged))

    if stage == "security":
        count = data.get("exposure_count")
        secure = len(data.get("secure_views") or [])
        if count is None:
            return ("**policy attachments unreadable — exposure UNKNOWN, "
                    "not zero**", True)
        text = f'{count} policy exposure(s), {secure} secure view(s)'
        # A policy object that exists while POLICY_REFERENCES (which lags
        # ~2 h) lists no attachment is UNCONFIRMED, not zero; and an empty
        # attachment list is uncorroborated when SHOW MASKING/ROW ACCESS
        # POLICIES was denied. `readable` defaults to True so an artefact
        # written before the `policies` block existed stays unflagged.
        unattached = data.get("policies_defined_without_attachment") or 0
        if unattached:
            return (text + f'; **{unattached} policy object(s) defined, '
                    'attachment UNCONFIRMED (ACCOUNT_USAGE.POLICY_REFERENCES '
                    'lags ~2 h)**', True)
        pol = data.get("policies") or {}
        denied = [k for k in ("masking", "row_access", "aggregation",
                              "projection")
                  if not (pol.get(k) or {}).get("readable", True)]
        if denied:
            return (text + f'; **{", ".join(denied)} policy objects could not '
                    'be enumerated — empty attachment list uncorroborated**',
                    True)
        # Tag attachments are a separate ACCOUNT_USAGE view with the same
        # failure mode: unreadable is UNKNOWN, never "no tags attached".
        tags = data.get("tag_references")
        if tags is not None and not tags.get("measured", True):
            return (text + '; **tag attachments unreadable — classification '
                    'UNKNOWN, not zero**', True)
        return (text, bool(count or secure))

    if stage == "compute":
        # compute.json is what propose_all writes: `proposals` and `blocked`.
        # A warehouse with no proposal is a decision still owed, not clean.
        blocked = len(data.get("blocked") or [])
        text = f'{len(data.get("proposals") or [])} warehouse(s) sized'
        if blocked:
            return (text + f', **{blocked} blocked (no shape proposed)**', True)
        return (text, False)

    if stage == "plan":
        s = data.get("summary") or {}
        text = (f'{s.get("can_migrate", "?")} can migrate, '
                f'{s.get("cannot_migrate", "?")} cannot')
        return (text, bool(s.get("cannot_migrate")))

    if stage == "ddl":
        blocked = len(data.get("blocked") or [])
        return (f'{len(data.get("statements") or [])} object(s) to create, '
                f'{blocked} blocked', bool(blocked))

    if stage == "smoke":
        verdict = smoke_verdict(data)
        if verdict == "PASS":
            return ("PASS", False)
        if verdict == "PARTIAL":
            return ("**PARTIAL** — destination not checked", True)
        return ("**FAIL**", True)

    if stage == "preflight":
        cfg = data.get("config") or {}
        failed, skipped = data.get("failed", 0), data.get("skipped", 0)
        text = (f'{len(cfg.get("fields") or [])} field(s) echoed; '
                f'{failed} check(s) failed, {skipped} skipped')
        if cfg.get("missing"):
            return (text + f' — **missing: {", ".join(cfg["missing"])}**', True)
        return (text, bool(failed))

    if stage == "provision":
        if data.get("dry_run"):
            return ("DRY RUN — nothing was provisioned", False)
        steps = data.get("steps") or []
        bad = [s for s in steps if s.get("verified") is False]
        # None in execute mode is "not confirmed, not assumed" (a library
        # install awaiting the restart, a folder create that may have hit
        # an existing one). Pending is pending; it does not read as clean.
        pending = [s for s in steps if s.get("verified") is None]
        text = (f'{len(steps)} step(s); workspace '
                f'{(data.get("workspace") or {}).get("name", "?")}')
        if bad:
            text += f' — **{len(bad)} failed/unverified**'
        if pending:
            text += f' — **{len(pending)} not confirmed**'
        return (text, bool(bad or pending))

    if stage == "catalog":
        if data.get("dry_run"):
            return (f'DRY RUN — {data.get("catalog", "?")} '
                    f'({data.get("catalog_type", "?")}) would be registered; '
                    f'nothing was', False)
        action = data.get("action", _UNKNOWN)
        text = (f'{data.get("catalog", "?")} '
                f'({data.get("catalog_type", "?")}): {action}')
        if action == "create_requested":
            # The create was accepted but the catalog never became visible.
            # Pending is pending; it must not read as success.
            return (text + " — **requested, never became visible**", True)
        return (text, False)

    if stage == "deploy":
        if data.get("dry_run"):
            return (f'DRY RUN — {data.get("statement_count", 0)} object(s) '
                    f'would be created; nothing was', False)
        verified = data.get("verified", 0)
        total = data.get("statement_count", 0)
        # Every outcome the deploy buckets, so the row adds up to the
        # statement count. The default transport (catalog_api) is the one
        # that produces "exists but its structure could not be read" and
        # derived type drift; neither was counted, so a board could read
        # "verified 4/7" with no warning and three tables unverified.
        bad = (len(data.get("failed") or [])
               + len(data.get("mismatched_targets") or []))
        unverified = len(data.get("unverified_structure_targets") or [])
        drift = len(data.get("derived_type_drift_targets") or [])
        errors = (len(data.get("errors") or [])
                  + len(data.get("chunk_errors") or []))
        text = f'**verified {verified}/{total}**'
        if bad:
            text += f', {bad} failed/mismatched'
        if unverified:
            text += f', {unverified} structure not verified'
        if drift:
            text += f', {drift} created with derived type drift'
        if errors:
            text += f', {errors} error(s)'
        return (text, bool(bad or unverified or drift or errors))

    if "_many" in data:
        # The last recorded result per job. A run whose budget ran out is
        # STILL RUNNING, never rounded to either verdict. A status watch_job
        # does not classify (`unrecognised`) is neither done nor running;
        # cmd_run says so and exits 1, and the board must not round it up.
        parts, attention = [], False
        for run in data.get("_many") or []:
            if run.get("_unreadable"):
                parts.append("a run artifact is unreadable")
                attention = True
                continue
            job = run.get("job") or run.get("job_key") or "?"
            clean = False
            if not run.get("terminal") and run.get("unrecognised"):
                verdict = f'**UNRECOGNISED STATE {run.get("status") or "?"}**'
            elif not run.get("terminal"):
                verdict = "STILL RUNNING"
            elif run.get("ok"):
                verdict, clean = run.get("status") or "SUCCESS", True
            else:
                verdict = f'**{run.get("status") or "FAILED"}**'
            attention = attention or not clean
            parts.append(f"{job}: {verdict}")
        return ("; ".join(parts) or "written", attention)

    if stage == "data-options":
        choice = data.get("choice")
        return (("architecture recorded" if choice else
                 "options presented; none chosen"), False)

    if "run_key" in data or stage.endswith("-workflow"):
        status = data.get("status") or _UNKNOWN
        restarts = len(data.get("restarts") or [])
        text = f"job run {status}" + (
            f" after {restarts} cold-start restart(s)" if restarts else "")
        return (text, not data.get("ok"))

    return ("written", False)


def build_stage_board(out_dir) -> dict:
    out_dir = pathlib.Path(out_dir)
    rows: list[dict] = []
    next_stage = None
    present = {spec["stage"] for spec in STAGES
               if _artifact_paths(out_dir, spec["artifact"])}
    for spec in STAGES:
        data = _load(out_dir, spec["artifact"])
        twin = spec.get("alternative_to")
        if data is None and twin in present:
            rows.append({**spec, "status": "SATISFIED",
                         "found": f"satisfied by `{twin}`", "attention": False})
            continue
        if data is None:
            rows.append({**spec, "status": "NOT_RUN", "found": "—",
                         "attention": False})
            # An optional stage that has not run is not "next": the pipeline
            # proceeds without it.
            if next_stage is None and not spec.get("optional"):
                next_stage = spec["stage"]
            continue
        found, attention = _finding(spec["stage"], data)
        rows.append({**spec, "status": "DONE", "found": found,
                     "attention": attention,
                     # A caveat and a failure both raise `attention`, and
                     # they are not the same for what comes next: `deps`
                     # over a manifest is flagged `not_extracted` on
                     # purpose and planning still proceeds, while a job run
                     # that answered `ok: false` did not do its work. Only
                     # the second one blocks.
                     "failed": bool(isinstance(data, dict)
                                    and (data.get("ok") is False
                                         or data.get("failed")
                                         or any(r.get("ok") is False
                                                for r in _job_runs(data)))),
                     # A dry run wrote its artifact and created nothing, so
                     # it satisfies no prerequisite.
                     "dry_run": bool(isinstance(data, dict)
                                     and data.get("dry_run"))})
    return {"out_dir": str(out_dir), "stages": rows, "next_stage": next_stage,
            "needs_attention": [r["stage"] for r in rows if r["attention"]]}


def stage_for(command: str, job: str | None = None) -> str:
    """The phase a CLI invocation belongs to. A workflow run is named by its
    job, so S6 and S10 are logged as themselves rather than as a flat `run`."""
    for spec in STAGES:
        if spec["command"] != command:
            continue
        if spec.get("job") is None and command != "run":
            return spec["stage"]
        if job and spec.get("job") == job:
            return spec["stage"]
        # A job per schema (snowmig_02_copy_<schema>) is the same stage.
        if job and spec.get("job_prefix") and job.startswith(spec["job_prefix"]):
            return spec["stage"]
    return command


def _ts(value) -> datetime.datetime | None:
    if not value:
        return None
    ts = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


def _legacy_run_stage(run: dict, out_dir: pathlib.Path) -> str:
    """A bare `run` logged before job names were recorded: attribute it to
    the workflow whose artifact was written inside that run's window."""
    lo, hi = _ts(run.get("started_at")), _ts(run.get("ended_at"))
    if not lo or not hi:
        return "run"
    for spec in STAGES:
        if not spec.get("job"):
            continue
        for art in _artifact_paths(out_dir, spec["artifact"]):
            if not art.is_file():
                continue
            written = datetime.datetime.fromtimestamp(
                art.stat().st_mtime, datetime.timezone.utc)
            if lo <= written <= hi + datetime.timedelta(seconds=5):
                return spec["stage"]
    return "run"


# `RETRY ...` as the notebooks print it, after their `[copy] ` log prefix.
_RETRY_LINE = re.compile(r"^\s*(?:\[[\w-]+\]\s+)?RETRY\s")


def _job_runs(art) -> list[dict]:
    """The job-run records behind a workflow artifact: one, or one per job."""
    if not isinstance(art, dict):
        return []
    many = art.get("_many")
    runs = many if many is not None else [art]
    return [r for r in runs if isinstance(r, dict)]


def _verdict(code) -> str:
    if code is None:
        return "UNKNOWN"
    return {0: "PASS", 3: "HALT"}.get(int(code), "FAIL")


def _phase_summary(rows: list[dict]) -> list[dict]:
    """Stage rows rolled up into their phases, in pipeline order.

    A phase FAILS if any of its stages' last run failed or halted; it is
    NOT_RUN while any required stage in it has not run; SKIPPED when every
    stage in it is optional and none ran; otherwise it PASSES."""
    out = []
    for phase in dict.fromkeys(r["phase"] for r in rows):
        mine = [r for r in rows if r["phase"] == phase]
        optional = {s["stage"] for s in STAGES if s.get("optional")}
        res = [r["result"] for r in mine]
        passed = sum(1 for x in res if x == "PASS" or x.startswith("DONE"))
        failed = sum(1 for x in res if x.startswith(("FAIL", "HALT")))
        not_run = [r["stage"] for r in mine if r["result"] == "NOT_RUN"]
        skipped = sum(1 for x in res if x.startswith("SKIPPED"))
        if failed:
            verdict = "FAIL"
        elif not_run:
            verdict = "NOT_RUN" if not passed else "PARTIAL"
        elif not passed and skipped == len(mine):
            verdict = "SKIPPED (optional)"
        else:
            verdict = "PASS"
        out.append({
            "phase": phase, "stages": len(mine), "passed": passed,
            "failed": failed, "not_run": len(not_run), "skipped": skipped,
            "duration_seconds": round(sum(r["duration_seconds"] or 0
                                          for r in mine), 1),
            "retries": sum(r.get("retries", 0) for r in mine),
            "runbook": " ".join(r["runbook"] for r in mine
                                if r["runbook"] != "-"),
            "verdict": verdict,
            "optional_only": all(r["stage"] in optional for r in mine)})
    return out


def phase_report(out_dir) -> dict:
    """Every phase with its start, end, duration and verdict.

    Read from run_log.jsonl. A phase that never ran is listed as NOT_RUN (or
    SKIPPED for an optional one) rather than left out; an artifact with no
    logged run is DONE (not logged), never given a time it was not measured
    at. The LAST run of a phase decides its verdict, and earlier failures
    are counted, not forgotten.
    """
    out_dir = pathlib.Path(out_dir)
    runs: dict[str, list[dict]] = {}
    path = out_dir / "run_log.jsonl"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            stage = rec.get("stage")
            if stage == "run":
                stage = (stage_for("run", rec.get("job")) if rec.get("job")
                         else _legacy_run_stage(rec, out_dir))
            runs.setdefault(stage, []).append(rec)

    phases = []
    for spec in STAGES:
        mine = sorted(runs.get(spec["stage"], []),
                      key=lambda r: r.get("started_at") or "")
        row = {"stage": spec["stage"], "phase": spec["phase"],
               "runbook": spec["runbook"], "runs_on": spec["runs_on"],
               "runs": len(mine),
               "failed_runs": sum(1 for r in mine
                                  if r.get("exit_code") not in (0, None)),
               "started_at": None, "ended_at": None,
               "duration_seconds": None, "retries": 0}
        if mine:
            last = mine[-1]
            lo, hi = _ts(last.get("started_at")), _ts(last.get("ended_at"))
            row.update({
                "started_at": last.get("started_at"),
                "ended_at": last.get("ended_at"),
                "duration_seconds": (round((hi - lo).total_seconds(), 1)
                                     if lo and hi else None),
                "result": _verdict(last.get("exit_code")),
                "retries": sum(int(r.get("retries") or 0) for r in mine)})
            # A workflow can exit 0 locally with a job that did not succeed.
            art = _load(out_dir, spec["artifact"])
            jobs = _job_runs(art) if spec.get("job") else []
            # Retries inside an AIDP job are in its own output, one RETRY
            # line each (the copy notebook writes them).
            for run in jobs:
                row["retries"] += sum(
                    1 for line in str(run.get("output") or "").splitlines()
                    if _RETRY_LINE.search(line))
            bad = [r for r in jobs if r.get("ok") is False]
            if bad and "_many" in art:
                # One job per schema: name the ones that did not succeed.
                row["result"] = "FAIL (" + ", ".join(
                    f'job {r.get("job")} {r.get("status", "?")}'
                    for r in bad) + ")"
            elif bad:
                row["result"] = f'FAIL (job {bad[0].get("status", "?")})'
        elif _artifact_paths(out_dir, spec["artifact"]):
            row["result"] = "DONE (not logged)"
        else:
            row["result"] = ("SKIPPED (optional)" if spec.get("optional")
                             else "NOT_RUN")
        phases.append(row)
    summary = _phase_summary(phases)
    from .resources import build_resources, resources_by_phase
    resources = build_resources(out_dir)
    grouped = resources_by_phase(resources)
    for ph in summary:
        ph["resources"] = grouped.get(ph["phase"], [])
    events = []
    rpath = out_dir / "retries.jsonl"
    if rpath.is_file():
        for line in rpath.read_text(encoding="utf-8").splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
    return {"out_dir": str(out_dir), "phases": phases,
            "phase_summary": summary,
            "resources": resources,
            "retry_events": events,
            "unattributed_runs": len(runs.get("run", []))}
