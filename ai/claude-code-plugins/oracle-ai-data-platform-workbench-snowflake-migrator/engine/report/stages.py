"""The stage board: what is supposed to run, what has run, what it found.

Read the run before executing it. This module makes no decisions and touches
no environment -- it reads the artifacts already in `--out-dir` and reports
the shape of the pipeline against them.

Two rules it holds to, both learned the hard way elsewhere in this plugin:

  * A stage that could not look is FLAGGED, never shown as clean. "0
    exposures" and "we could not read the policy references" are opposite
    findings and must not render the same.
  * Three stages write to AIDP -- `provision`, `catalog` and `deploy` -- and
    the board says which. (`smoke --write-probe` and `notebook --upload` can
    write too, narrowly and opt-in, and say so in their own reports.)
"""
from __future__ import annotations

import json
import pathlib

__all__ = ["STAGES", "build_stage_board"]

STAGES: tuple[dict, ...] = (
    # Optional only in the sense that a run can skip it; skipping it is how
    # a wrong host or role costs hours later.
    {"stage": "preflight", "needs": "a connection config", "writes": False,
     "optional": True, "artifact": "preflight.json",
     "purpose": "read the connection config back to the user, field by "
                "field, and test both ends before anything else runs"},
    {"stage": "assess", "needs": "Snowflake", "writes": False,
     "artifact": "inventory.json",
     "purpose": "inventory tables and views, plus a census of everything that "
                "is not one"},
    {"stage": "deps", "needs": "Snowflake", "writes": False,
     "artifact": "dependencies.json",
     "purpose": "lineage, so views land after their base tables"},
    {"stage": "maintenance", "needs": "Snowflake", "writes": False,
     "artifact": "maintenance.json",
     "purpose": "clustering, retention and churn — who inherits OPTIMIZE/VACUUM"},
    {"stage": "security", "needs": "Snowflake", "writes": False,
     "artifact": "security.json",
     "purpose": "masking/row-access policies, secure views, grants — what "
                "arrives unprotected"},
    {"stage": "compute", "needs": "Snowflake", "writes": False,
     "artifact": "compute.json",
     "purpose": "warehouse-to-cluster sizing proposal"},
    # Optional: it feeds `plan` when run, but `plan` runs without it, so the
    # board must not stall on it as "next".
    {"stage": "data-options", "needs": "nothing (offline)", "writes": False,
     "optional": True,
     "artifact": "data_options.json",
     "purpose": "the data-movement architecture options — presented, never chosen"},
    {"stage": "plan", "needs": "nothing (offline)", "writes": False,
     "artifact": "plan.json",
     "purpose": "what can migrate, in what order, to which target name"},
    {"stage": "ddl", "needs": "nothing (offline)", "writes": False,
     "artifact": "ddl_plan.json",
     "purpose": "the statements/bodies that would create the structure"},
    {"stage": "smoke", "needs": "Snowflake + AIDP", "writes": False,
     "artifact": "smoke.json",
     "purpose": "connectivity and permissions on both ends"},
    # Optional: required for the in-AIDP data path, not for a structure-only
    # clone, so the board must not stall on it as "next".
    {"stage": "provision", "needs": "AIDP", "writes": True, "optional": True,
     "artifact": "provision_result.json",
     "purpose": "workspace, migration-assets cluster, the backup-snowflake-"
                "migration/ folder with scripts + plan, and the four "
                "migration jobs. Dry-run unless --execute"},
    {"stage": "catalog", "needs": "AIDP", "writes": True,
     "artifact": "catalog_result.json",
     "purpose": "register the target catalog — EXTERNAL/SNOWFLAKE by default, "
                "a read-only pointer that copies nothing. Dry-run unless "
                "--execute"},
    {"stage": "deploy", "needs": "AIDP", "writes": True,
     "artifact": "deploy_result.json",
     "purpose": "create schemas, tables and views in a STANDARD catalog. "
                "Refuses an EXTERNAL target. Dry-run unless --execute"},
    {"stage": "notebook", "needs": "nothing (offline)", "writes": False,
     "artifact": "NOTEBOOK.md",
     "purpose": "the clone as an executable AIDP notebook"},
    {"stage": "summary", "needs": "nothing (offline)", "writes": False,
     "artifact": "SUMMARY.md",
     "purpose": "per-object roll-up: rows, risk, migration status"},
)

_UNKNOWN = "could not be determined"


def _load(out_dir: pathlib.Path, name: str):
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
        # `cycles` lives in plan.json, never here, so the old rule could not
        # fire. What dependencies.json does say is WHERE the graph came from:
        # account_usage is the full graph; parsed_ddl is partial (view DDL
        # only); not_extracted is "did not look" and must not read as clean.
        source = data.get("source_used", _UNKNOWN)
        edges = len(data.get("edges") or [])
        text = f'lineage from {source}; {edges} edge(s)'
        if source == "not_extracted":
            return (text + " -- **NOT extracted; view order unchecked**", True)
        if source != "account_usage":
            unresolved = len(data.get("unresolved_references") or [])
            text += " -- **partial graph (view DDL only)**"
            if unresolved:
                text += f', {unresolved} unresolved reference(s)'
            return (text, True)
        return (text, bool(data.get("cycles")))

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
        ok = data.get("ok")
        return ("PASS" if ok else "**FAIL**", not ok)

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

    if stage == "data-options":
        choice = data.get("choice")
        return (("architecture recorded" if choice else
                 "options presented; none chosen"), False)

    return ("written", False)


def build_stage_board(out_dir) -> dict:
    out_dir = pathlib.Path(out_dir)
    rows: list[dict] = []
    next_stage = None
    for spec in STAGES:
        data = _load(out_dir, spec["artifact"])
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
                     "attention": attention})
    return {"out_dir": str(out_dir), "stages": rows, "next_stage": next_stage,
            "needs_attention": [r["stage"] for r in rows if r["attention"]]}
