#!/usr/bin/env python3
"""snowmig -- Snowflake -> AIDP migrator CLI.

Five subcommands, one per pipeline stage. Each reads the previous stage's JSON
and writes its own plus a markdown report, so any stage can be re-run alone.

  assess  -> inventory.json      + INVENTORY.md            (needs Snowflake)
  deps    -> dependencies.json                              (needs Snowflake)
  plan    -> plan.json           + PLANNED_OBJECTS.md       (offline)
  ddl     -> ddl_plan.json       + DDL_PLAN.md              (offline)
  deploy  -> deploy_result.json  + SOFT_CLONE_SUMMARY.md    (dry-run offline;
                                                             --execute needs AIDP)
  compute -> compute.json        + COMPUTE_PROPOSAL.md      (needs Snowflake)
  smoke   -> smoke.json          + SMOKE_TEST.md            (source; dest if given)
  notebook-> <nb>.ipynb          + NOTEBOOK.md              (offline; --upload writes)
  summary -> SUMMARY.md                                     (offline)
  data-options -> data_options.json + DATA_MOVEMENT_OPTIONS.md  (offline; PROPOSAL
                  ONLY -- this plugin moves no bytes and implements no transfer)

Bronze mirrors the source: Snowflake database -> AIDP Standard Catalog, schema ->
schema, table -> table, view -> view. Silver and Gold get disabled job stubs.

Exit codes: 0 ok | 1 error | 3 HALT (identifier-case or target-name collision)
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import pathlib
import sys

from plan.build import TargetCollision, build_plan
from plan.restrictions import InvalidRestriction
from plan.data_movement import OPTIONS as DATA_OPTIONS
from plan.data_movement import options_for, record_choice
from plan.smoke import run_smoke
from target.notebook import build_notebook, notebook_workspace_path
from report.render import (
    render_maintenance,
    render_compute, render_ddl_plan, render_inventory, render_planned_objects,
    render_data_options, render_smoke, render_soft_clone_summary,
    render_summary,
)
from snowflake_source.conn import (
    AuthError, SourceWriteRefused, build_connect_kwargs, connect,
    make_run_sql,
)
from snowflake_source.extract.catalog import (
    ROW_COUNT_MODES, build_inventory)
from snowflake_source.extract.maintenance import build_maintenance
from snowflake_source.dialect.types import (
    GEOSPATIAL_MODES, SEMI_STRUCTURED_MODES)
from snowflake_source.extract.dependencies import extract_dependencies
from snowflake_source.extract.warehouses import extract_warehouses
from sizing.warehouse_map import propose_all
from target.coords import MissingTarget, resolve_target
from target.ddl import build_create_table, build_create_view
from target.deploy import RefusedToExecute, deploy
from target.executor import (
    NoBackendAvailable, build_command, detect_backend,
)
# Aliased: snowflake_source.conn also exports make_run_sql, and the
# unqualified import shadowed it.
from target.runner import BackendError
from target.runner import make_run_sql as make_aidp_run_sql

HALT = 3


def _read(out_dir: pathlib.Path, name: str) -> dict:
    path = out_dir / name
    if not path.is_file():
        raise FileNotFoundError(
            f"{name} not found in {out_dir}. Run the earlier stage first.")
    return json.loads(path.read_text())


def _write(out_dir: pathlib.Path, name: str, payload) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        (out_dir / name).write_text(payload)
    else:
        (out_dir / name).write_text(json.dumps(payload, indent=2, default=str))
    print(f"  -> {out_dir / name}")


def _run_sql_from_args(args):
    kwargs = build_connect_kwargs(
        args.auth, account=args.account, user=args.user, role=args.role,
        warehouse=args.warehouse, key_path=args.key_path,
        key_passphrase=args.key_passphrase, pat_path=args.pat_path,
        password_path=args.password_path)
    return make_run_sql(connect(**kwargs))


def _assess_inventory(args) -> dict:
    """Seam for tests: patched to avoid a live connection."""
    return build_inventory(
        _run_sql_from_args(args), args.database or None,
        row_counts=getattr(args, "row_counts", "metadata"),
        semi_structured=getattr(args, "semi_structured", "block"),
        geospatial=getattr(args, "geospatial", "block"))


def cmd_assess(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _assess_inventory(args)
    _write(out, "inventory.json", inv)
    _write(out, "INVENTORY.md", render_inventory(inv))
    if inv.get("identifier_case_collisions"):
        print("HALT: identifier-case collisions; see INVENTORY.md", file=sys.stderr)
        return HALT
    return 0


def cmd_deps(args) -> int:
    out = pathlib.Path(args.out_dir)
    deps = extract_dependencies(_run_sql_from_args(args), _read(out, "inventory.json"))
    _write(out, "dependencies.json", deps)
    print(f'  lineage source: {deps["source_used"]}')
    return 0


def cmd_maintenance(args) -> int:
    """Snowflake maintenance/layout state. Reports; proposes nothing."""
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    maint = build_maintenance(
        _run_sql_from_args(args), inv,
        history_days=args.history_days,
        probe_table_parameters=args.probe_table_parameters)
    _write(out, "maintenance.json", maint)
    _write(out, "MAINTENANCE.md", render_maintenance(maint))
    flagged = maint["objects_with_signals"]
    print(f'  {flagged} of {len(maint["tables"])} table(s) need a maintenance '
          f'decision; none applied')
    if not maint["account_usage"].get("readable", True):
        print("  ACCOUNT_USAGE unreadable: reclustering and churn NOT measured "
              "(not zero)", file=sys.stderr)
    return 0


def cmd_plan(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    deps = _read(out, "dependencies.json")
    restrictions = (json.loads(pathlib.Path(args.restrictions).read_text())
                    if args.restrictions else None)
    # A recorded architecture choice, if the data-options stage has been run.
    choice = None
    if (out / "data_options.json").is_file():
        choice = _read(out, "data_options.json").get("choice")
    try:
        built = build_plan(inv, deps, restrictions=restrictions,
                           bronze_catalog_prefix=args.bronze_catalog_prefix,
                           architecture_choice=choice)
    except TargetCollision as exc:
        print(f"HALT: {exc}", file=sys.stderr)
        return HALT
    _write(out, "plan.json", built)
    _write(out, "PLANNED_OBJECTS.md", render_planned_objects(built))
    s = built["summary"]
    from plan.data_movement import architecture_decision
    decision = architecture_decision(built.get("architecture_choice"))
    if decision["decided"]:
        state = decision["chosen"]["id"]
        custom = decision["chosen"].get("custom_architecture")
        if custom:
            state += f' — "{custom["name"]}" (customer-defined, not assessed)'
    elif decision["deferred"]:
        state = "DEFERRED by the customer — not a gap"
    else:
        state = f'UNDECIDED — {len(decision["options"])} options presented'
    print(f"  architecture: {state}")
    print(f'  planned {s["can_migrate"]} object(s) '
          f'({s["tables"]} table, {s["views"]} view); '
          f'{s["cannot_migrate"]} cannot move')
    return 0


def cmd_ddl(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    built = _read(out, "plan.json")
    by_id = {r["source_identifier"]: r for r in inv["inventory"]}

    name_map = built.get("target_names", {})
    # Wave order, so a view is always emitted after the tables it reads.
    ordered = [i for wave in built.get("waves", []) for i in wave]
    ordered += [i for i in built.get("clone_targets", []) if i not in ordered]

    statements, blocked = [], []
    for ident in ordered:
        rec = by_id.get(ident)
        if rec is None:
            continue
        builder = (build_create_view if rec.get("object_type") == "VIEW"
                   else build_create_table)
        res = (builder(rec, name_map[ident], name_map)
               if rec.get("object_type") == "VIEW"
               else builder(rec, name_map[ident]))
        if res.blocked:
            blocked.append({"source_identifier": ident,
                            "object_type": rec.get("object_type"),
                            "reason": res.blocked_reason})
            continue
        statements.append({
            "source_identifier": res.source_identifier,
            "object_type": rec.get("object_type"),
            "target_fqn": res.target_fqn, "sql": res.sql,
            "rules_applied": [dataclasses.asdict(r) for r in res.rules_applied],
            "warnings": res.warnings, "omitted_properties": res.omitted_properties,
            # Deployment verifies the structure against this, not just the name.
            "expected_columns": res.expected_columns,
            # Source settings with an AIDP equivalent that this version does
            # not apply. Reported, never silently invented.
            "deferred_properties": res.deferred_properties})

    payload = {"statements": statements, "blocked": blocked,
               "bronze_catalog_prefix": built.get("bronze_catalog_prefix")}
    _write(out, "ddl_plan.json", payload)
    _write(out, "DDL_PLAN.md", render_ddl_plan(payload))
    return 0


def cmd_deploy(args) -> int:
    out = pathlib.Path(args.out_dir)
    ddl_plan = _read(out, "ddl_plan.json")
    built = _read(out, "plan.json")
    target, run_sql = None, None
    if args.execute:
        target = resolve_target(datalake_ocid=args.datalake_ocid,
                                workspace=args.workspace,
                                cluster_id=args.cluster_id, catalog=args.catalog)
        backend = args.backend or detect_backend()
        print(f"  backend: {backend}")
        run_sql = make_aidp_run_sql(target, backend=backend)
    result = deploy(ddl_plan, target=target, execute=args.execute, run_sql=run_sql,
                    chunk_size=args.chunk_size)
    _write(out, "deploy_result.json", result)
    _write(out, "SOFT_CLONE_SUMMARY.md", render_soft_clone_summary(built, result))
    return 1 if result.get("failed") or result.get("chunk_errors") else 0


def cmd_compute(args) -> int:
    out = pathlib.Path(args.out_dir)
    warehouses = extract_warehouses(_run_sql_from_args(args))
    _write(out, "warehouses.json", warehouses)
    sizing = propose_all(warehouses["warehouses"],
                         credit_price_usd=args.credit_price)
    sizing["metering_source"] = warehouses["metering_source"]
    sizing["metering_note"] = warehouses["metering_note"]
    _write(out, "compute.json", sizing)
    _write(out, "COMPUTE_PROPOSAL.md", render_compute(sizing))
    print(f'  {warehouses["warehouse_count"]} warehouse(s), '
          f'metering: {warehouses["metering_source"]}')
    return 0


def _optional_target(args):
    """Resolve a target only if all four coordinates were supplied."""
    if not all([args.datalake_ocid, args.workspace, args.cluster_id, args.catalog]):
        return None
    return resolve_target(datalake_ocid=args.datalake_ocid,
                          workspace=args.workspace,
                          cluster_id=args.cluster_id, catalog=args.catalog)


def cmd_smoke(args) -> int:
    out = pathlib.Path(args.out_dir)
    target = _optional_target(args)
    dest_run_sql = None
    if target is not None:
        backend = args.backend or detect_backend()
        print(f"  destination backend: {backend}")
        dest_run_sql = make_aidp_run_sql(target, backend=backend)
    result = run_smoke(source_run_sql=_run_sql_from_args(args), target=target,
                       dest_run_sql=dest_run_sql, write_probe=args.write_probe,
                       database=(args.database or [None])[0]
                       if getattr(args, "database", None) else None)
    _write(out, "smoke.json", result)
    _write(out, "SMOKE_TEST.md", render_smoke(result))
    print(f'  verdict: {"PASS" if result["ok"] else "FAIL"}')
    return 0 if result["ok"] else 1


def cmd_notebook(args) -> int:
    out = pathlib.Path(args.out_dir)
    ddl_plan = _read(out, "ddl_plan.json")
    built = _read(out, "plan.json")
    inv = _read(out, "inventory.json")
    session = inv.get("session", {})

    # Do not pick a catalog on the user's behalf when there is a choice. One
    # notebook per catalog, and which one is a decision, not a default.
    candidates = built.get("catalogs_to_create") or []
    if args.catalog:
        catalog = args.catalog
    elif len(candidates) == 1:
        catalog = candidates[0]
    elif not candidates:
        raise ValueError("no catalog to generate a notebook for")
    else:
        raise ValueError(
            "the plan spans " + str(len(candidates)) + " catalogs ("
            + ", ".join(candidates) + "); pass --catalog to choose one. One "
            "notebook per catalog, and the choice is not assumed.")

    doc = build_notebook(ddl_plan, built, catalog=catalog,
                         source={"account": session.get("A"),
                                 "region": session.get("R")})
    local = out / f"snowmig_shallow_clone_{catalog}.ipynb"
    _write(out, local.name, doc)
    ws_path = notebook_workspace_path(catalog)

    lines = [f"# Shallow-clone notebook for `{catalog}`", "",
             f"Generated: `{local}`", f"Intended AIDP path: `{ws_path}`", "",
             f'{doc["metadata"]["snowmig"]["statement_count"]} statement(s); '
             f'{doc["metadata"]["snowmig"]["blocked_count"]} object(s) not '
             "attempted.", "",
             "**The notebook creates empty structure and moves no data.** It "
             "prints progress per object so a long run stays visible, and "
             "verifies each object individually at the end.", ""]

    if not args.upload:
        lines += ["Not uploaded. Re-run with `--upload` plus the AIDP target "
                  "coordinates to place it in the workspace.", ""]
        _write(out, "NOTEBOOK.md", "\n".join(lines))
        print("  generated only; pass --upload to place it in AIDP")
        return 0

    target = _optional_target(args)
    if target is None:
        raise MissingTarget(
            "--upload needs all four AIDP coordinates: --datalake-ocid, "
            "--workspace, --cluster-id, --catalog. Ask the user for them.")
    backend = args.backend or detect_backend()
    cmd = build_command(backend, "upload_notebook", target,
                        workspace_path=ws_path, local_path=str(local))
    print(f"  backend: {backend}")
    print(f'  command: {" ".join(cmd)}')
    if args.dry_run:
        lines += ["Upload was a **dry run**. The command above was not executed.",
                  ""]
        _write(out, "NOTEBOOK.md", "\n".join(lines))
        return 0

    import subprocess
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        print(f"error: upload failed: {(proc.stderr or '')[:400]}", file=sys.stderr)
        return 1
    lines += [f"Uploaded to `{ws_path}`.", "",
              "**Ask the user before executing it.** Then run it from the AIDP "
              "workspace, or via `aidp notebook run`.", ""]
    _write(out, "NOTEBOOK.md", "\n".join(lines))
    print(f"  uploaded to {ws_path}")
    return 0


def cmd_summary(args) -> int:
    out = pathlib.Path(args.out_dir)
    built = _read(out, "plan.json")
    inv = _read(out, "inventory.json")
    deployed = None
    if (out / "deploy_result.json").is_file():
        deployed = _read(out, "deploy_result.json")
    target = _optional_target(args)
    _write(out, "SUMMARY.md",
           render_summary(built, inv,
                          deployed,
                          dataclasses.asdict(target) if target else None))
    return 0


def cmd_data_options(args) -> int:
    out = pathlib.Path(args.out_dir)
    options = options_for(args.phase) if args.phase else list(DATA_OPTIONS)
    payload = {"options": options, "implemented": False,
               "note": ("Proposal only. This plugin moves no bytes and implements "
                        "no transfer path.")}
    if args.choose:
        if not args.rationale:
            raise ValueError("--choose requires --rationale")
        custom = None
        if args.custom_name or args.custom_description_file:
            if not (args.custom_name and args.custom_description_file):
                raise ValueError(
                    "a custom architecture needs both --custom-name and "
                    "--custom-description-file")
            custom = {
                "name": args.custom_name,
                "description": pathlib.Path(
                    args.custom_description_file).read_text().strip()}
        payload["choice"] = record_choice(
            args.choose, chosen_by=args.chosen_by, rationale=args.rationale,
            custom_architecture=custom)
        state = ("DEFERRED — the customer will specify it later"
                 if payload["choice"]["deferred"] else args.choose)
        print(f"  recorded: {state} (executed: False)")
    _write(out, "data_options.json", payload)
    _write(out, "DATA_MOVEMENT_OPTIONS.md", render_data_options(options))
    print(f"  {len(options)} option(s) presented; none implemented")
    return 0


def _add_target_args(p) -> None:
    p.add_argument("--datalake-ocid")
    p.add_argument("--workspace")
    p.add_argument("--cluster-id")
    p.add_argument("--catalog")
    p.add_argument("--backend", choices=["aidp_cli", "oci_raw"],
                   help="override backend detection")


def _add_snowflake_args(p) -> None:
    p.add_argument("--account")
    p.add_argument("--user")
    p.add_argument("--role")
    p.add_argument("--warehouse")
    p.add_argument("--auth", default="keypair",
                   choices=["keypair", "pat", "password", "externalbrowser"])
    p.add_argument("--key-path")
    p.add_argument("--key-passphrase")
    p.add_argument("--pat-path")
    p.add_argument("--password-path")


def build_parser() -> argparse.ArgumentParser:
    # --out-dir lives on a parent parser so it is accepted AFTER the subcommand,
    # which is how every caller writes it: `snowmig plan --out-dir ...`.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--out-dir", default="snowmig_out")

    ap = argparse.ArgumentParser(prog="snowmig", description=__doc__,
                                 parents=[common])
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("assess", parents=[common],
                       help="read-only estate inventory")
    _add_snowflake_args(a)
    a.add_argument("--database", action="append",
                   help="repeatable; omit to scan all non-system databases")
    a.add_argument("--row-counts", choices=list(ROW_COUNT_MODES),
                   default="metadata",
                   help="metadata (default): Snowflake's maintained count, free "
                        "to read. exact: COUNT(*) per object -- accurate, but it "
                        "EXECUTES every view and costs warehouse time. none: skip")
    a.add_argument("--semi-structured", choices=list(SEMI_STRUCTURED_MODES),
                   default="block",
                   help="block (default): VARIANT/OBJECT/ARRAY block their table "
                        "pending a typed design. string: carry the JSON as text, "
                        "with a warning on every affected column")
    a.add_argument("--geospatial", choices=list(GEOSPATIAL_MODES),
                   default="block",
                   help="block (default): GEOGRAPHY/GEOMETRY block their table. "
                        "string: carry as text, with no spatial type on the target")
    a.set_defaults(func=cmd_assess)

    d = sub.add_parser("deps", parents=[common], help="dependency edges")
    _add_snowflake_args(d)
    d.set_defaults(func=cmd_deps)

    mt = sub.add_parser("maintenance", parents=[common],
                        help="maintenance/layout state (needs Snowflake)")
    _add_snowflake_args(mt)
    mt.add_argument("--history-days", type=int, default=30,
                    help="ACCOUNT_USAGE window for reclustering credits and "
                         "DML churn (default 30)")
    mt.add_argument("--probe-table-parameters", action="store_true",
                    help="one SHOW PARAMETERS per table. Exact, but thousands "
                         "of round trips on a real estate; off by default, "
                         "where the level is inferred from effective values")
    mt.set_defaults(func=cmd_maintenance)

    p = sub.add_parser("plan", parents=[common], help="waves + medallion layout (offline)")
    p.add_argument("--restrictions",
                   help="JSON file of user restrictions (exclude_databases, "
                        "max_rows, exclude_name_patterns, ...)")
    p.add_argument("--bronze-catalog-prefix",
                   help="use ONE bronze catalog with this name instead of "
                        "catalog-per-database (default: mirror the source)")
    p.set_defaults(func=cmd_plan)

    g = sub.add_parser("ddl", parents=[common], help="generate target DDL (offline)")
    g.set_defaults(func=cmd_ddl)

    dep = sub.add_parser("deploy", parents=[common], help="dry-run by default")
    dep.add_argument("--execute", action="store_true")
    _add_target_args(dep)
    dep.add_argument("--chunk-size", type=int, default=25)
    dep.set_defaults(func=cmd_deploy)

    c = sub.add_parser("compute", parents=[common],
                       help="warehouse -> AIDP cluster proposal")
    _add_snowflake_args(c)
    c.add_argument("--credit-price", type=float,
                   help="USD per Snowflake credit; required for a cost model, "
                        "never assumed")
    c.set_defaults(func=cmd_compute)

    sm = sub.add_parser("smoke", parents=[common],
                        help="connectivity + permission check on both ends")
    _add_snowflake_args(sm)
    _add_target_args(sm)
    sm.add_argument("--database", action="append",
                    help="database to probe INFORMATION_SCHEMA in; auto-picked "
                         "from the first non-system database otherwise")
    sm.add_argument("--write-probe", action="store_true",
                    help="prove destination WRITE by creating a probe schema; it "
                         "is NOT dropped afterwards")
    sm.set_defaults(func=cmd_smoke)

    nb = sub.add_parser("notebook", parents=[common],
                        help="generate the shallow-clone notebook; --upload places "
                             "it in the AIDP workspace")
    _add_target_args(nb)
    nb.add_argument("--upload", action="store_true")
    nb.add_argument("--dry-run", action="store_true",
                    help="with --upload, print the command without running it")
    nb.set_defaults(func=cmd_notebook)

    su = sub.add_parser("summary", parents=[common],
                        help="migration summary table: rows, risk, status")
    _add_target_args(su)
    su.set_defaults(func=cmd_summary)

    do = sub.add_parser("data-options", parents=[common],
                        help="present the data-movement options (PROPOSAL ONLY)")
    do.add_argument("--phase", choices=["historic", "ongoing"])
    do.add_argument("--choose", help="record the chosen option id; executes nothing")
    do.add_argument("--chosen-by", default="unspecified")
    do.add_argument("--rationale", help="required with --choose")
    do.add_argument("--custom-name",
                    help="name of a customer architecture that is NOT one of the "
                         "listed options; use with --choose A6_CUSTOMER_DEFINED")
    do.add_argument("--custom-description-file",
                    help="file describing that architecture, recorded verbatim")
    do.set_defaults(func=cmd_data_options)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (AuthError, MissingTarget, RefusedToExecute, FileNotFoundError,
            InvalidRestriction, NoBackendAvailable, BackendError,
            SourceWriteRefused, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
