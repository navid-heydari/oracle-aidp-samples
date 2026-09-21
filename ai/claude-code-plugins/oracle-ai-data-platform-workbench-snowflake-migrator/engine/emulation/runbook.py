"""Dev mode: the whole pipeline, end to end, against the emulation.

Runs the SAME extractors, planner, DDL generator, smoke test, catalog
registration and deploy code a production run uses — only the two transports
are the fakes in this package. The artifacts written are real artifacts in the
real formats, so the demo doubles as documentation of every file a production
run produces.

Honesty rule: everything here is labelled emulated. The out-dir gets an
`emulation.json` marker and DEMO.md opens with the banner, because a demo
artifact mistaken for a customer run would be worse than no demo.
"""
from __future__ import annotations

import dataclasses
import datetime
import json
import pathlib

from plan.build import build_plan
from plan.data_movement import OPTIONS as DATA_OPTIONS
from plan.smoke import run_smoke
from report.render import (
    render_catalog, render_census, render_compute, render_data_options,
    render_ddl_plan, render_inventory, render_maintenance,
    render_planned_objects, render_preflight, render_security, render_smoke,
    render_soft_clone_summary, render_stages, render_summary,
)
from report.stages import build_stage_board
from sizing.warehouse_map import propose_all
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.census import build_census
from snowflake_source.extract.dependencies import extract_dependencies
from snowflake_source.extract.maintenance import build_maintenance
from snowflake_source.extract.security import build_security
from snowflake_source.extract.warehouses import extract_warehouses
from target.catalog_deploy import RefusedToExecute, deploy_catalog
from target.catalog_provision import ensure_catalog
from target.coords import resolve_target
from target.ddl import build_ddl_payload
from target.notebook import build_notebook, notebook_workspace_path

from .aidp_fake import EmulatedAidp
from .snowflake_fake import demo_run_sql

__all__ = ["run_demo", "DEMO_STANDARD_CATALOG", "DEMO_EXTERNAL_CATALOG"]

DEMO_STANDARD_CATALOG = "snowdemo"
DEMO_EXTERNAL_CATALOG = "snowdemo_ext"

# Syntactically valid, obviously fake. The region short code must be mapped
# (iad), because coords.py refuses unknown regions even in a demo.
_DEMO_OCID = "ocid1.aidataplatform.oc1.iad.demo00000000000000000000"

# The EXTERNAL registration's connection map, with obviously-fake values.
# The KEY NAMES are the live-verified contract (the API enumerated them);
# in production the values come from a YAML/JSON file and the credential is
# a PATH read at call time.
_DEMO_CONNECTION = {
    "SNOWFLAKE_HOST": "demo-account.snowflakecomputing.example",
    "SNOWFLAKE_PORT": "443",
    "SNOWFLAKE_USERNAME": "MIGRATION_READER",
    "SNOWFLAKE_DATABASE_NAME": "SNOWDEMO",
    "SNOWFLAKE_WAREHOUSE": "WH_ETL",
    "SNOWFLAKE_AUTHENTICATION_METHOD": "KeyPair",
    "SNOWFLAKE_PRIVATE_KEY_CONTENT": "<emulated - not a key>",
}

_ZERO_DELAYS = dict(retry_delays=(), verify_delays=(0.0,), schema_wait=(0.0,))


def _write(out: pathlib.Path, name: str, payload) -> pathlib.Path:
    out.mkdir(parents=True, exist_ok=True)
    path = out / name
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _target(catalog: str):
    return resolve_target(datalake_ocid=_DEMO_OCID, workspace="ws-demo",
                          cluster_id="cluster-demo", catalog=catalog)


def run_demo(out_dir) -> dict:
    """Run every stage against the emulation and write the real artifacts.

    Returns {"narrative": [...], "lessons": [...], "out_dir": ...} for the CLI
    to print. Raises on any internal inconsistency — a demo that half-works
    teaches the wrong thing.
    """
    out = pathlib.Path(out_dir)
    narrative: list[str] = []
    lessons: list[str] = []

    def stage(line: str) -> None:
        narrative.append(line)

    _write(out, "emulation.json", {
        "emulated": True,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "note": "Every artifact in this directory came from the EMULATED "
                "estate SNOWDEMO and an EMULATED AIDP. Nothing here describes "
                "a real system."})

    # 1-4 · investigate (read-only against the emulated Snowflake) ----------
    inv = build_inventory(demo_run_sql, None, row_counts="metadata",
                          semi_structured="block", geospatial="block",
                          timestamp_ntz="timestamp")
    inv["census"] = build_census(demo_run_sql, inv["databases_in_scope"])
    _write(out, "inventory.json", inv)
    _write(out, "INVENTORY.md", render_inventory(inv))
    _write(out, "CENSUS.md", render_census(inv["census"]))
    stage(f'assess: {inv["object_count"]} object(s) in {DEMO_STANDARD_CATALOG.upper()}; '
          f'census found {inv["census"]["total"]} object(s) that cannot migrate '
          f'(a task, a stream, a procedure, a JavaScript UDF)')
    lessons.append(
        "TIMESTAMP_NTZ was downgraded to TIMESTAMP on purpose "
        "(`--timestamp-ntz timestamp`): the AIDP catalog API silently rejects "
        "timestamp_ntz, and the downgrade changes timezone semantics — a "
        "decision the operator makes, never a default.")
    lessons.append(
        "SALES.EVENTS_RAW is BLOCKED: VARIANT has no typed Delta mapping, and "
        "the engine refuses rather than guesses. `--semi-structured string` "
        "would carry it as text — a deferral, not a solution.")
    lessons.append(
        "TASK_LOAD_ORDERS populates SALES.ORDERS and does NOT migrate: after "
        "a real cutover the cloned table quietly stops being loaded. The "
        "census exists so this is discovered now, not in production.")

    deps = extract_dependencies(demo_run_sql, inv)
    _write(out, "dependencies.json", deps)
    stage(f'deps: lineage from {deps["source_used"]}, {len(deps["edges"])} edge(s)')

    maint = build_maintenance(demo_run_sql, inv)
    _write(out, "maintenance.json", maint)
    _write(out, "MAINTENANCE.md", render_maintenance(maint))
    stage(f'maintenance: {maint["objects_with_signals"]} of '
          f'{len(maint["tables"])} table(s) need a maintenance decision')
    lessons.append(
        "SALES.ORDERS fires every maintenance signal: a clustering key (AIDP "
        "reclusters nothing on its own), change tracking (Delta CDF must be "
        "enabled explicitly), a table-level retention override, and >1M "
        "rewritten rows in 30 days (needs an OPTIMIZE cadence).")

    sec = build_security(demo_run_sql, inv)
    _write(out, "security.json", sec)
    _write(out, "SECURITY.md", render_security(sec))
    stage(f'security: {sec["exposure_count"]} policy exposure(s), '
          f'{len(sec["secure_views"])} secure view(s)')
    lessons.append(
        "CUSTOMERS.EMAIL is masked in Snowflake and arrives UNMASKED: AIDP "
        "has no masking API, so the equivalent is a restricted view plus "
        "sensitivity classification — a design decision the report forces "
        "into the open.")

    warehouses = extract_warehouses(demo_run_sql)
    _write(out, "warehouses.json", warehouses)
    sizing = propose_all(warehouses["warehouses"], credit_price_usd=3.0)
    sizing["metering_source"] = warehouses["metering_source"]
    sizing["metering_note"] = warehouses["metering_note"]
    _write(out, "compute.json", sizing)
    _write(out, "COMPUTE_PROPOSAL.md", render_compute(sizing))
    stage(f'compute: {warehouses["warehouse_count"]} warehouse(s) sized '
          f'(WH_ETL, WH_BI) — clusters are proposed, never created silently')

    # 5-6 · decide (offline) -------------------------------------------------
    options = list(DATA_OPTIONS)
    _write(out, "data_options.json",
           {"options": options, "implemented": False,
            "note": "Proposal only. This plugin moves no bytes and implements "
                    "no transfer path."})
    _write(out, "DATA_MOVEMENT_OPTIONS.md", render_data_options(options))
    stage(f'data-options: {len(options)} architecture option(s) presented; '
          f'the demo leaves the choice UNDECIDED, exactly as a run does until '
          f'the customer answers')

    built = build_plan(inv, deps)
    built["census"] = inv["census"]
    _write(out, "plan.json", built)
    _write(out, "PLANNED_OBJECTS.md", render_planned_objects(built))
    s = built["summary"]
    stage(f'plan: {s["can_migrate"]} object(s) can migrate, '
          f'{s["cannot_migrate"]} cannot — each "cannot" carries its category '
          f'and reason')
    lessons.append(
        "TOP_CUSTOMERS_VW (QUALIFY) and CUSTOMER_360_VW (SECURE) cannot "
        "migrate mechanically. QUALIFY has no exact Spark rewrite and a "
        "secure view's guarantees do not survive — both are flagged for a "
        "human, never approximated.")

    ddl = build_ddl_payload(inv, built)
    _write(out, "ddl_plan.json", ddl)
    _write(out, "DDL_PLAN.md", render_ddl_plan(ddl))
    stage(f'ddl: {len(ddl["statements"])} statement(s) generated, '
          f'{len(ddl["blocked"])} blocked; ORDER_SUMMARY_VW had IFF and :: '
          f'rewritten by exact rules, with each rule named in the artifact')

    # 7 · prove the destination (emulated AIDP) ------------------------------
    aidp = EmulatedAidp(standard_catalog=DEMO_STANDARD_CATALOG)
    std_target = _target(DEMO_STANDARD_CATALOG)

    smoke = run_smoke(source_run_sql=demo_run_sql, target=std_target,
                      dest_call=aidp, write_probe=True)
    _write(out, "smoke.json", smoke)
    _write(out, "SMOKE_TEST.md", render_smoke(smoke))
    stage(f'smoke: {"PASS" if smoke["ok"] else "FAIL"} — the write probe '
          f'created one uniquely-named schema and removed it again')

    # 7b · provisioning, shown as the dry run it defaults to ----------------
    from target.provisioning import provision, render_provision
    scripts_dir = (pathlib.Path(__file__).resolve().parents[2]
                   / "data-migration-scripts")
    prov = provision(call=None,
                     workspace_name="SNOWDEMO account — Migração",
                     scripts=sorted(scripts_dir.glob("*.py")),
                     external_catalog=DEMO_EXTERNAL_CATALOG,
                     target_catalog=DEMO_STANDARD_CATALOG, execute=False)
    _write(out, "provision_result.json", prov)
    _write(out, "PROVISION.md", render_provision(prov))
    stage(f'provision (dry run): would ensure workspace '
          f'`{prov["workspace"]["name"]}` (name translated from '
          f'"{prov["workspace"]["requested"]}"), the migration_assets '
          f'cluster, the backup-snowflake-migration/ folder with '
          f'{len(list(scripts_dir.glob("*.py")))} script(s), and 4 jobs — '
          f'nothing created without --execute')
    lessons.append(
        "The workspace name was TRANSLATED before any create "
        "(accents/spaces/dashes → the simplest safe charset): a name the API "
        "might reject never reaches it, and the rename is reported, never "
        "silent.")

    # 8 · register the EXTERNAL catalog (the production default) ------------
    cat = ensure_catalog(display_name=DEMO_EXTERNAL_CATALOG, call=aidp,
                         connection=dict(_DEMO_CONNECTION),
                         description="EMULATED demo registration",
                         verify_delays=(0.0,))
    cat["dry_run"] = False
    cat["source_type"] = "SNOWFLAKE"
    _write(out, "catalog_result.json", cat)
    _write(out, "CATALOG.md", render_catalog(cat))
    stage(f'catalog: {DEMO_EXTERNAL_CATALOG} registered as EXTERNAL/SNOWFLAKE '
          f'and read back ({cat["action"]}) — a read-only pointer at the live '
          f'source that copies nothing')

    # 9 · the deploy guard, demonstrated -------------------------------------
    try:
        deploy_catalog(ddl, target=_target(DEMO_EXTERNAL_CATALOG),
                       execute=True, call=aidp, **_ZERO_DELAYS)
        raise AssertionError(
            "the emulation expected deploy to refuse an EXTERNAL catalog")
    except RefusedToExecute as exc:
        lessons.append(
            f"deploy --execute against the EXTERNAL catalog was REFUSED "
            f"before anything was created: “{str(exc)[:180]}…” "
            f"Managed Delta cannot live in a read-only pointer.")
        stage("deploy (EXTERNAL): refused before the first create — the "
              "expected, designed outcome")

    # 10 · structure clone into the STANDARD catalog -------------------------
    preflight = render_preflight(
        built,
        source={"account": inv["session"].get("A"),
                "region": inv["session"].get("R"),
                "role": inv["session"].get("ROLE"),
                "databases": inv.get("databases_in_scope")},
        target=dataclasses.asdict(std_target))
    _write(out, "PREFLIGHT.md", preflight)

    deployed = deploy_catalog(ddl, target=std_target, execute=True, call=aidp,
                              diagnose=True, **_ZERO_DELAYS)
    _write(out, "deploy_result.json", deployed)
    _write(out, "SOFT_CLONE_SUMMARY.md", render_soft_clone_summary(built, deployed))
    stage(f'deploy (STANDARD): verified {deployed["verified"]}/'
          f'{deployed["statement_count"]} by reading each object back; '
          f'{len(deployed["poisoned_names"])} name(s) diagnosed as burned, '
          f'{len(deployed["derived_type_drift_targets"])} view(s) with '
          f'derived-type drift')
    lessons.append(
        "LEGACY_AUDIT returned 202 Accepted and never appeared — the "
        "asynchronous create failed reporting NOTHING. The diagnosis then "
        "created a novel name in the same schema, which landed, proving the "
        "planned name is permanently burned (a live-verified AIDP behaviour): "
        "the only recovery is a fresh schema.")
    lessons.append(
        "ORDER_SUMMARY_VW was created, but the engine re-derived "
        "TOTAL_AMOUNT as decimal(28,2) against a declared decimal(30,2) — a "
        "NARROWING that can overflow. Reported as drift, distinct from a "
        "mismatch, because the view is ours; its types are not.")

    # 11-13 · notebook, summary, board ---------------------------------------
    notebook = build_notebook(ddl, built, catalog=DEMO_STANDARD_CATALOG,
                              source={"account": inv["session"].get("A"),
                                      "region": inv["session"].get("R")})
    nb_name = f"snowmig_shallow_clone_{DEMO_STANDARD_CATALOG}.ipynb"
    _write(out, nb_name, notebook)
    _write(out, "NOTEBOOK.md", "\n".join([
        f"# Shallow-clone notebook for `{DEMO_STANDARD_CATALOG}` (EMULATED demo)",
        "",
        f"Generated: `{out / nb_name}`",
        f"Intended AIDP path: `{notebook_workspace_path(DEMO_STANDARD_CATALOG)}`",
        "",
        "Not uploaded: this is the demo. In production the structure is "
        "created by `provision --execute` and then `run --job "
        "snowmig_01_structure`; `notebook --upload` is a dry run, and is "
        "refused with `--execute` (GAPS.md 13).",
        ""]))
    stage("notebook: the Standard-catalog path generated as a script that "
          "runs on AIDP compute, where Spark reports real errors — the "
          "control-plane API can return 202 and create nothing")

    _write(out, "SUMMARY.md",
           render_summary(built, inv, deployed, dataclasses.asdict(std_target)))
    board = build_stage_board(out)
    _write(out, "STAGES.md", render_stages(board))
    stage("summary + stages: the per-object roll-up and the board that says "
          "where the run stands")

    _write(out, "DEMO.md", _render_demo(narrative, lessons))
    return {"out_dir": str(out), "narrative": narrative, "lessons": lessons,
            "deployed": {"verified": deployed["verified"],
                         "poisoned": deployed["poisoned_names"],
                         "drift": deployed["derived_type_drift_targets"]}}


def _render_demo(narrative: list[str], lessons: list[str]) -> str:
    lines = [
        "# Dev-mode walkthrough — EVERYTHING HERE IS EMULATED",
        "",
        "> **No Snowflake account and no AIDP DataLake were contacted.** The",
        "> estate `SNOWDEMO` and the AIDP behind these artifacts are fakes",
        "> built into the plugin (`engine/emulation/`). The *code* that ran is",
        "> the production code — only the two transports were replaced — so",
        "> every artifact in this directory has exactly the shape a real run",
        "> produces.",
        "",
        "Dev mode exists to answer one question cheaply: *what does this",
        "migrator actually do, stage by stage, and what does it refuse to",
        "do?* Prod mode is the same pipeline with credentials, real",
        "coordinates, and `--execute` gates in front of every write.",
        "",
        "## What ran",
        "",
    ]
    lines += [f"{i}. {line}" for i, line in enumerate(narrative, 1)]
    lines += [
        "",
        "## The lessons this estate was built to teach",
        "",
    ]
    lines += [f"- {lesson}" for lesson in lessons]
    lines += [
        "",
        "## What changes in prod",
        "",
        "- Snowflake is reached read-only over a real connection; the "
        "transport still refuses every non-read verb, whatever the grant "
        "allows.",
        "- AIDP coordinates (DataLake OCID, workspace, cluster, catalog) are "
        "asked for per conversation, never stored or assumed.",
        "- `catalog` and `deploy` are dry runs unless `--execute` is passed, "
        "and nothing is reported as done until it is read back and compared.",
        "- The asynchronous-create, name-poisoning and type-drift behaviours "
        "demonstrated here are not inventions: each one was observed against "
        "a live DataLake first and is regression-tested.",
        "",
    ]
    return "\n".join(lines)
