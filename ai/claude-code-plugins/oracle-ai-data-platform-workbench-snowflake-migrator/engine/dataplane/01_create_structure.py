#!/usr/bin/env python3
"""Create target schemas and EMPTY Delta tables for one schema (or all).

Runs on AIDP compute. Three structure sources, chosen with --mode:

  ddl-plan (default)  types come from `plan/ddl_plan.json`, which the
                      migrator's own type mapper produced -- it refuses what
                      it cannot map exactly instead of guessing, and the plan
                      was reviewed and signed off before this ran. Costs NO
                      source read per table, which is what makes a large
                      estate feasible: creating 100 tables by CTAS took
                      minutes on a live run, because each CTAS is its own
                      Snowflake round trip.
  ctas                CREATE TABLE ... USING DELTA AS SELECT * ... WHERE 1=0.
                      Spark derives the types through the connector, so the
                      copy cannot hit a type the table cannot hold -- but the
                      mapping is the connector's, not an audited one, and it
                      pays a source read per table.
  manifest            types come from discovery_manifest.json verbatim. Only
                      valid when the manifest carries SPARK types, i.e. it
                      was built in external-catalog mode (DESCRIBE); a
                      connector-mode manifest carries SNOWFLAKE types and is
                      refused here rather than mistranslated.

Safety: CREATE TABLE IF NOT EXISTS everywhere; nothing is ever dropped or
replaced here. The target catalog must be INTERNAL — this script REFUSES to
address the source catalog as its target, and AIDP refuses DDL on external
catalogs anyway (documented), so the failure would be loud, not silent.

The CREATE returning is not the claim: IF NOT EXISTS is a silent no-op on a
table that is already there, so every table is DESCRIBEd afterwards and
compared with the plan, column by column and in order.

Writes `structure_report_<schema>.json` per schema, one status per table:
  created          it was not there before, and it reads back as planned
  already_existed  it was there before, and it matches the plan
  type_drift       it was there with a layout the plan did not produce; it
                   is left as found, listed with the differing columns, and
                   counted as a problem (exit 1) -- the copy is a positional
                   INSERT, so a mismatched layout would land rows in the
                   wrong columns with matching counts
  not_in_plan      the approved plan carries no columns for it; NOT created
  failed           the CREATE raised; the error is the reason
Resumable: `created` and `already_existed` are skipped on a re-run (--force
re-checks them); `type_drift`, `failed` and `not_in_plan` are looked at
again every run, so fixing the table or the plan is enough.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, load_source_config, q)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
MANIFEST_NAME = "discovery_manifest.json"


def three(*parts: str) -> str:
    return ".".join(q(p) for p in parts)


def log(msg: str) -> None:
    print(f"[structure] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _report_path(reports: pathlib.Path, schema: str) -> pathlib.Path:
    return reports / f"structure_report_{schema.lower()}.json"


def _load_report(path: pathlib.Path, schema: str, target: str) -> dict:
    """The prior report for this schema, but ONLY if it targeted the same place.

    Resumability is keyed by SOURCE schema, so a report written while
    targeting one destination would otherwise let a run against a DIFFERENT
    destination skip every create as "already created" -- observed live, with
    an empty target schema reported as done. A changed target starts a fresh
    record and says so.
    """
    if not path.exists():
        return {"schema": schema, "objects": {}, "target": target}
    prior = json.loads(path.read_text(encoding="utf-8"))
    if prior.get("target") and prior["target"] != target:
        log(f"{schema}: the previous report targeted {prior['target']}, not "
            f"{target} — starting a fresh record for this target (the old "
            f"one is kept at {path.name}.{prior['target'].replace('.', '_')})")
        path.with_suffix(
            f".{prior['target'].replace('.', '_')}.json").write_text(
                json.dumps(prior, indent=2), encoding="utf-8")
        return {"schema": schema, "objects": {}, "target": target}
    return prior


class TypeDrift(Exception):
    """The table was already there with a layout the plan did not produce."""


def _describe_columns(spark, fqn: str) -> list[tuple[str, str]] | None:
    """(name, type) pairs from DESCRIBE, or None when the table is not there.

    Columns end at the first blank or `#` row (Delta's metadata section).
    """
    try:
        rows = spark.sql(f"DESCRIBE {fqn}").collect()
    except Exception:
        return None
    out: list[tuple[str, str]] = []
    for row in rows:
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        out.append((name, str(row["data_type"] or "")))
    return out


def _norm_type(value: str) -> str:
    """Compare types ignoring case and internal spacing only."""
    return "".join(str(value).split()).upper()


def _compare_columns(expected: list[dict],
                     actual: list[tuple[str, str]]) -> str | None:
    """None if the structures match, else a one-line description of the diff.

    Same rule as the control-plane deploy: names and types, in order. A
    same-count layout in another order is a diff -- the copy is a positional
    INSERT, so that is the case that lands rows in the wrong columns with
    matching counts.
    """
    want = [(str(c.get("name", "")).upper(), _norm_type(c.get("type", "")))
            for c in expected]
    got = [(n.upper(), _norm_type(ty)) for n, ty in actual]
    if want == got:
        return None
    if len(want) != len(got):
        return (f"column count differs: planned {len(want)}, found {len(got)} "
                f"(planned {[n for n, _ in want]}, found {[n for n, _ in got]})")
    diffs = [f"position {i + 1}: planned {w[0]} {w[1]}, found {g[0]} {g[1]}"
             for i, (w, g) in enumerate(zip(want, got)) if w != g]
    return "; ".join(diffs)


def create_table_ctas(source: SnowflakeSource, schema: str, name: str,
                      target_catalog: str, target_schema: str) -> str:
    """Empty table whose columns Spark derives from the SOURCE read.

    The source is addressed through `SnowflakeSource`, so this works in
    connector mode (a temp view over the connector read) as well as against
    an external catalog's three-part name. Returns `created`, or
    `already_existed` when the table was there before this run -- CTAS has
    no plan to compare that layout with, so it is reported, not checked.
    """
    fqn = three(target_catalog, target_schema, name)
    if _describe_columns(source.spark, fqn) is not None:
        return "already_existed"
    view = f"snowmig_src_{schema}_{name}".lower()[:120]
    ref = source.register_temp_view(schema, name, view)
    try:
        source.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} "
            f"USING DELTA AS SELECT * FROM {ref} WHERE 1=0")
    finally:
        source.drop_temp_view(view)
    return "created"


def create_table_from_columns(spark, columns: list[dict],
                              target_catalog: str, target_schema: str,
                              name: str) -> str:
    """CREATE TABLE from an explicit column list, then READ IT BACK.

    Types are used verbatim. `CREATE TABLE IF NOT EXISTS` is a silent no-op
    on a table that is already there, so the CREATE returning is not the
    claim: the table is DESCRIBEd afterwards and compared with the plan.
    Returns `created` (it was not there before and now matches),
    `already_existed` (it was there and matches), or raises TypeDrift when
    what is there differs from the plan -- the table is left as found.
    """
    if not columns:
        raise ValueError("no column list for this table; rediscover it or "
                         "use --mode ctas")
    fqn = three(target_catalog, target_schema, name)
    before = _describe_columns(spark, fqn)
    if before is None:
        cols = ", ".join(f'{q(c["name"])} {c["type"]}' for c in columns)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({cols}) USING DELTA")
        after = _describe_columns(spark, fqn)
        if after is None:
            raise RuntimeError("CREATE TABLE returned but the table does not "
                               "DESCRIBE afterwards; NOT created")
    else:
        after = before
    diff = _compare_columns(columns, after)
    if diff is None:
        return "created" if before is None else "already_existed"
    if before is None:
        raise TypeDrift(f"created by this run, but it reads back differently "
                        f"from the plan -- {diff}")
    raise TypeDrift(f"already there with a layout the plan did not produce -- "
                    f"{diff}. CREATE TABLE IF NOT EXISTS left it as found; "
                    f"NOT created from the plan")


# Types Snowflake reports but Spark/Delta does not accept verbatim. Their
# presence in a manifest means it was built in CONNECTOR mode, where the
# manifest records SOURCE types on purpose -- translating them here would
# duplicate (and inevitably diverge from) the migrator's own type mapper,
# which refuses ambiguous cases rather than guessing.
_SNOWFLAKE_ONLY_TYPES = ("NUMBER", "TEXT", "VARIANT", "OBJECT", "GEOGRAPHY",
                         "GEOMETRY", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")


def _looks_like_snowflake_types(columns: list[dict]) -> bool:
    return any(str(c.get("type", "")).upper().startswith(t)
               for c in columns for t in _SNOWFLAKE_ONLY_TYPES)


def columns_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], list[dict]]:
    """{(source_schema, table): [{name, type}]} from the engine's ddl_plan.

    The engine translated these types with its full discipline (it blocks a
    table it cannot map exactly), and the plan was the artifact the user
    signed off, so applying it needs no source read and no cluster-side
    translation.
    """
    out: dict[tuple[str, str], list[dict]] = {}
    for stmt in ddl_plan.get("statements") or []:
        ident = str(stmt.get("source_identifier") or "")
        parts = ident.split(".")
        if len(parts) != 3 or not stmt.get("expected_columns"):
            continue
        out[(parts[1], parts[2])] = stmt["expected_columns"]
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="how to READ the source when --mode ctas derives "
                         "types from it (see snowmig_source.py)")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--schema", action="append", default=None,
                    help="repeatable; default: every schema in the manifest")
    ap.add_argument("--target-schema", default=None,
                    help="override the target schema name (single --schema "
                         "runs only); default mirrors the source")
    ap.add_argument("--mode", choices=("ddl-plan", "ctas", "manifest"),
                    default="ddl-plan")
    ap.add_argument("--ddl-plan",
                    help="path to ddl_plan.json (default: ../plan/"
                         "ddl_plan.json next to --reports-dir)")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--dry-run", action="store_true",
                    help="print every statement; execute nothing")
    ap.add_argument("--force", action="store_true",
                    help="re-check tables the report already records as "
                         "created or already_existed")
    args = ap.parse_args(argv)

    if args.source_catalog and \
            args.source_catalog.lower() == args.target_catalog.lower():
        return fail("error: source and target catalog are the same. The source is "
              "the read-only EXTERNAL catalog; the target must be an INTERNAL "
              "one.")
    if args.target_schema and len(args.schema or []) != 1:
        return fail("error: --target-schema needs exactly one --schema")

    reports = pathlib.Path(args.reports_dir)
    manifest = json.loads((reports / MANIFEST_NAME).read_text(encoding="utf-8"))
    by_name = {s["name"]: s for s in manifest["schemas"]}
    schemas = args.schema or sorted(by_name)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    planned_columns: dict = {}
    if args.mode == "ddl-plan":
        ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                    else reports.parent / "plan" / "ddl_plan.json")
        if not ddl_path.is_file():
            return fail(f"--mode ddl-plan needs ddl_plan.json; {ddl_path} is "
                        f"not there. Run the migrator's `ddl` stage and let "
                        f"`provision` upload it, or pass --ddl-plan")
        planned_columns = columns_from_ddl_plan(
            json.loads(ddl_path.read_text(encoding="utf-8")))
        log(f"ddl plan: {len(planned_columns)} table(s) with engine-"
            f"translated types, from {ddl_path}")

    source = None
    if args.mode == "ctas":
        try:
            config = (load_source_config(args.source_config)
                      if args.source_config else None)
            source = SnowflakeSource(spark, mode=args.source_mode,
                                     config=config,
                                     external_catalog=args.source_catalog)
        except SourceConfigError as exc:
            return fail(f"error: {exc}")

    failures = 0
    for schema in schemas:
        record = by_name.get(schema)
        if record is None:
            return fail(f"error: schema {schema!r} is not in the manifest; run "
                  f"00_discover first")
        target_schema = args.target_schema or schema
        path = _report_path(reports, schema)
        target = f"{args.target_catalog}.{target_schema}"
        report = _load_report(path, schema, target)
        report["target"] = target
        report["mode"] = args.mode

        schema_sql = (f"CREATE SCHEMA IF NOT EXISTS "
                      f"{q(args.target_catalog)}.{q(target_schema)}")
        if args.dry_run:
            log(f"DRY RUN: {schema_sql}")
        else:
            spark.sql(schema_sql)

        for table in record["tables"]:
            name = table["name"]
            prior = report["objects"].get(name, {})
            if prior.get("status") in ("created", "already_existed") \
                    and not args.force:
                log(f"skip {schema}.{name}: already {prior['status']}")
                continue
            try:
                if args.dry_run:
                    log(f"DRY RUN: would create "
                        f"{args.target_catalog}.{target_schema}.{name} "
                        f"({args.mode})")
                    status = "dry_run"
                elif args.mode == "ctas":
                    status = create_table_ctas(source, schema, name,
                                               args.target_catalog,
                                               target_schema)
                elif args.mode == "ddl-plan":
                    columns = planned_columns.get((schema, name))
                    if not columns:
                        report["objects"][name] = {
                            "status": "not_in_plan",
                            "reason": "the approved ddl_plan carries no "
                                      "columns for this table -- the engine "
                                      "either blocked it or it was outside "
                                      "the plan's scope. NOT created."}
                        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
                        log(f"{schema}.{name}: not in the approved plan")
                        continue
                    status = create_table_from_columns(spark, columns,
                                                       args.target_catalog,
                                                       target_schema, name)
                else:
                    columns = table.get("columns") or []
                    if _looks_like_snowflake_types(columns):
                        raise ValueError(
                            "this manifest carries SNOWFLAKE types (it was "
                            "built in connector mode), which Delta will not "
                            "accept verbatim. Use --mode ddl-plan (engine-"
                            "translated types) or --mode ctas.")
                    status = create_table_from_columns(spark, columns,
                                                       args.target_catalog,
                                                       target_schema, name)
                report["objects"][name] = {"status": status}
                log(f"{schema}.{name}: {status}")
            except TypeDrift as exc:
                # A problem state, not a failure of THIS run: the table is
                # there, it is not what the plan says, and a positional copy
                # into it would land rows in the wrong columns with matching
                # counts. Re-checked on every run until it matches.
                failures += 1
                report["objects"][name] = {"status": "type_drift",
                                           "reason": str(exc)[:400]}
                log(f"{schema}.{name}: TYPE DRIFT — {str(exc)[:200]}")
            except Exception as exc:
                failures += 1
                report["objects"][name] = {"status": "failed",
                                           "reason": str(exc)[:400]}
                log(f"{schema}.{name}: FAILED — {str(exc)[:200]}")
            report["updated_at"] = datetime.datetime.now(
                datetime.timezone.utc).isoformat()
            path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        counts = {}
        for obj in report["objects"].values():
            counts[obj["status"]] = counts.get(obj["status"], 0) + 1
        log(f"{schema}: {counts} -> {path}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
