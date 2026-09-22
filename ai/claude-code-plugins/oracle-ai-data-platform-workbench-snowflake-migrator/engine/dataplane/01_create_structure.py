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

In --mode ddl-plan the plan's `NOT NULL`, column COMMENTs and table COMMENT
are applied, not just its names and types: they are in the CREATE TABLE the
reviewer approved, and this stage used to render `name type` and then compare
against the same reduced shape, so a table that differed from the approved
SQL still read back as matching. Nullability is read from the table's schema
(DESCRIBE does not report it); when that read fails the table's record says
the property is UNCHECKED rather than counting it as applied.

Writes `structure_report_<schema>.json` per schema, one status per table:
  created          it was not there before, and it reads back as planned
  already_existed  it was there before, and it matches the plan (in
                   --mode ctas there is no plan: the layout is NOT
                   compared, and the record's reason says so)
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


def _describe_comments(spark, fqn: str) -> dict[str, str] | None:
    """{column: comment} from DESCRIBE's third column, upper-cased keys.

    None when the read-back carries no comment column at all -- a comment
    that was NOT LOOKED AT must not read as a comment that is missing.
    DESCRIBE reports comments; it does NOT report nullability, which is why
    that is read from the table's schema instead.
    """
    try:
        rows = spark.sql(f"DESCRIBE {fqn}").collect()
    except Exception:
        return None
    out: dict[str, str] = {}
    carried = False
    for row in rows:
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        try:
            value = row["comment"]
            carried = True
        except Exception:
            value = None
        out[name.upper()] = str(value or "")
    return out if carried else None


def _nullability(spark, fqn: str) -> dict[str, bool] | None:
    """{column: nullable} from the table's schema, or None when unreadable.

    DESCRIBE has no nullability column, so the read-back for `NOT NULL` is
    the StructType. None means NOT CHECKED -- reported as such rather than
    passed off as a match, because "we did not look" and "it is right" are
    the two answers this whole stage exists to keep apart.
    """
    try:
        fields = spark.table(fqn).schema.fields
        return {str(f.name).upper(): bool(f.nullable) for f in fields}
    except Exception:
        return None


def _norm_type(value: str) -> str:
    """Compare types ignoring case and internal spacing only."""
    return "".join(str(value).split()).upper()


def _compare_columns(expected: list[dict],
                     actual: list[tuple[str, str]],
                     nullable: dict[str, bool] | None = None,
                     comments: dict[str, str] | None = None) -> str | None:
    """None if the structures match, else a one-line description of the diff.

    Same rule as the control-plane deploy: names and types, in order. A
    same-count layout in another order is a diff -- the copy is a positional
    INSERT, so that is the case that lands rows in the wrong columns with
    matching counts.

    `NOT NULL` and column COMMENTs are part of the approved DDL, so they are
    compared too when the read-back supplies them: a table created with the
    reviewed SQL and reported "verified" against a name-and-type-only
    comparison is how the reviewed artifact and the applied one came apart in
    the first place. `nullable=None` means the schema could not be read; the
    caller says so rather than counting it as a match.
    """
    want = [(str(c.get("name", "")).upper(), _norm_type(c.get("type", "")))
            for c in expected]
    got = [(n.upper(), _norm_type(ty)) for n, ty in actual]
    if want != got:
        if len(want) != len(got):
            return (f"column count differs: planned {len(want)}, found {len(got)} "
                    f"(planned {[n for n, _ in want]}, found {[n for n, _ in got]})")
        diffs = [f"position {i + 1}: planned {w[0]} {w[1]}, found {g[0]} {g[1]}"
                 for i, (w, g) in enumerate(zip(want, got)) if w != g]
        return "; ".join(diffs)

    property_diffs: list[str] = []
    for col in expected:
        name = str(col.get("name", "")).upper()
        if nullable is not None and col.get("nullable") is False \
                and nullable.get(name, True):
            property_diffs.append(
                f"{name}: planned NOT NULL, found nullable")
        if comments is not None:
            planned = str(col.get("description") or "")
            found = str(comments.get(name) or "")
            if planned and planned != found:
                property_diffs.append(
                    f"{name}: planned comment {planned!r}, found {found!r}")
    return "; ".join(property_diffs) or None


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


def lit(value: str) -> str:
    """A single-quoted Spark string literal, escaped the way Spark expects.

    Backslash, not doubling: Spark reads `'it\\'\\'s'` as two adjacent
    literals and concatenates them, so a doubled quote silently eats the
    apostrophe. Identical to `target.ddl.quote_spark_string`, which this
    stage cannot import (it is uploaded as a single standalone file).
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


def _column_sql(col: dict) -> str:
    """One column of the CREATE TABLE, from one `expected_columns` entry.

    The SAME rules as `target.ddl.render_column_sql`, which wrote the SQL the
    operator approved in DDL_PLAN.md -- a parity test in the engine's suite
    holds the two together. This stage used to render `name type` only, so
    the `NOT NULL` and the COMMENT in the approved SQL were dropped here and
    the comparison below then agreed with itself.
    """
    piece = f'{q(col["name"])} {col["type"]}'
    if col.get("nullable") is False:
        piece += " NOT NULL"
    if col.get("description"):
        piece += " COMMENT " + lit(col["description"])
    return piece


def create_table_from_columns(spark, columns: list[dict],
                              target_catalog: str, target_schema: str,
                              name: str, description: str = "",
                              notes: list | None = None) -> str:
    """CREATE TABLE from an explicit column list, then READ IT BACK.

    Types are used verbatim, and so are the plan's `nullable` and
    `description` -- the properties the approved SQL shows. `CREATE TABLE IF
    NOT EXISTS` is a silent no-op on a table that is already there, so the
    CREATE returning is not the claim: the table is DESCRIBEd afterwards and
    compared with the plan, nullability included.
    Returns `created` (it was not there before and now matches),
    `already_existed` (it was there and matches), or raises TypeDrift when
    what is there differs from the plan -- the table is left as found.
    `notes` collects what could NOT be checked, so an unverified property is
    never reported as a verified one.
    """
    if not columns:
        raise ValueError("no column list for this table; rediscover it or "
                         "use --mode ctas")
    fqn = three(target_catalog, target_schema, name)
    before = _describe_columns(spark, fqn)
    if before is None:
        cols = ", ".join(_column_sql(c) for c in columns)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({cols}) USING DELTA"
                  + (f" COMMENT {lit(description)}" if description else ""))
        after = _describe_columns(spark, fqn)
        if after is None:
            raise RuntimeError("CREATE TABLE returned but the table does not "
                               "DESCRIBE afterwards; NOT created")
    else:
        after = before

    # Both extra read-backs are skipped when the plan asks for nothing they
    # would check: a table with no NOT NULL and no comments costs exactly
    # what it cost before.
    wants_not_null = any(c.get("nullable") is False for c in columns)
    wants_comments = any(c.get("description") for c in columns)
    nullable = _nullability(spark, fqn) if wants_not_null else None
    comments = _describe_comments(spark, fqn) if wants_comments else None
    # Not a match and not a failure: it was not looked at. Said out loud,
    # because a property reported as applied when nobody checked is the
    # defect this stage is guarding against.
    if notes is not None:
        if wants_not_null and nullable is None:
            notes.append(
                "NOT NULL was requested but could not be verified: the "
                "table's schema could not be read back, so nullability is "
                "UNCHECKED on this table")
        if wants_comments and comments is None:
            notes.append(
                "column COMMENTs were requested but DESCRIBE carried no "
                "comment column, so they are UNCHECKED on this table")
    diff = _compare_columns(columns, after, nullable, comments)
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
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue                    # views are not created by this path
        out[(parts[1], parts[2])] = stmt["expected_columns"]
    return out


def descriptions_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], str]:
    """{(source_schema, table): table COMMENT} from the engine's ddl_plan.

    The source table's COMMENT is in the approved CREATE TABLE, so it is
    applied here too rather than being the one property the reviewer sees
    and the target never gets.
    """
    out: dict[tuple[str, str], str] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) != 3 or not stmt.get("description"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out[(parts[1], parts[2])] = str(stmt["description"])
    return out


def views_from_ddl_plan(ddl_plan: dict) -> set[tuple[str, str]]:
    """{(source_schema, view)} for every VIEW statement in the plan."""
    out: set[tuple[str, str]] = set()
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) == 3 and str(stmt.get("object_type") or "").upper() == "VIEW":
            out.add((parts[1], parts[2]))
    return out


# This stage creates TABLES. The plan's CREATE VIEW statements are qualified
# with the plan-time target and their bodies would need re-qualifying for a
# run-time --target-catalog, so views are the catalog path's job (`snowmig
# deploy --execute`). They are still LISTED here, so a view the plan promised
# can never be absent from every report with exit 0.
VIEW_NOT_CREATED = ("views are not created by this stage (tables only); "
                    "create them with `snowmig deploy --execute` and verify "
                    "them against the source")


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
    planned_descriptions: dict = {}
    planned_views: set | None = None
    if args.mode == "ddl-plan":
        ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                    else reports.parent / "plan" / "ddl_plan.json")
        if not ddl_path.is_file():
            return fail(f"--mode ddl-plan needs ddl_plan.json; {ddl_path} is "
                        f"not there. Run the migrator's `ddl` stage and let "
                        f"`provision` upload it, or pass --ddl-plan")
        ddl_plan = json.loads(ddl_path.read_text(encoding="utf-8"))
        planned_columns = columns_from_ddl_plan(ddl_plan)
        planned_descriptions = descriptions_from_ddl_plan(ddl_plan)
        planned_views = views_from_ddl_plan(ddl_plan)
        log(f"ddl plan: {len(planned_columns)} table(s) with engine-"
            f"translated types, from {ddl_path}"
            + (f"; {len(planned_views)} view(s) it carries are NOT created "
               f"by this stage" if planned_views else ""))

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
    # Run-wide, not per schema: a canary plan scoped to one schema
    # legitimately leaves every other schema all-`not_in_plan`.
    created_total = 0
    not_in_plan_total = 0
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
            notes: list[str] = []
            prior = report["objects"].get(name, {})
            if prior.get("status") in ("created", "already_existed") \
                    and not args.force:
                log(f"skip {schema}.{name}: already {prior['status']}")
                created_total += 1
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
                        not_in_plan_total += 1
                        continue
                    status = create_table_from_columns(
                        spark, columns, args.target_catalog, target_schema,
                        name,
                        description=planned_descriptions.get((schema, name),
                                                             ""),
                        notes=notes)
                else:
                    columns = table.get("columns") or []
                    if _looks_like_snowflake_types(columns):
                        raise ValueError(
                            "this manifest carries SNOWFLAKE types (it was "
                            "built in connector mode), which Delta will not "
                            "accept verbatim. Use --mode ddl-plan (engine-"
                            "translated types) or --mode ctas.")
                    status = create_table_from_columns(
                        spark, columns, args.target_catalog, target_schema,
                        name, notes=notes)
                report["objects"][name] = {"status": status}
                if notes:
                    # Properties that could NOT be read back. Recorded next
                    # to the status so "created" never implies "and every
                    # property was checked".
                    report["objects"][name]["unverified_properties"] = notes
                if args.mode == "ctas" and status == "already_existed":
                    # CTAS has no plan to compare the layout with: the
                    # table was there before this run and nobody has
                    # checked it. Said here, so the copy scope and the
                    # reconcile report carry it rather than a bare pass.
                    report["objects"][name]["reason"] = (
                        "there before this run; --mode ctas has no plan "
                        "to compare its layout with, so the layout was "
                        "NOT compared")
                log(f"{schema}.{name}: {status}")
                if status in ("created", "already_existed"):
                    created_total += 1
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

        # Views: listed, never created here. Kept apart from `objects` so
        # the table tally, the resume logic and the copy scope stay table-only.
        views = [v["name"] for v in record.get("views") or []]
        if views:
            report["views"] = {}
            for view in views:
                entry = {"status": "not_created_by_this_path",
                         "reason": VIEW_NOT_CREATED}
                if planned_views is not None:
                    entry["in_plan"] = (schema, view) in planned_views
                report["views"][view] = entry
            path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            log(f"{schema}: {len(views)} view(s) in the manifest are NOT "
                f"created by this stage ({', '.join(views[:5])}"
                f"{', ...' if len(views) > 5 else ''}); use `snowmig deploy "
                f"--execute` for views")

        counts = {}
        for obj in report["objects"].values():
            counts[obj["status"]] = counts.get(obj["status"], 0) + 1
        log(f"{schema}: {counts} -> {path}")

    log(f"run: created or already there {created_total}, not in plan "
        f"{not_in_plan_total}, failed or drifted {failures}")
    if failures:
        return 1
    if args.mode == "ddl-plan" and not args.dry_run and not created_total \
            and not_in_plan_total:
        # Every per-table record above is right; the RUN still did nothing.
        # Exit 0 here gave three SUCCESS jobs (structure, copy, reconcile)
        # for a plan that never overlapped the requested schema.
        return fail(f"error: created 0 table(s); {not_in_plan_total} were not "
                    f"in the approved plan ({ddl_path}). The plan and the "
                    f"requested schema(s) do not overlap -- is this the "
                    f"ddl_plan.json for THIS estate and wave? Nothing was "
                    f"created, so 02_copy_schema has nothing to copy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
