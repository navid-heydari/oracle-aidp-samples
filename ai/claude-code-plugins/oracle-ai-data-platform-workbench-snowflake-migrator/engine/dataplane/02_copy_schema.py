#!/usr/bin/env python3
"""Copy ONE schema's tables from the external catalog into Delta, verified.

Runs on AIDP compute. Per table:

  1. read the source count;
  2. move the rows —
       skip-existing (default): only into a table with 0 rows; a table that
                                already holds rows is `skipped_nonempty`
                                when its count equals the source's and
                                `count_mismatch` when it does not -- a
                                re-run never softens a recorded failure;
       append:                  INSERT INTO ... SELECT *;
       overwrite:               INSERT OVERWRITE ... SELECT * (rewrites ROWS,
                                never drops the table);
  3. VERIFY: target count == source count (both read AFTER the copy), and
     with --verify counts+sums an exact SUM over every DECIMAL column OF
     THE SOURCE, cast to DECIMAL(38,s) with the SOURCE's scale on both
     sides. Floats are never summed for equality — float tolerance is
     wrong for money.

Before any row moves, and in both verify modes, the source's DECIMAL
columns are checked against the target's types: a target column that is not
DECIMAL, or a DECIMAL with fewer integer digits or a smaller scale, would be
rounded or truncated by the INSERT with the row count intact. That table is
recorded `type_drift` and NOT copied.

The copy's claim is the verification, not the INSERT returning: exactly the
discipline the control-plane deploy learned from live AIDP (a 2xx is not the
claim).

CONSISTENCY: each table is read at its own moment. If the source is still
being written, per-table counts can be exact and the SCHEMA still be
internally inconsistent. For a cutover: freeze writers or copy from a
point-in-time Snowflake CLONE. The report records copy timestamps so drift is
attributable.

Resumable: a table the report records as `verified` is skipped (--force
re-copies). Failures are recorded and the run continues; the report is the
deliverable.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, load_source_config)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
MANIFEST_NAME = "discovery_manifest.json"

_DECIMAL = re.compile(r"^decimal\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE)

# Copy statuses that mean the table is NOT verified. A later run that copies
# nothing (skip-existing over a table with rows) never softens one of these.
_COPY_FAILURES = ("count_mismatch", "sum_mismatch", "type_drift", "failed")

# Structure statuses that mean the table IS there. A copy that then cannot
# find it has not "nothing to do": it failed to copy into a created table.
_STRUCTURE_PRESENT = ("created", "already_existed")


def q(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def three(*parts: str) -> str:
    return ".".join(q(p) for p in parts)


def log(msg: str) -> None:
    print(f"[copy] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _same(a: str, b: str) -> bool:
    """Spark resolves catalog and schema names case-insensitively, so two
    targets that differ only in case are the same place. Comparing them as
    exact strings dropped the structure report for `lake.core` when this run
    spelled it `lake.CORE`, and the copy fell back to the whole manifest."""
    return str(a).casefold() == str(b).casefold()


def planned_target_schemas(ddl_plan: dict, schema: str) -> set[str]:
    """Every target schema the approved plan puts source schema `schema` in.

    The same reading as 01_create_structure's `targets_from_ddl_plan`: a
    TABLE statement with a three-part `source_identifier` and a three-part
    `target_fqn`. 01 creates the table where the plan says, so the copy has
    to look there too -- deriving the schema from `--schema` again copied
    into `lake.CORE` while 01 had created `lake.db_core`.
    """
    out: set[str] = set()
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if source[1] == schema:
            out.add(target[1])
    return out


def plan_catalogs(ddl_plan: dict) -> set[str]:
    """Every catalog the plan targets (01 refuses a run for another one)."""
    out = set()
    for stmt in ddl_plan.get("statements") or []:
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(target) == 3:
            out.add(target[0])
    return out


def resolve_target_schema(schema: str, planned: set[str],
                          override: str | None) -> tuple[str | None, str | None]:
    """`(target_schema, None)`, or `(None, refusal)`: the rule 01 applies.

    The approved plan decides; `--target-schema` may restate it (in any
    case) but not contradict it; only where the plan is silent does the
    source schema name stand in.
    """
    if override:
        if not planned:
            return override, None
        match = next((p for p in sorted(planned) if _same(p, override)), None)
        if match is None:
            return None, (
                f"error: --target-schema {override!r} contradicts the "
                f"approved plan, which puts {schema} in "
                f"{', '.join(sorted(planned))} -- where 01_create_structure "
                f"created it. The plan is the reviewed artifact; change it, "
                f"or drop the flag.")
        return match, None
    if len(planned) == 1:
        return next(iter(planned)), None
    if len(planned) > 1:
        return None, (
            f"error: the approved plan puts source schema {schema} in more "
            f"than one target schema ({', '.join(sorted(planned))}); this "
            f"stage copies one schema per run. Pass --target-schema to say "
            f"which.")
    return schema, None


def _count(spark, fqn: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]


def _column_types(spark, fqn: str) -> dict[str, str]:
    """{column: data_type} from DESCRIBE, in column order; lower-cased types.

    Columns end at the first blank or `#` row (Delta's metadata section).
    """
    out: dict[str, str] = {}
    for row in spark.sql(f"DESCRIBE {fqn}").collect():
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        out[name] = str(row["data_type"] or "").strip().lower()
    return out


def _decimal_columns(types: dict[str, str]) -> list[tuple[str, int, int]]:
    """[(column, precision, scale)] for every DECIMAL column in `types`."""
    out = []
    for name, data_type in types.items():
        m = _DECIMAL.match(data_type)
        if m:
            out.append((name, int(m.group(1)), int(m.group(2))))
    return out


def _type_drift(src_types: dict[str, str], tgt_types: dict[str, str]) -> dict:
    """Source DECIMAL columns the target cannot hold without silent loss.

    Keyed off the SOURCE: a source decimal whose target column is not a
    decimal, or a decimal with fewer integer digits (precision - scale) or a
    smaller scale, would be rounded, truncated or overflowed by the INSERT's
    store-assignment cast -- with the row count intact. A wider target is
    fine. Non-decimal columns are the structure stage's business.
    """
    by_lower = {k.lower(): v for k, v in tgt_types.items()}
    drift = {}
    for name, precision, scale in _decimal_columns(src_types):
        target = by_lower.get(name.lower())
        m = _DECIMAL.match(target or "")
        if not m or int(m.group(2)) < scale or \
                int(m.group(1)) - int(m.group(2)) < precision - scale:
            drift[name] = {"source": src_types[name],
                           "target": target or "<missing>"}
    return drift


def _decimal_sums(spark, fqn: str, columns: list[tuple[str, int]]) -> dict:
    if not columns:
        return {}
    selects = ", ".join(
        f"CAST(SUM(CAST({q(c)} AS DECIMAL(38,{s}))) AS STRING) AS {q(c)}"
        for c, s in columns)
    row = spark.sql(f"SELECT {selects} FROM {fqn}").collect()[0].asDict()
    return {k: row[k] for k in row}


def _target_exists(spark, tgt: str) -> bool:
    try:
        spark.sql(f"DESCRIBE {tgt}")
        return True
    except Exception:
        return False


def copy_table(source, schema: str, table: str, tgt: str, *, mode: str,
               verify: str, retries: int = 2, retry_wait: float = 30.0,
               source_count: int | None = None) -> dict:
    """Copy ONE table and verify it. The source is addressed through
    `SnowflakeSource`, so connector mode (a temp view over the connector
    read) and external-catalog mode (a three-part name) share this path."""
    spark = source.spark
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # A table with no target is a FINDING, not a crash. Live, the copy died
    # on the sixth table of a schema because the approved plan covered five
    # and the manifest listed a thousand -- taking the whole run with it.
    if not _target_exists(spark, tgt):
        return {"status": "target_missing", "started_at": started,
                "reason": f"{tgt} does not exist, so there is nothing to copy "
                          f"into. Most often the table is not in the approved "
                          f"plan (structure reports it `not_in_plan`); run "
                          f"01_create_structure for it first if it should be."}

    view = f"snowmig_src_{schema}_{table}".lower()[:120]
    src = source.register_temp_view(schema, table, view)
    try:
        return _copy(spark, src, tgt, mode=mode, verify=verify,
                     retries=retries, retry_wait=retry_wait, started=started,
                     source_count=source_count)
    finally:
        source.drop_temp_view(view)


def _copy(spark, src: str, tgt: str, *, mode: str, verify: str,
          retries: int, retry_wait: float, started: str,
          source_count: int | None = None) -> dict:
    # The batched count from the caller when there is one: a per-table
    # COUNT(*) opens its own Snowflake session in connector mode.
    if source_count is None:
        source_count = _count(spark, src)

    # Pre-flight, before anything is written and in both verify modes:
    # metadata only (DESCRIBE on the registered source and on the target).
    src_types = _column_types(spark, src)
    tgt_types = _column_types(spark, tgt)
    drift = _type_drift(src_types, tgt_types)
    if drift:
        return {"status": "type_drift", "type_drift": drift,
                "source_count": source_count, "started_at": started,
                "reason": f"{len(drift)} DECIMAL column(s) are narrower or "
                          f"not DECIMAL on the target; an INSERT would round "
                          f"or truncate them silently. NOT copied. Recreate "
                          f"the table from the approved plan."}

    target_rows = _count(spark, tgt)
    if mode == "skip-existing" and target_rows > 0:
        # A verification, not a bypass: a target that holds rows but not the
        # source's count is a mismatch whether or not this run wrote it.
        if target_rows != source_count:
            return {"status": "count_mismatch", "source_count": source_count,
                    "target_count": target_rows, "started_at": started,
                    "reason": f"target already holds {target_rows} row(s) "
                              f"but the source has {source_count}; nothing "
                              f"was copied. Use --mode overwrite to rewrite "
                              f"it. NOT verified."}
        return {"status": "skipped_nonempty", "source_count": source_count,
                "target_count": target_rows, "started_at": started,
                "reason": f"target already holds {target_rows} row(s); use "
                          f"--mode overwrite to rewrite them or append to add"}

    statement = (f"INSERT OVERWRITE {tgt} SELECT * FROM {src}"
                 if mode == "overwrite"
                 else f"INSERT INTO {tgt} SELECT * FROM {src}")
    last_error = None
    for attempt in range(retries + 1):
        try:
            spark.sql(statement)
            last_error = None
            break
        except Exception as exc:
            last_error = str(exc)[:400]
            if attempt < retries:
                log(f"  retry {attempt + 1}/{retries} after error: "
                    f"{last_error[:120]}")
                time.sleep(retry_wait)
    if last_error is not None:
        return {"status": "failed", "source_count": source_count,
                "started_at": started, "reason": last_error}

    out = {"started_at": started,
           "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "mode": mode}

    # The verification IS the claim. The rows have landed by now, so a
    # failure from here on must say so: a `failed` record that looked like a
    # failed INSERT would invite an `append` re-run that duplicates every row.
    try:
        return _verify(spark, src, tgt, out, verify=verify,
                       source_count=source_count, src_types=src_types)
    except Exception as exc:
        out.update(status="failed", insert_completed=True,
                   reason=f"the INSERT completed but the verification raised: "
                          f"{str(exc)[:300]}. NOT verified; re-copy with "
                          f"--mode overwrite, not append")
        return out


def _verify(spark, src: str, tgt: str, out: dict, *, verify: str,
            source_count: int, src_types: dict[str, str]) -> dict:
    src_after = _count(spark, src)
    tgt_after = _count(spark, tgt)
    out.update(source_count=src_after, target_count=tgt_after)
    if src_after != source_count:
        out["source_moved_during_copy"] = (
            f"source count changed {source_count} -> {src_after} during the "
            f"copy; the source is still being written")
    if tgt_after != src_after:
        out["status"] = "count_mismatch"
        out["reason"] = (f"target has {tgt_after} row(s), source has "
                         f"{src_after}. NOT verified.")
        return out

    if verify == "counts+sums":
        # The SOURCE's decimal columns, cast to the SOURCE's scale on both
        # sides: the target is at least as wide (the pre-flight in _copy
        # refused it otherwise), so the comparison is exact rather than
        # rounded to whatever the target happens to be.
        columns = [(c, s) for c, _p, s in _decimal_columns(src_types)]
        src_sums = _decimal_sums(spark, src, columns)
        tgt_sums = _decimal_sums(spark, tgt, columns)
        sum_drift = {c: {"source": src_sums[c], "target": tgt_sums[c]}
                     for c, _ in columns if src_sums[c] != tgt_sums[c]}
        out["decimal_columns_checked"] = [c for c, _ in columns]
        if not columns:
            out["decimal_columns_note"] = ("the source has no DECIMAL "
                                           "columns; counts are the whole check")
        if sum_drift:
            out["status"] = "sum_mismatch"
            out["sum_drift"] = sum_drift
            out["reason"] = (f"{len(sum_drift)} decimal column(s) do not sum "
                             f"equal. NOT verified.")
            return out

    out["status"] = "verified"
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="how to READ the source (see snowmig_source.py). "
                         "connector is the live-verified default and needs "
                         "no successful catalog crawl")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--schema", required=True,
                    help="ONE schema per run — that is the operating unit")
    ap.add_argument("--target-schema", default=None,
                    help="default: the schema the approved plan's "
                         "target_fqn names for --schema, as in "
                         "01_create_structure; --schema itself only where "
                         "the plan is silent. Refused when it contradicts "
                         "the plan")
    ap.add_argument("--ddl-plan",
                    help="path to ddl_plan.json (default: ../plan/"
                         "ddl_plan.json next to --reports-dir); read for "
                         "the target schema only")
    ap.add_argument("--tables", nargs="*", default=None,
                    help="subset; default: every table the manifest lists")
    ap.add_argument("--mode", choices=("skip-existing", "append", "overwrite"),
                    default="skip-existing")
    ap.add_argument("--verify", choices=("counts", "counts+sums"),
                    default="counts")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-copy tables already recorded as verified; needs "
                         "--mode overwrite or append, since skip-existing "
                         "cannot re-copy a table that holds rows")
    args = ap.parse_args(argv)

    if args.source_catalog and \
            args.source_catalog.lower() == args.target_catalog.lower():
        return fail("error: source and target catalog are the same.")
    if args.force and args.mode == "skip-existing":
        # Under the default mode --force copied nothing (skip-existing never
        # writes into a table with rows) and overwrote a `verified` record
        # with `skipped_nonempty`. Refuse rather than guess at overwrite.
        return fail("error: --force re-copies tables already recorded as "
                    "verified, which --mode skip-existing cannot do; pass "
                    "--mode overwrite (rewrites rows) or --mode append")

    reports = pathlib.Path(args.reports_dir)
    manifest = json.loads((reports / MANIFEST_NAME).read_text(encoding="utf-8"))
    record = next((s for s in manifest["schemas"] if s["name"] == args.schema),
                  None)
    if record is None:
        return fail(f"error: schema {args.schema!r} not in the manifest")

    # The approved plan decides the target namespace, exactly as it does for
    # 01_create_structure. A plan that is not there is a silent plan (01 in
    # --mode ctas or manifest never reads one); a plan the operator NAMED
    # and that is not there is a mistake worth stopping on.
    ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                else reports.parent / "plan" / "ddl_plan.json")
    planned: set[str] = set()
    if ddl_path.is_file():
        ddl_plan = json.loads(ddl_path.read_text(encoding="utf-8"))
        stray = {c for c in plan_catalogs(ddl_plan)
                 if not _same(c, args.target_catalog)}
        if stray:
            return fail(
                f"error: the approved plan targets catalog(s) "
                f"{', '.join(sorted(stray))}, and this run was given "
                f"--target-catalog {args.target_catalog}; "
                f"01_create_structure refuses that pair, so there is "
                f"nothing here it created. Point this run at the catalog "
                f"the plan names.")
        planned = planned_target_schemas(ddl_plan, args.schema)
    elif args.ddl_plan:
        return fail(f"error: --ddl-plan {ddl_path} is not there")
    target_schema, refusal = resolve_target_schema(
        args.schema, planned, args.target_schema)
    if refusal:
        return fail(refusal)
    log(f"target schema {target_schema}: "
        + ("from the approved plan" if planned else
           "from --target-schema" if args.target_schema else
           "the source schema's own name (the plan names none for it)"))

    path = reports / f"copy_report_{args.schema.lower()}.json"
    target = f"{args.target_catalog}.{target_schema}"

    # What the structure step recorded for THIS target. A report for another
    # target is not evidence about this one -- and falling back to the whole
    # manifest because of it is how a drifted table, excluded there, came
    # back into the copy's scope. Refused unless --tables names the scope.
    structure_path = reports / f"structure_report_{args.schema.lower()}.json"
    objects = None
    if structure_path.is_file():
        s_prior = json.loads(structure_path.read_text(encoding="utf-8"))
        s_target = s_prior.get("target")
        if s_target and not _same(s_target, target):
            if not args.tables:
                return fail(
                    f"error: the structure report for {args.schema} targets "
                    f"{s_target}, and this copy resolves {target}. Taking "
                    f"the scope from the manifest instead would copy into "
                    f"tables the structure step never created or checked "
                    f"there. Re-run 01_create_structure (it creates what "
                    f"the plan names), pass --target-schema "
                    f"{s_target.split('.', 1)[-1]} where the plan names no "
                    f"target for this schema, or pass --tables")
            log(f"the structure report targets {s_target}, not {target}; "
                f"not used for this run (--tables sets the scope)")
        else:
            objects = s_prior.get("objects") or {}

    report = {"schema": args.schema, "tables": {}, "target": target}
    if path.exists():
        prior = json.loads(path.read_text(encoding="utf-8"))
        # Resumability is keyed by SOURCE schema, so a report written against
        # a DIFFERENT target must not let this run skip copies as already
        # verified (the same trap the structure script hit live). Compared
        # case-insensitively: `lake.CORE` and `lake.core` are one schema.
        if prior.get("target") and not _same(prior["target"], target):
            log(f"the previous report targeted {prior['target']}, not "
                f"{target} — starting a fresh record for this target")
            path.with_suffix(
                f".{prior['target'].replace('.', '_')}.json").write_text(
                    json.dumps(prior, indent=2), encoding="utf-8")
        else:
            report = prior
            report["target"] = target

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    try:
        config = (load_source_config(args.source_config)
                  if args.source_config else None)
        source = SnowflakeSource(spark, mode=args.source_mode, config=config,
                                 external_catalog=args.source_catalog)
    except SourceConfigError as exc:
        return fail(f"error: {exc}")
    report["source"] = source.describe()

    if args.tables:
        names = list(args.tables)
    else:
        # Default to what the structure step created for THIS target, when it
        # left a report: the manifest is the whole estate, and copying into
        # tables nobody approved is not a default worth having. A table it
        # found already there WITH the planned layout counts; one it recorded
        # as `type_drift` never does -- the copy below is a positional INSERT
        # INTO ... SELECT *, and that layout is not the plan's.
        created = [n for n, rec in (objects or {}).items()
                   if rec.get("status") in _STRUCTURE_PRESENT]
        if created:
            names = created
            log(f"scope: {len(names)} table(s) the structure step created for "
                f"{target} (the manifest lists "
                f'{len(record["tables"])} for this schema)')
        else:
            # The `created` filter above never runs when nothing was created,
            # and that is exactly the all-drift schema: a re-plan over tables
            # that all pre-exist with the old layout records every one of
            # them `type_drift`. They are excluded here too, or the fallback
            # copies into the very layout the structure step refused.
            drifted = {n for n, r in (objects or {}).items()
                       if r.get("status") == "type_drift"}
            names = [t["name"] for t in record["tables"]
                     if t["name"] not in drifted]
            if objects is None:
                why = f"no structure report for {target} was found"
            else:
                # The report IS there; saying it was not pointed the operator
                # away from the real cause (a plan that never covered this
                # schema). Tables it created nothing for will come back
                # `target_missing` below.
                nip = sum(1 for r in objects.values()
                          if r.get("status") == "not_in_plan")
                why = (f"the structure report for {target} records 0 created "
                       f"table(s) ({nip} not_in_plan, {len(drifted)} "
                       f"type_drift, {len(objects) - nip - len(drifted)} "
                       f"other) -- re-run 01_create_structure with the right "
                       f"ddl_plan.json, or pass --tables")
                if drifted:
                    why += (f"; {len(drifted)} type_drift table(s) excluded "
                            f"-- recreate them from the approved plan")
                if drifted and not names:
                    # Zero iterations below would be exit 0: a copy job that
                    # did nothing, reported as a success.
                    return fail(
                        f"error: every table the manifest lists for "
                        f"{args.schema} is recorded type_drift in the "
                        f"structure report for {target}; nothing to copy. "
                        f"Recreate them from the approved plan "
                        f"(01_create_structure) or pass --tables to override")
            log(f"scope: all {len(names)} table(s) the manifest lists for "
                f"this schema; {why}")

    todo = [n for n in names
            if args.force
            or report["tables"].get(n, {}).get("status") != "verified"]

    # ONE round trip for every source count in this schema, rather than one
    # per table: session setup dominated the copy on a live run.
    source_counts: dict = {}
    if todo and not args.dry_run:
        try:
            source_counts = source.source_counts(args.schema, todo)
            log(f"source counts for {len(source_counts)} table(s) in "
                f"{(len(todo) + 49) // 50} round trip(s)")
        except Exception as exc:
            log(f"batched source counts unavailable ({str(exc)[:120]}); "
                f"falling back to one count per table")

    failures = 0
    for name in names:
        prior = report["tables"].get(name, {})
        if prior.get("status") == "verified" and not args.force:
            log(f"skip {args.schema}.{name}: already verified")
            continue
        tgt = three(args.target_catalog, target_schema, name)
        if args.dry_run:
            log(f"DRY RUN: would copy {args.schema}.{name} -> {tgt} "
                f"({args.mode}, verify={args.verify}, "
                f"source={args.source_mode})")
            continue
        log(f"{args.schema}.{name}: copying ({args.mode})")
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            result = copy_table(source, args.schema, name, tgt, mode=args.mode,
                                verify=args.verify,
                                source_count=source_counts.get(name))
        except Exception as exc:
            # A failure is a finding, not the end of the run: live, one
            # connector login timeout would otherwise end the schema with
            # the failing table unrecorded and the rest never attempted.
            result = {"status": "failed", "started_at": started,
                      "reason": str(exc)[:400]}
            log(f"{args.schema}.{name}: FAILED — {str(exc)[:200]}")
        if result["status"] == "skipped_nonempty" and \
                prior.get("status") in _COPY_FAILURES:
            # Nothing was copied, so nothing was re-verified: a recorded
            # failure stands until a real re-copy verifies the table.
            result = dict(prior, reason=(
                f"{prior.get('reason') or prior['status']} (a re-run in "
                f"skip-existing mode left the target untouched; use --mode "
                f"overwrite to re-copy and re-verify it)"))
        # `target_missing` is a finding for a table the structure step never
        # created (not in the plan). For one it records as there, the copy
        # moved nothing into a table that should exist: a failure, or the
        # job reads SUCCESS with 0 rows copied.
        s_status = (objects or {}).get(name, {}).get("status")
        if result["status"] == "target_missing" and \
                s_status in _STRUCTURE_PRESENT:
            result["reason"] = (
                f"the structure report records this table `{s_status}` in "
                f"{target}, yet {tgt} is not there now -- dropped since, or "
                f"created somewhere else. NOT copied.")
            failures += 1
        elif result["status"] not in ("verified", "skipped_nonempty",
                                      "target_missing"):
            failures += 1
        report["tables"][name] = result
        report["updated_at"] = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        log(f"{args.schema}.{name}: {result['status']} "
            f"({result.get('target_count', '?')} row(s))")

    statuses = {}
    for t in report["tables"].values():
        statuses[t["status"]] = statuses.get(t["status"], 0) + 1
    log(f"{args.schema}: {statuses} -> {path}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
