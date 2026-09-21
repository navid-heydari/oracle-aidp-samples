#!/usr/bin/env python3
"""Copy ONE schema's tables from the external catalog into Delta, verified.

Runs on AIDP compute. Per table:

  1. read the source count;
  2. move the rows —
       skip-existing (default): only into a table with 0 rows;
       append:                  INSERT INTO ... SELECT *;
       overwrite:               INSERT OVERWRITE ... SELECT * (rewrites ROWS,
                                never drops the table);
  3. VERIFY: target count == source count (both read AFTER the copy), and
     with --verify counts+sums an exact SUM over every DECIMAL column,
     cast to DECIMAL(38,s) on both sides. Floats are never summed for
     equality — float tolerance is wrong for money.

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


def _count(spark, fqn: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]


def _decimal_columns(spark, fqn: str) -> list[tuple[str, int]]:
    """[(column, scale)] for every DECIMAL column of `fqn`."""
    out = []
    for row in spark.sql(f"DESCRIBE {fqn}").collect():
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        m = _DECIMAL.match(str(row["data_type"] or "").strip())
        if m:
            out.append((name, int(m.group(2))))
    return out


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

    target_rows = _count(spark, tgt)
    if mode == "skip-existing" and target_rows > 0:
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

    # The verification IS the claim.
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
        columns = _decimal_columns(spark, tgt)
        src_sums = _decimal_sums(spark, src, columns)
        tgt_sums = _decimal_sums(spark, tgt, columns)
        drift = {c: {"source": src_sums[c], "target": tgt_sums[c]}
                 for c, _ in columns if src_sums[c] != tgt_sums[c]}
        out["decimal_columns_checked"] = [c for c, _ in columns]
        if drift:
            out["status"] = "sum_mismatch"
            out["sum_drift"] = drift
            out["reason"] = (f"{len(drift)} decimal column(s) do not sum "
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
    ap.add_argument("--target-schema", default=None)
    ap.add_argument("--tables", nargs="*", default=None,
                    help="subset; default: every table the manifest lists")
    ap.add_argument("--mode", choices=("skip-existing", "append", "overwrite"),
                    default="skip-existing")
    ap.add_argument("--verify", choices=("counts", "counts+sums"),
                    default="counts")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-copy tables already recorded as verified")
    args = ap.parse_args(argv)

    if args.source_catalog and \
            args.source_catalog.lower() == args.target_catalog.lower():
        return fail("error: source and target catalog are the same.")

    reports = pathlib.Path(args.reports_dir)
    manifest = json.loads((reports / MANIFEST_NAME).read_text(encoding="utf-8"))
    record = next((s for s in manifest["schemas"] if s["name"] == args.schema),
                  None)
    if record is None:
        return fail(f"error: schema {args.schema!r} not in the manifest")

    target_schema = args.target_schema or args.schema
    path = reports / f"copy_report_{args.schema.lower()}.json"
    target = f"{args.target_catalog}.{target_schema}"
    report = {"schema": args.schema, "tables": {}, "target": target}
    if path.exists():
        prior = json.loads(path.read_text(encoding="utf-8"))
        # Resumability is keyed by SOURCE schema, so a report written against
        # a DIFFERENT target must not let this run skip copies as already
        # verified (the same trap the structure script hit live).
        if prior.get("target") and prior["target"] != target:
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
        structure_path = reports / f"structure_report_{args.schema.lower()}.json"
        created = []
        if structure_path.is_file():
            prior = json.loads(structure_path.read_text(encoding="utf-8"))
            if prior.get("target") in (None, target):
                created = [n for n, rec in (prior.get("objects") or {}).items()
                           if rec.get("status") in ("created", "already_existed")]
        if created:
            names = created
            log(f"scope: {len(names)} table(s) the structure step created for "
                f"{target} (the manifest lists "
                f'{len(record["tables"])} for this schema)')
        else:
            names = [t["name"] for t in record["tables"]]
            log(f"scope: all {len(names)} table(s) the manifest lists for "
                f"this schema; no structure report for {target} was found")

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
        result = copy_table(source, args.schema, name, tgt, mode=args.mode,
                            verify=args.verify,
                            source_count=source_counts.get(name))
        if result["status"] not in ("verified", "skipped_nonempty",
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
