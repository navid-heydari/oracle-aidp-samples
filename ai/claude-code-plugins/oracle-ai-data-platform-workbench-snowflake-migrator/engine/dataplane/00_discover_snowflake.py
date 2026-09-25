#!/usr/bin/env python3
"""Discover the Snowflake estate from inside AIDP. Writes discovery_manifest.json.

USE `connector`. Discovering schemas and tables by walking three-part names
against an EXTERNAL catalog is the slow path and should not be the starting
point -- see the table below before reaching for it.

  connector (default)   ONE pushdown query pair against INFORMATION_SCHEMA
                        returns every schema, table and column in the
                        database. This is the mode that scales: a
                        200k-table estate costs a handful of queries, not a
                        DESCRIBE per object. It needs only the credentials
                        smoke already proved -- no catalog crawl.

  external-catalog      SHOW SCHEMAS/TABLES + DESCRIBE per object against a
                        registered EXTERNAL catalog. Its cost grows with the
                        object count, so it does not finish at estate scale,
                        and it sees only what the crawler already discovered
                        -- one more precondition that has to be true first.
                        Use it only when a user explicitly asks.

The estate is READ-ONLY under both modes: the AIDP Snowflake connector is
read-only in 4.0, and an external catalog refuses DDL by contract. Nothing is
written anywhere except --reports-dir.

External-catalog mode is resumable per schema (each is flushed immediately);
connector mode returns the estate whole, so there is nothing to resume.

`--schemas` is pushed into the INFORMATION_SCHEMA queries as a predicate, not
applied after the fetch, and a scoped run MERGES into the manifest: the named
schemas are refreshed and every other schema is kept. `--force` alone (no
`--schemas`) re-discovers the whole estate from an empty manifest.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
import traceback

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, _sql_literal,
    load_source_config, q)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
MANIFEST_NAME = "discovery_manifest.json"

# Snowflake's own system schema: never a migration target.
_SYSTEM_SCHEMAS = {"information_schema"}

# `{where}` is the --schemas predicate, or nothing.
_TABLES_SQL = (
    "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES "
    "from INFORMATION_SCHEMA.TABLES{where} order by TABLE_SCHEMA, TABLE_NAME")

# COLUMN_DEFAULT, IDENTITY_* and COMMENT are here because the planning
# stages warn on them (R22, R23) and carry the comment into the CREATE
# TABLE. Without them a manifest-planned estate gets a DDL plan that is
# silent about defaults and identity columns that stop working at cutover,
# while the same estate planned from a laptop warns about both.
_COLUMNS_SQL = (
    "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
    "DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, "
    "CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, IDENTITY_START, "
    "IDENTITY_INCREMENT, COMMENT from INFORMATION_SCHEMA.COLUMNS{where} "
    "order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION")


def _schema_predicate(wanted: list[str] | None) -> str:
    """` where TABLE_SCHEMA in (...)` for --schemas, or an empty string.

    Pushed into the query rather than applied after the fetch. An unfiltered
    read over a large database's INFORMATION_SCHEMA can exceed Snowflake's
    result cap ("Information schema query returned too much data"), and a
    --schemas scope that only filtered client-side could do nothing about
    it. TABLE_SCHEMA is compared as a string value, so the names are
    literals, not identifiers.
    """
    if not wanted:
        return ""
    names = ", ".join(f"'{_sql_literal(s)}'" for s in wanted)
    return f" where TABLE_SCHEMA in ({names})"


def log(msg: str) -> None:
    print(f"[discover] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _plain(value):
    """JSON-safe scalar.

    Snowflake numerics arrive as `decimal.Decimal` through the connector and
    `json.dumps` refuses them ("Object of type Decimal is not JSON
    serializable") -- which happened AFTER a successful 1065-relation read,
    so the whole discovery was lost at the write. Integral values keep their
    exactness as ints; everything else becomes a string rather than a lossy
    float.
    """
    if value is None or isinstance(value, (int, str, bool)):
        return value
    try:
        as_int = int(value)
    except (TypeError, ValueError):
        return str(value)
    return as_int if as_int == value else str(value)


def _rows(df) -> list[dict]:
    return [r.asDict() for r in df.collect()]


def _first_key(row: dict, *candidates: str):
    """SHOW column names vary by engine version; take the first match rather
    than assuming one, and fail loudly when none is there."""
    for key in candidates:
        if key in row:
            return row[key]
    lowered = {k.lower(): v for k, v in row.items()}
    for key in candidates:
        if key.lower() in lowered:
            return lowered[key.lower()]
    raise KeyError(f"none of {candidates} in row: {sorted(row)}")


def _snowflake_type(col: dict) -> str:
    """The SOURCE type as Snowflake reports it, precision preserved.

    Deliberately NOT translated: translation belongs to the engine's type
    mapper, which refuses rather than guesses. This manifest records what IS.
    """
    base = str(col.get("DATA_TYPE") or "").upper()
    precision = _plain(col.get("NUMERIC_PRECISION"))
    scale = _plain(col.get("NUMERIC_SCALE"))
    length = _plain(col.get("CHARACTER_MAXIMUM_LENGTH"))
    if base in ("NUMBER", "DECIMAL", "NUMERIC") and precision is not None:
        return f"{base}({int(precision)},{int(scale or 0)})"
    if base in ("TEXT", "VARCHAR", "CHAR", "STRING") and length is not None:
        return f"{base}({int(length)})"
    return base


def discover_via_connector(source: SnowflakeSource, *,
                           wanted: list[str] | None,
                           exclude: set[str]) -> list[dict]:
    """Every schema/table/column of the database (or of `wanted`), in two
    queries."""
    # INFORMATION_SCHEMA is per-database. The connector's `schema` option
    # must name a REAL schema (it rejects INFORMATION_SCHEMA itself with
    # DATA_ACCESS_LAYER_0031); the SQL below then reads INFORMATION_SCHEMA
    # relative to the database. Both established live.
    where = _schema_predicate(wanted)
    tables = _rows(source.pushdown(_TABLES_SQL.format(where=where)))
    columns = _rows(source.pushdown(_COLUMNS_SQL.format(where=where)))
    log(f"INFORMATION_SCHEMA: {len(tables)} relation(s), "
        f"{len(columns)} column(s), in 2 queries"
        + (f" scoped to {len(wanted)} schema(s)" if wanted else ""))

    by_object: dict[tuple[str, str], list[dict]] = {}
    for col in columns:
        key = (str(col["TABLE_SCHEMA"]), str(col["TABLE_NAME"]))
        by_object.setdefault(key, []).append(
            # The formatted `type` is for humans reading the manifest. The
            # four raw INFORMATION_SCHEMA fields below are what the migrator's
            # type mapper consumes, and they are carried verbatim: re-parsing
            # "NUMBER(38,0)" back into precision and scale would be a second,
            # lossier implementation of something we already have exactly.
            {"name": str(col["COLUMN_NAME"]),
             "type": _snowflake_type(col),
             "nullable": str(col.get("IS_NULLABLE") or "").upper() != "NO",
             "data_type": _plain(col.get("DATA_TYPE")),
             "numeric_precision": _plain(col.get("NUMERIC_PRECISION")),
             "numeric_scale": _plain(col.get("NUMERIC_SCALE")),
             "character_maximum_length": _plain(
                 col.get("CHARACTER_MAXIMUM_LENGTH"))})

    schemas: dict[str, dict] = {}
    for rel in tables:
        schema = str(rel["TABLE_SCHEMA"])
        if schema.lower() in exclude or (wanted and schema not in wanted):
            continue
        name = str(rel["TABLE_NAME"])
        kind = str(rel.get("TABLE_TYPE") or "BASE TABLE").upper()
        record = schemas.setdefault(
            schema, {"name": schema, "tables": [], "views": [], "errors": []})
        entry = {"name": name,
                 "columns": by_object.get((schema, name), []),
                 "source_rows": _plain(rel.get("ROW_COUNT")),
                 "source_bytes": _plain(rel.get("BYTES"))}
        if not entry["columns"]:
            record["errors"].append(
                {"object": name, "kind": kind,
                 "error": "INFORMATION_SCHEMA.COLUMNS returned no column for "
                          "this relation, so it is INCOMPLETE, not empty"})
        bucket = record["views"] if "VIEW" in kind else record["tables"]
        bucket.append(entry)
    return [schemas[k] for k in sorted(schemas)]


def _describe(source: SnowflakeSource, schema: str, name: str) -> list[dict]:
    """Ordered [{name, type}] for one object. DESCRIBE emits section markers
    (`# Partitioning`, blank names) after the column list; stop at the first."""
    columns: list[dict] = []
    fqn = f"{q(source.external_catalog)}.{q(schema)}.{q(name)}"
    for row in _rows(source.spark.sql(f"DESCRIBE {fqn}")):
        col = str(row.get("col_name") or "").strip()
        if not col or col.startswith("#"):
            break
        columns.append({"name": col, "type": str(row.get("data_type") or "")})
    return columns


def discover_schema_via_catalog(source: SnowflakeSource, schema: str) -> dict:
    out = {"name": schema, "tables": [], "views": [], "errors": []}
    catalog = q(source.external_catalog)

    for row in _rows(source.spark.sql(f"SHOW TABLES IN {catalog}.{q(schema)}")):
        name = str(_first_key(row, "tableName", "table_name", "name"))
        try:
            out["tables"].append(
                {"name": name, "columns": _describe(source, schema, name)})
        except Exception as exc:  # one object must not eat the schema
            out["errors"].append({"object": name, "kind": "TABLE",
                                  "error": str(exc)[:300]})

    # SHOW VIEWS may not be supported against every external catalog; an
    # unreadable view list is RECORDED, never silently read as "no views".
    try:
        for row in _rows(source.spark.sql(
                f"SHOW VIEWS IN {catalog}.{q(schema)}")):
            name = str(_first_key(row, "viewName", "view_name", "name"))
            try:
                columns = _describe(source, schema, name)
            except Exception as exc:
                columns = []
                out["errors"].append({"object": name, "kind": "VIEW",
                                      "error": str(exc)[:300]})
            out["views"].append({"name": name, "columns": columns})
    except Exception as exc:
        out["errors"].append({"object": "*", "kind": "VIEW_LIST",
                              "error": f"SHOW VIEWS failed: {str(exc)[:300]}"})
    return out


def render_summary(manifest: dict) -> str:
    src = manifest.get("source") or {}
    where = (f'catalog `{src.get("external_catalog")}`'
             if src.get("mode") == "external-catalog"
             else f'`{src.get("host")}` / `{src.get("database")}` as '
                  f'`{src.get("user")}`')
    lines = ["# Discovery — what the Snowflake source exposes", "",
             f'Mode: `{src.get("mode")}` · {where}',
             f'{len(manifest["schemas"])} schema(s) · generated '
             f'{manifest.get("generated_at", "?")}', "",
             "| Schema | Tables | Views | Columns | Errors |",
             "|---|---|---|---|---|"]
    for s in manifest["schemas"]:
        cols = sum(len(t["columns"]) for t in s["tables"] + s["views"])
        mark = " ⚠️" if s["errors"] else ""
        lines.append(f'| {s["name"]} | {len(s["tables"])} | {len(s["views"])} '
                     f'| {cols} | {len(s["errors"])}{mark} |')
    lines += ["",
              "A schema with errors is INCOMPLETE, not empty — re-run with "
              "`--schemas <name>` (add `--force` in external-catalog mode) after "
              "fixing the cause; the other schemas are kept.", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="connector (default): read Snowflake directly from "
                         "the cluster — one INFORMATION_SCHEMA query pair for "
                         "the whole estate. external-catalog: SHOW/DESCRIBE "
                         "against a registered EXTERNAL catalog, which needs "
                         "a successful crawl")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--session-schema",
                    help="a REAL schema used only to scope the connector's "
                         "pushdown session (default: `schema` from the source "
                         "config). The connector refuses INFORMATION_SCHEMA "
                         "here, so one real schema name is required")
    ap.add_argument("--schemas", nargs="*", default=None,
                    help="only these schemas (default: all). Pushed into the "
                         "INFORMATION_SCHEMA queries as a predicate, and "
                         "merged into the existing manifest: the other "
                         "schemas are kept")
    ap.add_argument("--exclude-schemas", nargs="*", default=[])
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--force", action="store_true",
                    help="rediscover the named --schemas even if the manifest "
                         "already carries them (the others are kept); without "
                         "--schemas, rediscover the whole estate")
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    try:
        config = (load_source_config(args.source_config)
                  if args.source_config else None)
        source = SnowflakeSource(spark, mode=args.source_mode, config=config,
                                 external_catalog=args.source_catalog,
                                 session_schema=args.session_schema)
    except SourceConfigError as exc:
        return fail(f"error: {exc}")

    reports = pathlib.Path(args.reports_dir)
    reports.mkdir(parents=True, exist_ok=True)
    manifest_path = reports / MANIFEST_NAME
    identity = (source.external_catalog if args.source_mode
                == "external-catalog" else source.database())

    manifest = {"schemas": []}
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        # The identity guard holds under --force too: a --force against a
        # manifest written for another database must not overwrite it.
        if existing.get("source_identity") not in (None, identity):
            return fail(f"error: {manifest_path} describes "
                  f"{existing.get('source_identity')!r}, not {identity!r}. "
                  f"Use a fresh --reports-dir.")
        if args.force and not args.schemas:
            log("--force without --schemas: rediscovering the whole estate")
        else:
            manifest = existing

    manifest.update(source=source.describe(), source_identity=identity,
                    generated_at=datetime.datetime.now(
                        datetime.timezone.utc).isoformat())
    log(f"source: {json.dumps(source.describe())}")

    exclude = {s.lower() for s in args.exclude_schemas} | _SYSTEM_SCHEMAS
    failures = 0

    if args.source_mode == "connector":
        try:
            fresh = discover_via_connector(
                source, wanted=args.schemas, exclude=exclude)
        except Exception:
            failures += 1
            log(f"DISCOVERY FAILED:\n{traceback.format_exc(limit=3)}")
        else:
            if args.schemas:
                # A scoped run MERGES. Assigning the filtered result over the
                # loaded manifest used to drop every other schema -- exactly
                # what DISCOVERY.md's own "re-run with --schemas <name>"
                # advice then did to a finished discovery. An unscoped run is
                # the whole estate and stays authoritative.
                requested = set(args.schemas)
                fresh = sorted(
                    [s for s in manifest["schemas"] if s["name"] not in requested]
                    + fresh, key=lambda s: s["name"])
            manifest["schemas"] = fresh
    else:
        done = {s["name"] for s in manifest["schemas"]}
        listed = [str(_first_key(r, "namespace", "databaseName",
                                 "schema_name", "name"))
                  for r in _rows(spark.sql(
                      f"SHOW SCHEMAS IN {q(source.external_catalog)}"))]
        wanted = [s for s in (args.schemas or listed)
                  if s.lower() not in exclude]
        log(f"{len(wanted)} schema(s) to discover; {len(done)} already done")
        for schema in wanted:
            if schema in done and not args.force:
                log(f"skip {schema}: already discovered (--force to redo)")
                continue
            try:
                record = discover_schema_via_catalog(source, schema)
            except Exception:
                failures += 1
                log(f"SCHEMA FAILED {schema}:\n{traceback.format_exc(limit=3)}")
                record = {"name": schema, "tables": [], "views": [],
                          "errors": [{"object": "*", "kind": "SCHEMA",
                                      "error": traceback.format_exc(
                                          limit=1)[-300:]}]}
            manifest["schemas"] = sorted(
                [s for s in manifest["schemas"] if s["name"] != schema]
                + [record], key=lambda s: s["name"])
            # Flush after EVERY schema: a large estate resumes, not restarts.
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            log(f"{schema}: {len(record['tables'])} table(s), "
                f"{len(record['views'])} view(s)")

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (reports / "DISCOVERY.md").write_text(render_summary(manifest), encoding="utf-8")
    total = sum(len(s["tables"]) for s in manifest["schemas"])
    log(f"{len(manifest['schemas'])} schema(s), {total} table(s) "
        f"-> {manifest_path}")
    log(f"summary -> {reports / 'DISCOVERY.md'}")

    if not manifest["schemas"]:
        log("ZERO schemas discovered. In external-catalog mode that usually "
            "means the catalog never crawled successfully (check its refresh "
            "status and the crawler's network path to Snowflake); in "
            "connector mode it means these credentials see nothing, or "
            "DISCOVERY FAILED above (read that traceback first). Either "
            "way it is a FINDING, not a success.")
        return 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
