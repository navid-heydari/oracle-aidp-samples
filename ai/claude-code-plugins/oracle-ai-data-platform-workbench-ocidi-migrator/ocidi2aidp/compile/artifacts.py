"""Migration-wide artifacts: target DDL, watermark seed, reconciliation, sources.md."""
from __future__ import annotations

import datetime as dt
import json

from ..notebook import Notebook
from ..report import Finding
from .dataflow import pylit
from .runtime import RUNTIME

WATERMARK_DDL = ("CREATE TABLE IF NOT EXISTS {table} (task STRING, source STRING, "
                 "column_name STRING, value TIMESTAMP, updated_at TIMESTAMP) USING DELTA")


def _q(*parts) -> str:
    return ".".join("`" + str(p).replace("`", "``") + "`" for p in parts)


def merge_ddl(entries: list) -> tuple[list, list]:
    """Collapse per-flow DDL entries into one per table; report conflicts."""
    tables, findings = {}, []
    for e in entries:
        key = (e["schema"], e["table"])
        prev = tables.get(key)
        if prev is None or (not prev["columns"] and e["columns"]):
            tables[key] = e
        elif e["columns"] and prev["columns"] and [c[0].upper() for c in prev["columns"]] != \
                [c[0].upper() for c in e["columns"]]:
            findings.append(Finding("DD01_CONFLICT", f"{e['schema']}.{e['table']} is written by "
                                    f"more than one flow with different columns; the first "
                                    f"definition was used", "review"))
    return sorted(tables.values(), key=lambda t: (t["schema"], t["table"])), findings


def setup_sql(tables: list, control_schema: str, catalog_placeholder: str = "{catalog}") -> str:
    schemas = sorted({t["schema"] for t in tables} | {control_schema})
    lines = [f"-- ocidi2aidp target DDL. Replace {catalog_placeholder} with the target catalog.",
             ""]
    for s in schemas:
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {_q(catalog_placeholder, s)};")
    lines.append("")
    for t in tables:
        if not t["columns"]:
            lines.append(f"-- {t['schema']}.{t['table']}: column types unknown; created by the "
                         f"first write")
            continue
        cols = ",\n  ".join(f"`{n}` {typ}" for n, typ in t["columns"])
        lines.append(f"CREATE TABLE IF NOT EXISTS {_q(catalog_placeholder, t['schema'], t['table'])} "
                     f"(\n  {cols}\n) USING DELTA;")
    lines.append(WATERMARK_DDL.format(table=_q(catalog_placeholder, control_schema, "watermarks"))
                 + ";")
    return "\n".join(lines) + "\n"


def setup_notebook(tables: list, config) -> Notebook:
    nb = Notebook("00_setup")
    nb.meta.update({"source_kind": "SETUP", "source_name": "00_setup"})
    nb.md("# 00_setup -- target schemas and tables\n\nCreates the AIDP schemas and empty Delta "
          "tables the migrated notebooks write, plus the watermark table. Idempotent "
          "(`IF NOT EXISTS`). Run once, before any migrated job.")
    nb.code(RUNTIME, tags=["ocidi2aidp-runtime"])
    nb.code("\n".join([
        f'TARGET_CATALOG = _aidp_parameter("TARGET_CATALOG", {pylit(config.target.catalog)})',
        f'CONTROL_SCHEMA = _aidp_parameter("CONTROL_SCHEMA", {pylit(config.target.control_schema)})',
    ]), tags=["parameters"])
    stmts = []
    schemas = sorted({t["schema"] for t in tables})
    for s in schemas:
        stmts.append(f'spark.sql("CREATE SCHEMA IF NOT EXISTS " + _tbl(TARGET_CATALOG, {pylit(s)}))')
    stmts.append('spark.sql("CREATE SCHEMA IF NOT EXISTS " + _tbl(TARGET_CATALOG, CONTROL_SCHEMA))')
    for t in tables:
        if not t["columns"]:
            continue
        cols = ", ".join(f"`{n}` {typ}" for n, typ in t["columns"])
        stmts.append(f'spark.sql("CREATE TABLE IF NOT EXISTS " + _tbl(TARGET_CATALOG, '
                     f'{pylit(t["schema"])}, {pylit(t["table"])}) + {pylit(" (" + cols + ") USING DELTA")})')
    stmts.append('spark.sql("CREATE TABLE IF NOT EXISTS " + _tbl(TARGET_CATALOG, CONTROL_SCHEMA, '
                 '"watermarks") + " (task STRING, source STRING, column_name STRING, value '
                 'TIMESTAMP, updated_at TIMESTAMP) USING DELTA")')
    nb.code("\n".join(stmts))
    nb.code('print("setup complete")')
    return nb


def seed_sql(seeds: list, control_schema: str) -> str:
    """seeds: [{"task", "source", "column", "value": iso or None, "basis"}]"""
    lines = ["-- Watermarks to carry over from OCI-DI, so the first AIDP run is incremental.",
             "-- Replace {catalog}. Values are the start time of the last successful OCI-DI run",
             "-- of each task unless noted; rows a DI run was still reading at that instant are",
             "-- re-read, never skipped (the comparator is > the stored value).", ""]
    table = _q("{catalog}", control_schema, "watermarks")
    for s in seeds:
        if s.get("value") is None:
            lines.append(f"-- {s['task']} / {s['source']}: no successful OCI-DI run in the "
                         f"snapshot; the first run will be a full load")
            continue
        lines.append(f"-- {s['task']} / {s['source']} ({s['basis']})")
        lines.append(
            f"MERGE INTO {table} t USING (SELECT '{s['task']}' AS task, '{s['source']}' AS source, "
            f"'{s['column']}' AS column_name, TIMESTAMP'{s['value']}' AS value, "
            f"current_timestamp() AS updated_at) s ON t.task = s.task AND t.source = s.source "
            f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;")
    return "\n".join(lines) + "\n"


def millis_to_iso(ms) -> str:
    return dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S")


def reconcile_notebook(targets: list, config) -> Notebook:
    """One notebook comparing every legacy OCI-DI target with its new Delta table."""
    nb = Notebook("reconcile")
    nb.meta.update({"source_kind": "RECONCILE", "source_name": "reconcile"})
    nb.md("# Reconcile OCI-DI targets against the migrated Delta tables\n\nRun after the "
          "migrated jobs and the OCI-DI tasks have both processed the same input window. For "
          "each target: row counts on both sides, the sum of every numeric column, and -- where "
          "a key is known -- keys present on only one side. Only aggregates leave the cluster.")
    nb.code(RUNTIME, tags=["ocidi2aidp-runtime"])
    params = sorted({t["legacy"]["parameter"] for t in targets if t["legacy"].get("parameter")})
    lines = [f'TARGET_CATALOG = _aidp_parameter("TARGET_CATALOG", {pylit(config.target.catalog)})']
    for p in params:
        lines.append(f'{p} = _aidp_parameter({pylit(p)}, '
                     f'{pylit(config.sources.get(p, {}).get("value", p[4:].lower()))})')
    nb.code("\n".join(lines), tags=["parameters"])
    pairs = []
    seen = set()
    for t in targets:
        leg = t["legacy"]
        if (t["schema"], t["name"]) in seen:
            continue
        seen.add((t["schema"], t["name"]))
        if not leg.get("parameter") or "OBJECT_STORAGE" in leg.get("asset_type", ""):
            continue
        pairs.append({"new": [t["schema"], t["name"]], "legacy_param": leg["parameter"],
                      "legacy": [leg["schema"], leg["entity"]] if leg["schema"] else [leg["entity"]],
                      "keys": t.get("keys") or []})
    nb.code("PAIRS = " + json.dumps(pairs, indent=1))
    nb.code('''from pyspark.sql.types import NumericType

_results = []
for p in PAIRS:
    new_t = _tbl(TARGET_CATALOG, *p["new"])
    old_t = _tbl(globals()[p["legacy_param"]], *p["legacy"])
    row = {"table": ".".join(p["new"]), "legacy": ".".join(p["legacy"])}
    try:
        new_df, old_df = spark.table(new_t), spark.table(old_t)
        row["rows_new"], row["rows_legacy"] = new_df.count(), old_df.count()
        nums = [f.name for f in new_df.schema.fields if isinstance(f.dataType, NumericType)
                and f.name.upper() in {c.upper() for c in old_df.columns}]
        diffs = []
        for c in nums:
            a = new_df.agg(F.sum(F.col(c))).first()[0]
            b = old_df.agg(F.sum(F.col(c))).first()[0]
            if (a or 0) != (b or 0):
                diffs.append(f"{c}: {a} vs {b}")
        row["sum_mismatches"] = "; ".join(diffs)
        if p["keys"]:
            k = p["keys"]
            row["keys_only_new"] = new_df.select(*k).subtract(old_df.select(*k)).count()
            row["keys_only_legacy"] = old_df.select(*k).subtract(new_df.select(*k)).count()
        row["status"] = "MATCH" if (row["rows_new"] == row["rows_legacy"] and not diffs and
                                    not row.get("keys_only_new") and
                                    not row.get("keys_only_legacy")) else "DIFF"
    except Exception as exc:  # report every pair, not just the first failure
        row["status"] = "ERROR"
        row["error"] = str(exc)[:300]
    _results.append(row)
    print(row)

_bad = [r for r in _results if r["status"] != "MATCH"]
print(f"{len(_results) - len(_bad)} of {len(_results)} targets match")''')
    return nb


def sources_markdown(sources: list) -> str:
    lines = ["# Sources the migrated notebooks read", "",
             "Each source is reached through a notebook parameter. Its default must resolve on "
             "AIDP before the jobs run; override it per job task if your names differ.", "",
             "| DI data asset | Type | Access | Parameter | Default | What must exist |",
             "|---|---|---|---|---|---|"]
    for s in sources:
        lines.append(f"| {s['data_asset']} | {s['asset_type']} | {s['access']} | `{s['parameter']}` "
                     f"| `{s['default']}` | {s['action']} |")
    return "\n".join(lines) + "\n"
