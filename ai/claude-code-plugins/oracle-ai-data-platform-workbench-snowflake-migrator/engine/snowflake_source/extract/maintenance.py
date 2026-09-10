"""Snowflake maintenance and layout state. Read-only, I/O injected as `run_sql`.

Item M2. The plan cannot propose a maintenance cadence it cannot see, and the
extractor previously picked these settings up only incidentally.

REPORTS, NEVER PROPOSES. No OPTIMIZE, VACUUM, ZORDER or RETAIN appears here --
choosing a cadence needs the customer's recovery requirements and query
patterns, which this plugin does not know. See references/maintenance-and-
layout.md and ACTION-ITEMS.md (M3).

Two rules shape the implementation:

  * "NOT MEASURED" IS NOT "ZERO". ACCOUNT_USAGE needs a grant this plugin must
    not assume. An unreadable history reports `measured: False` with null
    counts, because "0 reclustering credits" and "we could not look" lead to
    opposite decisions about whether clustering matters here.

  * THE EXPENSIVE PROBE IS AVOIDED. `SHOW TABLES` already carries each table's
    EFFECTIVE retention, so the cascade is resolved with one account query plus
    one per database and one per schema, and the level is INFERRED by comparing
    the table's effective value with its schema default. One
    `SHOW PARAMETERS IN TABLE` per table would be thousands of round trips on a
    real estate, so it stays opt-in.
"""
from __future__ import annotations

import datetime
from typing import Callable

__all__ = ["build_maintenance", "NO_AIDP_EQUIVALENT", "CHURN_ROWS_SIGNAL"]

# Snowflake capabilities with no AIDP counterpart at all. Named explicitly
# (item M7) because both are discovered at the worst possible moment.
NO_AIDP_EQUIVALENT = (
    {"capability": "Fail-safe",
     "snowflake": "7 days of Snowflake-operated recovery after Time Travel "
                  "expires. Not user-controllable, and not something the "
                  "customer ever had to run.",
     "impact": "There is NO AIDP equivalent and no Delta setting restores it. "
               "Delta retention plus VACUUM is the whole recovery story, and "
               "it is operated by the customer."},
    {"capability": "Search Optimization Service",
     "snowflake": "Point lookups on high-cardinality, non-clustered columns.",
     "impact": "No AIDP equivalent. Delta data skipping plus ZORDER covers "
               "range and prefix predicates; arbitrary high-cardinality point "
               "lookups regress."},
    {"capability": "MAX_DATA_EXTENSION_TIME_IN_DAYS",
     "snowflake": "Bounds how long Snowflake may extend retention to keep a "
                  "stream from going stale.",
     "impact": "No equivalent. Delta retention is a single duration with no "
               "automatic extension."},
)

# Rows rewritten per window, above which compaction is worth planning for.
# A threshold, not a proposal: it decides whether the table is FLAGGED.
CHURN_ROWS_SIGNAL = 1_000_000

_ON = ("ON", "TRUE", "YES", "Y", "ENABLED")


def _truthy(value) -> bool:
    return str(value or "").strip().upper() in _ON


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parameter(run_sql, name: str, scope: str, notes: list[str]):
    """(value, level) for one parameter at one scope, or (None, None)."""
    try:
        rows = run_sql(f"show parameters like '{name.lower()}' {scope}")
    except Exception as exc:
        notes.append(f"SHOW PARAMETERS {name} {scope}: {str(exc)[:160]}")
        return None, None
    if not rows:
        return None, None
    row = rows[0]
    return _int(row.get("value")), (row.get("level") or "") or None


def _account_usage_summary(run_sql, history_days: int,
                           notes: list[str]) -> tuple[dict, dict, dict]:
    """(reclustering_by_table, churn_by_table, status).

    On failure the status says so and BOTH maps come back empty, so callers
    report "not measured" rather than fabricating zeros.
    """
    status = {"readable": True, "note": f"summarised over {history_days} day(s)"}
    reclustering: dict[str, dict] = {}
    churn: dict[str, dict] = {}
    try:
        rows = run_sql(
            "select database_name DATABASE_NAME, schema_name SCHEMA_NAME, "
            "table_name TABLE_NAME, count(*) EVENTS, "
            "sum(credits_used) CREDITS, sum(num_bytes_reclustered) BYTES, "
            "sum(num_rows_reclustered) ROWS_RECLUSTERED "
            "from snowflake.account_usage.automatic_clustering_history "
            f"where start_time >= dateadd('day', -{history_days}, current_timestamp()) "
            "group by 1, 2, 3")
        for r in rows:
            key = f'{r["DATABASE_NAME"]}.{r["SCHEMA_NAME"]}.{r["TABLE_NAME"]}'
            reclustering[key] = {
                "measured": True, "events": _int(r.get("EVENTS")),
                "credits": _num(r.get("CREDITS")),
                "bytes_reclustered": _int(r.get("BYTES")),
                "rows_reclustered": _int(r.get("ROWS_RECLUSTERED"))}
    except Exception as exc:
        status = {"readable": False, "note": str(exc)[:200]}
        notes.append(f"ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY: {str(exc)[:160]}")
        return {}, {}, status

    try:
        rows = run_sql(
            "select database_name DATABASE_NAME, schema_name SCHEMA_NAME, "
            "table_name TABLE_NAME, sum(rows_added) ROWS_ADDED, "
            "sum(rows_removed) ROWS_REMOVED, sum(rows_updated) ROWS_UPDATED, "
            "count(*) WINDOWS "
            "from snowflake.account_usage.table_dml_history "
            f"where start_time >= dateadd('day', -{history_days}, current_timestamp()) "
            "group by 1, 2, 3")
        for r in rows:
            key = f'{r["DATABASE_NAME"]}.{r["SCHEMA_NAME"]}.{r["TABLE_NAME"]}'
            removed = _int(r.get("ROWS_REMOVED")) or 0
            updated = _int(r.get("ROWS_UPDATED")) or 0
            churn[key] = {
                "measured": True,
                "rows_added": _int(r.get("ROWS_ADDED")),
                "rows_removed": removed, "rows_updated": updated,
                # Removals and updates are what rewrite files, so they are what
                # compaction has to keep up with. Appends alone fragment less.
                "rows_rewritten": removed + updated,
                "windows": _int(r.get("WINDOWS"))}
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.TABLE_DML_HISTORY: {str(exc)[:160]}")
        status = {"readable": True,
                  "note": f"reclustering read; DML history failed: {str(exc)[:120]}"}
    return reclustering, churn, status


_NOT_MEASURED_RECLUSTER = {"measured": False, "events": None, "credits": None,
                           "bytes_reclustered": None, "rows_reclustered": None}
_NOT_MEASURED_CHURN = {"measured": False, "rows_added": None,
                       "rows_removed": None, "rows_updated": None,
                       "rows_rewritten": None, "windows": None}


def _signals(rec: dict) -> list[dict]:
    """What about this table forces a maintenance decision on AIDP."""
    out: list[dict] = []
    if rec["clustered"] or rec["automatic_clustering"]:
        out.append({
            "signal": "clustering key in use",
            "detail": f'cluster_by={rec["cluster_by"] or "(none)"}, '
                      f'automatic_clustering='
                      f'{"ON" if rec["automatic_clustering"] else "OFF"}',
            "aidp_equivalent": "liquid clustering (CLUSTER BY) or ZORDER",
            "aidp_requires": "a scheduled job -- AIDP reclusters nothing on "
                             "its own, where Snowflake does it in the "
                             "background"})
    if rec["search_optimization"]:
        out.append({
            "signal": "Search Optimization Service enabled",
            "detail": f'search_optimization_bytes={rec.get("search_optimization_bytes")}',
            "aidp_equivalent": None,
            "aidp_requires": "no equivalent; point-lookup performance on "
                             "high-cardinality columns will regress"})
    if rec["change_tracking"]:
        out.append({
            "signal": "change tracking enabled",
            "detail": "streams or CDC consumers may depend on this",
            "aidp_equivalent": "Delta Change Data Feed "
                               "(delta.enableChangeDataFeed)",
            "aidp_requires": "enabling it explicitly on the target table"})
    if rec["retention_set_at"] == "table":
        out.append({
            "signal": "table-level Time Travel override",
            "detail": f'retention_days={rec["retention_days"]} differs from the '
                      f'schema default',
            "aidp_equivalent": "delta.deletedFileRetentionDuration + "
                               "delta.logRetentionDuration",
            "aidp_requires": "a deliberate retention, because on Delta the "
                             "reclamation setting is ALSO the time-travel "
                             "bound -- reclaiming aggressively deletes the "
                             "recovery window"})
    churn = rec["dml_churn"]
    if churn["measured"] and (churn["rows_rewritten"] or 0) >= CHURN_ROWS_SIGNAL:
        out.append({
            "signal": "high row-rewrite churn",
            "detail": f'{churn["rows_rewritten"]:,} rows removed/updated over '
                      f'{churn["windows"]} window(s)',
            "aidp_equivalent": "OPTIMIZE (compaction)",
            "aidp_requires": "a compaction cadence, and a VACUUM to follow it "
                             "-- compaction alone leaves the old files and "
                             "increases storage"})
    return out


def build_maintenance(run_sql: Callable[..., list[dict]], inventory: dict, *,
                      history_days: int = 30,
                      probe_table_parameters: bool = False) -> dict:
    notes: list[str] = []
    databases = inventory.get("databases_in_scope") or []
    tables = [r for r in inventory.get("inventory") or []
              if r.get("object_type") == "TABLE"]

    acct_retention, acct_level = _parameter(
        run_sql, "DATA_RETENTION_TIME_IN_DAYS", "in account", notes)
    acct_extension, _ = _parameter(
        run_sql, "MAX_DATA_EXTENSION_TIME_IN_DAYS", "in account", notes)

    db_retention: dict[str, int | None] = {}
    for db in databases:
        db_retention[db], _ = _parameter(
            run_sql, "DATA_RETENTION_TIME_IN_DAYS", f'in database "{db}"', notes)

    schema_retention: dict[str, int | None] = {}
    for scope in sorted({(r["source_database"], r["source_schema"]) for r in tables}):
        db, schema = scope
        schema_retention[f"{db}.{schema}"], _ = _parameter(
            run_sql, "DATA_RETENTION_TIME_IN_DAYS",
            f'in schema "{db}"."{schema}"', notes)

    reclustering, churn, acct_status = _account_usage_summary(
        run_sql, history_days, notes)

    records: list[dict] = []
    for r in tables:
        meta = r.get("source_metadata") or {}
        ident = r["source_identifier"]
        db, schema = r["source_database"], r["source_schema"]
        effective = _int(meta.get("retention_time"))
        inherited = schema_retention.get(f"{db}.{schema}")
        if inherited is None:
            inherited = db_retention.get(db)
        if inherited is None:
            inherited = acct_retention

        rec = {
            "source_identifier": ident,
            "cluster_by": meta.get("cluster_by") or "",
            "clustered": bool((meta.get("cluster_by") or "").strip()),
            "automatic_clustering": _truthy(meta.get("automatic_clustering")),
            "change_tracking": _truthy(meta.get("change_tracking")),
            "search_optimization": _truthy(meta.get("search_optimization")),
            "search_optimization_bytes": _int(meta.get("search_optimization_bytes")),
            "retention_days": effective,
            # Inferred, not probed: one SHOW PARAMETERS per table would be
            # thousands of round trips on a real estate.
            "retention_set_at": (
                "table" if (effective is not None and inherited is not None
                            and effective != inherited)
                else "inherited" if effective is not None else "unknown"),
            "retention_inherited_value": inherited,
            "rows": _int(meta.get("rows")),
            "bytes": _int(meta.get("bytes")),
            "reclustering": reclustering.get(ident, dict(_NOT_MEASURED_RECLUSTER)
                                             if not acct_status["readable"]
                                             else {**_NOT_MEASURED_RECLUSTER,
                                                   "measured": True,
                                                   "events": 0, "credits": 0.0}),
            "dml_churn": churn.get(ident, dict(_NOT_MEASURED_CHURN)
                                   if not acct_status["readable"]
                                   else {**_NOT_MEASURED_CHURN,
                                         "measured": True, "rows_added": 0,
                                         "rows_removed": 0, "rows_updated": 0,
                                         "rows_rewritten": 0, "windows": 0}),
        }
        if probe_table_parameters:
            value, level = _parameter(
                run_sql, "DATA_RETENTION_TIME_IN_DAYS",
                f'in table "{db}"."{schema}"."{ident.rsplit(".", 1)[1]}"', notes)
            if value is not None:
                rec["retention_days"] = value
                rec["retention_set_at"] = (level or "inherited").lower()
        rec["signals"] = _signals(rec)
        records.append(rec)

    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "history_days": history_days,
        "table_parameters_probed": probe_table_parameters,
        "retention": {
            "account": {"data_retention_time_in_days": acct_retention,
                        "max_data_extension_time_in_days": acct_extension,
                        "set_at": acct_level or "default"},
            "databases": db_retention,
            "schemas": schema_retention,
        },
        "account_usage": acct_status,
        "tables": records,
        "objects_with_signals": sum(1 for r in records if r["signals"]),
        "no_equivalent": [dict(g) for g in NO_AIDP_EQUIVALENT],
        "unreadable": notes,
    }
