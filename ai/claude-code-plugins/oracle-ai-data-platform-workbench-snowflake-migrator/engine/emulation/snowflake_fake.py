"""An emulated Snowflake estate for dev mode. Zero network, zero credentials.

One database, SNOWDEMO, small but deliberately awkward — every object exists
to teach one real migration lesson:

  SALES.ORDERS        clusters, tracks changes, overrides retention, churns
                      >1M rewritten rows: every maintenance signal fires.
  SALES.CUSTOMERS     carries a masking policy on EMAIL: the security stage
                      must report the exposure.
  SALES.EVENTS_RAW    a VARIANT column: blocked at assess unless the operator
                      opts into string.
  SALES.LEGACY_AUDIT  fine here — the EMULATED AIDP silently drops its create,
                      demonstrating the poisoned-name diagnosis.
  ANALYTICS.ORDER_SUMMARY_VW   translatable view (IFF, ::) — and the emulated
                               AIDP re-derives one column type, demonstrating
                               derived-type drift.
  ANALYTICS.TOP_CUSTOMERS_VW   QUALIFY: refused, never guessed.
  ANALYTICS.CUSTOMER_360_VW    SECURE view: unsupported object.
  TASK_LOAD_ORDERS             populates a migrating table — the blast-radius
                               lesson: the clone succeeds and then goes stale.

`demo_run_sql(sql, params)` answers exactly the read-only queries the real
extractors issue, in the row shapes Snowflake returns. An unrecognised query
raises — the emulation must never silently answer a question it was not built
for, because that is how a fake starts lying.
"""
from __future__ import annotations

__all__ = ["DEMO_DB", "demo_run_sql"]

DEMO_DB = "SNOWDEMO"

_SESSION = {"U": "DEMO_MIGRATOR", "A": "DEMO_ACCOUNT", "R": "EMULATED_REGION",
            "ROLE": "MIGRATION_READER", "WH": "WH_ETL", "V": "emulated"}

_SCHEMAS = ("SALES", "ANALYTICS")

_ORDER_SUMMARY_SQL = (
    "CREATE OR REPLACE VIEW SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW AS\n"
    "select o.CUSTOMER_ID::string as CUSTOMER,\n"
    "       iff(o.AMOUNT > 100, 'BIG', 'SMALL') as BUCKET,\n"
    "       sum(o.AMOUNT) as TOTAL_AMOUNT,\n"
    "       count(*) as ORDER_COUNT\n"
    "from SNOWDEMO.SALES.ORDERS o\n"
    "group by 1, 2")

_TOP_CUSTOMERS_SQL = (
    "CREATE OR REPLACE VIEW SNOWDEMO.ANALYTICS.TOP_CUSTOMERS_VW AS\n"
    "select CUSTOMER_ID, sum(AMOUNT) as TOTAL\n"
    "from SNOWDEMO.SALES.ORDERS\n"
    "group by 1\n"
    "qualify row_number() over (order by TOTAL desc) <= 10")

_CUSTOMER_360_SQL = (
    "CREATE OR REPLACE SECURE VIEW SNOWDEMO.ANALYTICS.CUSTOMER_360_VW AS\n"
    "select c.CUSTOMER_ID, c.FULL_NAME, count(o.ORDER_ID) as ORDERS\n"
    "from SNOWDEMO.SALES.CUSTOMERS c\n"
    "join SNOWDEMO.SALES.ORDERS o on o.CUSTOMER_ID = c.CUSTOMER_ID\n"
    "group by 1, 2")

_VIEW_SQL = {"ORDER_SUMMARY_VW": _ORDER_SUMMARY_SQL,
             "TOP_CUSTOMERS_VW": _TOP_CUSTOMERS_SQL,
             "CUSTOMER_360_VW": _CUSTOMER_360_SQL}

_SHOW_TABLES = {
    "SALES": [
        {"name": "ORDERS", "rows": 1_250_000, "bytes": 402_653_184,
         "comment": "order fact table", "owner": "SYSADMIN",
         "cluster_by": "LINEAR(ORDER_DATE)", "automatic_clustering": "ON",
         "change_tracking": "ON", "retention_time": "7",
         "search_optimization": "OFF"},
        {"name": "CUSTOMERS", "rows": 240_000, "bytes": 58_720_256,
         "comment": "customer dimension", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
        {"name": "EVENTS_RAW", "rows": 9_800_000, "bytes": 5_368_709_120,
         "comment": "raw event payloads (VARIANT)", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
        {"name": "LEGACY_AUDIT", "rows": 3_100_000, "bytes": 1_073_741_824,
         "comment": "append-only audit trail", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
    ],
    "ANALYTICS": [],
}

_SHOW_VIEWS = {
    "SALES": [],
    "ANALYTICS": [
        {"name": "ORDER_SUMMARY_VW", "text": _ORDER_SUMMARY_SQL,
         "is_secure": "false", "comment": "translatable: IFF and :: rewrite"},
        {"name": "TOP_CUSTOMERS_VW", "text": _TOP_CUSTOMERS_SQL,
         "is_secure": "false", "comment": "QUALIFY has no exact Spark rewrite"},
        {"name": "CUSTOMER_360_VW", "text": _CUSTOMER_360_SQL,
         "is_secure": "true", "comment": "secure view"},
    ],
}


def _col(table, pos, name, data_type, *, precision=None, scale=None,
         char_length=None, nullable="YES", comment=None):
    return {"TABLE_SCHEMA": None, "TABLE_NAME": table, "ORDINAL_POSITION": pos,
            "COLUMN_NAME": name, "DATA_TYPE": data_type,
            "IS_NULLABLE": nullable, "NUMERIC_PRECISION": precision,
            "NUMERIC_SCALE": scale, "CHARACTER_MAXIMUM_LENGTH": char_length,
            "DATETIME_PRECISION": 9 if data_type.startswith("TIMESTAMP") else None,
            "COMMENT": comment}


_COLUMNS = {
    "SALES": [
        _col("ORDERS", 1, "ORDER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _col("ORDERS", 2, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("ORDERS", 3, "AMOUNT", "NUMBER", precision=18, scale=2),
        _col("ORDERS", 4, "STATUS", "TEXT", char_length=20),
        _col("ORDERS", 5, "CREATED_AT", "TIMESTAMP_NTZ"),
        _col("ORDERS", 6, "IS_PRIORITY", "BOOLEAN"),
        _col("ORDERS", 7, "ORDER_DATE", "DATE"),
        _col("CUSTOMERS", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _col("CUSTOMERS", 2, "FULL_NAME", "TEXT", char_length=200),
        _col("CUSTOMERS", 3, "EMAIL", "TEXT", char_length=320,
             comment="masked by MASK_EMAIL in Snowflake"),
        _col("CUSTOMERS", 4, "SEGMENT", "TEXT", char_length=40),
        _col("EVENTS_RAW", 1, "EVENT_ID", "NUMBER", precision=38, scale=0),
        _col("EVENTS_RAW", 2, "PAYLOAD", "VARIANT"),
        _col("EVENTS_RAW", 3, "INGESTED_AT", "TIMESTAMP_NTZ"),
        _col("LEGACY_AUDIT", 1, "AUDIT_ID", "NUMBER", precision=38, scale=0),
        _col("LEGACY_AUDIT", 2, "NOTE", "TEXT", char_length=4000),
    ],
    "ANALYTICS": [
        _col("ORDER_SUMMARY_VW", 1, "CUSTOMER", "TEXT", char_length=39),
        _col("ORDER_SUMMARY_VW", 2, "BUCKET", "TEXT", char_length=5),
        _col("ORDER_SUMMARY_VW", 3, "TOTAL_AMOUNT", "NUMBER", precision=30, scale=2),
        _col("ORDER_SUMMARY_VW", 4, "ORDER_COUNT", "NUMBER", precision=18, scale=0),
        _col("TOP_CUSTOMERS_VW", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("TOP_CUSTOMERS_VW", 2, "TOTAL", "NUMBER", precision=30, scale=2),
        _col("CUSTOMER_360_VW", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("CUSTOMER_360_VW", 2, "FULL_NAME", "TEXT", char_length=200),
        _col("CUSTOMER_360_VW", 3, "ORDERS", "NUMBER", precision=18, scale=0),
    ],
}

_DEPENDENCY_ROWS = [
    {"REFERENCING": "SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.TOP_CUSTOMERS_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.CUSTOMER_360_VW",
     "REFERENCED": "SNOWDEMO.SALES.CUSTOMERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.CUSTOMER_360_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
]

_WAREHOUSES = [
    {"name": "WH_ETL", "size": "Medium", "state": "SUSPENDED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 3, "auto_suspend": 300,
     "auto_resume": "true", "owner": "SYSADMIN", "comment": "nightly loads"},
    {"name": "WH_BI", "size": "X-Small", "state": "SUSPENDED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 1, "auto_suspend": 60,
     "auto_resume": "true", "owner": "SYSADMIN", "comment": "dashboards"},
]

_METERING = [
    {"WAREHOUSE_NAME": "WH_ETL", "CREDITS": 412.5, "DAYS": 30},
    {"WAREHOUSE_NAME": "WH_BI", "CREDITS": 36.2, "DAYS": 30},
]

_POLICY_REFS = [
    {"REF_DATABASE_NAME": "SNOWDEMO", "REF_SCHEMA_NAME": "SALES",
     "REF_ENTITY_NAME": "CUSTOMERS", "REF_COLUMN_NAME": "EMAIL",
     "POLICY_KIND": "MASKING_POLICY", "POLICY_NAME": "MASK_EMAIL"},
]

_GRANTS = [
    {"NAME": "ORDERS", "TABLE_SCHEMA": "SALES", "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "TABLE",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "SALES", "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "TABLE",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    # Grants the old three-class read never saw: on a schema, and on a
    # warehouse (which has no database at all).
    {"NAME": "SALES", "TABLE_SCHEMA": None, "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "SCHEMA",
     "PRIVILEGE": "USAGE", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "ANALYTICS_WH", "TABLE_SCHEMA": None, "DATABASE_NAME": None,
     "GRANTED_ON": "WAREHOUSE",
     "PRIVILEGE": "USAGE", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
]

# One tag attachment, so the demo's SECURITY.md teaches that a classification
# does not travel: PII on the e-mail column of a migrated table.
_TAG_REFS = [
    {"TAG_DATABASE": "SNOWDEMO", "TAG_SCHEMA": "SALES", "TAG_NAME": "PII",
     "TAG_VALUE": "EMAIL", "OBJECT_DATABASE": "SNOWDEMO",
     "OBJECT_SCHEMA": "SALES", "OBJECT_NAME": "CUSTOMERS",
     "COLUMN_NAME": "EMAIL", "DOMAIN": "COLUMN"},
]

_CLUSTERING_HISTORY = [
    {"DATABASE_NAME": "SNOWDEMO", "SCHEMA_NAME": "SALES",
     "TABLE_NAME": "ORDERS", "EVENTS": 14, "CREDITS": 3.7,
     "BYTES": 9_663_676_416, "ROWS_RECLUSTERED": 41_000_000},
]

_DML_HISTORY = [
    {"DATABASE_NAME": "SNOWDEMO", "SCHEMA_NAME": "SALES",
     "TABLE_NAME": "ORDERS", "ROWS_ADDED": 2_400_000,
     "ROWS_REMOVED": 780_000, "ROWS_UPDATED": 420_000, "WINDOWS": 30},
]

_PROCEDURES = [
    {"PROCEDURE_NAME": "REFRESH_ORDERS", "PROCEDURE_SCHEMA": "SALES",
     "PROCEDURE_LANGUAGE": "SQL", "ARGUMENT_SIGNATURE": "()",
     "PROCEDURE_OWNER": "SYSADMIN"},
]

_FUNCTIONS = [
    {"FUNCTION_NAME": "CLEAN_EMAIL", "FUNCTION_SCHEMA": "SALES",
     "FUNCTION_LANGUAGE": "JAVASCRIPT", "ARGUMENT_SIGNATURE": "(S VARCHAR)",
     "FUNCTION_OWNER": "SYSADMIN"},
]

_TASKS = [
    {"name": "TASK_LOAD_ORDERS", "schema_name": "SALES", "state": "started"},
]

_STREAMS = [
    {"name": "ORDERS_STREAM", "schema_name": "SALES", "mode": "DEFAULT"},
]

_TAGS = [
    {"name": "PII", "database_name": "SNOWDEMO", "schema_name": "SALES",
     "kind": "TAG"},
]


def _schema_in(flat: str) -> str:
    """The schema name out of `... in schema "SNOWDEMO"."<schema>" ...`."""
    for schema in _SCHEMAS:
        if f'"{schema.lower()}"' in flat or f".{schema.lower()}" in flat:
            return schema
    raise ValueError(f"emulation: no known schema in: {flat[:160]}")


def demo_run_sql(sql: str, params: dict | None = None) -> list[dict]:
    """Answer one read-only query against the emulated estate.

    Raises on anything unrecognised: a fake that improvises answers is worse
    than no fake, because the pipeline would then report an estate nobody
    defined.
    """
    flat = " ".join(sql.split()).lower()
    p = params or {}

    if "current_user()" in flat:
        return [dict(_SESSION)]
    if flat.startswith("show databases"):
        return [{"name": DEMO_DB}]
    if "show schemas in database" in flat:
        return [{"name": s} for s in _SCHEMAS]
    if "show tables in schema" in flat:
        return [dict(r) for r in _SHOW_TABLES[_schema_in(flat)]]
    if "show views in schema" in flat:
        return [dict(r) for r in _SHOW_VIEWS[_schema_in(flat)]]
    if "information_schema.columns" in flat:
        schema = p.get("schema")
        return [{**c, "TABLE_SCHEMA": schema}
                for c in _COLUMNS.get(schema, [])]
    if "get_ddl" in flat:
        name = str(p.get("f", "")).replace('"', "").rsplit(".", 1)[-1]
        return [{"D": _VIEW_SQL[name]}]

    # --- census -----------------------------------------------------------
    if "information_schema.procedures" in flat:
        return [dict(r) for r in _PROCEDURES]
    if "information_schema.functions" in flat:
        return [dict(r) for r in _FUNCTIONS]
    if ("information_schema.sequences" in flat
            or "information_schema.stages" in flat
            or "information_schema.file_formats" in flat
            or "information_schema.pipes" in flat):
        return []
    if "show tasks in database" in flat:
        return [dict(r) for r in _TASKS]
    if "show streams in database" in flat:
        return [dict(r) for r in _STREAMS]
    if ("show materialized views in database" in flat
            or "show dynamic tables in database" in flat):
        return []
    # One alert, so CENSUS.md teaches the lesson it can now teach: an alert
    # that watched a migrated table stops firing at cutover, unannounced.
    if "show alerts in database" in flat:
        return [{"name": "LOW_STOCK_ALERT", "database_name": DEMO_DB,
                 "schema_name": "SALES", "state": "started",
                 "condition": "select 1 from SNOWDEMO.SALES.ORDERS"}]
    if ("show secrets in database" in flat
            or "show network rules in database" in flat
            or "show streamlits in database" in flat
            or "show notebooks in database" in flat
            or "show services in database" in flat):
        return []
    # Constraints: one primary key, so DDL_PLAN.md's R20 names a real one.
    if "show primary keys in database" in flat:
        return [{"database_name": DEMO_DB, "schema_name": "SALES",
                 "table_name": "ORDERS", "column_name": "ORDER_ID",
                 "key_sequence": 1, "constraint_name": "ORDERS_PK",
                 "rely": "false"}]
    if ("show unique keys in database" in flat
            or "show imported keys in database" in flat):
        return []
    # Account-scoped reads, issued once per run. One outbound share: a live
    # contract with a consumer account, which finds out at cutover.
    if flat.startswith("show shares"):
        return [{"name": "SNOWDEMO_SALES_SHARE", "kind": "OUTBOUND",
                 "database_name": DEMO_DB, "to": "PARTNER_ACCOUNT",
                 "owner": "ACCOUNTADMIN"}]
    if (flat.startswith("show roles") or flat.startswith("show network policies")
            or flat.startswith("show applications")
            or flat.startswith("show compute pools")):
        return []

    # --- lineage / compute / security / maintenance ------------------------
    if "object_dependencies" in flat:
        return [dict(r) for r in _DEPENDENCY_ROWS]
    if flat.startswith("show warehouses"):
        return [dict(r) for r in _WAREHOUSES]
    if "warehouse_metering_history" in flat:
        return [dict(r) for r in _METERING]
    if "show masking policies" in flat:
        return [{"name": "MASK_EMAIL", "database_name": DEMO_DB,
                 "schema_name": "SALES", "kind": "MASKING_POLICY"}]
    if "show row access policies" in flat:
        return []
    # Enumerated, and empty: the report may say so because it asked.
    if "show aggregation policies" in flat or "show projection policies" in flat:
        return []
    if "show tags" in flat:
        return [dict(r) for r in _TAGS]
    if "tag_references" in flat:
        return [dict(r) for r in _TAG_REFS]
    if "policy_references" in flat:
        return [dict(r) for r in _POLICY_REFS]
    if "grants_to_roles" in flat:
        return [dict(r) for r in _GRANTS]
    if "show parameters like 'max_data_extension_time_in_days'" in flat:
        return [{"value": "14", "level": "ACCOUNT"}]
    if "show parameters like 'data_retention_time_in_days'" in flat:
        # Account, database and schema all agree on 1 day; ORDERS' effective 7
        # then reads as a table-level override, which is the signal.
        level = ("ACCOUNT" if "in account" in flat else "")
        return [{"value": "1", "level": level}]
    if "automatic_clustering_history" in flat:
        return [dict(r) for r in _CLUSTERING_HISTORY]
    if "table_dml_history" in flat:
        return [dict(r) for r in _DML_HISTORY]

    # --- smoke -------------------------------------------------------------
    if "information_schema.tables" in flat and flat.startswith("select count"):
        return [{"N": len(_SHOW_TABLES["SALES"])}]

    raise ValueError(f"the emulated Snowflake has no answer for: {flat[:200]}")
