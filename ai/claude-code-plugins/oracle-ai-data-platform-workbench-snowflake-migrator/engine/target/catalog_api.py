"""Create AIDP structure through the catalog REST API. Pure body construction.

Why this exists: `POST /workspaces/<ws>/sql/execute` returns 404 -- verified
against a live DataLake -- so the SQL transport cannot create anything. Schema,
table and view CRUD is GA on the catalog API, and for a STRUCTURE-ONLY clone it
is the better transport anyway:

  * it needs NO Spark cluster, so nothing has to be started and nothing is
    billed for the clone
  * there is no session, so the "batch DDL is silently discarded when the
    session closes" behaviour the SQL deploy path was built around cannot
    happen
  * columns are posted as a FIELD LIST rather than rendered into CREATE TABLE
    text, so the structure is data rather than a string to be re-parsed

Two shapes were read off a real table in the target environment rather than
taken from documentation:

  * `fieldPrecision` and `fieldScale` are STRINGS, not numbers
  * `schemaKey` must be fully qualified `<catalog>.<schema>`; a bare schema
    returns 400 InvalidParameter

Body construction is pure and unit-tested. Nothing here performs I/O.
"""
from __future__ import annotations

import re

__all__ = ["UnmappableFieldType", "field_from_spark_type", "build_schema_body",
           "build_table_body", "build_view_body", "MANAGED_FORMAT"]

# Managed Delta, always. The environment's own tables report CSV in places;
# this plugin creates Delta and says so explicitly rather than inheriting a
# catalog default.
MANAGED_FORMAT = "DELTA"

# Spark type -> catalog fieldType. Lower-case, matching what the API returns.
_SIMPLE = {
    "STRING": "string",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BINARY": "binary",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP_NTZ": "timestamp_ntz",
    "INT": "int",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
}

_DECIMAL = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$", re.I)

# Verified live: the catalog API accepts timestamp, date, boolean, binary,
# double, bigint, int and float -- but NOT timestamp_ntz. A POST carrying it
# returns 202 Accepted and the asynchronous create then fails SILENTLY: the
# table never appears and no error is reported anywhere.
_SILENTLY_REJECTED = {
    "TIMESTAMP_NTZ": (
        "the catalog API accepts `timestamp` but not `timestamp_ntz`. A POST "
        "carrying it returns 202 Accepted and the create then fails SILENTLY "
        "-- the table never appears and nothing says why. Snowflake "
        "TIMESTAMP_NTZ is mapped to Spark TIMESTAMP_NTZ on purpose, because "
        "bare TIMESTAMP is session-timezone-dependent and the wrong choice "
        "shifts every timestamp. Downgrading is therefore a DECISION: pass "
        "timestamp_ntz_as_timestamp=True (CLI: --timestamp-ntz timestamp) to "
        "accept the timezone semantics change."),
}


class UnmappableFieldType(ValueError):
    """A Spark type with no catalog field equivalent. Refused, never guessed.

    A field type invented here would create a table whose columns silently
    differ from the plan, and the structure probe would then report a mismatch
    it could not explain.
    """


def field_from_spark_type(name: str, spark_type: str | None,
                          description: str | None = None, *,
                          timestamp_ntz_as_timestamp: bool = False) -> dict:
    """One `tableFields` entry for a column."""
    if not name:
        raise ValueError("column name is required")
    if not spark_type:
        raise UnmappableFieldType(f"{name}: no target type was resolved")

    text = " ".join(str(spark_type).strip().upper().split())
    field = {"fieldName": name, "fieldDescription": description}

    if text == "TIMESTAMP_NTZ":
        if not timestamp_ntz_as_timestamp:
            raise UnmappableFieldType(f"{name}: {_SILENTLY_REJECTED[text]}")
        field["fieldType"] = "timestamp"
        field["fieldDescription"] = (
            (description + " | " if description else "")
            + "was TIMESTAMP_NTZ in Snowflake; stored as timestamp because the "
              "catalog API has no timestamp_ntz. TIMEZONE SEMANTICS DIFFER: "
              "Spark timestamp is session-timezone-dependent.")
        return field

    match = _DECIMAL.match(text)
    if match:
        precision, scale = match.group(1), match.group(2) or "0"
        # Strings on purpose: that is what the API returns for these.
        field.update({"fieldType": "decimal", "fieldPrecision": precision,
                      "fieldScale": scale})
        return field

    if text in _SIMPLE:
        field["fieldType"] = _SIMPLE[text]
        return field

    raise UnmappableFieldType(
        f"{name}: Spark type {spark_type!r} has no catalog field equivalent. "
        f"Refusing to guess one -- a wrong field type creates a table that "
        f"silently differs from the plan.")


def _fields(columns: list[dict], *,
            timestamp_ntz_as_timestamp: bool = False) -> list[dict]:
    return [field_from_spark_type(
                c.get("name"), c.get("type"), c.get("description"),
                timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
            for c in columns]


def build_schema_body(catalog: str, schema: str,
                      description: str = "") -> dict:
    if not catalog or not schema:
        raise ValueError("catalog and schema are both required")
    return {"displayName": schema, "catalogName": catalog,
            "description": description}


def build_table_body(catalog: str, schema: str, table: str,
                     columns: list[dict], description: str = "", *,
                     timestamp_ntz_as_timestamp: bool = False) -> dict:
    """A managed Delta table with zero rows. Structure only, always."""
    if not (catalog and schema and table):
        raise ValueError("catalog, schema and table are all required")
    if not columns:
        raise ValueError(
            f"{table}: refusing to create a table with no columns")
    return {
        "displayName": table,
        "catalogKey": catalog,
        # Fully qualified: a bare schema returns 400 InvalidParameter.
        "schemaKey": f"{catalog}.{schema}",
        "description": description,
        "tableType": "MANAGED",
        "managedTableDefinition": {"managedTableDataFormat": MANAGED_FORMAT},
        "tableFields": _fields(
            columns, timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp),
        "partitionKeys": [],
    }


def build_view_body(catalog: str, schema: str, view: str, view_text: str,
                    columns: list[dict], description: str = "", *,
                    timestamp_ntz_as_timestamp: bool = False) -> dict:
    if not (catalog and schema and view):
        raise ValueError("catalog, schema and view are all required")
    if not (view_text or "").strip():
        raise ValueError(f"{view}: a view needs its defining SQL")
    return {
        "displayName": view,
        "catalogKey": catalog,
        "schemaKey": f"{catalog}.{schema}",
        "description": description,
        "viewText": view_text,
        "viewFields": _fields(
            columns,
            timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
        if columns else [],
    }
