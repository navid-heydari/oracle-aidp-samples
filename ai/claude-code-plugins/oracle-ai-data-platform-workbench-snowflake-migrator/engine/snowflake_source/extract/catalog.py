"""Read-only Snowflake estate inventory. I/O injected as `run_sql`.

Issues only SHOW / SELECT / GET_DDL. Cannot modify the estate.

Three things are captured HERE rather than reconstructed later, because they
cannot be recovered afterwards:
  * identifier_case_form -- SHOW output tells us which form was used
  * numeric precision/scale -- from INFORMATION_SCHEMA, never from sampled data
  * view SQL, verbatim -- both SHOW VIEWS.text and GET_DDL()

A per-object failure is recorded in extraction_notes and extraction continues.
"no objects" and "extraction failed" are different outcomes and must not be
conflated.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect.identifiers import case_form, detect_collisions
from ..dialect.types import map_type

__all__ = ["build_inventory", "SYSTEM_DBS"]

SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})

_META_KEYS = ("rows", "bytes", "created_on", "comment", "cluster_by", "is_dynamic",
              "is_iceberg", "is_secure", "is_materialized", "owner",
              "change_tracking", "retention_time")


def _jsonable(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return value


def build_inventory(run_sql: Callable[..., list[dict]],
                    databases: list[str] | None = None) -> dict:
    notes: list[str] = []
    session = run_sql(
        "select current_user() U, current_account() A, current_region() R, "
        "current_role() ROLE, current_warehouse() WH, current_version() V")[0]

    if not databases:
        databases = [r["name"] for r in run_sql("show databases")
                     if r["name"] not in SYSTEM_DBS]

    inventory: list[dict] = []
    for db in databases:
        try:
            schemas = [r["name"] for r in run_sql(f'show schemas in database "{db}"')
                       if r["name"] != "INFORMATION_SCHEMA"]
        except Exception as exc:
            notes.append(f"database {db}: {exc}")
            continue

        cols_by_obj: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
        try:
            for c in run_sql(
                    f'select table_schema, table_name, ordinal_position, column_name, '
                    f'data_type, is_nullable, numeric_precision, numeric_scale, '
                    f'character_maximum_length, datetime_precision, comment '
                    f'from "{db}".information_schema.columns '
                    f'order by table_schema, table_name, ordinal_position'):
                cols_by_obj[(c["TABLE_SCHEMA"], c["TABLE_NAME"])].append(c)
        except Exception as exc:
            notes.append(f'{db}.information_schema.columns: {exc}')

        for schema in schemas:
            for kind, show in (("TABLE", "tables"), ("VIEW", "views")):
                try:
                    objects = run_sql(f'show {show} in schema "{db}"."{schema}"')
                except Exception as exc:
                    notes.append(f"{db}.{schema} {show}: {exc}")
                    continue
                for obj in objects:
                    inventory.append(
                        _record(run_sql, db, schema, kind, obj,
                                cols_by_obj.get((schema, obj["name"]), [])))

    collisions = detect_collisions([r["source_identifier"] for r in inventory])
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "session": {k: _jsonable(v) for k, v in session.items()},
        "databases_in_scope": databases,
        "object_count": len(inventory),
        "counts_by_type": dict(collections.Counter(r["object_type"] for r in inventory)),
        "identifier_case_collisions": collisions,
        "extraction_notes": notes,
        "inventory": inventory,
    }


def _record(run_sql, db: str, schema: str, kind: str, obj: dict,
            columns: list[dict]) -> dict:
    name = obj["name"]
    blocked_reasons: list[str] = []
    warnings: list[str] = []

    enriched = []
    for c in columns:
        m = map_type(c.get("DATA_TYPE"),
                     precision=c.get("NUMERIC_PRECISION"),
                     scale=c.get("NUMERIC_SCALE"),
                     char_length=c.get("CHARACTER_MAXIMUM_LENGTH"))
        if m.blocked:
            blocked_reasons.append(f'{c["COLUMN_NAME"]}: {m.reason}')
        if m.warning:
            warnings.append(f'{c["COLUMN_NAME"]}: {m.warning}')
        enriched.append({**c, "target_type": m.spark_type})

    rec = {
        "source_identifier": f"{db}.{schema}.{name}",
        "object_type": kind,
        "source_database": db,
        "source_schema": schema,
        "identifier_case_form": case_form(name),
        "migration_status": "discovered",
        "compatibility_status": "blocked" if blocked_reasons else "supported",
        "blocked_reasons": blocked_reasons,
        "warnings": warnings,
        "evidence_location": f"show {kind.lower()}s in {db}.{schema}",
        "columns": enriched,
        "source_metadata": {k: _jsonable(obj[k]) for k in _META_KEYS if k in obj},
    }

    try:
        rec["row_count_exact"] = run_sql(
            f'select count(*) N from "{db}"."{schema}"."{name}"')[0]["N"]
    except Exception as exc:
        rec["row_count_exact"] = None
        rec["row_count_error"] = str(exc)[:200]

    if kind == "VIEW":
        rec["view_text_show"] = obj.get("text")
        try:
            rec["view_ddl_get_ddl"] = run_sql(
                "select get_ddl('view', %(f)s) D",
                {"f": f'"{db}"."{schema}"."{name}"'})[0]["D"]
        except Exception as exc:
            rec["view_ddl_error"] = str(exc)[:200]
        # MVP-1 clones tables only. The existing Databricks rewriter raises
        # UnsupportedDDL(R15_VIEW_DEFERRED) for every view; there is no prior art.
        rec["compatibility_status"] = "requires_manual_design"
        rec["risk_level"] = "high"
        rec["recommended_approach"] = (
            "View translation is out of MVP-1 scope. Source SQL captured verbatim "
            "so a later translation can be diffed against it.")
    return rec
