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

Two cost decisions, because an assessment must not be expensive:

  * ROW COUNTS default to SHOW metadata, which Snowflake maintains and serves
    for free and which is exact for a table. `COUNT(*)` is opt-in. On a VIEW a
    count has no metadata to read and must EXECUTE the view, so views are not
    counted unless asked -- on a wide join that is minutes of warehouse time
    per view, spent during what the user asked to be an assessment.
  * SHOW output is PAGINATED. SHOW caps at 10k rows, and a silently truncated
    inventory is the worst outcome available here: it looks complete.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer
from ..dialect.identifiers import case_form, detect_collisions
from ..dialect.types import map_type

__all__ = ["build_inventory", "SYSTEM_DBS", "ROW_COUNT_MODES", "SHOW_PAGE_SIZE"]

SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})

ROW_COUNT_MODES = ("metadata", "exact", "none")

# Snowflake truncates SHOW at 10k rows. Page just under it.
SHOW_PAGE_SIZE = 10_000

# Everything SHOW already hands us that we might need later. Free to capture,
# and the maintenance/layout group is the entire input to the maintenance
# assessment -- without it that question cannot even be asked.
_META_KEYS = ("rows", "bytes", "created_on", "comment", "owner",
              # layout and maintenance
              "cluster_by", "automatic_clustering", "change_tracking",
              "retention_time", "search_optimization",
              "search_optimization_bytes", "search_optimization_progress",
              # table kind, which changes what maintenance even applies
              "is_dynamic", "is_iceberg", "is_secure", "is_materialized",
              "is_external", "is_hybrid", "is_event", "is_immutable",
              "enable_schema_evolution")

# Deliberately NOT called exact. Snowflake maintains this count and it agrees
# with COUNT(*) for a settled standard table, but it can lag very recent DML
# and is not maintained for external tables. Labelling it "exact" would be the
# same overstatement as reporting a structure clone as a data clone.
_METADATA_COUNT_NOTE = (
    "Snowflake's maintained row count, from SHOW. Free to read, and agrees "
    "with COUNT(*) for a settled standard table; it can lag very recent DML "
    "and is not maintained for external tables. Use --row-counts exact for a "
    "verified COUNT(*).")

_VIEW_COUNT_NOTE = (
    "not counted: a view has no stored row count, so counting it means "
    "executing the view. Re-run with --row-counts exact to count views.")


def _jsonable(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return value


def _show_all(run_sql: Callable[..., list[dict]], statement: str) -> list[dict]:
    """Run a SHOW statement, following pages until it stops filling one.

    SHOW returns at most 10k rows. `LIMIT n FROM '<name>'` resumes after a
    given name, and SHOW orders by name, so paging is exact rather than
    best-effort.
    """
    rows: list[dict] = []
    cursor: str | None = None
    while True:
        page_sql = f"{statement} limit {SHOW_PAGE_SIZE}"
        if cursor is not None:
            page_sql += f" from '{lexer.like_literal(cursor)}'"
        page = run_sql(page_sql)
        rows.extend(page)
        if len(page) < SHOW_PAGE_SIZE:
            return rows
        last = page[-1].get("name")
        if not last or last == cursor:
            # No usable cursor: stop rather than loop forever, and say so.
            return rows
        cursor = last


def build_inventory(run_sql: Callable[..., list[dict]],
                    databases: list[str] | None = None, *,
                    row_counts: str = "metadata",
                    semi_structured: str = "block",
                    geospatial: str = "block",
                    timestamp_ntz: str = "preserve") -> dict:
    if row_counts not in ROW_COUNT_MODES:
        raise ValueError(
            f"unknown row_counts mode {row_counts!r}; expected one of "
            f"{list(ROW_COUNT_MODES)}")

    notes: list[str] = []
    session = run_sql(
        "select current_user() U, current_account() A, current_region() R, "
        "current_role() ROLE, current_warehouse() WH, current_version() V")[0]

    if not databases:
        databases = [r["name"] for r in _show_all(run_sql, "show databases")
                     if r["name"] not in SYSTEM_DBS]

    inventory: list[dict] = []
    for db in databases:
        try:
            schemas = [r["name"] for r
                       in _show_all(run_sql, f"show schemas in database {lexer.qualify(db)}")
                       if r["name"] != "INFORMATION_SCHEMA"]
        except Exception as exc:
            notes.append(f"database {db}: {exc}")
            continue

        for schema in schemas:
            columns = _columns(run_sql, db, schema, notes)
            for kind, show in (("TABLE", "tables"), ("VIEW", "views")):
                try:
                    objects = _show_all(
                        run_sql,
                        f"show {show} in schema {lexer.qualify(db, schema)}")
                except Exception as exc:
                    notes.append(f"{db}.{schema} {show}: {exc}")
                    continue
                for obj in objects:
                    inventory.append(
                        _record(run_sql, db, schema, kind, obj,
                                columns.get(obj["name"], []),
                                row_counts=row_counts, notes=notes,
                                semi_structured=semi_structured,
                                geospatial=geospatial,
                                timestamp_ntz=timestamp_ntz))

    collisions = detect_collisions([r["source_identifier"] for r in inventory])
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "session": {k: _jsonable(v) for k, v in session.items()},
        "databases_in_scope": databases,
        "row_count_mode": row_counts,
        "semi_structured_mode": semi_structured,
        "geospatial_mode": geospatial,
        "timestamp_ntz_mode": timestamp_ntz,
        "object_count": len(inventory),
        "counts_by_type": dict(collections.Counter(r["object_type"] for r in inventory)),
        "identifier_case_collisions": collisions,
        "extraction_notes": notes,
        "inventory": inventory,
    }


def _columns(run_sql, db: str, schema: str, notes: list[str]) -> dict[str, list[dict]]:
    """Column metadata for one schema, keyed by object name.

    Read per schema rather than per database: one unfiltered query over a large
    database's INFORMATION_SCHEMA.COLUMNS can exceed Snowflake's result limit
    and fail, taking every object's types with it.
    """
    by_obj: dict[str, list[dict]] = collections.defaultdict(list)
    try:
        rows = run_sql(
            f"select table_schema, table_name, ordinal_position, column_name, "
            f"data_type, is_nullable, numeric_precision, numeric_scale, "
            f"character_maximum_length, datetime_precision, comment "
            f"from {lexer.qualify(db)}.information_schema.columns "
            f"where table_schema = %(schema)s "
            f"order by table_name, ordinal_position", {"schema": schema})
    except Exception as exc:
        notes.append(f"{db}.{schema} columns: {exc}")
        return by_obj
    for c in rows:
        by_obj[c["TABLE_NAME"]].append(c)
    return by_obj


def _row_count(run_sql, db: str, schema: str, name: str, kind: str, *,
               row_counts: str, obj: dict, notes: list[str]) -> dict:
    """(count, source, note) for one object, per the chosen strategy."""
    if row_counts == "none":
        return {"row_count_exact": None, "row_count_source": "not_counted",
                "row_count_note": "row counts were not requested"}

    if row_counts == "metadata":
        if kind == "VIEW":
            return {"row_count_exact": None, "row_count_source": "not_counted",
                    "row_count_note": _VIEW_COUNT_NOTE}
        rows = obj.get("rows")
        if rows is None:
            return {"row_count_exact": None, "row_count_source": "not_counted",
                    "row_count_note": "SHOW returned no row count for this object"}
        return {"row_count_exact": rows, "row_count_source": "show_metadata",
                "row_count_note": _METADATA_COUNT_NOTE}

    try:
        value = run_sql(
            f"select count(*) N from {lexer.qualify(db, schema, name)}")[0]["N"]
    except Exception as exc:
        detail = str(exc)[:200]
        notes.append(f"{db}.{schema}.{name}: row count failed: {detail}")
        return {"row_count_exact": None, "row_count_source": "error",
                "row_count_note": detail}
    return {"row_count_exact": value, "row_count_source": "count_query",
            "row_count_note": "exact, from COUNT(*)"}


def _record(run_sql, db: str, schema: str, kind: str, obj: dict,
            columns: list[dict], *, row_counts: str, notes: list[str],
            semi_structured: str = "block", geospatial: str = "block",
            timestamp_ntz: str = "preserve") -> dict:
    name = obj["name"]
    blocked_reasons: list[str] = []
    warnings: list[str] = []
    type_notes: list[str] = []

    enriched = []
    for c in columns:
        m = map_type(c.get("DATA_TYPE"),
                     precision=c.get("NUMERIC_PRECISION"),
                     scale=c.get("NUMERIC_SCALE"),
                     char_length=c.get("CHARACTER_MAXIMUM_LENGTH"),
                     semi_structured=semi_structured,
                     geospatial=geospatial,
                     timestamp_ntz=timestamp_ntz)
        if m.blocked:
            blocked_reasons.append(f'{c["COLUMN_NAME"]}: {m.reason}')
        if m.warning:
            warnings.append(f'{c["COLUMN_NAME"]}: {m.warning}')
        # Notes are informational and must not raise the object's risk level,
        # so they are kept apart from warnings.
        if m.note and m.note not in type_notes:
            type_notes.append(m.note)
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
        "type_notes": type_notes,
        "evidence_location": f"show {kind.lower()}s in {db}.{schema}",
        "columns": enriched,
        "source_metadata": {k: _jsonable(obj[k]) for k in _META_KEYS if k in obj},
    }
    rec.update(_row_count(run_sql, db, schema, name, kind,
                          row_counts=row_counts, obj=obj, notes=notes))

    if kind == "VIEW":
        # Captured verbatim so a translation can be diffed against the source.
        # Migratability is decided by the dialect translator in plan/build.py,
        # not asserted here: this module reports what IS, not what we will do.
        rec["view_text_show"] = obj.get("text")
        try:
            rec["view_ddl_get_ddl"] = run_sql(
                "select get_ddl('view', %(f)s) D",
                {"f": lexer.qualify(db, schema, name)})[0]["D"]
        except Exception as exc:
            rec["view_ddl_error"] = str(exc)[:200]
            notes.append(f"{db}.{schema}.{name}: GET_DDL failed: {str(exc)[:200]}")
    return rec
