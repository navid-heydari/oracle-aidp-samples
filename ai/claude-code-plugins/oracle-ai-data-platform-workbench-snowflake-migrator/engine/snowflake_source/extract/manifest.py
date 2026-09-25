"""Turn the in-AIDP discovery manifest into the operator-side inventory.

Runbook S6 discovers the estate from inside AIDP, as a workflow, and writes
`discovery_manifest.json`. Every planning stage -- `deps`, `plan`, `ddl`,
`summary` -- reads `inventory.json`, the shape the laptop-side `assess`
produces. Without this module the runbook says "discover in AIDP, then plan
from it" and the second half has nowhere to read from.

The translation is deliberately thin. It does NOT re-implement the type
mapper: it calls the same `map_type` that `catalog.py` calls, with the same
four raw INFORMATION_SCHEMA fields, so a column planned from a manifest and
the same column planned from a live `assess` reach byte-identical verdicts.
Two mappers that agree today would disagree after the first change to either.

What a manifest cannot carry:

  * **View SQL.** Discovery records a view's columns, not its definition, so
    a view arrives here with no text. It is recorded as such and the planner's
    existing `no_definition` path refuses it -- a view whose SQL nobody read
    is not a view anybody can translate. Views planned this way must come from
    a live `assess`, or discovery must be extended to capture `GET_DDL`.
  * **A session identity.** The manifest is written by the cluster, not by
    this process, so `session` is recorded as reported-by-discovery rather
    than invented here.
"""
from __future__ import annotations

import collections
import datetime

from ..dialect.identifiers import case_form, detect_collisions
from ..dialect.types import map_type

__all__ = ["inventory_from_manifest", "ManifestShapeError"]


class ManifestShapeError(ValueError):
    """The manifest is not the shape `00_discover_snowflake.py` writes."""


# The fields a manifest must carry beyond type and nullability, because the
# planning stages report on them. A manifest that carries none of them was
# written by an older discovery, which is a different fact from a column
# that genuinely has no default -- see `facts_recorded` below.
FACT_KEYS = ("column_default", "identity_start", "identity_increment",
             "comment")


def manifest_records_column_facts(manifest: dict) -> bool:
    """Did the discovery that wrote this manifest read the fact columns?

    True when any column carries any of them, or when discovery said so
    outright with `facts_recorded`. False means UNKNOWN, never "none".
    """
    for schema in manifest.get("schemas") or []:
        for key in ("tables", "views"):
            for obj in schema.get(key) or []:
                for col in obj.get("columns") or []:
                    if col.get("facts_recorded"):
                        return True
                    if any(col.get(k) is not None for k in FACT_KEYS):
                        return True
    return False


def _column_record(col: dict, *, semi_structured: str, geospatial: str,
                   timestamp_ntz: str) -> tuple[dict, object]:
    """One column, mapped. Returns (enriched column, mapping verdict)."""
    if "data_type" not in col:
        raise ManifestShapeError(
            f"column {col.get('name')!r} carries no `data_type`. This "
            f"manifest predates the raw-type fields; re-run discovery "
            f"(00_discover_snowflake.py) so the mapper reads the same "
            f"INFORMATION_SCHEMA values a live assess reads. Parsing the "
            f"formatted `type` string back apart would be a second, lossier "
            f"mapper.")
    m = map_type(col.get("data_type"),
                 precision=col.get("numeric_precision"),
                 scale=col.get("numeric_scale"),
                 char_length=col.get("character_maximum_length"),
                 semi_structured=semi_structured,
                 geospatial=geospatial,
                 timestamp_ntz=timestamp_ntz)
    enriched = {
        "COLUMN_NAME": col.get("name"),
        "DATA_TYPE": col.get("data_type"),
        "NUMERIC_PRECISION": col.get("numeric_precision"),
        "NUMERIC_SCALE": col.get("numeric_scale"),
        "CHARACTER_MAXIMUM_LENGTH": col.get("character_maximum_length"),
        "IS_NULLABLE": "YES" if col.get("nullable", True) else "NO",
        # Reported on by R22/R23 and carried into the CREATE TABLE. Absent
        # from an older manifest, which the caller records as unknown rather
        # than letting it read as "no default".
        "COLUMN_DEFAULT": col.get("column_default"),
        "IDENTITY_START": col.get("identity_start"),
        "IDENTITY_INCREMENT": col.get("identity_increment"),
        "COMMENT": col.get("comment"),
        "target_type": m.spark_type,
    }
    return enriched, m


def _record(db: str, schema: str, kind: str, obj: dict, *,
            semi_structured: str, geospatial: str, timestamp_ntz: str,
            notes: list[str], read_error: str | None = None) -> dict:
    name = obj["name"]
    blocked_reasons: list[str] = []
    warnings: list[str] = []
    type_notes: list[str] = []
    enriched: list[dict] = []

    for col in obj.get("columns") or []:
        col_rec, m = _column_record(col, semi_structured=semi_structured,
                                    geospatial=geospatial,
                                    timestamp_ntz=timestamp_ntz)
        if m.blocked:
            blocked_reasons.append(f'{col_rec["COLUMN_NAME"]}: {m.reason}')
        if m.warning:
            warnings.append(f'{col_rec["COLUMN_NAME"]}: {m.warning}')
        if m.note and m.note not in type_notes:
            type_notes.append(m.note)
        enriched.append(col_rec)

    # Discovery records an object whose columns it could not read in the
    # schema's `errors`. With no column here and such an error, the verdict
    # below would be computed over nothing -- the same `unassessed` case as
    # a failed column read in a live assess.
    unread = read_error if not enriched and read_error else None
    if not enriched:
        # Discovery records this as an error too; carrying it forward keeps
        # "we read it and it has no columns" distinct from "we never read it".
        notes.append(f"{db}.{schema}.{name}: the manifest carries no column "
                     f"for this object, so it is INCOMPLETE, not empty")

    rec = {
        "source_identifier": f"{db}.{schema}.{name}",
        "object_type": kind,
        "source_database": db,
        "source_schema": schema,
        "identifier_case_form": case_form(name),
        "migration_status": "discovered",
        "compatibility_status": ("unassessed" if unread else
                                 "blocked" if blocked_reasons else
                                 "supported"),
        "blocked_reasons": blocked_reasons,
        "warnings": warnings,
        "type_notes": type_notes,
        "evidence_location": "discovery_manifest.json (in-AIDP workflow)",
        "columns_read": "failed" if unread else "ok",
        "columns": enriched,
        # The keys are catalog._META_KEYS, spelled exactly as a live `assess`
        # writes them: restrictions.max_bytes, render_inventory, maintenance
        # and ddl all read the lowercase form. Under any other spelling the
        # size is invisible to every consumer and a size cap excludes nothing
        # while plan.json records it as applied.
        "source_metadata": {k: v for k, v in (
            ("rows", obj.get("source_rows")),
            ("bytes", obj.get("source_bytes"))) if v is not None},
    }
    if unread:
        rec["columns_read_error"] = str(unread)[:300]

    rows = obj.get("source_rows")
    if rows is None:
        rec.update({"row_count_exact": None,
                    "row_count_source": "not_counted",
                    "row_count_note": "the manifest carries no row count for "
                                      "this object"})
    else:
        rec.update({
            "row_count_exact": rows,
            # Same provenance class as a `SHOW` count: Snowflake's own
            # maintained number, not a COUNT(*). Never call it verified.
            "row_count_source": "show_metadata",
            "row_count_note": "Snowflake's maintained count, read from "
                              "INFORMATION_SCHEMA.TABLES.ROW_COUNT by the "
                              "in-AIDP discovery workflow. Not a COUNT(*); it "
                              "can lag very recent DML."})

    if kind == "VIEW":
        # A manifest has columns, never the definition. Say so: the planner
        # refuses a view with no SQL, which is the correct outcome -- silently
        # emitting a view record with no text would read as translatable.
        rec["view_text_show"] = None
        rec["view_ddl_get_ddl"] = None
        rec["view_ddl_error"] = (
            "the in-AIDP discovery manifest carries a view's COLUMNS but not "
            "its SQL, so this view cannot be dialect-translated from it. Plan "
            "views from a live `assess`, or extend discovery to capture "
            "GET_DDL.")
        notes.append(f"{db}.{schema}.{name}: view SQL absent from the "
                     f"manifest; the view cannot be translated from it")
    return rec


def inventory_from_manifest(manifest: dict, *, database: str,
                            semi_structured: str = "block",
                            geospatial: str = "block",
                            timestamp_ntz: str = "preserve") -> dict:
    """`discovery_manifest.json` -> the `inventory.json` shape, losslessly.

    `database` is required and is not guessed: the manifest is per-database
    and does not name the database it came from, and a wrong name here would
    silently produce a plan targeting the wrong catalog.
    """
    if not isinstance(manifest, dict) or "schemas" not in manifest:
        raise ManifestShapeError(
            "expected a manifest with a top-level `schemas` list, as written "
            f"by 00_discover_snowflake.py; got keys "
            f"{sorted(manifest)[:8] if isinstance(manifest, dict) else type(manifest).__name__}")
    if not database:
        raise ManifestShapeError(
            "inventory_from_manifest needs `database`: a manifest does not "
            "record which database it describes, and guessing it would aim "
            "the plan at the wrong catalog.")

    notes: list[str] = []
    inventory: list[dict] = []

    # A manifest from an older discovery carries no DEFAULT, IDENTITY or
    # COMMENT at all. Rendering that as "this column has no default" is the
    # same false negative the census rule exists to prevent, and it is the
    # quiet kind: the plan simply omits the warning.
    facts_known = manifest_records_column_facts(manifest)
    if not facts_known:
        notes.append(
            "column DEFAULT, IDENTITY and COMMENT are UNKNOWN for every "
            "object here, not absent: this manifest was written by a "
            "discovery that did not read them. R22/R23 cannot fire and no "
            "column comment can travel. Re-run "
            "00_discover_snowflake.py to capture them, or plan these "
            "objects from a live `assess`.")

    for schema in manifest.get("schemas") or []:
        sname = schema.get("name")
        if not sname:
            notes.append("a schema entry in the manifest carries no name and "
                         "was skipped rather than guessed")
            continue
        read_errors = {str(e.get("object")): str(e.get("error") or "")
                       for e in schema.get("errors") or []
                       if e.get("object") and e.get("error")}
        for kind, key in (("TABLE", "tables"), ("VIEW", "views")):
            for obj in schema.get(key) or []:
                if not obj.get("name"):
                    notes.append(f"{sname}: an object entry carries no name "
                                 f"and was skipped rather than guessed")
                    continue
                record = _record(
                    database, sname, kind, obj,
                    semi_structured=semi_structured, geospatial=geospatial,
                    timestamp_ntz=timestamp_ntz, notes=notes,
                    read_error=read_errors.get(str(obj["name"])))
                if not facts_known:
                    record["column_facts_unknown"] = True
                inventory.append(record)
        # Discovery's own per-schema errors are extraction notes here: an
        # object it could not read is absent from the inventory, and absence
        # must never read as "it does not exist".
        for err in schema.get("errors") or []:
            notes.append(f'{database}.{sname}.{err.get("object")}: '
                         f'{err.get("error")}')

    collisions = detect_collisions([r["source_identifier"] for r in inventory])
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "session": {"source": "in-AIDP discovery workflow",
                    "manifest_generated_at": manifest.get("generated_at")},
        "databases_in_scope": [database],
        "row_count_mode": "metadata",
        "semi_structured_mode": semi_structured,
        "geospatial_mode": geospatial,
        "timestamp_ntz_mode": timestamp_ntz,
        "object_count": len(inventory),
        "counts_by_type": dict(collections.Counter(
            r["object_type"] for r in inventory)),
        "identifier_case_collisions": collisions,
        "extraction_notes": notes,
        "inventory": inventory,
    }
