"""User-supplied migration restrictions. Pure, zero I/O.

Restrictions narrow the estate before planning: exclude a database, cap object
size, skip a naming convention, drop a whole object type. Every exclusion records
WHICH restriction fired and a brief reason, because the plan has to explain what
cannot be migrated and why -- an object that silently vanished from the plan is
indistinguishable from one that was never there.

An unrecognised restriction key is an ERROR, not an ignored line. A typo'd key
would apply nothing while appearing to succeed.
"""
from __future__ import annotations

import re

__all__ = ["InvalidRestriction", "SCHEMA", "apply_restrictions",
           "validate_restrictions"]


class InvalidRestriction(ValueError):
    """A restriction key is unknown, or its value has the wrong type."""


# key -> expected python type
SCHEMA: dict[str, type] = {
    "include_databases": list,
    "exclude_databases": list,
    "include_schemas": list,
    "exclude_schemas": list,
    "include_object_types": list,
    "exclude_object_types": list,
    "include_objects": list,
    "exclude_objects": list,
    "include_name_patterns": list,
    "exclude_name_patterns": list,
    "max_rows": int,
    "max_bytes": int,
}


def validate_restrictions(restrictions: dict | None) -> dict:
    if not restrictions:
        return {}
    for key, value in restrictions.items():
        if key not in SCHEMA:
            raise InvalidRestriction(
                f"unknown restriction {key!r}; expected one of {sorted(SCHEMA)}")
        expected = SCHEMA[key]
        if expected is list and not isinstance(value, list):
            raise InvalidRestriction(f"{key!r} must be a list, got "
                                     f"{type(value).__name__}")
        if expected is int and not isinstance(value, int):
            raise InvalidRestriction(f"{key!r} must be an integer, got "
                                     f"{type(value).__name__}")
    for key in ("include_name_patterns", "exclude_name_patterns"):
        for pattern in restrictions.get(key) or []:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise InvalidRestriction(
                    f"{key} entry {pattern!r} is not a valid regex: {exc}") from exc
    return restrictions


def _upper(values) -> set[str]:
    return {str(v).upper() for v in values or []}


def _exclusion(rec: dict, restriction: str, reason: str) -> dict:
    return {"source_identifier": rec["source_identifier"],
            "object_type": rec.get("object_type"),
            "restriction": restriction, "reason": reason}


def apply_restrictions(records: list[dict],
                       restrictions: dict | None) -> tuple[list[dict], list[dict]]:
    """Split records into (kept, excluded). Each exclusion carries its reason."""
    r = validate_restrictions(restrictions)
    if not r:
        return list(records), []

    inc_db, exc_db = _upper(r.get("include_databases")), _upper(r.get("exclude_databases"))
    inc_sc, exc_sc = _upper(r.get("include_schemas")), _upper(r.get("exclude_schemas"))
    inc_ty, exc_ty = _upper(r.get("include_object_types")), _upper(r.get("exclude_object_types"))
    inc_ob, exc_ob = _upper(r.get("include_objects")), _upper(r.get("exclude_objects"))
    inc_pat = [re.compile(p) for p in r.get("include_name_patterns") or []]
    exc_pat = [re.compile(p) for p in r.get("exclude_name_patterns") or []]
    max_rows, max_bytes = r.get("max_rows"), r.get("max_bytes")

    kept, excluded = [], []
    for rec in records:
        ident = rec["source_identifier"]
        db = str(rec.get("source_database", "")).upper()
        schema = str(rec.get("source_schema", "")).upper()
        kind = str(rec.get("object_type", "")).upper()
        name = ident.rsplit(".", 1)[-1]

        if exc_ob and ident.upper() in exc_ob:
            excluded.append(_exclusion(rec, "exclude_objects",
                                       "explicitly excluded by the user"))
            continue
        if inc_ob and ident.upper() not in inc_ob:
            excluded.append(_exclusion(rec, "include_objects",
                                       "not in the user's include_objects list"))
            continue
        if exc_db and db in exc_db:
            excluded.append(_exclusion(rec, "exclude_databases",
                                       f"database {db} excluded by the user"))
            continue
        if inc_db and db not in inc_db:
            excluded.append(_exclusion(rec, "include_databases",
                                       f"database {db} is not in the include list"))
            continue
        if exc_sc and schema in exc_sc:
            excluded.append(_exclusion(rec, "exclude_schemas",
                                       f"schema {schema} excluded by the user"))
            continue
        if inc_sc and schema not in inc_sc:
            excluded.append(_exclusion(rec, "include_schemas",
                                       f"schema {schema} is not in the include list"))
            continue
        if exc_ty and kind in exc_ty:
            excluded.append(_exclusion(rec, "exclude_object_types",
                                       f"object type {kind} excluded by the user"))
            continue
        if inc_ty and kind not in inc_ty:
            excluded.append(_exclusion(rec, "include_object_types",
                                       f"object type {kind} is not in the include list"))
            continue

        hit = next((p for p in exc_pat if p.search(name)), None)
        if hit:
            excluded.append(_exclusion(rec, "exclude_name_patterns",
                                       f"name matches excluded pattern {hit.pattern!r}"))
            continue
        if inc_pat and not any(p.search(name) for p in inc_pat):
            excluded.append(_exclusion(rec, "include_name_patterns",
                                       "name matches no include pattern"))
            continue

        rows = rec.get("row_count_exact")
        if max_rows is not None and rows is not None and rows > max_rows:
            excluded.append(_exclusion(rec, "max_rows",
                                       f"{rows} rows exceeds max_rows {max_rows}"))
            continue
        byts = (rec.get("source_metadata") or {}).get("bytes")
        if max_bytes is not None and byts is not None and byts > max_bytes:
            excluded.append(_exclusion(rec, "max_bytes",
                                       f"{byts} bytes exceeds max_bytes {max_bytes}"))
            continue

        kept.append(rec)
    return kept, excluded
