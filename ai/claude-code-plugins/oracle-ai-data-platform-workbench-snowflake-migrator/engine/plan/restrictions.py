"""User-supplied migration restrictions. Pure, zero I/O.

Restrictions narrow the estate before planning: exclude a database, cap object
size, skip a naming convention, drop a whole object type. Every exclusion records
WHICH restriction fired and a brief reason, because the plan has to explain what
cannot be migrated and why -- an object that silently vanished from the plan is
indistinguishable from one that was never there.

An unrecognised restriction key is an ERROR, not an ignored line. A typo'd key
would apply nothing while appearing to succeed.

`include_objects` / `exclude_objects` entries follow Snowflake's own case rule:
an unquoted part folds to upper (`d.s.orders` is D.S.ORDERS), a double-quoted
part is case-sensitive (`"D"."S"."orders"` is only the lower-case object). That
is what lets a restriction resolve an identifier-case collision -- the plan
HALTs on ORDERS vs "orders" rather than guessing, and the operator defers one
twin by spelling it exactly. An unquoted entry matches every case-variant, as
it always did; its exclusion reason names the entry and says the match was
case-insensitive, and only a quoted, exact hit is reported as the operator's
explicit choice, so the report never blames a twin the operator did not name
on the operator.
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


def _split_quoted(entry: str) -> list[tuple[str, bool]]:
    """`d.s."Orders"` -> [("d", False), ("s", False), ("Orders", True)].

    A doubled quote inside a quoted part is not unescaped: assert_safe_identifier
    forbids `"` in a name, so no inventory object needs one.
    """
    parts: list[tuple[str, bool]] = []
    buf: list[str] = []
    in_quotes = was_quoted = False
    for ch in entry:
        if ch == '"':
            in_quotes = not in_quotes
            was_quoted = True
        elif ch == "." and not in_quotes:
            parts.append(("".join(buf), was_quoted))
            buf, was_quoted = [], False
        else:
            buf.append(ch)
    parts.append(("".join(buf), was_quoted))
    return parts


def _object_matcher(entries):
    """Build ident -> (entry, how) | None for include_objects/exclude_objects.

    An entry with no double quote is folded to upper on both sides, as before.
    An entry with quotes is compared part by part: a quoted part must match
    exactly, an unquoted part folds. `how` is the word the exclusion reason
    uses, so a case-insensitive hit on a collision twin is never reported as
    the operator's explicit choice.
    """
    folded: dict[str, str] = {}
    quoted: list[tuple[str, list[tuple[str, bool]]]] = []
    for entry in entries or []:
        text = str(entry)
        if '"' in text:
            quoted.append((text, _split_quoted(text)))
        else:
            folded[text.upper()] = text

    def match(ident: str):
        hit = folded.get(ident.upper())
        if hit is not None:
            return hit, "case-insensitively"
        parts = ident.split(".")
        for text, spec in quoted:
            if len(spec) == len(parts) and all(
                    (have == want) if exact else (have.upper() == want.upper())
                    for (want, exact), have in zip(spec, parts)):
                return text, "exactly"
        return None

    return match


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
    match_inc = _object_matcher(r.get("include_objects"))
    match_exc = _object_matcher(r.get("exclude_objects"))
    has_inc_ob = bool(r.get("include_objects"))
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

        hit = match_exc(ident)
        if hit:
            entry, how = hit
            # Only a quoted, exact hit is the operator's explicit choice; a
            # folded hit may be a case twin the operator never named.
            reason = (f"explicitly excluded by the user (exclude_objects entry "
                      f"{entry!r})" if how == "exactly" else
                      f"excluded by exclude_objects entry {entry!r}, which "
                      f"matched case-insensitively")
            excluded.append(_exclusion(rec, "exclude_objects", reason))
            continue
        if has_inc_ob and not match_inc(ident):
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
