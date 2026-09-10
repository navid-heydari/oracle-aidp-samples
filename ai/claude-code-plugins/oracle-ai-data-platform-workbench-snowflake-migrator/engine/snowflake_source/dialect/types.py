"""Snowflake type -> Spark/Delta type. Pure functions, zero I/O.

Precision and scale are always passed in from INFORMATION_SCHEMA.COLUMNS and are
never inferred from sampled data: NUMBER is Snowflake's default numeric type, and
getting its scale wrong does not raise -- it silently changes values.

An unmapped type is BLOCKED, never approximated.

Two switches exist because default-deny with no alternative is not a usable
tool. Semi-structured columns (VARIANT/OBJECT/ARRAY) are everywhere in a real
Snowflake estate -- order payloads, event bodies -- and ONE of them blocked the
whole table, with no way to proceed. So:

  * semi_structured="block" (default) -- a typed struct/map/array design is a
    decision to make with the customer, not one to guess
  * semi_structured="string"          -- carry the JSON as text, explicitly and
    loudly, and revisit it later

Geospatial types get their OWN switch: deciding to carry JSON as text is not
the same decision as carrying a geography as text.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["TypeMapping", "map_type", "SEMI_STRUCTURED_MODES", "GEOSPATIAL_MODES"]

SEMI_STRUCTURED_MODES = ("block", "string")
GEOSPATIAL_MODES = ("block", "string")


@dataclass(frozen=True)
class TypeMapping:
    spark_type: str | None
    blocked: bool = False
    reason: str | None = None
    warning: str | None = None
    # Informational only, and deliberately NOT a warning: notes do not raise an
    # object's risk level. A warning on every integer column would mark every
    # table MEDIUM and drown the warnings that matter.
    note: str | None = None


_DIRECT = {
    "TEXT": "STRING", "VARCHAR": "STRING", "CHAR": "STRING", "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "BINARY": "BINARY", "VARBINARY": "BINARY",
    "FLOAT": "DOUBLE", "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE",
    "DOUBLE": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "REAL": "DOUBLE",
}

_NUMERIC = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
            "SMALLINT", "TINYINT", "BYTEINT"}

_SEMI_STRUCTURED = {
    "VARIANT": "semi-structured; needs an explicit struct/map/array target design",
    "OBJECT": "semi-structured; needs an explicit struct/map target design",
    "ARRAY": "semi-structured; needs an explicit array target design",
}

_GEOSPATIAL = {
    "GEOGRAPHY": "no Spark/Delta target type",
    "GEOMETRY": "no Spark/Delta target type",
}

_SEMI_STRUCTURED_AS_STRING = (
    "carried as STRING: the JSON text is preserved verbatim, but this is NOT a "
    "typed mapping. Nothing on the target can address a field inside it, and "
    "any query using Snowflake path syntax will not work until a struct/map "
    "design is agreed. Deliberately deferred, not solved.")

_GEOSPATIAL_AS_STRING = (
    "carried as STRING: the value arrives as text (WKT/GeoJSON as Snowflake "
    "renders it) with no spatial type, index or predicate support on the "
    "target. Spatial queries will not work until a target design is agreed.")

# Snowflake integer aliases are all NUMBER(38,0).
_INTEGER_ALIASES = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}


def map_type(data_type: str, *, precision: int | None = None,
             scale: int | None = None, char_length: int | None = None,
             semi_structured: str = "block",
             geospatial: str = "block") -> TypeMapping:
    if semi_structured not in SEMI_STRUCTURED_MODES:
        raise ValueError(
            f"unknown semi_structured mode {semi_structured!r}; expected one of "
            f"{list(SEMI_STRUCTURED_MODES)}")
    if geospatial not in GEOSPATIAL_MODES:
        raise ValueError(
            f"unknown geospatial mode {geospatial!r}; expected one of "
            f"{list(GEOSPATIAL_MODES)}")
    if data_type is None:
        return TypeMapping(None, True, "missing data_type")
    key = " ".join(data_type.strip().upper().split())

    if key in _SEMI_STRUCTURED:
        if semi_structured == "block":
            return TypeMapping(None, True, f"{key}: {_SEMI_STRUCTURED[key]}")
        return TypeMapping("STRING", warning=f"{key} {_SEMI_STRUCTURED_AS_STRING}")

    if key in _GEOSPATIAL:
        if geospatial == "block":
            return TypeMapping(None, True, f"{key}: {_GEOSPATIAL[key]}")
        return TypeMapping("STRING", warning=f"{key} {_GEOSPATIAL_AS_STRING}")

    if key in _NUMERIC:
        if precision is None:
            return TypeMapping(
                None, True,
                f"{key} with no numeric_precision from INFORMATION_SCHEMA; "
                "refusing to guess precision")
        resolved = f"DECIMAL({precision},{scale if scale is not None else 0})"
        note = None
        if key in _INTEGER_ALIASES or (key in ("NUMBER", "DECIMAL", "NUMERIC")
                                       and not scale):
            note = (f"{key} -> {resolved}. Faithful: every Snowflake integer is "
                    f"NUMBER(38,0). Spark would normally use BIGINT here, so "
                    f"expect DECIMAL in the target schema and in any downstream "
                    f"cast. Fidelity was chosen over familiarity.")
        return TypeMapping(resolved, note=note)

    if key == "TIMESTAMP_NTZ":
        return TypeMapping("TIMESTAMP_NTZ")
    if key in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"):
        return TypeMapping(
            "TIMESTAMP",
            warning=f"{key} -> Spark TIMESTAMP: timezone semantics differ; "
                    "Spark TIMESTAMP is session-timezone-dependent")

    if key == "TIME":
        # Spark has no TIME type. STRING preserves the value but changes
        # ordering and comparison semantics, so it must not be silent.
        return TypeMapping(
            "STRING",
            warning="TIME -> STRING: Spark has no TIME type. The text is "
                    "preserved, but ordering, comparison and time arithmetic "
                    "become string operations on the target")

    if key in _DIRECT:
        warning = None
        if _DIRECT[key] == "STRING" and char_length is not None:
            warning = (f"declared length {char_length} is not enforced by Delta; "
                       "recorded only")
        return TypeMapping(_DIRECT[key], warning=warning)

    return TypeMapping(None, True, f"unmapped Snowflake type: {key}")
