"""Snowflake type -> Spark/Delta type. Pure functions, zero I/O.

Precision and scale are always passed in from INFORMATION_SCHEMA.COLUMNS and are
never inferred from sampled data: NUMBER is Snowflake's default numeric type, and
getting its scale wrong does not raise -- it silently changes values.

An unmapped type is BLOCKED, never approximated.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["TypeMapping", "map_type"]


@dataclass(frozen=True)
class TypeMapping:
    spark_type: str | None
    blocked: bool = False
    reason: str | None = None
    warning: str | None = None


_DIRECT = {
    "TEXT": "STRING", "VARCHAR": "STRING", "CHAR": "STRING", "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "BINARY": "BINARY", "VARBINARY": "BINARY",
    "FLOAT": "DOUBLE", "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE",
    "DOUBLE": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "REAL": "DOUBLE",
    "TIME": "STRING",
}

_NUMERIC = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
            "SMALLINT", "TINYINT", "BYTEINT"}

_BLOCKED = {
    "VARIANT": "semi-structured; needs an explicit struct/map/array target design",
    "OBJECT": "semi-structured; needs an explicit struct/map target design",
    "ARRAY": "semi-structured; needs an explicit array target design",
    "GEOGRAPHY": "no Spark/Delta target type",
    "GEOMETRY": "no Spark/Delta target type",
}


def map_type(data_type: str, *, precision: int | None = None,
             scale: int | None = None, char_length: int | None = None) -> TypeMapping:
    if data_type is None:
        return TypeMapping(None, True, "missing data_type")
    key = " ".join(data_type.strip().upper().split())

    if key in _BLOCKED:
        return TypeMapping(None, True, f"{key}: {_BLOCKED[key]}")

    if key in _NUMERIC:
        if precision is None:
            return TypeMapping(
                None, True,
                f"{key} with no numeric_precision from INFORMATION_SCHEMA; "
                "refusing to guess precision")
        return TypeMapping(f"DECIMAL({precision},{scale if scale is not None else 0})")

    if key == "TIMESTAMP_NTZ":
        return TypeMapping("TIMESTAMP_NTZ")
    if key in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"):
        return TypeMapping(
            "TIMESTAMP",
            warning=f"{key} -> Spark TIMESTAMP: timezone semantics differ; "
                    "Spark TIMESTAMP is session-timezone-dependent")

    if key in _DIRECT:
        warning = None
        if _DIRECT[key] == "STRING" and char_length is not None:
            warning = (f"declared length {char_length} is not enforced by Delta; "
                       "recorded only")
        return TypeMapping(_DIRECT[key], warning=warning)

    return TypeMapping(None, True, f"unmapped Snowflake type: {key}")
