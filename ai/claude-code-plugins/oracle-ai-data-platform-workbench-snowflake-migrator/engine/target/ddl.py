"""Spark/Delta DDL generation. Pure functions, zero I/O.

Every transformation is attributable to a named rule, and every dropped property
is recorded. This audit shape is carried over from the Databricks migrator's
catalog_ddl_rewriter.py, which is the one part of it worth keeping.

Two AIDP-specific behaviours are encoded here rather than rediscovered:
  * CREATE SCHEMA ... COMMENT silently fails to persist -- specifically when the
    comment contains ISO-timestamp colons -- so COMMENT is never emitted on a
    schema create.
  * USING DELTA is always explicit, so the managed-table format does not depend
    on a cluster default.
"""
from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["RuleApplication", "RewriteResult", "UnsupportedDDL",
           "SCRUBBED_PROPERTIES", "build_create_schema", "build_create_table"]

# Snowflake table properties with no Delta equivalent. Recorded, never emitted.
SCRUBBED_PROPERTIES = (
    "cluster_by", "retention_time", "change_tracking", "is_iceberg", "is_dynamic",
    "is_secure", "max_data_extension_time_in_days", "data_retention_time_in_days",
    "owner", "rows", "bytes", "created_on",
)


@dataclass(frozen=True)
class RuleApplication:
    rule_id: str
    detail: str


@dataclass
class RewriteResult:
    source_identifier: str
    target_fqn: str
    sql: str | None
    rules_applied: list[RuleApplication] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    omitted_properties: list[str] = field(default_factory=list)
    blocked: bool = False
    blocked_reason: str | None = None


class UnsupportedDDL(Exception):
    def __init__(self, rule_id: str, message: str):
        self.rule_id = rule_id
        super().__init__(f"{rule_id}: {message}")


def _q(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _qualify(fqn: str) -> str:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"target_fqn must be three-part catalog.schema.table: {fqn!r}")
    return ".".join(_q(p) for p in parts)


def build_create_schema(catalog: str, schema: str) -> str:
    # R14: no COMMENT here, ever. See module docstring.
    return f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}"


def build_create_table(record: dict, target_fqn: str) -> RewriteResult:
    qualified = _qualify(target_fqn)
    res = RewriteResult(record["source_identifier"], target_fqn, None)
    res.rules_applied.append(RuleApplication(
        "R01_TARGET_NAME", f'{record["source_identifier"]} -> {target_fqn}'))
    res.rules_applied.append(RuleApplication(
        "R02_QUOTE_BACKTICK", "Snowflake double-quote identifiers -> Spark backticks"))

    if record.get("compatibility_status") == "blocked":
        res.blocked = True
        res.blocked_reason = "; ".join(record.get("blocked_reasons") or ["unspecified"])
        return res

    columns = sorted(record.get("columns") or [],
                     key=lambda c: c.get("ORDINAL_POSITION") or 0)
    if not columns:
        res.blocked = True
        res.blocked_reason = ("table has no columns visible to this role "
                              "(Delta-shared or insufficient privilege)")
        return res

    unmapped = [c["COLUMN_NAME"] + ": " + str(c.get("DATA_TYPE"))
                for c in columns if not c.get("target_type")]
    if unmapped:
        res.blocked = True
        res.blocked_reason = "unmapped column types: " + "; ".join(unmapped)
        return res

    lines = []
    for c in columns:
        piece = f'  {_q(c["COLUMN_NAME"])} {c["target_type"]}'
        if str(c.get("IS_NULLABLE", "YES")).upper() == "NO":
            piece += " NOT NULL"
        if c.get("COMMENT"):
            piece += " COMMENT '" + str(c["COMMENT"]).replace("'", "''") + "'"
        lines.append(piece)
        res.rules_applied.append(RuleApplication(
            "R03_TYPE_MAP",
            f'{c["COLUMN_NAME"]}: {c.get("DATA_TYPE")} -> {c["target_type"]}'))

    for prop, value in (record.get("source_metadata") or {}).items():
        if prop in SCRUBBED_PROPERTIES and value not in (None, "", "false"):
            res.omitted_properties.append(f"{prop}={value}")
    if res.omitted_properties:
        res.rules_applied.append(RuleApplication(
            "R10_PROP_SCRUB",
            "dropped Snowflake properties with no Delta equivalent: "
            + ", ".join(res.omitted_properties)))

    if record.get("constraints"):
        res.rules_applied.append(RuleApplication(
            "R20_CONSTRAINTS_NOT_EMITTED",
            "Snowflake PK/FK/UNIQUE are unenforced metadata and Delta does not "
            "enforce them either; captured in the inventory, not emitted as DDL"))

    res.rules_applied.append(RuleApplication(
        "R30_USING_DELTA", "explicit USING DELTA so format is not cluster-default"))
    res.sql = (f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
               + ",\n".join(lines) + "\n)\nUSING DELTA")

    for c in columns:
        for w in record.get("warnings") or []:
            if w.startswith(c["COLUMN_NAME"] + ":") and w not in res.warnings:
                res.warnings.append(w)
    return res
