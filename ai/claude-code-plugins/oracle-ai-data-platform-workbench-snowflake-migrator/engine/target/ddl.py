"""Spark/Delta DDL generation. Pure functions, zero I/O.

Every transformation is attributable to a named rule, and every dropped property
is recorded. A migration is reviewable only if each change to a schema can be
traced to the rule that made it, so the audit trail is part of the output rather
than a debugging aid.

Two AIDP-specific behaviours are encoded here rather than rediscovered:
  * CREATE SCHEMA ... COMMENT silently fails to persist -- specifically when the
    comment contains ISO-timestamp colons -- so COMMENT is never emitted on a
    schema create.
  * USING DELTA is always explicit, so the managed-table format does not depend
    on a cluster default.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from snowflake_source.dialect import lexer

from snowflake_source.dialect.views import (  # noqa: F401  (re-exported)
    detect_unsupported_constructs, extract_view_body, translate_view_body,
)

__all__ = ["RuleApplication", "RewriteResult", "UnsupportedDDL",
           "SCRUBBED_PROPERTIES", "DEFERRED_EQUIVALENT_PROPERTIES", "build_create_schema", "quote_backtick", "quote_spark_string", "build_create_table",
           "build_create_view", "build_ddl_payload", "uncarried_column_facts",
           "TARGET_REJECTED_COLUMN_TYPES", "unsupported_target_types"]

# COLUMN TYPES THE TARGET REJECTS AT `CREATE TABLE`, LIVE-VERIFIED.
#
# Not a style list and not a portability opinion: each entry is a type the
# AIDP metastore refused on a real run, paired with the remedy that cleared
# that refusal on the same estate. They are caught HERE -- in an offline stage, in under a
# second -- because the alternative is finding out from a job run, five to six
# minutes of cluster startup later, with the failure buried in a Java
# traceback partway down a thousand-line log.
#
# `TIMESTAMP_NTZ` is genuinely supported by Delta and by Spark 3.4+; it is the
# HIVE METASTORE behind the catalog that refuses it, with
# `InvalidObjectException: Invalid column type: timestamp_ntz`. So the type is
# not "wrong" -- it simply cannot be declared to this catalog, which is why
# the remedy is a documented mapping flag rather than a fix to the mapper.
TARGET_REJECTED_COLUMN_TYPES: dict[str, str] = {
    "TIMESTAMP_NTZ":
        "the AIDP Hive metastore refuses it with `InvalidObjectException: "
        "Invalid column type: timestamp_ntz` (live-verified 2026-09-19). "
        "Re-run `ingest`/`assess` with `--timestamp-ntz timestamp`. That is a "
        "SEMANTIC DOWNGRADE, not a rename: Spark TIMESTAMP is an instant read "
        "through the session timezone, while TIMESTAMP_NTZ is wall-clock with "
        "no zone, so the same value can read back differently under a "
        "different session. The mapper records it as a warning for that "
        "reason -- decide it, do not inherit it.",
}


def unsupported_target_types(statements: list[dict]) -> list[dict]:
    """Column types in `statements` that the target will refuse.

    Reads the EMITTED sql, not the source inventory: the question is what this
    plan would actually declare, after every mapping flag has been applied.
    """
    found: list[dict] = []
    for stmt in statements:
        sql = stmt.get("sql") or ""
        if "CREATE TABLE" not in sql.upper():
            continue
        for type_name, remedy in TARGET_REJECTED_COLUMN_TYPES.items():
            # Anchored on the backtick-quoted column the emitter writes, so a
            # type NAMED in a comment or a rule note is not a false positive.
            hits = re.findall(r"`(\w+)`\s+" + type_name + r"\b", sql)
            if hits:
                found.append({"target_fqn": stmt.get("target_fqn"),
                              "source_identifier": stmt.get(
                                  "source_identifier"),
                              "type": type_name, "columns": hits,
                              "remedy": remedy})
    return found

# Snowflake table PROPERTIES that genuinely have NO AIDP equivalent. A value
# here was a deliberate source-side setting, so dropping it is a decision worth
# reporting -- but it is a decision with nowhere to go.
SCRUBBED_PROPERTIES = (
    "is_iceberg", "is_dynamic", "is_secure",
    "max_data_extension_time_in_days",
)

# Properties that DO have an AIDP equivalent, which this version does not
# apply. These were previously reported as "dropped, no Delta equivalent",
# which is false for every one of them: telling a customer their clustering key
# has no equivalent invites them to accept a silent performance regression on
# their largest tables.
#
# They are DEFERRED, not dropped: named, carried into the report with the
# equivalent, and left for a maintenance decision. No maintenance DDL is
# emitted here -- see references/maintenance-and-layout.md.
DEFERRED_EQUIVALENT_PROPERTIES = {
    "cluster_by": (
        "Delta liquid clustering (`CLUSTER BY`) or `OPTIMIZE … ZORDER BY`. "
        "Neither is automatic: Snowflake reclusters in the background, AIDP "
        "needs a scheduled job"),
    "retention_time": (
        "`delta.deletedFileRetentionDuration` + `delta.logRetentionDuration`, "
        "which bound how far `VERSION AS OF` / `TIMESTAMP AS OF` can reach"),
    "data_retention_time_in_days": (
        "`delta.deletedFileRetentionDuration` + `delta.logRetentionDuration` "
        "(same setting as retention_time)"),
    "change_tracking": (
        "Delta Change Data Feed (`delta.enableChangeDataFeed`)"),
}

# Observational SHOW metadata. Never emitted either, but it was never a property
# to preserve, so listing it as "dropped" is misleading noise in the report.
_INFORMATIONAL_METADATA = ("rows", "bytes", "created_on", "owner", "comment")

# Values that mean "this property is not set" and so are not worth reporting.
_UNSET = (None, "", "false", "FALSE", "N", "OFF", "null", "NULL")


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
    # The columns this statement intends to create, in order. Carried so that
    # deployment can verify the STRUCTURE that arrived rather than only that
    # something with the right name exists.
    expected_columns: list[dict] = field(default_factory=list)
    # Source settings with a real AIDP equivalent that this version does not
    # apply. Distinct from omitted_properties, which have nowhere to go.
    deferred_properties: list[dict] = field(default_factory=list)


class UnsupportedDDL(Exception):
    def __init__(self, rule_id: str, message: str):
        self.rule_id = rule_id
        super().__init__(f"{rule_id}: {message}")


def quote_backtick(identifier: str) -> str:
    """A backtick-quoted Spark identifier, with embedded backticks doubled."""
    return "`" + identifier.replace("`", "``") + "`"


def quote_spark_string(value: str) -> str:
    """A single-quoted Spark string literal, escaped the way Spark expects.

    Spark escapes with a BACKSLASH. Doubling the quote -- correct in Snowflake
    and in standard SQL -- is not an escape here: Spark reads `\'it\'\'s\'` as
    two adjacent literals and concatenates them, so a comment of "Customer's
    orders" silently became "Customers orders". Backslash first, so an escape
    we add is not itself re-escaped.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


_q = quote_backtick


def _qualify(fqn: str) -> str:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"target_fqn must be three-part catalog.schema.table: {fqn!r}")
    return ".".join(_q(p) for p in parts)


def build_create_schema(catalog: str, schema: str) -> str:
    # R14: no COMMENT here, ever. See module docstring.
    return f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}"


# Column facts Snowflake holds that this version does NOT put in the target
# DDL, because no offline evidence says the AIDP target accepts them.
#
# Delta supports `GENERATED ALWAYS AS IDENTITY` and column `DEFAULT` on recent
# versions, but both are table-feature-gated and neither has been verified
# against this catalog. Emitting them would risk a CREATE the target rejects,
# or one it accepts and silently drops. So they are CAPTURED and WARNED, never
# invented: after cutover an insert Snowflake would have populated arrives NULL
# or fails, and that has to be a decision somebody made.
def _column_fact_pairs(record: dict) -> list[tuple[str, str, str]]:
    """`(kind, column, sentence)` for every DEFAULT / IDENTITY on a record."""
    out: list[tuple[str, str, str]] = []
    for c in sorted(record.get("columns") or [],
                    key=lambda c: c.get("ORDINAL_POSITION") or 0):
        name = c.get("COLUMN_NAME")
        default = c.get("COLUMN_DEFAULT")
        start, step = c.get("IDENTITY_START"), c.get("IDENTITY_INCREMENT")
        if default not in (None, ""):
            out.append(("DEFAULT", str(name),
                f"{name}: column DEFAULT {default} is NOT carried to the "
                f"target. Delta can express a default, but nothing offline "
                f"confirms this catalog accepts one, so it is recorded rather "
                f"than emitted: after cutover an INSERT that omits this "
                f"column arrives NULL (or fails, if the column is NOT NULL) "
                f"where Snowflake would have supplied the default."))
        if start not in (None, "") or step not in (None, ""):
            out.append(("IDENTITY", str(name),
                f"{name}: IDENTITY / AUTOINCREMENT (start {start}, increment "
                f"{step}) is NOT carried to the target. Delta's GENERATED "
                f"ALWAYS AS IDENTITY is the nearest equivalent and is "
                f"unverified on this catalog, so no generator is created: "
                f"after cutover this key stops generating and every insert "
                f"must supply it."))
    return out


def uncarried_column_facts(record: dict) -> list[str]:
    """`COLUMN: sentence` warnings for DEFAULT / IDENTITY on one record."""
    return [text for _, _, text in _column_fact_pairs(record)]


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
            piece += " COMMENT " + quote_spark_string(c["COMMENT"])
        lines.append(piece)
        res.rules_applied.append(RuleApplication(
            "R03_TYPE_MAP",
            f'{c["COLUMN_NAME"]}: {c.get("DATA_TYPE")} -> {c["target_type"]}'))

    for prop, value in (record.get("source_metadata") or {}).items():
        if prop in _INFORMATIONAL_METADATA or value in _UNSET:
            continue
        if prop in DEFERRED_EQUIVALENT_PROPERTIES:
            res.deferred_properties.append({
                "property": prop, "value": value,
                "aidp_equivalent": DEFERRED_EQUIVALENT_PROPERTIES[prop]})
        elif prop in SCRUBBED_PROPERTIES:
            res.omitted_properties.append(f"{prop}={value}")
    if res.omitted_properties:
        res.rules_applied.append(RuleApplication(
            "R10_PROP_SCRUB",
            "dropped Snowflake properties with no AIDP equivalent: "
            + ", ".join(res.omitted_properties)))
    if res.deferred_properties:
        res.rules_applied.append(RuleApplication(
            "R11_MAINTENANCE_DEFERRED",
            "source settings with an AIDP equivalent that this version does NOT "
            "apply, carried into the maintenance decision instead: "
            + ", ".join(f'{d["property"]}={d["value"]}'
                        for d in res.deferred_properties)))

    if record.get("constraints"):
        res.rules_applied.append(RuleApplication(
            "R20_CONSTRAINTS_NOT_EMITTED",
            "Snowflake PK/FK/UNIQUE are unenforced metadata and Delta does not "
            "enforce them either; captured in the inventory, not emitted as DDL"))

    facts = _column_fact_pairs(record)
    res.warnings.extend(text for _, _, text in facts
                        if text not in res.warnings)
    defaults = [name for kind, name, _ in facts if kind == "DEFAULT"]
    identities = [name for kind, name, _ in facts if kind == "IDENTITY"]
    if defaults:
        res.rules_applied.append(RuleApplication(
            "R22_COLUMN_DEFAULT_NOT_EMITTED",
            f"column DEFAULT is read from the source and NOT emitted for "
            f"{', '.join(defaults)}: nothing offline confirms this catalog "
            f"accepts one. Inserts that omit these columns after cutover "
            f"arrive NULL."))
    if identities:
        res.rules_applied.append(RuleApplication(
            "R23_IDENTITY_NOT_EMITTED",
            f"IDENTITY / AUTOINCREMENT is read from the source and NOT "
            f"emitted for {', '.join(identities)}: Delta's GENERATED ALWAYS "
            f"AS IDENTITY is unverified on this catalog. The key stops "
            f"generating at cutover; every insert must supply it."))
    if record.get("column_facts_unknown"):
        res.warnings.append(
            "column DEFAULT / IDENTITY are UNKNOWN for this table, not absent: "
            "it was planned from a manifest whose discovery did not read them.")

    res.rules_applied.append(RuleApplication(
        "R30_USING_DELTA", "explicit USING DELTA so format is not cluster-default"))
    res.sql = (f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
               + ",\n".join(lines) + "\n)\nUSING DELTA")
    res.expected_columns = [{"name": c["COLUMN_NAME"], "type": c["target_type"]}
                            for c in columns]

    for c in columns:
        for w in record.get("warnings") or []:
            if w.startswith(c["COLUMN_NAME"] + ":") and w not in res.warnings:
                res.warnings.append(w)
    return res



# A one- or two-part name can only be read as a table where nothing else can
# appear: directly after FROM or JOIN. Strings and comments are blanked first,
# so text that merely looks like a reference is never rewritten.
_TABLE_REF = re.compile(
    r'(?i)\b(from|join)(\s+)'
    r'(?:("?)([A-Za-z_][\w$]*)\3\s*\.\s*)?'
    r'("?)([A-Za-z_][\w$]*)\5'
    r'(?![\w$."`])')
_NOT_A_TABLE = {"lateral", "select", "table", "unnest", "values"}


def _code_mask(sql: str) -> str:
    """`sql` with string and comment text blanked, offsets preserved."""
    return "".join(
        "".join("\n" if c == "\n" else " " for c in text)
        if kind in ("string", "comment") else text
        for kind, text in lexer.segments(sql))


def _rewrite_positional_refs(sql: str, name_map: dict[str, str], db: str,
                             schema: str) -> tuple[str, list[str], list[str]]:
    """Qualify the one- and two-part references Snowflake would resolve.

    Snowflake resolves `SCHEMA.NAME` against the view's own DATABASE and a
    bare `NAME` against the view's own SCHEMA, so both targets are known,
    not guessed. Nothing crosses that boundary: a bare ORDERS in ANALYTICS
    never becomes COMMERCE.ORDERS. Returns (sql, rewrites, unresolved).
    """
    two_part: dict[tuple[str, str], str] = {}
    bare: dict[str, str] = {}
    for src, tgt in name_map.items():
        parts = src.split(".", 2)
        if len(parts) != 3 or parts[0].upper() != db.upper():
            continue
        two_part[(parts[1].upper(), parts[2].upper())] = tgt
        if parts[1].upper() == schema.upper():
            bare[parts[2].upper()] = tgt

    def resolve(q_schema, part_schema, q_name, part_name):
        # A quoted part keeps its exact case; unquoted folds to upper, as in
        # Snowflake.
        s_key = part_schema if q_schema else (part_schema or "").upper()
        n_key = part_name if q_name else part_name.upper()
        return (two_part.get((s_key, n_key)) if part_schema
                else bare.get(n_key))

    edits, rewrites, unresolved = [], [], []
    for m in _TABLE_REF.finditer(_code_mask(sql)):
        q_schema, part_schema, q_name, part_name = m.group(3, 4, 5, 6)
        if not part_schema and part_name.lower() in _NOT_A_TABLE:
            continue
        written = sql[m.start(3) if part_schema else m.start(5):m.end()]
        target = resolve(q_schema, part_schema, q_name, part_name)
        if target is None:
            unresolved.append(written)
            continue
        replacement = ".".join(p if re.match(r"^[a-z_][a-z0-9_]*$", p)
                               else _q(p) for p in target.split("."))
        start = m.start(3) if part_schema else m.start(5)
        edits.append((start, m.end(), replacement))
        rewrites.append(f"{written} -> {target}")
    for start, end, text in reversed(edits):
        sql = sql[:start] + text + sql[end:]
    return sql, rewrites, list(dict.fromkeys(unresolved))


def build_create_view(record: dict, target_fqn: str,
                      name_map: dict[str, str] | None = None) -> RewriteResult:
    """Generate CREATE VIEW, or block with the reason it cannot be migrated."""
    res = RewriteResult(record["source_identifier"], target_fqn, None)
    res.rules_applied.append(RuleApplication(
        "R01_TARGET_NAME", f'{record["source_identifier"]} -> {target_fqn}'))

    meta = record.get("source_metadata") or {}
    if str(meta.get("is_secure", "")).lower() in ("true", "y", "yes"):
        res.blocked = True
        res.blocked_reason = ("Snowflake secure view: its definition and row "
                              "visibility rules have no Delta equivalent")
        return res
    if str(meta.get("is_materialized", "")).lower() in ("true", "y", "yes"):
        res.blocked = True
        res.blocked_reason = ("Snowflake materialized view: no AIDP equivalent; "
                              "rebuild as a table plus a refresh job")
        return res

    ddl = record.get("view_ddl_get_ddl") or record.get("view_text_show")
    if not ddl:
        res.blocked = True
        res.blocked_reason = ("no view SQL was captured during extraction; "
                             "GET_DDL and SHOW VIEWS both returned nothing")
        return res

    try:
        body = extract_view_body(ddl)
    except ValueError as exc:
        res.blocked = True
        res.blocked_reason = str(exc)
        return res

    translated = translate_view_body(body)
    if translated.unsupported:
        res.blocked = True
        res.blocked_reason = "Snowflake-only SQL: " + "; ".join(
            f'{u["construct"]} ({u["detail"]})' for u in translated.unsupported)
        return res

    for applied in translated.applied:
        res.rules_applied.append(RuleApplication(
            applied["rule_id"], f'{applied["construct"]}: {applied["detail"]}'))

    rewritten, changed = translated.sql, []
    for source_name, target_name in sorted((name_map or {}).items(),
                                           key=lambda kv: -len(kv[0])):
        if source_name != target_name and re.search(
                re.escape(source_name), rewritten, re.IGNORECASE):
            rewritten = re.sub(re.escape(source_name), target_name, rewritten,
                               flags=re.IGNORECASE)
            changed.append(f"{source_name} -> {target_name}")

    rewritten, positional, unresolved = _rewrite_positional_refs(
        rewritten, name_map or {}, str(record.get("source_database") or ""),
        str(record.get("source_schema") or ""))
    changed += positional
    if unresolved:
        res.rules_applied.append(RuleApplication(
            "R44_VIEW_REFS_UNRESOLVED",
            "left as written, outside the migration and with no target name: "
            + ", ".join(unresolved)))
        res.warnings.append(
            "unresolved view reference(s) " + ", ".join(unresolved)
            + ": not part of this migration, so there is no target name to "
            "qualify them with. The CREATE VIEW will fail on the target until "
            "they exist there.")

    if changed:
        res.rules_applied.append(RuleApplication(
            "R41_VIEW_REFS_REWRITTEN",
            "rewrote object references: " + ", ".join(changed)))
    else:
        res.rules_applied.append(RuleApplication(
            "R40_VIEW_REFS_IDENTITY",
            "bronze mirrors the source 1:1, so object references are unchanged"))

    if translated.applied:
        res.rules_applied.append(RuleApplication(
            "R43_VIEW_DIALECT_TRANSLATED",
            f"{len(translated.applied)} dialect rule(s) applied; every one is an "
            "exact rewrite"))
        res.warnings.append(
            f"View SQL was dialect-translated by "
            f"{len(translated.applied)} exact rule(s). Verify its result against "
            "the source before relying on it.")
    else:
        res.rules_applied.append(RuleApplication(
            "R42_VIEW_PORTABLE_SQL",
            "no Snowflake-only construct present; body carried over verbatim"))
        res.warnings.append(
            "View SQL was carried over unchanged. Verify its result against the "
            "source before relying on it.")
    res.sql = f"CREATE VIEW IF NOT EXISTS {_qualify(target_fqn)} AS\n{rewritten}"
    res.expected_columns = [
        {"name": c["COLUMN_NAME"], "type": c["target_type"]}
        for c in sorted(record.get("columns") or [],
                        key=lambda c: c.get("ORDINAL_POSITION") or 0)
        if c.get("target_type")]
    return res


def _view_text(sql: str | None) -> str:
    """The SELECT body of a generated CREATE VIEW, for the catalog API."""
    try:
        return extract_view_body(sql or "")
    except ValueError:
        return ""


def build_ddl_payload(inventory: dict, plan: dict) -> dict:
    """The whole `ddl` stage as a pure function: inventory + plan -> payload.

    Emits in wave order, so a view always follows the tables it reads. Shared
    by the CLI stage and the emulated (demo) pipeline, so the two cannot
    drift apart.
    """
    by_id = {r["source_identifier"]: r for r in inventory["inventory"]}
    name_map = plan.get("target_names", {})
    ordered = [i for wave in plan.get("waves", []) for i in wave]
    ordered += [i for i in plan.get("clone_targets", []) if i not in ordered]

    statements, blocked = [], []
    for ident in ordered:
        rec = by_id.get(ident)
        if rec is None:
            continue
        if rec.get("object_type") == "VIEW":
            res = build_create_view(rec, name_map[ident], name_map)
        else:
            res = build_create_table(rec, name_map[ident])
        if res.blocked:
            blocked.append({"source_identifier": ident,
                            "object_type": rec.get("object_type"),
                            "reason": res.blocked_reason})
            continue
        statements.append({
            "source_identifier": res.source_identifier,
            "object_type": rec.get("object_type"),
            "target_fqn": res.target_fqn, "sql": res.sql,
            "rules_applied": [asdict(r) for r in res.rules_applied],
            "warnings": res.warnings,
            "omitted_properties": res.omitted_properties,
            # Deployment verifies the structure against this, not just the name.
            "expected_columns": res.expected_columns,
            # Source settings with an AIDP equivalent that this version does
            # not apply. Reported, never silently invented.
            "deferred_properties": res.deferred_properties,
            # The catalog API takes a view's body as a field, not as CREATE
            # VIEW text, so it is carried separately.
            **({"view_text": _view_text(res.sql)}
               if rec.get("object_type") == "VIEW" else {})})

    return {"statements": statements, "blocked": blocked,
            # Checked on the way out so no caller can forget to ask: a plan
            # that cannot be created is worth knowing about before it is
            # handed to a workflow.
            "target_rejected": unsupported_target_types(statements),
            "bronze_catalog_prefix": plan.get("bronze_catalog_prefix")}
