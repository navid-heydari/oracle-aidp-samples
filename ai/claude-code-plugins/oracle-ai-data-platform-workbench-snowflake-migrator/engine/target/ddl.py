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

THE SQL AND THE COLUMN LIST ARE RENDERED FROM ONE SPEC. `expected_columns` is
what both execution paths apply -- the catalog-API body is built from it and
the in-AIDP structure notebook renders `CREATE TABLE` from it -- so it carries
every property the reviewed SQL shows, and the SQL is written from the same
dicts (`_column_spec` -> `render_column_sql`). It used to carry name and type
alone, which meant the `NOT NULL` and the column `COMMENT` an operator
approved in DDL_PLAN.md were dropped by BOTH appliers and the structure
verification then compared the same reduced shape and reported "verified".
A property one path cannot apply is named per object in `rules_applied`, which
is where DDL_PLAN.md lists them -- never silently reduced.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from snowflake_source.dialect import lexer
from snowflake_source.dialect.views import (  # noqa: F401  (re-exported)
    detect_unsupported_constructs, extract_view_body, extract_view_columns,
    translate_view_body,
)

__all__ = ["RuleApplication", "RewriteResult", "UnsupportedDDL",
           "SCRUBBED_PROPERTIES", "DEFERRED_EQUIVALENT_PROPERTIES", "build_create_schema", "quote_backtick", "quote_spark_string", "build_create_table",
           "build_create_view", "build_ddl_payload",
           "TARGET_REJECTED_COLUMN_TYPES", "unsupported_target_types",
           "render_column_sql", "uncarried_column_facts",
           "describe_constraints"]

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
        "Re-run `ddl --timestamp-ntz timestamp` (offline, re-mapped from "
        "inventory.json, no Snowflake re-read), or `assess`/`ingest` with "
        "`--timestamp-ntz timestamp` if INVENTORY.md should show the "
        "downgraded type too. That is a "
        "SEMANTIC DOWNGRADE, not a rename: Spark TIMESTAMP is an instant read "
        "through the session timezone, while TIMESTAMP_NTZ is wall-clock with "
        "no zone, so the same value can read back differently under a "
        "different session. The mapper records it as a warning for that "
        "reason -- decide it, do not inherit it.",
}


def unsupported_target_types(statements: list[dict]) -> list[dict]:
    """Column types in `statements` that the target will refuse.

    Reads what this plan would actually DECLARE, after every mapping flag has
    been applied: the statement's expected_columns, which build_ddl_plan fills
    from the same target_type the SQL was written from, or -- for a caller that
    hands over raw SQL -- the emitted text itself.
    """
    found: list[dict] = []
    for stmt in statements:
        sql = stmt.get("sql") or ""
        # Views carry their SOURCE column types in expected_columns and declare
        # none in their SQL, so they are out regardless of the path taken.
        if "CREATE TABLE" not in sql.upper():
            continue
        columns = stmt.get("expected_columns") or []
        for type_name, remedy in TARGET_REJECTED_COLUMN_TYPES.items():
            if columns:
                hits = [c["name"] for c in columns
                        if str(c.get("type") or "").upper() == type_name]
            else:
                # Anchored on the backtick-quoted column the emitter writes, so
                # a type NAMED in a comment or a rule note is not a false
                # positive. Any character may appear inside the backticks --
                # `(\w+)` missed every name with a space, hyphen or dot, and
                # truncated one with an embedded (doubled) backtick.
                hits = [h.replace("``", "`") for h in re.findall(
                    r"`((?:[^`]|``)+)`\s+" + re.escape(type_name) + r"\b", sql)]
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
    # The source object's COMMENT, carried so the transports can apply it.
    # Empty string, never None: it is passed straight to the catalog API's
    # `description`, which is a string field.
    description: str = ""
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


def _column_spec(c: dict) -> dict:
    """The one description of a planned column, for SQL and for both appliers.

    `nullable` and `description` are the properties the reviewed SQL shows;
    they are part of the spec so no applier can render less than the plan
    without the comparison catching it.
    """
    return {"name": c["COLUMN_NAME"], "type": c["target_type"],
            "nullable": str(c.get("IS_NULLABLE", "YES")).upper() != "NO",
            "description": c.get("COMMENT") or None}


def render_column_sql(spec: dict) -> str:
    """One column of a CREATE TABLE, from a spec entry.

    The in-AIDP structure notebook renders the same spec with the same rules
    (`_column_sql` in dataplane/01_create_structure.py); a parity test holds
    the two together, because the defect this fixes was exactly the two
    drifting apart.
    """
    piece = f'{_q(spec["name"])} {spec["type"]}'
    if not spec.get("nullable", True):
        piece += " NOT NULL"
    if spec.get("description"):
        piece += " COMMENT " + quote_spark_string(spec["description"])
    return piece


# Column facts Snowflake holds that this version does NOT put in the target
# DDL, because no offline evidence says the AIDP target accepts them.
#
# Delta supports `GENERATED ALWAYS AS IDENTITY` and column `DEFAULT` on
# recent versions, but both are table-feature-gated and neither has been
# verified against this catalog -- and the catalog API body has no field for
# either (`build_table_body`, catalog_api.py). Emitting them would risk a
# CREATE the target rejects, or, worse, one it accepts and silently drops.
# So they are CAPTURED and WARNED, never invented: after cutover an insert
# Snowflake would have populated arrives NULL or fails, and that has to be a
# decision somebody made rather than something they discover.
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
    """`COLUMN: sentence` warnings for DEFAULT / IDENTITY on one record.

    Shared with plan/build.py so DDL_PLAN.md and PLANNED_OBJECTS.md say the
    same thing about the same column.
    """
    return [text for _, _, text in _column_fact_pairs(record)]


def describe_constraints(constraints: list[dict] | None) -> str:
    """The constraints of one object, as one line for the R20 audit detail."""
    parts = []
    for c in constraints or []:
        text = f'{c.get("constraint_type")} ({", ".join(c.get("columns") or [])})'
        if c.get("references"):
            text += (f' -> {c["references"]}'
                     f'({", ".join(c.get("referenced_columns") or [])})')
        parts.append(text)
    return "; ".join(parts)


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
        # A failed INFORMATION_SCHEMA.COLUMNS read leaves the list empty too,
        # and guessing "privilege" for it sends the operator after grants
        # when the read timed out. The extractor records the failure; say it.
        error = record.get("columns_read_error")
        res.blocked_reason = (
            f"the column list could not be read from the source "
            f"(INFORMATION_SCHEMA.COLUMNS failed: {error}), so no structure "
            f"can be emitted" if error else
            "table has no columns visible to this role "
            "(Delta-shared or insufficient privilege)")
        return res

    unmapped = [c["COLUMN_NAME"] + ": " + str(c.get("DATA_TYPE"))
                for c in columns if not c.get("target_type")]
    if unmapped:
        res.blocked = True
        res.blocked_reason = "unmapped column types: " + "; ".join(unmapped)
        return res

    specs = [_column_spec(c) for c in columns]
    lines = ["  " + render_column_sql(s) for s in specs]
    for c in columns:
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
            "Snowflake PK/FK/UNIQUE are unenforced metadata and Delta does "
            "not enforce them either, so they are captured in the inventory "
            "and not emitted as DDL: "
            + describe_constraints(record["constraints"])
            + ". Snowflake has no CHECK constraint, so the one constraint "
              "class Delta would actually enforce has nothing to carry over."))

    # NOT NULL is in the SQL above and the structure-notebook path applies it.
    # The catalog-API path cannot: the body has no nullability field (see
    # catalog_api.build_table_body), so it is named here -- per object, next
    # to the rules DDL_PLAN.md prints -- rather than quietly dropped.
    not_null = [s["name"] for s in specs if not s["nullable"]]
    if not_null:
        res.rules_applied.append(RuleApplication(
            "R21_NOT_NULL_CATALOG_API_GAP",
            f"{len(not_null)} column(s) are NOT NULL in the source and in "
            f"this SQL ({', '.join(not_null)}). The in-AIDP structure "
            f"notebook applies them. The catalog-API transport (`snowmig "
            f"deploy --execute`) CANNOT: its table body has no nullability "
            f"field, so on that path those columns arrive NULLABLE. Create "
            f"this table with the structure notebook if the constraint "
            f"matters, or re-apply it afterwards."))

    facts = _column_fact_pairs(record)
    res.warnings.extend(text for _, _, text in facts
                        if text not in res.warnings)
    defaults = [name for kind, name, _ in facts if kind == "DEFAULT"]
    identities = [name for kind, name, _ in facts if kind == "IDENTITY"]
    if defaults:
        res.rules_applied.append(RuleApplication(
            "R22_COLUMN_DEFAULT_NOT_EMITTED",
            f"column DEFAULT is read from the source and NOT emitted for "
            f"{', '.join(defaults)}: Delta can express a default, but nothing "
            f"offline confirms this catalog accepts one, and a DDL the target "
            f"rejects is worse than a stated gap. Inserts that omit these "
            f"columns after cutover arrive NULL."))
    if identities:
        res.rules_applied.append(RuleApplication(
            "R23_IDENTITY_NOT_EMITTED",
            f"IDENTITY / AUTOINCREMENT is read from the source and NOT "
            f"emitted for {', '.join(identities)}: Delta's GENERATED ALWAYS "
            f"AS IDENTITY is unverified on this catalog. The key stops "
            f"generating at cutover; every insert must supply it."))

    description = str((record.get("source_metadata") or {}).get("comment")
                      or "")
    if description:
        res.description = description
        res.rules_applied.append(RuleApplication(
            "R24_TABLE_COMMENT_CARRIED",
            "the source table COMMENT is emitted on the CREATE TABLE and "
            "sent as the catalog API's `description`. A schema COMMENT is "
            "still never emitted (R14)."))

    res.rules_applied.append(RuleApplication(
        "R30_USING_DELTA", "explicit USING DELTA so format is not cluster-default"))
    res.sql = (f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
               + ",\n".join(lines) + "\n)\nUSING DELTA"
               + (f"\nCOMMENT {quote_spark_string(description)}"
                  if description else ""))
    res.expected_columns = specs

    for c in columns:
        for w in record.get("warnings") or []:
            if w.startswith(c["COLUMN_NAME"] + ":") and w not in res.warnings:
                res.warnings.append(w)
    return res



_PLAIN_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ref_part_pattern(part: str) -> str:
    # One part of a 3-part name as it may appear in a body: unquoted (Snowflake
    # folds it, so case-insensitive), or quoted -- Spark backticks after the
    # dialect pass, Snowflake double quotes before it -- which is exact-case,
    # because "orders" and ORDERS are different objects in Snowflake.
    backticked = re.escape("`" + part.replace("`", "``") + "`")
    double_quoted = re.escape('"' + part.replace('"', '""') + '"')
    return f"(?:(?-i:{backticked}|{double_quoted})|{re.escape(part)})"


# A bare or two-part name may only be read as a table where nothing else
# can appear. FROM and JOIN are those places in a view body.
_TABLE_POSITION = r"(?i)\b(from|join)(\s+)"


def _context_refs(name_map: dict[str, str], db: str, schema: str
                  ) -> dict[str, str]:
    """The one- and two-part forms of every mapped object in this view's own
    schema, which is where Snowflake resolves an unqualified reference.

    Only for the view's own database and schema: a bare `ORDERS` in a view
    in SALES cannot mean `MARKETING.ORDERS`, and guessing across schemas is
    how a view silently reads from the wrong table.
    """
    out: dict[str, str] = {}
    if not db or not schema:
        return out
    prefix = f"{db}.{schema}.".upper()
    for src, tgt in name_map.items():
        if not src.upper().startswith(prefix):
            continue
        name = src.split(".", 2)[2]
        out[f"{schema}.{name}"] = tgt
        out[name] = tgt
    return out


def _rewrite_view_refs(body: str, name_map: dict[str, str],
                       positional: dict[str, str] | None = None
                       ) -> tuple[str, list[str]]:
    """Rewrite whole 3-part object references in `body` per `name_map`.

    Matches run over code and identifier segments only. A 3-part name inside a
    string literal is DATA the view returns, and one inside a comment is prose;
    a plain re.sub over the body rewrote both. Each part must be whole: word
    boundaries stop `DB.S.ORDERS` from hitting `DB.S.ORDERS_ARCHIVE`, and a hit
    is blanked before shorter names are tried so nothing matches inside it.
    Returns the rewritten body and the `src -> tgt` pairs that actually hit.

    A bare name after FROM or JOIN that is one of the view's own CTEs, where
    that CTE is in scope, is the CTE and is left alone: rewriting `from
    ORDERS` to the base table when ORDERS is `with ORDERS as (... where
    STATUS = 'OPEN')` silently dropped the CTE's filter.
    """
    ctes = lexer.cte_scopes(body)
    mask = "".join(
        "".join("\n" if c == "\n" else " " for c in text)
        if kind in ("string", "comment") else text
        for kind, text in lexer.segments(body))
    edits: list[tuple[int, int, str]] = []
    changed: list[str] = []
    # Longest first, and the fully-qualified map before the positional one:
    # a three-part hit is unambiguous and blanks the span before any shorter
    # form is tried against it.
    ordered = [(s, t, False)
               for s, t in sorted(name_map.items(), key=lambda kv: -len(kv[0]))]
    ordered += [(s, t, True)
                for s, t in sorted((positional or {}).items(),
                                   key=lambda kv: -len(kv[0]))]
    for src, tgt, after_keyword in ordered:
        if src == tgt:
            continue
        ref = (r"\s*\.\s*".join(_ref_part_pattern(p) for p in src.split("."))
               + r'(?![\w`"$])')
        if after_keyword:
            # The keyword is matched so the span is unambiguous, and put back
            # verbatim so spacing and case are untouched.
            pattern = _TABLE_POSITION + ref
        else:
            pattern = r'(?<![\w`"$.])' + ref
        hits = [(m.span(), (m.group(1) + m.group(2)) if after_keyword else "")
                for m in re.finditer(pattern, mask, re.IGNORECASE)
                if not (after_keyword and "." not in src
                        and _is_cte(body, m.end(2), m.end(), ctes))]
        if not hits:
            continue
        # A target part the Spark parser would not read as one word (a hyphen
        # from a prefixed catalog name) is backticked; a plain one is emitted
        # as-is, so the common case stays byte-identical to the planned name.
        replacement = ".".join(p if _PLAIN_PART.match(p) else _q(p)
                               for p in tgt.split("."))
        for (start, end), keep in hits:
            edits.append((start, end, keep + replacement))
            mask = mask[:start] + " " * (end - start) + mask[end:]
        changed.append(f"{src} -> {tgt}")
    out = body
    for start, end, replacement in sorted(edits, reverse=True):
        out = out[:start] + replacement + out[end:]
    return out, changed


def _is_cte(sql: str, start: int, end: int,
            ctes: list[tuple[str, int, int]]) -> bool:
    """Whether the bare name at sql[start:end] is a CTE visible there."""
    text = sql[start:end]
    key = _cte_key(text)
    return any(name == key and lo <= start < hi for name, lo, hi in ctes)


def _cte_key(text: str) -> str:
    # Compared as lexer.cte_scopes records it: unquoted folds to upper case,
    # a quoted name (double quote or, after translation, backtick) is exact.
    if text[:1] in ('"', "`"):
        return text[1:-1].replace(text[0] * 2, text[0])
    return text.upper()


def _unqualified_refs(sql: str) -> set[str]:
    """Bare names still sitting where only a table can go -- other than the
    view's own CTE names, which the target resolves from the WITH clause."""
    ctes = lexer.cte_scopes(sql)
    mask = "".join(
        "".join("\n" if c == "\n" else " " for c in text)
        if kind in ("string", "comment") else text
        for kind, text in lexer.segments(sql))
    out = set()
    for m in re.finditer(_TABLE_POSITION + r'([\w$]+)(?![\w`"$.])', mask):
        name = m.group(3)
        if name.lower() in ("lateral", "select", "unnest", "values", "table"):
            continue
        if _is_cte(sql, m.start(3), m.end(3), ctes):
            continue
        out.add(name)
    return out


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
        view_columns = extract_view_columns(ddl)
        # Inside the guard on purpose: a translator that cannot read one view
        # blocks THAT view with the reason, it does not abort the stage.
        translated = translate_view_body(body)
    except ValueError as exc:
        res.blocked = True
        res.blocked_reason = str(exc)
        return res

    if translated.unsupported:
        res.blocked = True
        res.blocked_reason = "Snowflake-only SQL: " + "; ".join(
            f'{u["construct"]} ({u["detail"]})' for u in translated.unsupported)
        return res

    for applied in translated.applied:
        res.rules_applied.append(RuleApplication(
            applied["rule_id"], f'{applied["construct"]}: {applied["detail"]}'))
    # The type mapper's notes on a `::TIMESTAMP` cast travel with the view,
    # the same way a column's mapping warning travels with a table. (They are
    # also a caveat on the T02 application, so R43 below is not "exact".)
    res.warnings.extend(translated.warnings)

    positional = _context_refs(name_map or {},
                               str(record.get("source_database") or ""),
                               str(record.get("source_schema") or ""))
    rewritten, changed = _rewrite_view_refs(translated.sql, name_map or {},
                                            positional)

    # Anything still unqualified after FROM or JOIN is a reference the target
    # has no way to resolve. On the catalog-API transport that is a bare 500
    # with no detail, so it is named here instead of discovered there.
    leftover = _unqualified_refs(rewritten)
    if leftover:
        res.warnings.append(
            "unqualified object reference(s) remain in the view SQL: "
            + ", ".join(sorted(leftover))
            + ". They are not part of this migration, so there is no target "
              "name to qualify them with. A view whose body the target cannot "
              "resolve is rejected -- the catalog API answers 500 with no "
              "detail -- so create this view only once those objects exist "
              "and are named in full.")
        res.rules_applied.append(RuleApplication(
            "R42_VIEW_REFS_UNRESOLVED",
            "left unqualified, no target name known: "
            + ", ".join(sorted(leftover))))

    if changed:
        res.rules_applied.append(RuleApplication(
            "R41_VIEW_REFS_REWRITTEN",
            "rewrote object references: " + ", ".join(changed)))
    else:
        res.rules_applied.append(RuleApplication(
            "R40_VIEW_REFS_IDENTITY",
            "bronze mirrors the source 1:1, so object references are unchanged"))

    if translated.applied:
        n = len(translated.applied)
        # A rule that is exact only under a condition says so here, next to
        # the count, rather than letting the plan call the whole view exact.
        caveats = [f'{a["rule_id"]}: {a["caveat"]}'
                   for a in translated.applied if a.get("caveat")]
        if caveats:
            res.rules_applied.append(RuleApplication(
                "R43_VIEW_DIALECT_TRANSLATED",
                f"{n} dialect rule(s) applied; NOT all exact: "
                + "; ".join(caveats)))
            res.warnings.append(
                f"View SQL was dialect-translated by {n} rule(s), not all exact: "
                + "; ".join(caveats)
                + ". Confirm the operand types against the source before "
                "relying on it.")
        else:
            res.rules_applied.append(RuleApplication(
                "R43_VIEW_DIALECT_TRANSLATED",
                f"{n} dialect rule(s) applied; every one is an exact rewrite"))
            res.warnings.append(
                f"View SQL was dialect-translated by {n} exact rule(s). Verify "
                "its result against the source before relying on it.")
    else:
        # This is what was checked, no more: the rule table matched nothing.
        # A function outside the table (GREATEST/LEAST null handling, SPLIT's
        # regex separator, ZEROIFNULL) ships as written and is not parsed here.
        res.rules_applied.append(RuleApplication(
            "R42_VIEW_PORTABLE_SQL",
            "no known Snowflake-only construct matched; functions not in the "
            "rule table are carried verbatim and may fail or differ at Spark "
            "parse time"))
        res.warnings.append(
            "View SQL was carried over unchanged: no known Snowflake-only "
            "construct matched, and functions not in the rule table were not "
            "checked. Verify its result against the source before relying on it.")
    # The view's COMMENT is emitted here AND sent as the catalog API's
    # `description`, so the reviewed statement and the applied object carry
    # the same documentation rather than one of them quietly carrying less.
    res.description = str(meta.get("comment") or "")
    # The header column list RENAMES the body's output columns, so it is
    # emitted as written. Dropped, `V(CUSTOMER, TOTAL) as select CUST_ID,
    # SUM(AMT)` was created with columns CUST_ID and SUM(AMT) while the plan
    # and the catalog API's viewFields said CUSTOMER and TOTAL.
    if view_columns:
        res.rules_applied.append(RuleApplication(
            "R44_VIEW_COLUMN_LIST",
            "the source view's column list is carried: "
            + ", ".join(view_columns)
            + ". The catalog-API body has no column-list field, so its "
              "viewText names them with SELECT * FROM (<body>) AS "
              "named_columns(<list>)"))
    res.sql = (f"CREATE VIEW IF NOT EXISTS {_qualify(target_fqn)}"
               + (" (" + ", ".join(_q(c) for c in view_columns) + ")"
                  if view_columns else "")
               + (f" COMMENT {quote_spark_string(res.description)}"
                  if res.description else "")
               + f" AS\n{rewritten}")
    # Same four-key spec as a table's, so one shape travels the whole plan.
    # A view's column types are re-derived by the target from the SQL (see
    # catalog_deploy), which is why nothing here is compared as strictly.
    res.expected_columns = [
        _column_spec(c)
        for c in sorted(record.get("columns") or [],
                        key=lambda c: c.get("ORDINAL_POSITION") or 0)
        if c.get("target_type")]
    return res


def _view_text(sql: str | None) -> str:
    """The SELECT of a generated CREATE VIEW, for the catalog API.

    The API takes the query alone, so a column list in the CREATE VIEW has
    nowhere to go but into the query: the body is wrapped so its output
    columns carry the list's names, as they do in the reviewed SQL.

    Comments are removed. Live, the API refused a view carrying one --
    "inline SQL comments are not allowed" -- in any style, where Spark SQL
    takes it. A comment carries no meaning; the reviewed CREATE VIEW keeps
    it, and the lexer leaves a `--` inside a literal alone.
    """
    try:
        body = lexer.strip_comments(extract_view_body(sql or "")).strip()
        columns = extract_view_columns(sql or "")
    except ValueError:
        return ""
    if not columns:
        return body
    return (f"SELECT * FROM (\n{body}\n) AS named_columns("
            + ", ".join(_q(c) for c in columns) + ")")


def build_ddl_payload(inventory: dict, plan: dict) -> dict:
    """The whole `ddl` stage as a pure function: inventory + plan -> payload.

    Emits in wave order, so a view always follows the tables it reads. Shared
    by the CLI stage and the emulated (demo) pipeline, so the two cannot
    drift apart.

    A member of a dependency cycle is NOT emitted. PLANNED_OBJECTS.md lists
    those objects as excluded from the ordering pending a human decision, and
    the ddl stage used to re-add every un-waved clone target -- exactly the
    cycle members -- so the two artifacts contradicted each other and deploy
    attempted views whose dependency did not exist.
    """
    by_id = {r["source_identifier"]: r for r in inventory["inventory"]}
    name_map = plan.get("target_names", {})
    cycles = [list(c) for c in plan.get("cycles", [])]
    # Stuck, but in no cycle: it depends on one. Named as such -- it used to
    # be folded into the cycle and told it was in "dependency cycle with"
    # objects it only reads.
    behind = dict(plan.get("blocked_behind_cycle") or {})
    in_cycle = {n for c in cycles for n in c} | set(behind)
    ordered = [i for wave in plan.get("waves", []) for i in wave]
    ordered += [i for i in plan.get("clone_targets", [])
                if i not in ordered and i not in in_cycle]

    statements, blocked = [], []
    for ident in sorted(i for i in plan.get("clone_targets", []) if i in in_cycle):
        rec = by_id.get(ident) or {}
        if ident in behind:
            reason = ("not emitted: not in a cycle, but it depends on the "
                      "dependency cycle " + ", ".join(behind[ident])
                      + ", which cannot be ordered; PLANNED_OBJECTS.md lists "
                      "it as blocked behind that cycle, and no edge was "
                      "broken to force an order")
        else:
            others = sorted(n for c in cycles if ident in c
                            for n in c if n != ident)
            reason = ("not emitted: dependency cycle with "
                      + (", ".join(others) or "itself")
                      + "; PLANNED_OBJECTS.md lists it under Dependency "
                      "cycles for a human decision, and no edge was broken "
                      "to force an order")
        blocked.append({"source_identifier": ident,
                        "object_type": rec.get("object_type"),
                        "reason": reason})
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
            # The source COMMENT, for the transports that can apply it.
            "description": res.description,
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
