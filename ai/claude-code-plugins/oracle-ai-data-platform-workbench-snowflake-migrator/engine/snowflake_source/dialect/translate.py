"""Snowflake -> AIDP (Spark SQL) dialect translation. SKELETON.

The framework is real and the coverage is honest. Rules come in two states, and
`coverage()` reports the split so the gap is inspectable rather than implied:

  implemented -- a provably EXACT rewrite. Applied automatically.
  declared    -- recognised, described, and deliberately NOT rewritten, because
                 doing it safely needs statement-level restructuring rather than
                 a token substitution.

The governing rule is the same one that runs through the rest of this plugin:
**never approximate.** A rule either produces SQL that means the same thing, or
it reports what is needed and leaves the input untouched. SQL that "mostly works"
returns numbers, and wrong numbers are worse than a blocked object.

Adding a rule: append to RULES with status="implemented" and a `translate`
callable, or status="declared" with `translate=None` and a `detail` that says
what a real implementation would have to do.
"""
from __future__ import annotations

import re

from . import lexer
from dataclasses import dataclass, field
from typing import Callable

__all__ = ["RULES", "TranslationResult", "TranslationRule", "coverage",
           "translate_sql"]


@dataclass
class TranslationResult:
    sql: str
    applied: list[dict] = field(default_factory=list)
    unsupported: list[dict] = field(default_factory=list)

    @property
    def fully_translated(self) -> bool:
        return not self.unsupported


@dataclass(frozen=True)
class TranslationRule:
    rule_id: str
    construct: str
    description: str
    status: str                      # "implemented" | "declared"
    detect: str                      # regex, case-insensitive
    translate: Callable[[str], tuple[str, str | None]] | None = None
    detail: str = ""


# --------------------------------------------------------------------------
# implemented rewrites -- exact, token-level
# --------------------------------------------------------------------------

def _iff(sql: str) -> tuple[str, str | None]:
    return lexer.sub_code(r"\bIFF\s*\(", "IF(", sql)[0], None


# Only a bare identifier, qualified column or literal. Anything else (a closing
# paren, an operator) means the operand's left edge is ambiguous.
_CAST_SIMPLE = (
    r"(?<![\w).\"'])([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*|'[^']*'|\d+(?:\.\d+)?)"
    r"\s*(?P<op>::)\s*([A-Za-z_][\w$]*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)")


def _cast(sql: str) -> tuple[str, str | None]:
    # The operand may legitimately BE a literal ('x'::int), so the anchor is the
    # `::` operator: that is the token which must be code.
    out = lexer.sub_code(
        _CAST_SIMPLE, lambda m: f"CAST({m.group(1)} AS {m.group(3)})", sql,
        anchor_group="op")[0]
    if lexer.find_code(r"::", out):
        return sql, ("a `::` cast whose left operand is an expression, not a bare "
                     "column or literal. Rewriting it needs the expression "
                     "boundary, which a token rule cannot determine safely")
    return out, None


def _rename(pattern: str, replacement: str):
    def fn(sql: str) -> tuple[str, str | None]:
        return lexer.sub_code(pattern, replacement, sql)[0], None
    return fn


# DATEADD(unit, n, col). Snowflake's argument order differs from Spark's, and
# the unit decides which Spark function applies -- a rename would be wrong.
_DATE_UNITS = {
    "year": "months", "y": "months", "yy": "months", "yyyy": "months",
    "month": "months", "mm": "months", "mon": "months",
    "day": "days", "d": "days", "dd": "days",
    "week": "weeks", "w": "weeks", "wk": "weeks",
    "hour": "interval", "h": "interval", "hh": "interval",
    "minute": "interval", "mi": "interval", "n": "interval",
    "second": "interval", "s": "interval", "ss": "interval",
}
_CANONICAL_INTERVAL = {"hour": "HOUR", "h": "HOUR", "hh": "HOUR",
                       "minute": "MINUTE", "mi": "MINUTE", "n": "MINUTE",
                       "second": "SECOND", "s": "SECOND", "ss": "SECOND"}
_DATEADD = (
    r"\b(?P<kw>DATEADD)\s*\(\s*([A-Za-z]+)\s*,\s*([^,()]+?)\s*,\s*([^,()]+?)\s*\)")


def _dateadd(sql: str) -> tuple[str, str | None]:
    problems: list[str] = []

    def repl(m: re.Match) -> str:
        unit, amount, col = m.group(2).lower(), m.group(3).strip(), m.group(4).strip()
        kind = _DATE_UNITS.get(unit)
        if kind is None:
            problems.append(unit)
            return m.group(0)
        if kind == "days":
            return f"date_add({col}, {amount})"
        if kind == "weeks":
            return f"date_add({col}, {amount} * 7)"
        if kind == "months":
            if unit.startswith("y"):
                return f"add_months({col}, {amount} * 12)"
            return f"add_months({col}, {amount})"
        return f"({col} + INTERVAL {amount} {_CANONICAL_INTERVAL[unit]})"

    out = lexer.sub_code(_DATEADD, repl, sql, anchor_group="kw")[0]
    if problems:
        return sql, (f"unrecognised DATEADD unit(s): {', '.join(sorted(set(problems)))}. "
                     "Units are not guessed -- add them to _DATE_UNITS once the "
                     "intended granularity is confirmed")
    return out, None


_LISTAGG = (
    r"(?P<kw>\bLISTAGG\s*\()\s*([^,()]+?)\s*,\s*('(?:[^']*)')\s*\)")


def _listagg(sql: str) -> tuple[str, str | None]:
    if lexer.find_code(r"\bWITHIN\s+GROUP\b", sql):
        return sql, ("LISTAGG ... WITHIN GROUP (ORDER BY ...) -- Spark's "
                     "collect_list does not guarantee ordering, so the ordering "
                     "semantics would be lost silently")
    # The separator IS a literal and is reproduced verbatim, so the anchor is
    # the LISTAGG keyword rather than the whole span.
    out = lexer.sub_code(
        _LISTAGG, lambda m: f"concat_ws({m.group(3)}, collect_list({m.group(2)}))",
        sql, anchor_group="kw")[0]
    if lexer.find_code(r"\bLISTAGG\s*\(", out):
        return sql, ("a LISTAGG form beyond LISTAGG(expr, 'sep') -- e.g. DISTINCT "
                     "or an ON OVERFLOW clause")
    return out, None


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------

RULES: tuple[TranslationRule, ...] = (
    TranslationRule(
        "T01_IFF", "IFF", "IFF(c, a, b) -> IF(c, a, b)", "implemented",
        r"\bIFF\s*\(", _iff),
    TranslationRule(
        "T02_CAST_SHORTHAND", "::", "x::TYPE -> CAST(x AS TYPE)", "implemented",
        r"::\s*[A-Za-z]", _cast),
    TranslationRule(
        "T03_ARRAY_CONSTRUCT", "ARRAY_CONSTRUCT",
        "ARRAY_CONSTRUCT(...) -> array(...)", "implemented",
        r"\bARRAY_CONSTRUCT\s*\(",
        _rename(r"\bARRAY_CONSTRUCT\s*\(", "array(")),
    TranslationRule(
        "T04_OBJECT_CONSTRUCT", "OBJECT_CONSTRUCT",
        "OBJECT_CONSTRUCT('k', v, ...) -> named_struct('k', v, ...)",
        "implemented", r"\bOBJECT_CONSTRUCT\s*\(",
        _rename(r"\bOBJECT_CONSTRUCT\s*\(", "named_struct(")),
    TranslationRule(
        "T05_DATEADD", "DATEADD",
        "DATEADD(unit, n, col) -> date_add / add_months / + INTERVAL, per unit",
        "implemented", r"\bDATEADD\s*\(", _dateadd),
    TranslationRule(
        "T06_LISTAGG", "LISTAGG",
        "LISTAGG(x, sep) -> concat_ws(sep, collect_list(x))", "implemented",
        r"\bLISTAGG\s*\(", _listagg),

    TranslationRule(
        "T10_QUALIFY", "QUALIFY", "QUALIFY <predicate on a window function>",
        "declared", r"\bQUALIFY\b", None,
        "Needs statement restructuring: the window expression must be projected "
        "into a subquery and the predicate moved to an outer WHERE. That changes "
        "the select list and cannot be done by token substitution."),
    TranslationRule(
        "T11_LATERAL_FLATTEN", "LATERAL FLATTEN",
        "LATERAL FLATTEN(input => v) -> explode / LATERAL VIEW", "declared",
        r"\bLATERAL\s+FLATTEN\b|\bFLATTEN\s*\(", None,
        "The target shape depends on the VARIANT's structure and on which of "
        "value/index/key the query reads, so it needs the semi-structured design "
        "decision made first."),
    TranslationRule(
        "T12_GENERATOR", "GENERATOR / SEQ4",
        "TABLE(GENERATOR(ROWCOUNT => n)) -> range(n)", "declared",
        r"\bGENERATOR\s*\(|\bSEQ[48]\s*\(", None,
        "Replaces a table function in the FROM clause and SEQ4() has no exact "
        "Spark equivalent (monotonically_increasing_id is not gapless), so row "
        "identity would change."),
    TranslationRule(
        "T13_PIVOT", "PIVOT / UNPIVOT", "PIVOT/UNPIVOT clause differences",
        "declared", r"\b(?:UN)?PIVOT\s*\(", None,
        "Spark's PIVOT syntax and aggregate placement differ materially; a "
        "mechanical rewrite risks changing the grouping."),
    TranslationRule(
        "T14_SYSTEM_FUNCTION", "SYSTEM$", "SYSTEM$* built-ins", "declared",
        r"\bSYSTEM\$", None,
        "Snowflake-internal functions with no AIDP equivalent. Each needs an "
        "explicit decision about what, if anything, replaces it."),
    TranslationRule(
        "T15_TIME_TRAVEL", "Time Travel", "AT / BEFORE clauses", "declared",
        r"\bAT\s*\(\s*(?:TIMESTAMP|OFFSET|STATEMENT)\b|\bBEFORE\s*\(", None,
        "Delta time travel uses VERSION AS OF / TIMESTAMP AS OF and its retention "
        "is configured differently, so the two are not interchangeable."),
    TranslationRule(
        "T16_VARIANT_PATH", "VARIANT path", "col:field.sub -> struct access",
        "declared", r"[A-Za-z_][\w]*\s*:\s*[A-Za-z_]", None,
        "Requires the VARIANT column to have been given a concrete struct type "
        "first; until then there is no field to address."),
    TranslationRule(
        "T17_DECODE", "DECODE", "DECODE(x, a, b, ...) -> CASE", "declared",
        r"\bDECODE\s*\(", None,
        "Argument count is variable and the trailing default is positional, so "
        "a correct CASE needs the argument list parsed, not matched."),
    TranslationRule(
        "T18_NVL2", "NVL2", "NVL2(a, b, c) -> CASE WHEN a IS NOT NULL ...",
        "declared", r"\bNVL2\s*\(", None,
        "Mechanically simple but the operands may themselves contain commas, so "
        "it needs argument parsing to split safely."),
)


def coverage() -> dict:
    implemented = [r for r in RULES if r.status == "implemented"]
    declared = [r for r in RULES if r.status == "declared"]
    return {
        "total": len(RULES),
        "implemented": len(implemented),
        "declared": len(declared),
        "implemented_rule_ids": [r.rule_id for r in implemented],
        "declared_rule_ids": [r.rule_id for r in declared],
    }


def translate_sql(sql: str) -> TranslationResult:
    """Apply every implemented rule; report every declared one that matches."""
    result = TranslationResult(sql=sql)
    for rule in RULES:
        # Detection runs over CODE only. Otherwise a row containing the text
        # "QUALIFY", or a JSON-ish literal like '{"a": 1}' matching the VARIANT
        # path rule, blocks a view that has no such construct in it.
        if not lexer.find_code(rule.detect, result.sql):
            continue
        if rule.status == "declared" or rule.translate is None:
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": rule.detail or rule.description})
            continue
        new_sql, problem = rule.translate(result.sql)
        if problem:
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": problem})
            continue
        if new_sql != result.sql:
            result.applied.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": rule.description})
            result.sql = new_sql
    return result
