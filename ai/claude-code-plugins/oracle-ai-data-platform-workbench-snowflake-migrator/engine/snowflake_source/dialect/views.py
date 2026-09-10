"""Is a Snowflake view body portable to Spark SQL? Pure leaf, zero I/O.

This is a SOURCE-dialect question, not a target one, which is why it lives
here: both the planner (deciding what can migrate) and the DDL generator
(emitting it) read this module, and neither imports the other.

A Snowflake-only construct is DETECTED AND REPORTED, never rewritten on a
guess. A view that ships with a subtly wrong translation is worse than one
reported as needing manual work, because the wrong one returns numbers.
"""
from __future__ import annotations

import re

from .translate import translate_sql

__all__ = ["UNSUPPORTED_CONSTRUCTS", "detect_unsupported_constructs",
           "extract_view_body", "translate_view_body"]

# construct -> (regex, brief reason)
UNSUPPORTED_CONSTRUCTS: dict[str, tuple[str, str]] = {
    "QUALIFY": (
        r"\bQUALIFY\b",
        "no Spark equivalent; must be rewritten as a subquery with WHERE on the "
        "window result"),
    "LATERAL FLATTEN": (
        r"\bLATERAL\s+FLATTEN\b|\bFLATTEN\s*\(",
        "semi-structured expansion; maps to explode / LATERAL VIEW but the "
        "mapping depends on the VARIANT shape"),
    "IFF": (r"\bIFF\s*\(", "Spark uses IF(); a rename is safe but is not applied "
                              "automatically in MVP-1"),
    "DECODE": (r"\bDECODE\s*\(", "must become a CASE expression"),
    "NVL2": (r"\bNVL2\s*\(", "no Spark equivalent; must become CASE"),
    ":: CAST SHORTHAND": (
        r"::\s*[A-Za-z]", "Snowflake cast shorthand; Spark requires CAST(x AS t)"),
    "LISTAGG": (r"\bLISTAGG\s*\(",
                "no Spark equivalent; becomes collect_list + concat_ws"),
    "GENERATOR": (r"\bGENERATOR\s*\(|\bSEQ[48]\s*\(",
                  "row generation; Spark uses range()"),
    "ARRAY_CONSTRUCT": (r"\bARRAY_CONSTRUCT\s*\(", "Spark uses array()"),
    "OBJECT_CONSTRUCT": (r"\bOBJECT_CONSTRUCT\s*\(",
                         "Spark uses named_struct() or map()"),
    "SYSTEM$ FUNCTION": (r"\bSYSTEM\$", "Snowflake-internal function with no target"),
    "TIME TRAVEL": (r"\bAT\s*\(\s*(?:TIMESTAMP|OFFSET|STATEMENT)\b|\bBEFORE\s*\(",
                    "Snowflake Time Travel has no Delta equivalent in this form"),
    "VARIANT PATH": (r"[A-Za-z_][A-Za-z0-9_]*\s*:\s*[A-Za-z_]",
                     "Snowflake VARIANT path access; needs an explicit struct design"),
    "DATEADD/DATEDIFF": (
        r"\bDATE(?:ADD|DIFF)\s*\(",
        "argument order and unit strings differ from Spark's date functions; a "
        "signature mapping is required, not a rename"),
    "PIVOT/UNPIVOT": (r"\b(?:UN)?PIVOT\s*\(", "Spark syntax differs materially"),
}

# The optional column list after the view name contains ONLY identifiers, quotes,
# commas and whitespace -- never a paren or an operator. That restriction is the
# whole point: an earlier version matched the first `) as` ANYWHERE, so a body
# like `select IFF(a, 'y', 'n') AS f` had its `) AS` mistaken for the end of the
# header and the body was silently truncated to `f, ...`. Truncated-but-valid SQL
# is the worst failure mode available here, because it still runs.
_VIEW_HEADER_WITH_COLS = re.compile(
    r"(?is)^\s*create\s+(?:or\s+replace\s+)?(?:secure\s+)?(?:recursive\s+)?"
    r"view\s+[\w$\".]+\s*\(\s*[\w$\"',\s]*\)\s*as\s+(.*)$")
_VIEW_HEADER_BARE = re.compile(
    r"(?is)^\s*create\s+(?:or\s+replace\s+)?(?:secure\s+)?(?:recursive\s+)?"
    r"view\s+[\w$\".]+\s+as\s+(.*)$")


def extract_view_body(ddl: str) -> str:
    """Return the SELECT body of a CREATE VIEW statement, without the trailing `;`."""
    if not ddl:
        raise ValueError("could not locate a view body: empty DDL")
    for pattern in (_VIEW_HEADER_WITH_COLS, _VIEW_HEADER_BARE):
        match = pattern.match(ddl)
        if match:
            return match.group(1).strip().rstrip(";").strip()
    raise ValueError("could not locate a view body in the supplied DDL")


def translate_view_body(body: str):
    """Translate what can be translated; report what cannot.

    Delegates to the dialect translator so the planner and the DDL generator
    cannot disagree about whether a view is migratable.
    """
    return translate_sql(body)


def detect_unsupported_constructs(body: str) -> list[dict]:
    """Constructs that BLOCK migration -- i.e. cannot be translated exactly.

    A construct the translator handles exactly is no longer a blocker, so this
    reports only the residue. Anything listed here needs statement-level
    restructuring or a design decision, not a substitution.
    """
    result = translate_sql(body)
    return [{"construct": u["construct"], "reason": u["detail"],
             "rule_id": u["rule_id"]} for u in result.unsupported]


