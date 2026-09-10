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

from . import lexer
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

# The header is located by SCANNING, not by regex. Two failures drove that:
#
#   * `view\s+[\w$".]+` cannot match a legal quoted name containing a space,
#     so `create view "my view" as ...` was unreadable
#   * a scan for ` as ` finds the one inside `COMMENT = \'defined as a rollup\'`
#     before the real one, and an earlier version matched the first `) as`
#     anywhere, so `select IFF(a, \'y\', \'n\') AS f` had its `) AS` mistaken for
#     the header end and the body was silently TRUNCATED to `f, ...`
#
# Truncated-but-valid SQL is the worst failure mode available here: it runs.
_CREATE_VIEW_PREFIX = re.compile(
    r"(?is)^\s*create\s+(?:or\s+replace\s+)?"
    r"(?:(?:secure|recursive|transient|volatile)\s+)*view\b")


def extract_view_body(ddl: str) -> str:
    """Return the SELECT body of a CREATE VIEW statement, without the trailing `;`.

    The body starts after the first `AS` keyword that is in code and at paren
    depth zero -- so an `AS` inside a literal, a comment, a quoted identifier
    or the optional column list is not mistaken for the end of the header.
    """
    if not ddl:
        raise ValueError("could not locate a view body: empty DDL")
    try:
        parts = lexer.segments(ddl)
    except lexer.UnterminatedLiteral as exc:
        raise ValueError(f"could not scan the view DDL: {exc}") from exc

    prefix = _CREATE_VIEW_PREFIX.match(lexer.code_only(ddl))
    if not prefix:
        raise ValueError(
            "could not locate a view body: the DDL does not begin with CREATE VIEW")

    offset, depth = 0, 0
    for kind, text in parts:
        if kind != "code":
            offset += len(text)
            continue
        for m in re.finditer(r"[()]|\bAS\b", text, re.IGNORECASE):
            token = m.group(0)
            if token == "(":
                depth += 1
            elif token == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and offset + m.start() >= prefix.end():
                body = ddl[offset + m.end():].strip().rstrip(";").strip()
                if not body:
                    raise ValueError("view body is empty after the AS keyword")
                return body
        offset += len(text)
    raise ValueError("could not locate a view body: no AS keyword outside parentheses")


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


