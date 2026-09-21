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
from .translate import RULES, translate_sql

__all__ = ["UNSUPPORTED_CONSTRUCTS", "detect_unsupported_constructs",
           "extract_view_body", "translate_view_body"]

# construct -> (regex, brief reason). Derived from the translator's declared
# rules, so this table cannot disagree with what detection actually does. A
# hand-maintained copy did: it listed DATEDIFF as blocking while no rule
# detected it, and listed IFF as blocking long after IFF was translated.
UNSUPPORTED_CONSTRUCTS: dict[str, tuple[str, str]] = {
    r.construct: (r.detect, r.detail or r.description)
    for r in RULES if r.status == "declared"}

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


