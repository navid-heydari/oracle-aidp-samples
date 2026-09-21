"""String-aware SQL scanner. Pure leaf, zero I/O, no SQL dialect knowledge.

Everything upstream of this module used regexes and `str.split(";")` over raw
SQL, which cannot tell code from the inside of a string literal. That single
confusion produced four separate defects:

  * a comment stripper that ate the rest of the line on `'a -- b'`
  * a statement splitter that turned `select 'a;drop table t'` into two
    statements, the second of which leads with DROP
  * a construct detector that reported QUALIFY because a row contained the
    word in a literal
  * `f'"{name}"'` identifier building, which produces broken SQL for a name
    containing a double quote and pushes the tail of the name outside the
    quotes, where it is parsed as code

So the scan happens ONCE, here, and the rest of the codebase asks this module
rather than re-deriving it. Literals and quoted identifiers are opaque: their
contents are never interpreted, matched against, or split on.

Scope: this recognises literal boundaries, not grammar. It is not a parser and
does not validate SQL. Block comments do not nest -- the first `*/` closes,
which is what every engine we target does in practice.
"""
from __future__ import annotations

import re

__all__ = ["UnterminatedLiteral", "SEGMENT_KINDS", "segments", "code_only",
           "strip_comments", "split_statements", "leading_verb", "quote_ident",
           "qualify", "like_literal", "find_code", "sub_code",
           "cte_body_verb"]

SEGMENT_KINDS = ("code", "string", "ident", "comment")

# Opaque kinds: their contents are text, never code.
_OPAQUE = ("string", "ident", "comment")

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9$]*")


class UnterminatedLiteral(ValueError):
    """A literal, quoted identifier or block comment never closed.

    Raised rather than guessed at. An unterminated literal means the input is
    not the SQL we think it is, and every downstream answer would be built on
    a mis-read of where code ends.
    """


def _closing_quote(sql: str, start: int, quote: str) -> int:
    """Index just past the closing `quote`, honouring doubling and backslash.

    Snowflake accepts both `''` and `\\'` inside a single-quoted literal, and
    `""` inside a quoted identifier.
    """
    i = start + 1
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "\\" and quote == "'":
            i += 2
            continue
        if ch == quote:
            if i + 1 < n and sql[i + 1] == quote:
                i += 2          # doubled -- an escaped quote, not the end
                continue
            return i + 1
        i += 1
    raise UnterminatedLiteral(
        f"unterminated {'identifier' if quote == chr(34) else 'string'} "
        f"starting at offset {start}")


def segments(sql: str) -> list[tuple[str, str]]:
    """Split `sql` into (kind, text) runs. Concatenating the texts rebuilds it.

    Adjacent code is coalesced into one segment so that callers can regex over
    code without a construct being split across two segments.
    """
    if not sql:
        return []
    out: list[tuple[str, str]] = []
    buf: list[str] = []
    i, n = 0, len(sql)

    def flush() -> None:
        if buf:
            out.append(("code", "".join(buf)))
            buf.clear()

    while i < n:
        ch = sql[i]
        two = sql[i:i + 2]

        if two == "--":
            end = sql.find("\n", i)
            end = n if end == -1 else end
            flush()
            out.append(("comment", sql[i:end]))
            i = end
        elif two == "/*":
            end = sql.find("*/", i + 2)
            if end == -1:
                raise UnterminatedLiteral(
                    f"unterminated block comment starting at offset {i}")
            flush()
            out.append(("comment", sql[i:end + 2]))
            i = end + 2
        elif two == "$$":
            end = sql.find("$$", i + 2)
            if end == -1:
                raise UnterminatedLiteral(
                    f"unterminated dollar-quoted string starting at offset {i}")
            flush()
            out.append(("string", sql[i:end + 2]))
            i = end + 2
        elif ch == "'":
            end = _closing_quote(sql, i, "'")
            flush()
            out.append(("string", sql[i:end]))
            i = end
        elif ch == '"':
            end = _closing_quote(sql, i, '"')
            flush()
            out.append(("ident", sql[i:end]))
            i = end
        else:
            buf.append(ch)
            i += 1

    flush()
    return out


def code_only(sql: str) -> str:
    """`sql` with every literal, identifier and comment blanked out.

    Same length as the input and newlines preserved, so offsets and line
    numbers still line up. Run pattern detection over THIS, never over raw
    SQL: a match here is necessarily in code.
    """
    parts: list[str] = []
    for kind, text in segments(sql):
        if kind in _OPAQUE:
            parts.append("".join("\n" if c == "\n" else " " for c in text))
        else:
            parts.append(text)
    return "".join(parts)


def strip_comments(sql: str) -> str:
    """Remove comments. Literals and identifiers are returned untouched."""
    return "".join(
        " " if kind == "comment" else text for kind, text in segments(sql))


def split_statements(sql: str) -> list[str]:
    """Split on top-level `;` only. Empty fragments are dropped.

    A `;` inside a literal, identifier or comment is text and does not split.
    """
    stmts: list[str] = []
    current: list[str] = []

    def close() -> None:
        text = "".join(current).strip()
        if text:
            stmts.append(text)
        current.clear()

    for kind, text in segments(sql):
        if kind != "code":
            current.append(text)
            continue
        start = 0
        for idx, ch in enumerate(text):
            if ch == ";":
                current.append(text[start:idx])
                close()
                start = idx + 1
        current.append(text[start:])
    close()
    return stmts


def leading_verb(statement: str) -> str | None:
    """The first SQL keyword, or None if the statement does not start with one.

    Comments, whitespace and opening parens are skipped. A statement whose
    first real content is a literal or a quoted identifier has NO verb -- it
    returns None rather than reaching inside the literal for a word.
    """
    for kind, text in segments(statement):
        if kind == "comment":
            continue
        if kind in ("string", "ident"):
            return None
        stripped = text.lstrip(" \t\r\n(")
        if not stripped:
            continue
        match = _WORD.match(stripped)
        return match.group(0).upper() if match else None
    return None


def quote_ident(name: str) -> str:
    """A double-quoted Snowflake identifier with embedded quotes doubled."""
    if not isinstance(name, str) or not name:
        raise ValueError(f"identifier must be a non-empty string, got {name!r}")
    return '"' + name.replace('"', '""') + '"'


def qualify(*parts: str) -> str:
    """Dotted, individually quoted identifier: qualify("DB","SC") -> "DB"."SC"."""
    if not parts:
        raise ValueError("qualify() needs at least one identifier")
    return ".".join(quote_ident(p) for p in parts)


def like_literal(name: str) -> str:
    """`name` as the body of a LIKE pattern that matches it and nothing else.

    `_` and `%` are wildcards, and `_` appears in most real table names, so an
    unescaped LIKE probe matches names other than the one asked for. The
    escape character is the SQL default backslash; single quotes are doubled so
    the result is safe to embed in a literal.
    """
    if not isinstance(name, str):
        raise ValueError(f"name must be a string, got {name!r}")
    escaped = (name.replace("\\", "\\\\")
                   .replace("%", r"\%")
                   .replace("_", r"\_"))
    return escaped.replace("'", "''")


def find_code(pattern: str, sql: str, *, flags: int = re.IGNORECASE
              ) -> list[tuple[int, int]]:
    """(start, end) spans of `pattern` in the CODE of `sql`.

    Matches inside literals, quoted identifiers and comments are not returned:
    the word QUALIFY in a customer name is data, not a construct.
    """
    mask = code_only(sql)
    spans: list[tuple[int, int]] = []
    for m in re.finditer(pattern, mask, flags):
        start, end = m.span()
        # A span that crosses a blanked region is not wholly in code.
        if mask[start:end] == sql[start:end]:
            spans.append((start, end))
    return spans


def sub_code(pattern: str, repl, sql: str, *, flags: int = re.IGNORECASE,
             anchor_group: int | str | None = None) -> tuple[str, int]:
    """`re.sub` restricted to code. Returns (new_sql, replacement_count).

    A rewrite that reaches into a string literal does not change the SQL, it
    changes the DATA the query returns -- so literals, quoted identifiers and
    comments are never rewritten. `repl` is a replacement string or a callable
    taking the match, as with `re.sub`.

    By default the WHOLE match must be code, which is the safe rule for a
    keyword rewrite. Some rules match a literal on purpose -- `LISTAGG(a, ',')`
    contains its separator, `'x'::int` casts one -- and reproduce it in the
    replacement. Those pass `anchor_group` naming the group that must be code
    (the keyword or the operator); the rest of the span may then be anything,
    because the rule has shown it knows the literal is there.
    """
    mask = code_only(sql)
    out = sql
    count = 0
    # An anchored rule matches a literal on purpose, so it cannot be found in
    # the blanked mask -- scan the raw text and vet the anchor. An unanchored
    # rule scans the mask, where a match is code by construction.
    haystack = sql if anchor_group is not None else mask
    # Right to left, so earlier offsets stay valid as we splice.
    for m in reversed(list(re.finditer(pattern, haystack, flags))):
        span = m.span()
        check = m.span(anchor_group) if anchor_group is not None else span
        if check == (-1, -1) or mask[check[0]:check[1]] != sql[check[0]:check[1]]:
            continue
        replacement = repl(m) if callable(repl) else m.expand(repl)
        out = out[:span[0]] + replacement + out[span[1]:]
        count += 1
    return out, count


def cte_body_verb(statement: str) -> str | None:
    """The keyword after a statement's CTE list, upper-cased, or None.

    `leading_verb` reports WITH for `WITH x AS (...) SELECT ...` and for
    `WITH x AS (...) INSERT ...` alike, and only the first is a read. This
    walks the CTE list -- `WITH [RECURSIVE] name [(cols)] AS (body) [, ...]`
    -- at paren depth 0 and returns the first keyword after it. Everything
    inside a parenthesis, a literal, a quoted identifier or a comment is
    skipped, so a write verb inside a CTE body or a string is never read as
    the statement's verb.

    None whenever the walk does not land on a bare keyword: the statement
    does not start with WITH, the CTE list is not shaped as above, or the
    body is itself parenthesised (`WITH x AS (...) (SELECT ...)`). None means
    "could not identify", never "harmless" -- the caller refuses on it.
    """
    depth = 0
    # with -> first_name -> as -> [cols -> as_only ->] body_open -> body
    #   -> next -> either `,` -> name -> as ... or the keyword we return.
    state = "with"
    for kind, text in segments(statement):
        if kind == "comment":
            continue
        if kind != "code":
            if depth:
                continue                        # inside a body or column list
            if kind == "ident" and state in ("first_name", "name"):
                state = "as"                    # a quoted CTE name
                continue
            return None                         # a literal where a keyword belongs
        i, n = 0, len(text)
        while i < n:
            ch = text[i]
            if ch in " \t\r\n":
                i += 1
            elif ch == "(":
                depth += 1
                i += 1
                if depth > 1:
                    continue
                if state == "as":
                    state = "cols"
                elif state == "body_open":
                    state = "body"
                else:
                    return None                 # a parenthesised body, or a stray paren
            elif ch == ")":
                depth -= 1
                i += 1
                if depth > 0:
                    continue
                if depth < 0:
                    return None
                state = {"cols": "as_only", "body": "next"}.get(state)
                if state is None:
                    return None
            elif depth:
                i += 1                          # inside a paren: not ours to read
            elif ch == "," and state == "next":
                state = "name"
                i += 1
            else:
                match = _WORD.match(text, i)
                if match is None:
                    return None
                word = match.group(0).upper()
                i = match.end()
                if state == "with":
                    if word != "WITH":
                        return None
                    state = "first_name"
                elif state == "first_name":
                    state = "name" if word == "RECURSIVE" else "as"
                elif state == "name":
                    state = "as"
                elif state in ("as", "as_only"):
                    if word != "AS":
                        return None
                    state = "body_open"
                elif state == "next":
                    return word
                else:
                    return None
    return None
