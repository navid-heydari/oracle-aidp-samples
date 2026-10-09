"""OCI-DI expression -> Spark SQL expression.

OCI-DI data flows run on Spark and the expression language is close to Spark
SQL, so the translation is mostly *resolution*, not rewriting:

  * ``OPERATOR.ENTITY.ATTR`` / ``OPERATOR.ATTR`` references become a column of
    the DataFrame that operator feeds in through -- ```col``` for a
    single-input operator, ``l.`col``` / ``r.`col``` for a join or lookup.
  * ``$P`` / ``${P}`` parameters become ``${P}`` placeholders; the generated
    notebook substitutes a correctly quoted SQL literal at run time
    (``_sql(...)`` in the runtime cell), so a parameter value is never pasted
    into SQL unquoted.
  * ``SYS.X`` system parameters become ``${SYS_X}`` placeholders the runtime
    cell defines.
  * User-defined functions are inlined (macro expansion).
  * Functions go through ``FUNCTIONS``, a curated table. A name that is not
    in it raises ``Untranslatable`` -- which the compiler turns into an LLM
    work order -- because a DI function and a Spark function with the same
    name do not necessarily mean the same thing (``DECODE`` is the standing
    example: Oracle-style CASE in DI, a charset decoder in Spark).

The table is also the source of references/expression-functions.md, and a
test keeps the two in step.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional

from ..report import Finding


class Untranslatable(Exception):
    """This expression has no deterministic translation; carries the reason."""


# --------------------------------------------------------------------- tokens
_TOKEN = re.compile(r"""
 (?P<ws>\s+)
|(?P<str>'(?:[^']|'')*')
|(?P<dq>"(?:[^"]|"")*")
|(?P<bq>`(?:[^`]|``)*`)
|(?P<macro>%%?[A-Za-z_][A-Za-z0-9_]*%%?)
|(?P<param>\$\{[A-Za-z_][A-Za-z0-9_.]*\}|\$[A-Za-z_][A-Za-z0-9_]*)
|(?P<num>(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?[LSYDB]?(?![A-Za-z_]))
|(?P<ident>[A-Za-z_][A-Za-z0-9_$#]*)
|(?P<op>->|<=>|<>|!=|>=|<=|==|\|\||::|[-+*/%=<>(),.\[\]:!&|^~])
""", re.X)


@dataclass
class Tok:
    kind: str
    text: str


@dataclass
class Chain:
    parts: list            # raw part texts (identifiers or backticked)

    @property
    def text(self) -> str:
        return ".".join(self.parts)


@dataclass
class Call:
    name: Chain
    args: list             # list[list[item]]


@dataclass
class Group:
    items: list


def tokenize(text: str) -> list:
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise Untranslatable(f"unexpected character {text[pos]!r} at offset {pos}")
        out.append(Tok(m.lastgroup, m.group()))
        pos = m.end()
    return _chains(out)


def _chains(tokens: list) -> list:
    """Merge IDENT ('.' IDENT|BQ)* into one Chain."""
    out, i = [], 0
    while i < len(tokens):
        t = tokens[i]
        if t.kind in ("ident", "bq"):
            parts = [t.text]
            j = i + 1
            while (j + 1 < len(tokens) and tokens[j].kind == "op" and tokens[j].text == "."
                   and tokens[j + 1].kind in ("ident", "bq")):
                parts.append(tokens[j + 1].text)
                j += 2
            out.append(Chain(parts))
            i = j
        else:
            out.append(t)
            i += 1
    return out


def _next_sig(items: list, i: int):
    j = i
    while j < len(items) and isinstance(items[j], Tok) and items[j].kind == "ws":
        j += 1
    return (items[j], j) if j < len(items) else (None, j)


def _is(item, text: str) -> bool:
    return isinstance(item, Tok) and item.kind == "op" and item.text == text


def parse(tokens: list) -> list:
    items, i = _parse(tokens, 0, top=True)
    return items


def _parse(tokens: list, i: int, top: bool = False):
    items = []
    while i < len(tokens):
        t = tokens[i]
        if _is(t, ")"):
            if top:
                raise Untranslatable("unbalanced ')'")
            return items, i
        if _is(t, "("):
            prev = _prev_sig(items)
            if isinstance(prev, Chain) and _callable(prev) and not _after_as(items):
                # the name and any whitespace between it and '('
                del items[_prev_index(items):]
                inner, i = _parse(tokens, i + 1)
                if i >= len(tokens):
                    raise Untranslatable("unbalanced '('")
                items.append(Call(prev, _split_args(inner)))
            else:
                inner, i = _parse(tokens, i + 1)
                if i >= len(tokens):
                    raise Untranslatable("unbalanced '('")
                items.append(Group(inner))
            i += 1
            continue
        items.append(t)
        i += 1
    if not top:
        return items, i
    return items, i


def _prev_index(items: list) -> int:
    j = len(items) - 1
    while j >= 0 and isinstance(items[j], Tok) and items[j].kind == "ws":
        j -= 1
    return j


def _prev_sig(items: list):
    j = _prev_index(items)
    return items[j] if j >= 0 else None


def _split_args(items: list) -> list:
    if not any(not (isinstance(x, Tok) and x.kind == "ws") for x in items):
        return []
    args, cur = [], []
    for item in items:
        if _is(item, ","):
            args.append(cur)
            cur = []
        else:
            cur.append(item)
    args.append(cur)
    return args


KEYWORDS = {
    "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "RLIKE", "ILIKE", "REGEXP", "BETWEEN",
    "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "TRUE", "FALSE", "DISTINCT", "OVER",
    "PARTITION", "BY", "ORDER", "ASC", "DESC", "NULLS", "FIRST", "LAST", "ROWS", "RANGE",
    "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW", "INTERVAL", "EXISTS", "ANY",
    "SOME", "ALL", "ESCAPE", "FROM", "FOR", "BOTH", "LEADING", "TRAILING", "YEAR", "MONTH",
    "DAY", "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER", "FILTER", "WITHIN", "GROUP",
}
# Type names that take arguments inside CAST: never function calls.
TYPE_WORDS = {"DECIMAL", "NUMERIC", "DEC", "VARCHAR", "CHAR", "CHARACTER", "VARCHAR2",
              "NVARCHAR", "NVARCHAR2", "NCHAR", "NUMBER", "ARRAY", "MAP", "STRUCT"}


def _callable(chain: Chain) -> bool:
    if len(chain.parts) == 1:
        return chain.parts[0].upper() not in KEYWORDS
    return True


def _after_as(items: list) -> bool:
    """True when the chain just parsed is a type name following ``AS`` (inside CAST)."""
    j = _prev_index(items)
    if j < 0 or not isinstance(items[j], Chain) or len(items[j].parts) != 1:
        return False
    if items[j].parts[0].upper() not in TYPE_WORDS:
        return False
    k = j - 1
    while k >= 0 and isinstance(items[k], Tok) and items[k].kind == "ws":
        k -= 1
    return k >= 0 and isinstance(items[k], Chain) and items[k].text.upper() == "AS"


# --------------------------------------------------------------------- scope
@dataclass
class ParamSpec:
    name: str
    type: str = "STRING"
    default: object = None


@dataclass
class Udf:
    name: str
    args: list             # argument names
    body: str


@dataclass
class Scope:
    """What names mean inside one operator's expressions.

    ``aliases`` maps an upper-cased operator name/identifier (and every name
    upstream of it) to the DataFrame alias its columns are reached by ("" for
    a single-input operator). ``entities`` does the same for entity names, for
    the ``ENTITY.ATTR`` form.
    """
    aliases: dict = field(default_factory=dict)
    entities: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)        # UPPER name -> ParamSpec
    udfs: dict = field(default_factory=dict)          # UPPER name -> Udf
    macro_input: Optional[str] = None
    node: str = ""
    sys_params: set = field(default_factory=set)      # filled: SYS_* names used


@dataclass
class Translation:
    sql: str
    params: set
    findings: list


class _Ctx:
    def __init__(self, scope: Scope, depth: int = 0, udf_args: Optional[dict] = None):
        self.scope = scope
        self.params: set = set()
        self.findings: list = []
        self.depth = depth
        self.udf_args = udf_args or {}
        self.lambda_vars: set = set()

    def note(self, code: str, message: str, severity: str = "info") -> None:
        f = Finding(code, message, severity, self.scope.node)
        if f not in self.findings:
            self.findings.append(f)


def translate(text: Optional[str], scope: Scope) -> Translation:
    if text is None or not str(text).strip():
        raise Untranslatable("empty expression")
    ctx = _Ctx(scope)
    sql = _translate(str(text), ctx)
    return Translation(sql.strip(), ctx.params, ctx.findings)


def _translate(text: str, ctx: _Ctx) -> str:
    items = parse(tokenize(text))
    ctx.lambda_vars |= _lambda_vars(items)
    return _render(items, ctx)


def _lambda_vars(items: list) -> set:
    found = set()
    for idx, item in enumerate(items):
        nxt, _ = _next_sig(items, idx + 1)
        if _is(nxt, "->"):
            if isinstance(item, Chain) and len(item.parts) == 1:
                found.add(item.parts[0].upper())
            elif isinstance(item, Group):
                for x in item.items:
                    if isinstance(x, Chain) and len(x.parts) == 1:
                        found.add(x.parts[0].upper())
        if isinstance(item, Group):
            found |= _lambda_vars(item.items)
        if isinstance(item, Call):
            for arg in item.args:
                found |= _lambda_vars(arg)
    return found


def _render(items: list, ctx: _Ctx) -> str:
    out, skip = [], set()
    for idx, item in enumerate(items):
        if idx in skip:
            continue
        if isinstance(item, Tok):
            out.append(_render_tok(item, ctx))
        elif isinstance(item, Chain):
            prev = _prev_sig(items[:idx])
            if (len(item.parts) == 1 and item.parts[0].upper() in TYPE_WORDS
                    and isinstance(prev, Chain) and prev.text.upper() == "AS"):
                nxt, j = _next_sig(items, idx + 1)
                group = nxt if isinstance(nxt, Group) else None
                if group is not None:
                    skip.update(range(idx + 1, j + 1))
                out.append(_render_type(item.parts[0], group, ctx))
                continue
            out.append(_render_chain(item, ctx, items, idx))
        elif isinstance(item, Group):
            out.append("(" + _render(item.items, ctx) + ")")
        elif isinstance(item, Call):
            out.append(_render_call(item, ctx))
    return "".join(out)


_STRING_TYPES = {"VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "CHAR", "NCHAR", "CHARACTER"}


def _render_type(word: str, group: Optional[Group], ctx: _Ctx) -> str:
    """A type name after AS. Spark 3 refuses CHAR/VARCHAR outside a table schema."""
    upper = word.upper()
    inner = _render(group.items, ctx).strip() if group is not None else ""
    if upper in _STRING_TYPES:
        if group is not None:
            ctx.note("EX05_CAST_STRING", f"CAST AS {word}({inner}) -> STRING; Spark does not "
                     f"enforce the length in a CAST", "info")
        return "STRING"
    if upper == "NUMBER":
        if group is None:
            ctx.note("EX14_TO_NUMBER", "CAST AS NUMBER -> DECIMAL(38,10)", "assume")
            return "DECIMAL(38,10)"
        return f"DECIMAL({inner})"
    return word + (f"({inner})" if group is not None else "")


def _render_tok(t: Tok, ctx: _Ctx) -> str:
    if t.kind == "param":
        name = t.text[2:-1] if t.text.startswith("${") else t.text[1:]
        upper = name.upper()
        if upper in ctx.udf_args:
            return ctx.udf_args[upper]
        if upper.startswith("SYS."):
            return _sys_param(upper, ctx)
        if upper not in ctx.scope.params:
            raise Untranslatable(f"parameter {name!r} is not declared on the data flow or task")
        ctx.params.add(ctx.scope.params[upper].name)
        return "${" + ctx.scope.params[upper].name + "}"
    if t.kind == "macro":
        if t.text.strip("%").upper() == "MACRO_INPUT" and ctx.scope.macro_input:
            return ctx.scope.macro_input
        raise Untranslatable(f"macro token {t.text} outside a resolvable bulk expression")
    if t.kind == "op" and t.text == "==":
        return "="
    return t.text


_SYS = {
    "SYS.TASK_START_TIME": "SYS_TASK_START_TIME",
    "SYS.TASK_RUN_KEY": "SYS_TASK_RUN_KEY",
    "SYS.TASK_RUN_NAME": "SYS_TASK_RUN_NAME",
    "SYS.TASK_SCHEDULE_TRIGGER_TIME": "SYS_TASK_SCHEDULE_TRIGGER_TIME",
    "SYS.TASK_SCHEDULE_TIMEZONE": "SYS_TASK_SCHEDULE_TIMEZONE",
    "SYS.RETRY_ATTEMPT": "SYS_RETRY_ATTEMPT",
    "SYS.LAST_LOAD_DATE": "SYS_LAST_LOAD_DATE",
}


def _sys_param(upper: str, ctx: _Ctx) -> str:
    if upper not in _SYS:
        raise Untranslatable(f"system parameter {upper} has no AIDP equivalent")
    name = _SYS[upper]
    ctx.scope.sys_params.add(name)
    ctx.params.add(name)
    if name in ("SYS_TASK_RUN_KEY", "SYS_TASK_RUN_NAME", "SYS_RETRY_ATTEMPT",
                "SYS_TASK_SCHEDULE_TRIGGER_TIME"):
        ctx.note("EX20_SYS_PARAM", f"{upper} is supplied by the notebook runtime cell, not "
                 f"by OCI-DI; its value has a different format on AIDP", "review")
    return "${" + name + "}"


def _unq(part: str) -> str:
    return part[1:-1].replace("``", "`") if part.startswith("`") else part


def _q(part: str) -> str:
    return "`" + _unq(part).replace("`", "``") + "`"


def _render_chain(chain: Chain, ctx: _Ctx, items: list, idx: int) -> str:
    parts = chain.parts
    first = _unq(parts[0]).upper()
    if len(parts) == 1:
        if first in ctx.udf_args:
            return ctx.udf_args[first]
        if first in ("SYSDATE", "SYSTIMESTAMP"):
            ctx.note("EX02_SYSDATE", f"{parts[0]} -> current_timestamp()", "info")
            return "current_timestamp()"
        return parts[0]
    if first == "SYS":
        return _sys_param(".".join(_unq(p).upper() for p in parts), ctx)
    if first in ctx.lambda_vars:
        return chain.text
    if first in ctx.scope.aliases:
        alias = ctx.scope.aliases[first]
        if alias == "?":
            raise Untranslatable(f"{chain.text!r} is reachable through both inputs; which side "
                                 f"it means cannot be decided from the JSON")
        rest = [p for p in parts[1:]]
        # OPERATOR.ENTITY.ATTR (.FIELD...) or OPERATOR.ATTR
        if len(rest) >= 2 and _unq(rest[0]).upper() in ctx.scope.entities:
            rest = rest[1:]
        elif len(rest) >= 2:
            rest = rest[1:]   # OPERATOR.<entity we do not know>.ATTR
        col = _q(rest[0]) + "".join("." + _q(p) for p in rest[1:])
        return f"{alias}.{col}" if alias else col
    if first in ctx.scope.entities:
        alias = ctx.scope.entities[first]
        if alias == "?":
            raise Untranslatable(f"entity {parts[0]} feeds both inputs; {chain.text!r} is "
                                 f"ambiguous")
        col = _q(parts[1]) + "".join("." + _q(p) for p in parts[2:])
        return f"{alias}.{col}" if alias else col
    ctx.note("EX10_UNKNOWN_QUALIFIER",
             f"{chain.text!r}: {parts[0]} is not an operator or entity in scope; "
             f"kept as written (read as struct field access)", "assume")
    return chain.text


def _render_call(call: Call, ctx: _Ctx) -> str:
    raw_name = call.name.parts[-1]
    name = _unq(raw_name).upper()
    qualified = len(call.name.parts) > 1
    udf = ctx.scope.udfs.get(name) or (ctx.scope.udfs.get(_unq(call.name.text).upper())
                                       if qualified else None)
    if udf is not None:
        return _expand_udf(udf, call, ctx)
    if qualified:
        raise Untranslatable(f"call to {call.name.text}(): not a known user-defined function")
    spec = FUNCTIONS.get(name)
    if spec is None:
        raise Untranslatable(f"function {raw_name}() is not in the translation table "
                             f"(references/expression-functions.md)")
    args = [_render(a, ctx).strip() for a in call.args]
    return spec.handler(raw_name, args, call.args, ctx)


def _expand_udf(udf: Udf, call: Call, ctx: _Ctx) -> str:
    if ctx.depth > 8:
        raise Untranslatable(f"user-defined function {udf.name} nests more than 8 deep")
    if len(call.args) != len(udf.args):
        raise Untranslatable(f"user-defined function {udf.name} takes {len(udf.args)} "
                             f"argument(s), called with {len(call.args)}")
    rendered = {a.upper(): "(" + _render(arg, ctx).strip() + ")"
                for a, arg in zip(udf.args, call.args)}
    inner = _Ctx(ctx.scope, ctx.depth + 1, rendered)
    body = _translate(udf.body, inner)
    ctx.params |= inner.params
    ctx.findings += [f for f in inner.findings if f not in ctx.findings]
    ctx.note("EX30_UDF_INLINED", f"user-defined function {udf.name} inlined", "info")
    return "(" + body.strip() + ")"


# ------------------------------------------------------------ function table
@dataclass(frozen=True)
class FunctionSpec:
    category: str
    kind: str          # same | rename | rewrite | fallback
    note: str
    handler: Callable


def _same(name, args, raw, ctx):
    return f"{name}({', '.join(args)})"


def _rename(target: str, note_code: str = ""):
    def handler(name, args, raw, ctx):
        if note_code:
            ctx.note(note_code, f"{name}() -> {target}()", "info")
        return f"{target}({', '.join(args)})"
    return handler


def _literal(raw_arg) -> Optional[str]:
    sig = [x for x in raw_arg if not (isinstance(x, Tok) and x.kind == "ws")]
    if len(sig) == 1 and isinstance(sig[0], Tok) and sig[0].kind == "str":
        return sig[0].text[1:-1].replace("''", "'")
    return None


def _number_literal(raw_arg) -> Optional[str]:
    sig = [x for x in raw_arg if not (isinstance(x, Tok) and x.kind == "ws")]
    if len(sig) == 1 and isinstance(sig[0], Tok) and sig[0].kind == "num":
        return sig[0].text
    return None


def _fallback(reason: str):
    def handler(name, args, raw, ctx):
        raise Untranslatable(f"{name}(): {reason}")
    return handler


def _decode(name, args, raw, ctx):
    if len(args) < 3:
        raise Untranslatable(f"{name}() needs at least 3 arguments")
    subject, rest = args[0], args[1:]
    default = rest.pop() if len(rest) % 2 == 1 else "NULL"
    whens = " ".join(f"WHEN ({subject}) <=> ({rest[i]}) THEN {rest[i + 1]}"
                     for i in range(0, len(rest), 2))
    ctx.note("EX03_DECODE", "DECODE() -> CASE (NULL matches NULL, as in Oracle)", "info")
    return f"CASE {whens} ELSE {default} END"


_DATE_PATTERN = re.compile(r"[yMdHhmsSaEzZXkKuDFwW]")
_NUMBER_PATTERN = re.compile(r"^[09.,$SDGLMIPR\s]+$", re.I)
_SPARK3_RISKY = re.compile(r"[YuwWe]")


def _check_date_pattern(fmt: str, ctx: _Ctx, name: str) -> None:
    if _SPARK3_RISKY.search(re.sub(r"'[^']*'", "", fmt)):
        ctx.note("EX12_DATE_PATTERN", f"{name}() pattern {fmt!r} uses week-based or day-of-week "
                 f"letters, which Spark 3 parses differently from Java SimpleDateFormat", "review")


def _to_char(name, args, raw, ctx):
    if len(args) == 1:
        return f"CAST({args[0]} AS STRING)"
    if len(args) == 2:
        fmt = _literal(raw[1])
        if fmt is None:
            raise Untranslatable(f"{name}() with a non-literal format cannot be classified "
                                 f"as a date or number format")
        if _NUMBER_PATTERN.match(fmt) and not _DATE_PATTERN.search(fmt.replace("S", "")):
            ctx.note("EX13_NUMBER_FORMAT", f"{name}() number format {fmt!r} -> Spark to_char(); "
                     f"Oracle and Spark number masks differ at the edges (rounding, sign)",
                     "review")
            return f"to_char({args[0]}, {args[1]})"
        _check_date_pattern(fmt, ctx, name)
        return f"date_format({args[0]}, {args[1]})"
    raise Untranslatable(f"{name}() with a locale argument has no Spark equivalent")


def _to_number(name, args, raw, ctx):
    if len(args) == 1:
        ctx.note("EX14_TO_NUMBER", f"{name}(x) -> CAST(x AS DECIMAL(38,10)); confirm the "
                 f"precision the target column needs", "assume")
        return f"CAST({args[0]} AS DECIMAL(38,10))"
    if len(args) == 2:
        ctx.note("EX13_NUMBER_FORMAT", f"{name}() with a format -> Spark to_number()", "review")
        return f"to_number({args[0]}, {args[1]})"
    raise Untranslatable(f"{name}() with a locale argument has no Spark equivalent")


def _to_datetime(target: str):
    def handler(name, args, raw, ctx):
        if len(args) == 2:
            fmt = _literal(raw[1])
            if fmt is not None:
                _check_date_pattern(fmt, ctx, name)
            return f"{target}({args[0]}, {args[1]})"
        if len(args) == 1:
            return f"{target}({args[0]})"
        raise Untranslatable(f"{name}() with a locale argument has no Spark equivalent")
    return handler


_TRUNC_UNITS = {"YEAR", "YYYY", "YY", "MONTH", "MON", "MM", "WEEK", "QUARTER"}
_DATE_TRUNC_UNITS = {"DD", "DAY", "HOUR", "MINUTE", "SECOND"}


def _trunc(name, args, raw, ctx):
    if len(args) == 2:
        unit = _literal(raw[1])
        if unit is not None and unit.upper() in _TRUNC_UNITS:
            return f"trunc({args[0]}, {args[1]})"
        if unit is not None and unit.upper() in _DATE_TRUNC_UNITS:
            ctx.note("EX15_TRUNC", f"{name}(x, {unit!r}) -> date_trunc(), which returns a "
                     f"TIMESTAMP", "review")
            return f"date_trunc({args[1]}, {args[0]})"
        if _number_literal(raw[1]) is not None:
            raise Untranslatable(f"{name}() of a number: Spark has no numeric truncation "
                                 f"function with Oracle semantics")
    raise Untranslatable(f"{name}() is ambiguous between date and number truncation here")


def _instr(name, args, raw, ctx):
    if len(args) == 2:
        return f"instr({args[0]}, {args[1]})"
    raise Untranslatable(f"{name}() with position/occurrence arguments: Spark instr() takes two")


def _numeric_id(name, args, raw, ctx):
    ctx.note("EX16_GENERATED_ID", f"{name}() -> monotonically_increasing_id(): unique, but "
             f"not the values or density OCI-DI produced", "review")
    return "monotonically_increasing_id()"


def _cast_types(name, args, raw, ctx):
    return f"{name}({', '.join(args)})"


_CATEGORIES: dict = {
    "aggregate": ["AVG", "COUNT", "MAX", "MIN", "SUM", "COLLECT_LIST", "COLLECT_SET",
                  "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP",
                  "APPROX_COUNT_DISTINCT", "PERCENTILE", "PERCENTILE_APPROX", "MEDIAN",
                  "CORR", "COVAR_POP", "COVAR_SAMP", "BOOL_AND", "BOOL_OR", "ANY_VALUE"],
    "window": ["ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST", "NTILE",
               "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "FIRST", "LAST"],
    "numeric": ["ABS", "CEIL", "CEILING", "FLOOR", "MOD", "POWER", "POW", "ROUND", "BROUND",
                "SQRT", "EXP", "LN", "LOG", "LOG10", "LOG2", "SIGN", "SIGNUM", "GREATEST",
                "LEAST", "RAND", "RANDN", "PMOD", "CBRT", "DEGREES", "RADIANS", "SIN", "COS",
                "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "PI", "E", "FACTORIAL", "HEX", "UNHEX",
                "CONV", "BIN", "SHIFTLEFT", "SHIFTRIGHT", "TRY_ADD", "TRY_DIVIDE",
                "TRY_MULTIPLY", "TRY_SUBTRACT", "WIDTH_BUCKET"],
    "string": ["ASCII", "BASE64", "UNBASE64", "BIT_LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH",
               "CHR", "CONCAT", "CONCAT_WS", "CONTAINS", "ENDSWITH", "STARTSWITH", "FORMAT_NUMBER",
               "FORMAT_STRING", "INITCAP", "LCASE", "LEFT", "LENGTH", "LEVENSHTEIN", "LOCATE",
               "LOWER", "LPAD", "LTRIM", "OCTET_LENGTH", "OVERLAY", "POSITION", "REGEXP_EXTRACT",
               "REGEXP_EXTRACT_ALL", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR",
               "REGEXP_COUNT", "REGEXP_INSTR", "REPEAT", "REPLACE", "REVERSE", "RIGHT", "RPAD",
               "RTRIM", "SOUNDEX", "SPACE", "SPLIT", "SPLIT_PART", "SUBSTR", "SUBSTRING",
               "SUBSTRING_INDEX", "TRANSLATE", "TRIM", "UCASE", "UPPER", "ENCODE", "MASK"],
    "date": ["ADD_MONTHS", "CURRENT_DATE", "CURRENT_TIMESTAMP", "NOW", "DATE_ADD", "DATE_SUB",
             "DATEADD", "DATEDIFF", "DATE_DIFF", "DATE_FORMAT", "DATE_PART", "DATE_TRUNC",
             "DAY", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "EXTRACT", "FROM_UNIXTIME",
             "FROM_UTC_TIMESTAMP", "HOUR", "LAST_DAY", "MAKE_DATE", "MAKE_TIMESTAMP",
             "MINUTE", "MONTH", "MONTHS_BETWEEN", "NEXT_DAY", "QUARTER", "SECOND",
             "TIMESTAMP_SECONDS", "TIMESTAMP_MILLIS", "TO_UTC_TIMESTAMP", "UNIX_TIMESTAMP",
             "UNIX_SECONDS", "UNIX_MILLIS", "WEEKDAY", "WEEKOFYEAR", "YEAR",
             "CURRENT_TIMEZONE", "TO_UNIX_TIMESTAMP"],
    "conditional": ["COALESCE", "IF", "IFNULL", "NANVL", "NULLIF", "NVL", "NVL2", "ISNULL",
                    "ISNOTNULL", "ISNAN", "ASSERT_TRUE"],
    "conversion": ["CAST", "TRY_CAST", "STRING", "INT", "BIGINT", "DOUBLE", "FLOAT", "BOOLEAN",
                   "DATE", "TIMESTAMP", "TINYINT", "SMALLINT", "BINARY"],
    "hash": ["MD5", "SHA", "SHA1", "SHA2", "CRC32", "HASH", "XXHASH64"],
    "json": ["FROM_JSON", "TO_JSON", "SCHEMA_OF_JSON", "GET_JSON_OBJECT", "JSON_TUPLE",
             "JSON_ARRAY_LENGTH", "JSON_OBJECT_KEYS"],
    "array_map": ["ARRAY", "ARRAY_CONTAINS", "ARRAY_DISTINCT", "ARRAY_EXCEPT", "ARRAY_INTERSECT",
                  "ARRAY_JOIN", "ARRAY_MAX", "ARRAY_MIN", "ARRAY_POSITION", "ARRAY_REMOVE",
                  "ARRAY_REPEAT", "ARRAY_SORT", "ARRAY_UNION", "ARRAYS_OVERLAP", "ARRAYS_ZIP",
                  "CARDINALITY", "ELEMENT_AT", "EXPLODE", "FLATTEN", "MAP", "MAP_CONCAT",
                  "MAP_ENTRIES", "MAP_FROM_ARRAYS", "MAP_FROM_ENTRIES", "MAP_KEYS",
                  "MAP_VALUES", "NAMED_STRUCT", "SEQUENCE", "SHUFFLE", "SIZE", "SLICE",
                  "SORT_ARRAY", "STRUCT", "TRY_ELEMENT_AT"],
    "higher_order": ["AGGREGATE", "REDUCE", "EXISTS", "FILTER", "FORALL", "MAP_FILTER",
                     "MAP_ZIP_WITH", "TRANSFORM", "TRANSFORM_KEYS", "TRANSFORM_VALUES",
                     "ZIP_WITH"],
    "generated": ["UUID", "MONOTONICALLY_INCREASING_ID", "SPARK_PARTITION_ID"],
}

FUNCTIONS: dict = {}
for _cat, _names in _CATEGORIES.items():
    for _n in _names:
        FUNCTIONS[_n] = FunctionSpec(_cat, "same", "Spark function of the same name", _same)

FUNCTIONS.update({
    "DECODE": FunctionSpec("conditional", "rewrite",
                           "Oracle-style DECODE -> CASE WHEN x <=> s THEN r ... (Spark's "
                           "decode() is a charset decoder)", _decode),
    "TO_CHAR": FunctionSpec("conversion", "rewrite",
                            "date format -> date_format(); number format -> to_char(); "
                            "one argument -> CAST AS STRING; locale -> fallback", _to_char),
    "TO_NUMBER": FunctionSpec("conversion", "rewrite",
                              "one argument -> CAST AS DECIMAL(38,10) (assumption reported); "
                              "with format -> to_number()", _to_number),
    "TO_DATE": FunctionSpec("conversion", "same", "to_date(str[, fmt]); pattern checked for "
                            "Spark 3 differences", _to_datetime("to_date")),
    "TO_TIMESTAMP": FunctionSpec("conversion", "same", "to_timestamp(str[, fmt]); pattern "
                                 "checked", _to_datetime("to_timestamp")),
    "TRUNC": FunctionSpec("date", "rewrite", "date unit -> trunc() or date_trunc(); numeric "
                          "truncation -> fallback", _trunc),
    "INSTR": FunctionSpec("string", "rewrite", "two arguments -> instr(); position/occurrence "
                          "-> fallback", _instr),
    "NUMERIC_ID": FunctionSpec("generated", "rewrite", "-> monotonically_increasing_id() "
                               "(review: values differ)", _numeric_id),
    "ROWID": FunctionSpec("generated", "rewrite", "-> monotonically_increasing_id() (review: "
                          "values differ)", _numeric_id),
    "ORA_HASH": FunctionSpec("hash", "fallback", "no Spark function returns Oracle's hash "
                             "values", _fallback("no Spark function returns Oracle's hash values")),
    "LISTAGG": FunctionSpec("aggregate", "fallback", "Spark 3.5 has no LISTAGG; needs "
                            "array_join(collect_list) with ordering",
                            _fallback("Spark 3.5 has no LISTAGG")),
    "SYS_GUID": FunctionSpec("generated", "rename", "-> uuid()", _rename("uuid", "EX04_RENAMED")),
})
for _t in ("DECIMAL", "NUMERIC", "VARCHAR", "CHAR"):
    FUNCTIONS.pop(_t, None)


def function_table_markdown() -> str:
    """The function table, as published in references/expression-functions.md."""
    lines = ["| Function | Category | Translation | Note |", "|---|---|---|---|"]
    for name in sorted(FUNCTIONS):
        spec = FUNCTIONS[name]
        lines.append(f"| `{name}` | {spec.category} | {spec.kind} | {spec.note} |")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------- DI types
_ORACLE_TYPES = [
    (re.compile(r"^(N?VARCHAR2?|N?CHAR|CHARACTER|CLOB|NCLOB|STRING|TEXT|LONG VARCHAR)(\(.*\))?$"), "STRING"),
    (re.compile(r"^(NUMBER|NUMERIC|DECIMAL)\((\d+)\s*,\s*(-?\d+)\)$"), None),
    (re.compile(r"^(NUMBER|NUMERIC|DECIMAL)\((\d+)\)$"), None),
    (re.compile(r"^(NUMBER|NUMERIC|DECIMAL)$"), "DECIMAL(38,10)"),
    (re.compile(r"^(INTEGER|INT)$"), "INT"),
    (re.compile(r"^(BIGINT|LONG)$"), "BIGINT"),
    (re.compile(r"^(SMALLINT|SHORT)$"), "SMALLINT"),
    (re.compile(r"^TINYINT$"), "TINYINT"),
    (re.compile(r"^(FLOAT|BINARY_FLOAT|REAL)$"), "FLOAT"),
    (re.compile(r"^(DOUBLE|BINARY_DOUBLE|DOUBLE PRECISION)$"), "DOUBLE"),
    (re.compile(r"^(BOOLEAN|BOOL)$"), "BOOLEAN"),
    (re.compile(r"^DATE$"), "DATE"),
    (re.compile(r"^(DATETIME|TIMESTAMP)(\(\d+\))?( WITH (LOCAL )?TIME ZONE)?$"), "TIMESTAMP"),
    (re.compile(r"^(RAW|BLOB|BINARY|VARBINARY)(\(.*\))?$"), "BINARY"),
]


def spark_type(di_type: str) -> tuple[str, bool]:
    """DI/native type name -> (Spark SQL type, exact?)."""
    t = (di_type or "").strip().upper()
    for pattern, target in _ORACLE_TYPES:
        m = pattern.match(t)
        if not m:
            continue
        if target is not None:
            return target, True
        groups = m.groups()
        precision = int(groups[1])
        scale = int(groups[2]) if len(groups) > 2 and groups[2] is not None else 0
        if precision > 38 or scale < 0 or scale > precision:
            return "DECIMAL(38,10)", False
        return f"DECIMAL({precision},{scale})", True
    return "STRING", False
