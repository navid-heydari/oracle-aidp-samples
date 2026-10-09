"""Regenerate references/expression-functions.md from the function table.

    python -m ocidi2aidp.docs
"""
from pathlib import Path

from .compile.expressions import function_table_markdown

HEADER = """# OCI-DI expression functions and their Spark SQL translation

Generated from `ocidi2aidp/compile/expressions.py` by `python -m ocidi2aidp.docs`;
`tests/test_expressions.py` fails if this file and the table drift apart.

How to read the **Translation** column:

| Value | Meaning |
|---|---|
| same | Emitted unchanged. OCI-DI runs data flows on Spark, and for these the DI function is the Spark function. |
| rename | Emitted as the Spark function named in the note. |
| rewrite | Rewritten (shape-dependent; the note says how). Rewrites that change semantics at the edges produce a review finding in the migration. |
| fallback | Never translated deterministically. The operator becomes an LLM work order. |

A function that is **not in this table** is never passed through on the
assumption that Spark has the same function: the operator becomes an LLM
work order. `DECODE` is why -- in OCI-DI it is Oracle's CASE shorthand, in
Spark it is a charset decoder.

Also rewritten, outside function calls:

- `OPERATOR.ENTITY.ATTR` / `OPERATOR.ATTR` / `ENTITY.ATTR` -> the column, aliased `l.`/`r.` inside joins and lookups.
- `$P` / `${P}` -> a `${P}` placeholder the notebook fills with a quoted SQL literal at run time.
- `SYS.TASK_START_TIME`, `SYS.LAST_LOAD_DATE`, ... -> runtime-cell values (`SYS.LAST_LOAD_DATE` comes from the watermark table).
- `SYSDATE`, `SYSTIMESTAMP` -> `current_timestamp()`.
- `CAST(x AS VARCHAR2(n))` / `NUMBER(p,s)` -> `STRING` / `DECIMAL(p,s)` (Spark 3 rejects CHAR/VARCHAR in a CAST).
- User-defined functions -> inlined at the call site.

"""


def main() -> None:
    path = Path(__file__).resolve().parents[1] / "references" / "expression-functions.md"
    path.write_text(HEADER + function_table_markdown(), encoding="utf-8")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
