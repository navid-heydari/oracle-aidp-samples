"""Work orders: load them, and render the prompt an author (LLM or human) answers."""
from __future__ import annotations

import json
from pathlib import Path

# What the generated function may touch, per work-order kind. Everything else
# is refused by validate.py.
CAPABILITIES = {
    "sql_task": {"secrets", "jvm"},
    "rest_task": {"secrets", "urllib"},
}

_KIND_GUIDANCE = {
    "operator": "Re-implement the operator with PySpark DataFrame operations.",
    "expression": "One expression could not be translated (the reason names it). Re-implement "
                  "the whole operator with PySpark, translating that expression by hand.",
    "custom_sql": "The source is a custom SQL query written in the source database's dialect "
                  "(see di_json.operator.entity.sqlQuery). Rewrite it as Spark SQL / DataFrame "
                  "code reading tables as spark.table(f'`{cat}`.`SCHEMA`.`TABLE`') where cat = "
                  "params['__sources__'][<SRC_* parameter of the data asset>] (the AIDP catalog "
                  "the source database is registered as).",
    "table_function": "A Table Function operator (Spark SQL statement, cube, rollup, dedup, ...). "
                      "Its configuration is in di_json. Create temp views from the inputs if the "
                      "statement needs them; drop them afterwards.",
    "oci_function": "The operator calls an OCI Function whose code is NOT available. Do not call "
                    "the function over the network. If its purpose is unambiguous from names and "
                    "shapes, re-implement it in PySpark and mark every inference with "
                    "'# ASSUMPTION:'; otherwise raise NotImplementedError explaining what is missing.",
    "compiler_error": "The deterministic compiler failed on this operator (a compiler defect). "
                      "Re-implement the operator from di_json.",
    "sql_task": "The task runs a stored procedure / SQL script inside the source database, which "
                "stays where it is. Call it over JDBC through the JVM "
                "(spark._jvm.java.sql.DriverManager) with url/user/password read from the AIDP "
                "credential store: aidputils.secrets.get(name=<credential>, key='url'|'user'|"
                "'password'). The credential name is context.credential_hint. Close the connection "
                "in a finally block. Raise on any SQL error.",
    "rest_task": "Re-implement the REST call with urllib.request. Credentials, if any, come from "
                 "aidputils.secrets.get(name=..., key=...). Implement polling / cancel only as the "
                 "task JSON describes; raise on failure.",
}


def load_orders(out_dir: Path) -> list:
    return [json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(Path(out_dir, "fallback").glob("FB_*.json"))]


def response_path(out_dir: Path, order_id: str) -> Path:
    return Path(out_dir, "fallback", f"{order_id}.py")


# Operators whose output port lists only what they add: the input attributes
# pass through. Saying "exactly the listed columns" for these broke the next
# cell in the first in-session run (two authors flagged it independently).
_PASS_THROUGH = {"EXPRESSION_OPERATOR", "FUNCTION_OPERATOR", "LOOKUP_OPERATOR",
                 "FLATTEN_OPERATOR"}


def _columns_rule(order: dict) -> str:
    op = (order.get("node") or {}).get("op_type", "")
    if op in _PASS_THROUGH:
        return ("This operator passes its input attributes through: keep every input column "
                "(unless the OCI-DI JSON excludes it) and add or replace the listed fields.")
    if not order.get("outputs"):
        return "No DataFrame is expected."
    return ("When that list is non-empty it is the complete output: return exactly those "
            "columns (names, then types).")


def render_prompt(order: dict) -> str:
    fn = order["function"]
    outputs = order.get("outputs") or []
    if len(outputs) > 1:
        returns = "a dict mapping each output port name to its DataFrame: " + ", ".join(
            f"{o['port']!r}" for o in outputs)
    elif outputs:
        returns = "one DataFrame"
    else:
        returns = "None (the task has no DataFrame output)"
    caps = CAPABILITIES.get(order["kind"], set())
    allowed = ["pyspark (functions as F, Window, types)", "datetime", "decimal", "re", "json",
               "math", "functools", "itertools"]
    if "urllib" in caps:
        allowed.append("urllib.request / urllib.parse")
    forbidden = ["credentials, passwords, tokens or JDBC URLs written into the code",
                 "os, sys, subprocess, socket, shutil, pathlib, open(), exec/eval/compile, "
                 "__import__", "spark.conf.set / changing session configuration",
                 "dbutils, oidlUtils (the notebook already read its parameters)"]
    if "jvm" in caps:
        forbidden.append("any network access other than that one JDBC connection")
    elif "urllib" not in caps:
        forbidden.append("any network access")
    if "secrets" not in caps:
        forbidden.append("aidputils")
    if "jvm" not in caps:
        forbidden.append("spark._jvm / spark._sc")
    context = {k: order.get(k) for k in ("reason", "kind", "object", "node", "inputs", "outputs",
                                         "parameters", "context")}
    return f"""You are completing one work order from an OCI Data Integration -> Oracle AI Data \
Platform (AIDP) migration. A deterministic compiler translated everything else in this \
notebook; this construct it could not translate safely.

Why: {order['reason']}

Task: {_KIND_GUIDANCE.get(order['kind'], _KIND_GUIDANCE['operator'])}

Write exactly one top-level Python function, with this exact signature:

    def {fn}(spark, inputs, params):

- `spark` is the SparkSession (Spark 3.5, Delta Lake).
- `inputs` maps "in0", "in1", ... to the input DataFrames listed under "inputs" below, in order.
- `params` maps each OCI-DI parameter name to its typed value (see "parameters").
- Return {returns}. "outputs" lists the fields the operator's output port declares. {_columns_rule(order)}

Allowed imports: {', '.join(allowed)}. Put imports inside the function.
Not allowed: {'; '.join(forbidden)}.

Be faithful, not plausible: if the OCI-DI semantics cannot be reproduced from the information \
below, the function must raise NotImplementedError with a one-line explanation of what is \
missing. Mark every inference you make with a comment starting "# ASSUMPTION:".

Reply with only the function, in one ```python fenced block.

Work order {order['id']}:
```json
{json.dumps(context, indent=2, default=str)}
```

The OCI-DI JSON of the construct (verbatim):
```json
{json.dumps(order.get('di_json'), indent=2, default=str)[:60000]}
```
"""
