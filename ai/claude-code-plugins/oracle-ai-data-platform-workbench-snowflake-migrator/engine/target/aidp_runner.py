"""Adapter: AIDP kernel -> the run_sql(sql, params=None) -> list[dict] shape.

⚠️ THE LIVE PATH IS UNVERIFIED. No AIDP environment was available when this was
written, so `make_aidp_run_sql` has never been executed against a real cluster.
The two pure helpers below (`wrap_sql`, `unwrap_outputs`) ARE tested, so a
failure here is isolated to the connection and event-loop handling.

Why an adapter is needed at all: the fork's ClusterSession is asyncio-based and
executes *Python code* on a Jupyter kernel, not SQL. So SQL is wrapped in a
spark.sql(...) call that prints its result rows as JSON, and the printed JSON is
parsed back out of the kernel's text output.

Kernel stdout arrives wrapped as [{"type": "TEXT_PLAIN", "value": "..."}]; every
consumer in the fork re-implements that unwrapping, so it is done once here.
"""
from __future__ import annotations

import json
from typing import Callable

__all__ = ["make_aidp_run_sql", "unwrap_outputs", "wrap_sql"]

_SENTINEL = "__SNOWMIG_ROWS__"


def wrap_sql(sql: str) -> str:
    """Wrap a SQL statement (or a `;`-joined batch) as printable kernel code.

    A batch is split and run statement by statement inside ONE kernel execution.
    That is deliberate: AIDP discards per-statement DDL when the session closes,
    so the whole chunk has to share a single execution context. Only the last
    statement's rows are returned, which is all `deploy` needs -- it probes each
    object separately rather than trusting the batch result.
    """
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        raise ValueError("no SQL statements to run")
    body = "\n".join(
        f"_r = spark.sql({stmt!r})" for stmt in statements)
    return (
        f"{body}\n"
        "try:\n"
        "    _rows = [r.asDict() for r in _r.collect()]\n"
        "except Exception:\n"
        "    _rows = []\n"
        f"print({_SENTINEL!r} + __import__('json').dumps(_rows, default=str))\n"
    )


def unwrap_outputs(result: dict) -> list[dict]:
    """Extract the row list a wrapped execution printed.

    Raises RuntimeError on a kernel error, because a silent empty list here would
    make a failed DDL batch look like a successful one that returned no rows.
    """
    if not isinstance(result, dict):
        raise RuntimeError(f"unexpected kernel result type: {type(result).__name__}")
    if result.get("status") not in (None, "ok"):
        raise RuntimeError(f"kernel error: {json.dumps(result)[:400]}")

    text_parts: list[str] = []
    for out in result.get("outputs") or []:
        if not isinstance(out, dict):
            continue
        if out.get("type") == "error" or out.get("ename"):
            raise RuntimeError(
                f'kernel error: {out.get("ename")}: {out.get("evalue")}')
        value = out.get("value") or out.get("text") or ""
        if isinstance(value, str):
            text_parts.append(value)

    for line in "".join(text_parts).splitlines():
        if line.startswith(_SENTINEL):
            return json.loads(line[len(_SENTINEL):] or "[]")
    return []


def make_aidp_run_sql(target, *, timeout: float = 300) -> Callable[..., list[dict]]:
    """Connect to the AIDP cluster and return a synchronous run_sql callable.

    ⚠️ UNVERIFIED against a live cluster -- see the module docstring.
    """
    import asyncio

    from .cluster_session import cluster

    loop = asyncio.new_event_loop()
    loop.run_until_complete(cluster.connect(
        target.cluster_id,
        datalake_ocid=target.datalake_ocid,
        workspace_id=target.workspace,
        session_name="aidp_snowmig",
    ))

    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        if params:
            raise ValueError(
                "bound parameters are not supported on the AIDP kernel; "
                "interpolate before calling")
        result = loop.run_until_complete(cluster.execute(wrap_sql(sql), timeout))
        return unwrap_outputs(result)

    return run_sql
