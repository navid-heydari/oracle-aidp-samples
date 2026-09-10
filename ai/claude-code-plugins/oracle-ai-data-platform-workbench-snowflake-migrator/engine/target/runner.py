"""Bind a resolved target to a `run_sql(sql, params=None) -> list[dict]` callable
backed by the `aidp` CLI or `oci raw-request`.

The subprocess call is injected so this module is unit-testable without either
CLI installed. A non-zero exit or non-JSON output raises: a silent empty result
would make a failed CREATE indistinguishable from a success returning no rows.

`dry_run=True` returns (run_sql, planned_commands) and invokes nothing, so the
exact command can be shown to a human before anything executes.
"""
from __future__ import annotations

import subprocess
from typing import Callable

from .executor import build_command, parse_cli_json

__all__ = ["BackendError", "make_run_sql"]

_MAX_STDERR = 500


class BackendError(RuntimeError):
    """The aidp/oci CLI exited non-zero."""


def _default_run_process(cmd: list[str]):
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def make_run_sql(target, *, backend: str,
                 run_process: Callable[..., object] | None = None,
                 dry_run: bool = False):
    proc = run_process or _default_run_process
    planned: list[str] = []

    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        if params:
            raise ValueError(
                "bound parameters are not supported by the AIDP backends; "
                "interpolate before calling")
        cmd = build_command(backend, "sql", target, sql=sql)
        if dry_run:
            planned.append(" ".join(cmd))
            return []
        result = proc(cmd)
        if getattr(result, "returncode", 0) != 0:
            raise BackendError(
                f"{backend} exited {result.returncode}: "
                f"{(result.stderr or '')[:_MAX_STDERR]}")
        return parse_cli_json(getattr(result, "stdout", ""))

    return (run_sql, planned) if dry_run else run_sql


def make_call(target, *, backend: str, run_process=None):
    """A `call(operation, **kwargs) -> dict` for the catalog CRUD transport.

    Separate from `make_run_sql` because the catalog API is not SQL: it takes
    an operation plus a JSON body and returns one object, not rows.
    """
    from .executor import build_command, parse_cli_json

    runner = run_process or _default_run_process

    def call(operation: str, **kwargs) -> dict:
        cmd = build_command(backend, operation, target, **kwargs)
        printable = [c if len(c) < 200 else c[:200] + "…<truncated>" for c in cmd]
        print("  $ " + " ".join(printable))
        proc = runner(cmd)
        if proc.returncode != 0:
            raise RuntimeError(
                f"{operation} failed (exit {proc.returncode}): "
                f"{(proc.stderr or proc.stdout or '')[:300]}")
        rows = parse_cli_json(proc.stdout)
        # A list operation returns a COLLECTION; a create/get returns ONE
        # object. Collapsing both to rows[0] made key resolution see a single
        # schema instead of the list, so every read-back failed while the
        # objects had in fact been created.
        if operation.startswith("list_"):
            return {"items": rows}
        return rows[0] if rows else {}

    return call
