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
