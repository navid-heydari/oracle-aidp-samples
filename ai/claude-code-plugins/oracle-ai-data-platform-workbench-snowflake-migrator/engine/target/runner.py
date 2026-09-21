"""Bind a resolved target to a `run_sql(sql, params=None) -> list[dict]` callable
backed by the `aidp` CLI or `oci raw-request`.

The subprocess call is injected so this module is unit-testable without either
CLI installed. A non-zero exit or non-JSON output raises: a silent empty result
would make a failed CREATE indistinguishable from a success returning no rows.

`dry_run=True` returns (run_sql, planned_commands) and invokes nothing, so the
exact command can be shown to a human before anything executes.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Callable

from .executor import build_command, parse_cli_json

__all__ = ["BackendError", "make_run_sql", "spool_body"]

_MAX_STDERR = 500


def spool_body(body: dict, *, prefix: str) -> str:
    """Write a credential-bearing request body to a private temp file.

    In argv a body is visible to every user on the host via `ps`, and
    process-creation auditing records it permanently. So a body that carries
    `connectionDetails` travels by file -- created 0600 where the OS has mode
    bits -- and the command references the path. The caller unlinks it after
    the call, success or failure. One helper for every transport, so the
    `create_catalog` and `testConnection` paths cannot drift apart again.
    """
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(body, fh)
    return path


class BackendError(RuntimeError):
    """The aidp/oci CLI exited non-zero."""


def _default_run_process(cmd: list[str]):
    return subprocess.run(cmd, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")


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


def _printable(arg: str) -> str:
    """One argv element, safe to print.

    `connectionDetails` carries the Snowflake credential. The old flat
    truncation only hid it by luck -- any reordering of the body and it
    printed -- so redaction is explicit and keyed on content: the field NAMES
    are shown, the values never are.
    """
    if "connectionDetails" in arg:
        try:
            payload = json.loads(arg)
        except ValueError:
            payload = None
        if isinstance(payload, dict) \
                and isinstance(payload.get("connectionDetails"), dict):
            payload = {**payload, "connectionDetails": {
                k: "<redacted>" for k in payload["connectionDetails"]}}
            arg = json.dumps(payload)
        else:
            return "<redacted: carries connectionDetails>"
    return arg if len(arg) < 200 else arg[:200] + "…<truncated>"


def make_call(target, *, backend: str, run_process=None):
    """A `call(operation, **kwargs) -> dict` for the catalog CRUD transport.

    Separate from `make_run_sql` because the catalog API is not SQL: it takes
    an operation plus a JSON body and returns one object, not rows.
    """
    from .executor import build_command, parse_cli_json

    runner = run_process or _default_run_process

    def call(operation: str, **kwargs) -> dict:
        body = kwargs.get("body")
        spooled = None
        # A create_catalog body carries the credential. In argv it is visible
        # to every user on the host via `ps`, so it travels by file (0600,
        # removed after the call) and the command references the path.
        if operation == "create_catalog" and isinstance(body, dict) \
                and "connectionDetails" in body:
            spooled = spool_body(body, prefix="snowmig_catalog_")
            kwargs = {**kwargs, "body_file": spooled}
        try:
            cmd = build_command(backend, operation, target, **kwargs)
            print("  $ " + " ".join(_printable(c) for c in cmd))
            proc = runner(cmd)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"{operation} failed (exit {proc.returncode}): "
                    f"{(proc.stderr or proc.stdout or '')[:300]}")
            rows = parse_cli_json(proc.stdout)
        finally:
            if spooled:
                try:
                    os.unlink(spooled)
                except OSError:
                    pass
        # A list operation returns a COLLECTION; a create/get returns ONE
        # object. Collapsing both to rows[0] made key resolution see a single
        # schema instead of the list, so every read-back failed while the
        # objects had in fact been created.
        if operation.startswith("list_"):
            return {"items": rows}
        return rows[0] if rows else {}

    return call
