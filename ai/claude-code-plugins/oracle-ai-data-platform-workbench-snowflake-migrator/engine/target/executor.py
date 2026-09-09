"""AIDP execution backends. Command construction is pure and testable.

The ladder, in order of preference:

  1. `aidp` CLI      -- the official client. Preferred when installed.
  2. `oci raw-request` -- the same REST API without the CLI. `oci ai-data-platform`
     covers only the control plane (instance lifecycle, work requests), NOT
     catalogs, schemas, tables or clusters, so the data plane goes through
     raw-request.

If neither is present that is a loud failure, not a silent no-op.

⚠️ The exact `aidp` CLI flags and the REST paths below are UNVERIFIED against a
live deployment -- the CLI is not installed here and no AIDP environment was
available. That is why `build_command` is pure and every command is printed
before it runs: a human can check the command against their deployment before
anything executes. Getting a flag wrong should produce an obvious CLI usage
error, not a silent partial migration.
"""
from __future__ import annotations

import json
import shutil
from typing import Callable

from .coords import region_from_ocid

__all__ = ["NoBackendAvailable", "BACKENDS", "build_command", "detect_backend",
           "parse_cli_json"]

BACKENDS = ("aidp_cli", "oci_raw")

_API_VERSION = "20240831"


class NoBackendAvailable(RuntimeError):
    """Neither the aidp CLI nor the oci CLI is installed."""


def detect_backend(*, which: Callable[[str], str | None] = shutil.which) -> str:
    if which("aidp"):
        return "aidp_cli"
    if which("oci"):
        return "oci_raw"
    raise NoBackendAvailable(
        "no AIDP execution backend found: install the `aidp` CLI (preferred) or "
        "the `oci` CLI. The migrator will not guess at a transport.")


def _endpoint(target) -> str:
    region = region_from_ocid(target.datalake_ocid)
    return f"https://aidp.{region}.oci.oraclecloud.com/{_API_VERSION}"


def build_command(backend: str, operation: str, target, **kwargs) -> list[str]:
    """Build the argv for one operation. Pure -- runs nothing."""
    if backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}; expected one of {BACKENDS}")

    if operation == "sql":
        sql = kwargs["sql"]
        if backend == "aidp_cli":
            return ["aidp", "sql", "execute",
                    "--datalake-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--cluster-id", target.cluster_id,
                    "--catalog", target.catalog,
                    "--statement", sql,
                    "--output", "json"]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/sql/execute",
                "--request-body",
                json.dumps({"clusterId": target.cluster_id,
                            "catalog": target.catalog,
                            "statement": sql})]

    if operation == "list_tables":
        schema = kwargs["schema"]
        if backend == "aidp_cli":
            return ["aidp", "catalog", "list-tables",
                    "--datalake-id", target.datalake_ocid,
                    "--catalog", target.catalog,
                    "--schema", schema,
                    "--output", "json"]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}/tables"
                f"?catalogKey={target.catalog}&schemaKey={schema}"]

    raise ValueError(
        f"unknown operation {operation!r}; this executor deliberately supports "
        "only 'sql' and 'list_tables' -- there is no drop or delete path")


def parse_cli_json(stdout: str) -> list[dict]:
    """Parse CLI/REST JSON into a row list.

    Non-JSON output raises. A silent empty list here would make a failed create
    indistinguishable from a success that returned no rows.
    """
    text = (stdout or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"backend output is not JSON ({exc.msg}): {text[:300]}") from exc
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "rows", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    raise RuntimeError(f"unexpected backend payload type: {type(payload).__name__}")
