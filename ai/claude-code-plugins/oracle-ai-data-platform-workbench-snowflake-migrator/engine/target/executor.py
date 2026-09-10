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
import re
import shutil
from typing import Callable

from .coords import region_from_ocid

__all__ = ["BackendError", "NoBackendAvailable", "StatementTooLarge",
           "BACKENDS",
           "build_command", "detect_backend", "parse_cli_json",
           "MAX_ARGV_STATEMENT"]

# SQL is passed as one argv element. A single argument is capped well below
# ARG_MAX (128KB on Linux, 256KB on macOS), and exceeding it produces a bare
# E2BIG from the kernel with no hint about what to do. Refuse earlier, and say
# which knob fixes it.
MAX_ARGV_STATEMENT = 100_000

BACKENDS = ("aidp_cli", "oci_raw")

_API_VERSION = "20240831"


class BackendError(RuntimeError):
    """The backend returned an error. Raised, never returned as data.

    `oci raw-request` exits 0 on an HTTP error and puts the error in the
    response BODY, so exit status proves nothing. Treating that body as data
    made a 404 look like one row of results, and the smoke test reported PASS
    against an endpoint that does not exist.
    """


class NoBackendAvailable(RuntimeError):
    """Neither the aidp CLI nor the oci CLI is installed."""


class StatementTooLarge(ValueError):
    """The SQL will not fit in a single command-line argument."""


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
        if len(sql.encode()) > MAX_ARGV_STATEMENT:
            raise StatementTooLarge(
                f"the batch is {len(sql.encode()):,} bytes, over the "
                f"{MAX_ARGV_STATEMENT:,}-byte limit for one command-line "
                f"argument. Lower --chunk-size and re-run; the deployment is "
                f"chunked precisely so this is adjustable.")
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

    # --- catalog CRUD: the working transport for a structure-only clone ---
    if operation in ("create_schema", "create_table", "create_view"):
        relation = {"create_schema": "schemas", "create_table": "tables",
                    "create_view": "views"}[operation]
        body = json.dumps(kwargs["body"])
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    {"create_schema": "create", "create_table": "create-table",
                     "create_view": "create-view"}[operation],
                    "--datalake-id", target.datalake_ocid,
                    "--from-json", body, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/{relation}",
                "--request-body", body]

    if operation == "list_schemas":
        if backend == "aidp_cli":
            return ["aidp", "schema", "list", "--datalake-id",
                    target.datalake_ocid, "--catalog-key", target.catalog,
                    "--output", "json"]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/schemas"
                                f"?catalogKey={target.catalog}"]

    if operation in ("list_tables_in", "list_views_in"):
        relation = "tables" if operation == "list_tables_in" else "views"
        # Fully qualified: a bare schemaKey returns 400 InvalidParameter.
        schema = kwargs["schema"]
        qualified = (schema if schema.startswith(f'{kwargs["catalog"]}.')
                     else f'{kwargs["catalog"]}.{schema}')
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    "list-tables" if relation == "tables" else "list-views",
                    "--datalake-id", target.datalake_ocid,
                    "--catalog-key", kwargs["catalog"],
                    "--schema-key", qualified, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/{relation}"
                                f'?catalogKey={kwargs["catalog"]}'
                                f"&schemaKey={qualified}"]

    if operation in ("get_table", "get_view"):
        relation = "tables" if operation == "get_table" else "views"
        name = kwargs.get("table") or kwargs.get("view")
        # Objects are addressed by their fully-qualified KEY.
        key = f'{kwargs["catalog"]}.{kwargs["schema"]}.{name}'
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    "get-table" if operation == "get_table" else "get-view",
                    "--datalake-id", target.datalake_ocid,
                    "--key", key, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/{relation}/{key}"]

    if operation == "list_tables":
        schema = kwargs["schema"]
        if backend == "aidp_cli":
            return ["aidp", "catalog", "list-tables",
                    "--datalake-id", target.datalake_ocid,
                    "--catalog", target.catalog,
                    "--schema", schema,
                    "--output", "json"]
        # schemaKey must be FULLY QUALIFIED. A bare schema returns 400
        # InvalidParameter -- verified live.
        qualified = (schema if schema.startswith(f"{target.catalog}.")
                     else f"{target.catalog}.{schema}")
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}/tables"
                f"?catalogKey={target.catalog}&schemaKey={qualified}"]

    if operation == "upload_notebook":
        path, local = kwargs["workspace_path"], kwargs["local_path"]
        if backend == "aidp_cli":
            return ["aidp", "workspace", "upload",
                    "--datalake-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--path", path, "--file", local, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "PUT",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/notebook/workspaces/{target.workspace}/api/contents{path}",
                "--request-body", f"file://{local}"]

    if operation == "run_notebook":
        path = kwargs["workspace_path"]
        if backend == "aidp_cli":
            return ["aidp", "notebook", "run",
                    "--datalake-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--cluster-id", target.cluster_id,
                    "--path", path, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/notebookRuns",
                "--request-body",
                json.dumps({"clusterId": target.cluster_id, "notebookPath": path})]

    if operation == "run_status":
        run_id = kwargs["run_id"]
        if backend == "aidp_cli":
            return ["aidp", "notebook", "run-status",
                    "--datalake-id", target.datalake_ocid,
                    "--run-id", run_id, "--output", "json"]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/notebookRuns/{run_id}"]

    raise ValueError(
        f"unknown operation {operation!r}; this executor deliberately supports "
        "only 'sql', 'list_tables', 'upload_notebook', 'run_notebook' and "
        "'run_status' -- there is no drop or delete path")


def parse_cli_json(stdout: str) -> list[dict]:
    """Parse CLI/REST JSON into a row list.

    Non-JSON output raises. A silent empty list here would make a failed create
    indistinguishable from a success that returned no rows.

    An HTTP error carried in the BODY also raises. `oci raw-request` exits 0 on
    a 404 and returns `{"data": {"code": ...}, "status": "404 Not Found"}`, so
    the exit code proves nothing and the old `return [payload]` turned that
    error object into one row of "results".
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
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"unexpected backend payload type: {type(payload).__name__}")

    _raise_if_error(payload)

    data = payload.get("data")
    # Collections arrive as {"data": {"items": [...]}} -- unwrap, or three
    # schemas get reported as one.
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    for key in ("data", "items", "rows", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    if isinstance(data, dict):
        return [data]
    return [payload]


def _raise_if_error(payload: dict) -> None:
    """Raise BackendError if this envelope reports an HTTP or service error."""
    status = str(payload.get("status") or "")
    code_match = re.match(r"\s*(\d{3})", status)
    if code_match and not 200 <= int(code_match.group(1)) < 300:
        detail = payload.get("data")
        raise BackendError(
            f"backend returned {status.strip()}: "
            f"{json.dumps(detail)[:300] if detail is not None else '(no body)'}")

    # Defence in depth: an OCI error object is recognisable without a status.
    data = payload.get("data")
    if isinstance(data, dict) and "code" in data and "message" in data \
            and "items" not in data:
        raise BackendError(
            f'backend returned an error: {data["code"]} — {data["message"]}')
