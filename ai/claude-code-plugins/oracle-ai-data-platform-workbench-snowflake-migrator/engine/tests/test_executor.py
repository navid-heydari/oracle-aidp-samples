"""AIDP execution backend: aidp CLI preferred, oci raw-request fallback."""
import json

import pytest

from target.coords import resolve_target
from target.executor import (
    BackendError, NoBackendAvailable, StatementTooLarge, build_command, detect_backend,
    parse_cli_json,
)

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.aaaa",
                        workspace="ws-1", cluster_id="cl-1", catalog="MYDB")


# --- backend detection ----------------------------------------------------

def test_prefers_the_aidp_cli_when_present():
    assert detect_backend(which=lambda n: "/usr/bin/" + n) == "aidp_cli"


def test_falls_back_to_oci_raw_when_aidp_is_absent():
    assert detect_backend(which=lambda n: None if n == "aidp" else "/x/oci") == "oci_raw"


def test_no_backend_is_a_loud_failure():
    with pytest.raises(NoBackendAvailable, match="aidp"):
        detect_backend(which=lambda n: None)


# --- command construction -------------------------------------------------

def test_aidp_cli_sql_command_shape():
    cmd = build_command("aidp_cli", "sql", TARGET, sql="CREATE SCHEMA x")
    assert cmd[0] == "aidp"
    assert "--cluster-id" in cmd and "cl-1" in cmd
    assert "CREATE SCHEMA x" in cmd


def test_oci_raw_sql_command_targets_the_datalake_endpoint():
    cmd = build_command("oci_raw", "sql", TARGET, sql="CREATE SCHEMA x")
    assert cmd[0] == "oci"
    assert "raw-request" in cmd
    joined = " ".join(cmd)
    assert "ocid1.aidataplatform.oc1.iad.aaaa" in joined
    assert "us-ashburn-1" in joined, "region derived from the OCID"


def test_region_comes_from_the_ocid_not_a_default():
    phx = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.phx.a",
                         workspace="w", cluster_id="c", catalog="C")
    assert "us-phoenix-1" in " ".join(build_command("oci_raw", "sql", phx, sql="x"))


def test_unmapped_region_refuses_rather_than_defaulting():
    bad = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.zzz.a",
                         workspace="w", cluster_id="c", catalog="C")
    with pytest.raises(ValueError, match="zzz"):
        build_command("oci_raw", "sql", bad, sql="x")


def test_list_tables_command_available_on_both_backends():
    for backend in ("aidp_cli", "oci_raw"):
        cmd = build_command(backend, "list_tables", TARGET, schema="PUBLIC")
        assert cmd and isinstance(cmd, list)


def test_unknown_operation_rejected():
    with pytest.raises(ValueError, match="unknown operation"):
        build_command("aidp_cli", "drop_everything", TARGET)


def test_unknown_backend_rejected():
    with pytest.raises(ValueError, match="unknown backend"):
        build_command("telepathy", "sql", TARGET, sql="x")


def test_no_command_contains_a_destructive_verb():
    for backend in ("aidp_cli", "oci_raw"):
        for op, kw in (("sql", {"sql": "CREATE SCHEMA x"}),
                       ("list_tables", {"schema": "S"})):
            joined = " ".join(build_command(backend, op, TARGET, **kw)).upper()
            for verb in (" DROP ", " TRUNCATE ", " DELETE ", "OR REPLACE"):
                assert verb not in joined


# --- response parsing -----------------------------------------------------

def test_parses_json_output():
    assert parse_cli_json('{"data": [{"a": 1}]}') == [{"a": 1}]


def test_parses_a_bare_json_list():
    assert parse_cli_json('[{"a": 1}]') == [{"a": 1}]


def test_empty_output_is_an_empty_list_not_an_error():
    assert parse_cli_json("") == []
    assert parse_cli_json("   \n") == []


def test_non_json_output_raises_rather_than_returning_empty():
    # A silent [] here would make a failed create look like a success that
    # returned no rows.
    with pytest.raises(RuntimeError, match="not JSON"):
        parse_cli_json("ServiceError: NotAuthorizedOrNotFound")


def test_json_object_without_data_key_is_wrapped():
    assert parse_cli_json('{"tableName": "T"}') == [{"tableName": "T"}]


# --- notebook operations --------------------------------------------------

@pytest.mark.parametrize("backend", ["aidp_cli", "oci_raw"])
def test_upload_notebook_command_carries_both_paths(backend):
    cmd = build_command(backend, "upload_notebook", TARGET,
                        workspace_path="/Workspace/Shared/nb.ipynb",
                        local_path="/tmp/nb.ipynb")
    joined = " ".join(cmd)
    assert "/Workspace/Shared/nb.ipynb" in joined
    assert "/tmp/nb.ipynb" in joined


@pytest.mark.parametrize("backend", ["aidp_cli", "oci_raw"])
def test_run_notebook_command_names_the_cluster(backend):
    cmd = build_command(backend, "run_notebook", TARGET,
                        workspace_path="/Workspace/Shared/nb.ipynb")
    assert "cl-1" in " ".join(cmd)


@pytest.mark.parametrize("backend", ["aidp_cli", "oci_raw"])
def test_run_status_command_carries_the_run_id(backend):
    cmd = build_command(backend, "run_status", TARGET, run_id="run-42")
    assert "run-42" in " ".join(cmd)


def test_notebook_operations_contain_no_destructive_verb():
    for backend in ("aidp_cli", "oci_raw"):
        for op, kw in (("upload_notebook", {"workspace_path": "/p",
                                            "local_path": "/l"}),
                       ("run_notebook", {"workspace_path": "/p"}),
                       ("run_status", {"run_id": "r"})):
            joined = " ".join(build_command(backend, op, TARGET, **kw)).upper()
            for verb in (" DROP ", " DELETE ", " TRUNCATE "):
                assert verb not in joined


# --------------------------------------------------------------------------
# A batch too large for argv must fail with advice, not E2BIG (issue #19).
# --------------------------------------------------------------------------

def test_an_oversized_statement_is_refused_with_advice():
    target = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                            workspace="ws", cluster_id="cl", catalog="CAT")
    huge = "SELECT 1;\n" * 40_000
    with pytest.raises(StatementTooLarge) as exc:
        build_command("aidp_cli", "sql", target, sql=huge)
    assert "--chunk-size" in str(exc.value)


def test_a_normal_statement_is_not_refused():
    target = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                            workspace="ws", cluster_id="cl", catalog="CAT")
    assert build_command("aidp_cli", "sql", target, sql="SELECT 1")


# ==========================================================================
# `oci raw-request` exits 0 on an HTTP error and puts the error in the BODY.
#
# Found live: POST to a non-existent endpoint returned exit 0 with
#   {"data": {"code": "NotAuthorizedOrNotFound", ...}, "status": "404 Not Found"}
# `return [payload]` turned that error object into ONE ROW, so the SMOKE TEST
# REPORTED PASS on a 404 -- in the one stage whose entire job is to tell the
# user whether the destination works.
# ==========================================================================

_ERR = {"data": {"code": "NotAuthorizedOrNotFound",
                 "message": "Authorization failed or requested resource not found."},
        "status": "404 Not Found"}


def test_an_http_error_status_in_the_body_is_raised_not_returned_as_a_row():
    with pytest.raises(BackendError) as exc:
        parse_cli_json(json.dumps(_ERR))
    assert "404" in str(exc.value)
    assert "NotAuthorizedOrNotFound" in str(exc.value)


@pytest.mark.parametrize("status", ["400 Bad Request", "401 Unauthorized",
                                    "403 Forbidden", "404 Not Found",
                                    "409 Conflict", "500 Internal Server Error"])
def test_every_error_class_is_raised(status):
    with pytest.raises(BackendError):
        parse_cli_json(json.dumps({"data": {"code": "X"}, "status": status}))


def test_a_2xx_status_is_accepted():
    rows = parse_cli_json(json.dumps({"data": {"items": [{"key": "a"}]},
                                      "status": "200 OK"}))
    assert rows == [{"key": "a"}]


def test_nested_data_items_is_unwrapped_to_rows():
    # The real shape of every AIDP collection response. Without unwrapping,
    # three schemas were reported as "1 schema(s) visible".
    rows = parse_cli_json(json.dumps({"data": {"items": [
        {"key": "lake.bronze"}, {"key": "lake.default"}, {"key": "lake.scd"}]}}))
    assert len(rows) == 3
    assert rows[0]["key"] == "lake.bronze"


def test_an_empty_collection_is_zero_rows_not_one_envelope():
    assert parse_cli_json(json.dumps({"data": {"items": []}})) == []


def test_an_error_code_without_a_status_field_is_still_caught():
    with pytest.raises(BackendError):
        parse_cli_json(json.dumps(
            {"data": {"code": "NotAuthorizedOrNotFound", "message": "nope"}}))


def test_a_legitimate_row_containing_the_word_code_is_not_an_error():
    rows = parse_cli_json(json.dumps({"data": {"items": [{"code": "US"}]}}))
    assert rows[0]["code"] == "US"


def test_a_response_with_no_status_field_is_still_accepted():
    assert parse_cli_json(json.dumps({"rows": [{"a": 1}]})) == [{"a": 1}]


# --------------------------------------------------------------------------
# Catalog CRUD transport (T1) and the qualified schemaKey (T3).
# --------------------------------------------------------------------------

def _t():
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="ws", cluster_id="cl", catalog="lake")


def test_create_table_posts_to_the_tables_collection():
    cmd = build_command("oci_raw", "create_table", _t(),
                        catalog="lake", schema="DB", table="T",
                        body={"displayName": "T"})
    assert cmd[:4] == ["oci", "raw-request", "--http-method", "POST"]
    assert cmd[cmd.index("--target-uri") + 1].endswith("/tables")
    assert '"displayName": "T"' in cmd[cmd.index("--request-body") + 1]


def test_get_table_addresses_the_fully_qualified_key():
    cmd = build_command("oci_raw", "get_table", _t(),
                        catalog="lake", schema="DB", table="T")
    assert cmd[cmd.index("--target-uri") + 1].endswith("/tables/lake.DB.T")


def test_list_tables_qualifies_a_bare_schema_key():
    # A bare schemaKey returns 400 InvalidParameter -- verified live.
    uri = build_command("oci_raw", "list_tables", _t(), schema="DB")[
        build_command("oci_raw", "list_tables", _t(), schema="DB").index(
            "--target-uri") + 1]
    assert "schemaKey=lake.DB" in uri


def test_an_already_qualified_schema_key_is_not_doubled():
    uri = build_command("oci_raw", "list_tables", _t(), schema="lake.DB")[
        build_command("oci_raw", "list_tables", _t(), schema="lake.DB").index(
            "--target-uri") + 1]
    assert "schemaKey=lake.DB" in uri
    assert "lake.lake" not in uri
