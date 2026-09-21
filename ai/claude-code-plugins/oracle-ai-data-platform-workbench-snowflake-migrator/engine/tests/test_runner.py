"""run_sql over the CLI backends. The subprocess call is injected."""
import json
import types
import pytest

from target.coords import resolve_target
from target.runner import make_call, BackendError, make_run_sql

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="w", cluster_id="c", catalog="MYDB")


class FakeProc:
    def __init__(self, stdout="[]", returncode=0, stderr=""):
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr
        self.commands = []

    def __call__(self, cmd, **kw):
        self.commands.append(cmd)
        return type("R", (), {"stdout": self.stdout, "stderr": self.stderr,
                              "returncode": self.returncode})()


def test_select_returns_parsed_rows():
    proc = FakeProc('{"data": [{"tableName": "T"}]}')
    run_sql = make_run_sql(TARGET, backend="aidp_cli", run_process=proc)
    assert run_sql("SHOW TABLES IN x") == [{"tableName": "T"}]


def test_sql_is_passed_through_to_the_backend_command():
    proc = FakeProc()
    make_run_sql(TARGET, backend="aidp_cli", run_process=proc)("CREATE SCHEMA s")
    assert "CREATE SCHEMA s" in proc.commands[0]


def test_oci_backend_selected():
    proc = FakeProc()
    make_run_sql(TARGET, backend="oci_raw", run_process=proc)("CREATE SCHEMA s")
    assert proc.commands[0][0] == "oci"


def test_nonzero_exit_raises_with_stderr():
    proc = FakeProc(stdout="", returncode=1, stderr="NotAuthorizedOrNotFound")
    run_sql = make_run_sql(TARGET, backend="aidp_cli", run_process=proc)
    with pytest.raises(BackendError, match="NotAuthorizedOrNotFound"):
        run_sql("CREATE SCHEMA s")


def test_bound_parameters_are_refused():
    run_sql = make_run_sql(TARGET, backend="aidp_cli", run_process=FakeProc())
    with pytest.raises(ValueError, match="parameters"):
        run_sql("select 1", {"a": 1})


def test_non_json_output_surfaces_as_a_backend_error():
    proc = FakeProc(stdout="Usage: aidp sql execute [OPTIONS]")
    run_sql = make_run_sql(TARGET, backend="aidp_cli", run_process=proc)
    with pytest.raises(RuntimeError, match="not JSON"):
        run_sql("CREATE SCHEMA s")


def test_dry_run_records_commands_without_running_them():
    proc = FakeProc()
    run_sql, planned = make_run_sql(TARGET, backend="aidp_cli",
                                    run_process=proc, dry_run=True)
    assert run_sql("CREATE SCHEMA s") == []
    assert proc.commands == [], "dry run must not invoke the backend"
    assert "CREATE SCHEMA s" in planned[0]


# --------------------------------------------------------------------------
# A list operation returns a COLLECTION, a create/get returns ONE object.
# Returning rows[0] for both meant key resolution saw a single schema instead
# of the list, could not find its match, and every read-back failed while the
# objects had in fact been created.
# --------------------------------------------------------------------------

def _target():
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="ws", cluster_id="cl", catalog="lake")


def test_a_list_operation_returns_every_row_under_items():
    def fake(cmd):
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps({"data": {"items": [
                {"key": "lake.a"}, {"key": "lake.b"}]}}), stderr="")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    out = call("list_schemas", catalog="lake")
    assert [i["key"] for i in out["items"]] == ["lake.a", "lake.b"]


def test_an_empty_list_operation_returns_an_empty_items_list():
    def fake(cmd):
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps({"data": {"items": []}}), stderr="")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    assert call("list_tables_in", catalog="lake", schema="lake.s") == {"items": []}


def test_a_single_object_operation_returns_that_object():
    def fake(cmd):
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"data": {"key": "lake.s.t",
                                        "displayName": "t"}}), stderr="")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    out = call("create_table", catalog="lake", schema="s", table="t",
               body={"displayName": "t"})
    assert out["key"] == "lake.s.t"


def test_a_nonzero_exit_raises():
    def fake(cmd):
        return types.SimpleNamespace(returncode=1, stdout="", stderr="boom")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    with pytest.raises(RuntimeError):
        call("list_schemas", catalog="lake")


# --------------------------------------------------------------------------
# The create_catalog body carries the Snowflake credential.
#
# In argv it is visible to every user on the host via `ps`, and the old
# 200-char print truncation only hid it by luck -- reorder the body and it
# printed. So the body travels by FILE, and the printed command redacts the
# connectionDetails VALUES while keeping the field names, which the dry-run
# artifact already records.
# --------------------------------------------------------------------------

_SECRET = "s3cr3t-hunter2-Zx9"


def _catalog_body():
    return {"displayName": "snowcat", "catalogType": "EXTERNAL",
            "connectionDetails": {"accountName": "acct",
                                  "password": _SECRET}}


def test_the_catalog_credential_never_reaches_argv(capsys):
    seen = {}

    def fake(cmd):
        seen["cmd"] = list(cmd)
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps({"data": {"key": "k"}}), stderr="")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    call("create_catalog", body=_catalog_body())
    blob = " ".join(seen["cmd"])
    assert _SECRET not in blob, "the secret must not be an argv element"
    assert any(a.startswith("file://") for a in seen["cmd"]), \
        "the body must travel by file"
    assert _SECRET not in capsys.readouterr().out


def test_the_spooled_body_file_is_removed_after_the_call():
    import os
    seen = {}

    def fake(cmd):
        path = next(a[len("file://"):] for a in cmd if a.startswith("file://"))
        seen["path"] = path
        assert os.path.exists(path), "the file must exist while the CLI runs"
        with open(path, encoding="utf-8") as fh:
            assert _SECRET in fh.read(), "the CLI reads the real body"
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps({"data": {"key": "k"}}), stderr="")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    call("create_catalog", body=_catalog_body())
    assert not os.path.exists(seen["path"]), "the spool must not outlive the call"


def test_the_spool_is_removed_even_when_the_call_fails():
    import os
    seen = {}

    def fake(cmd):
        seen["path"] = next(a[len("file://"):] for a in cmd
                            if a.startswith("file://"))
        return types.SimpleNamespace(returncode=1, stdout="", stderr="denied")

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    with pytest.raises(RuntimeError):
        call("create_catalog", body=_catalog_body())
    assert not os.path.exists(seen["path"])


def test_the_printed_command_keeps_the_field_names(capsys):
    # The names are what a human checks against the deployment; only the
    # values are secret.
    from target.runner import _printable
    line = _printable(json.dumps(_catalog_body()))
    assert _SECRET not in line
    assert "accountName" in line and "password" in line
    assert "<redacted>" in line


def test_an_unparseable_argument_mentioning_the_details_is_fully_redacted():
    from target.runner import _printable
    assert "hunter" not in _printable('not-json connectionDetails hunter')
    assert "redacted" in _printable('not-json connectionDetails hunter')


def test_run_refuses_a_param_it_cannot_deliver():
    """AIDP job parameters reach a notebook as neither argv nor environment,
    so `--param schema=X` used to start a run that ignored it and executed
    whatever the PARAMS cell already held. A scope flag that silently does
    nothing reads as applied, which is worse than one that is absent."""
    import argparse
    import snowmig

    args = argparse.Namespace(
        out_dir=".", datalake_ocid="ocid1.aidataplatform.oc1.iad.aaaa",
        workspace="ws", cluster_id=None, catalog=None, backend=None,
        job="snowmig_01_structure", job_key="k", param=["schema=SALES"],
        poll_seconds=1, max_polls=1)
    with pytest.raises(snowmig.MissingTarget) as exc:
        snowmig.cmd_run(args)
    msg = str(exc.value)
    assert "schema" in msg
    assert "PARAMS" in msg
    assert "provision" in msg


# --------------------------------------------------------------------------
# A list operation follows `opc-next-page` until the server stops sending
# one. Reading page one only made every object past it "absent": a table
# read back after its create "never appeared", a schema was re-POSTed.
# --------------------------------------------------------------------------

def _envelope(items, next_page=None):
    env = {"data": {"items": items}, "status": "200 OK"}
    if next_page:
        env["headers"] = {"opc-next-page": next_page}
    return json.dumps(env)


def _paged_server(pages):
    """`pages` maps a page token (None for the first page) to
    (items, next_token). Records every URI asked."""
    asked = []

    def fake(cmd):
        uri = cmd[cmd.index("--target-uri") + 1]
        asked.append(uri)
        token = None
        if "page=" in uri:
            token = uri.rsplit("page=", 1)[1]
        items, nxt = pages[token]
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout=_envelope(items, nxt))

    return fake, asked


def test_a_list_operation_follows_opc_next_page():
    fake, asked = _paged_server({None: ([{"key": "c.s.t1"}], "P2"),
                                 "P2": ([{"key": "c.s.t2"}], None)})
    call = make_call(_target(), backend="oci_raw", run_process=fake)
    out = call("list_tables_in", catalog="c", schema="s")
    assert [i["key"] for i in out["items"]] == ["c.s.t1", "c.s.t2"]
    assert len(asked) == 2
    assert "page=" not in asked[0]
    assert asked[1].endswith("&page=P2")


def test_a_list_without_a_token_makes_one_request():
    fake, asked = _paged_server({None: ([{"key": "c.s.t1"}], None)})
    call = make_call(_target(), backend="oci_raw", run_process=fake)
    assert call("list_schemas", catalog="c")["items"] == [{"key": "c.s.t1"}]
    assert len(asked) == 1


def test_a_repeating_page_token_raises_instead_of_looping():
    fake, asked = _paged_server({None: ([{"key": "a"}], "P2"),
                                 "P2": ([{"key": "b"}], "P2")})
    call = make_call(_target(), backend="oci_raw", run_process=fake)
    with pytest.raises(RuntimeError, match="list_catalogs"):
        call("list_catalogs")
    assert len(asked) <= 3


def test_create_and_get_operations_are_not_paginated():
    asked = []

    def fake(cmd):
        asked.append(cmd)
        return types.SimpleNamespace(
            returncode=0, stderr="",
            stdout=json.dumps({"data": {"key": "lake.s.t"},
                               "headers": {"opc-next-page": "P2"}}))

    call = make_call(_target(), backend="oci_raw", run_process=fake)
    assert call("get_table", catalog="lake", schema="s",
                table="t")["key"] == "lake.s.t"
    assert len(asked) == 1


def test_the_aidp_cli_backend_refuses_a_truncated_listing():
    """The CLI's paging flags are undocumented, so a second page cannot be
    asked for. Returning page one as the whole would make every object past
    it absent; refusing says why."""
    def fake(cmd):
        return types.SimpleNamespace(
            returncode=0, stderr="",
            stdout="Response:\n" + _envelope([{"key": "lake.a"}], "P2"))

    call = make_call(_target(), backend="aidp_cli", run_process=fake)
    with pytest.raises(RuntimeError, match="page"):
        call("list_schemas", catalog="lake")


def test_read_back_finds_an_object_created_past_page_one():
    """deploy_catalog through the real transport against a server that pages
    every list at ONE item: both tables must verify, and nothing may be
    diagnosed as burned."""
    from target.catalog_deploy import deploy_catalog

    state = {"schemas": {}, "tables": {}}

    def page(items, token):
        # One item per page, tokens are 1-based offsets.
        start = int(token or 0)
        chunk = items[start:start + 1]
        nxt = str(start + 1) if start + 1 < len(items) else None
        return _envelope(chunk, nxt)

    def fake(cmd):
        method = cmd[cmd.index("--http-method") + 1]
        uri = cmd[cmd.index("--target-uri") + 1]
        token = uri.rsplit("page=", 1)[1] if "page=" in uri else None
        path = uri.split("?", 1)[0]
        body = (json.loads(cmd[cmd.index("--request-body") + 1])
                if "--request-body" in cmd else {})
        if method == "GET" and path.endswith("/catalogs"):
            return types.SimpleNamespace(returncode=0, stderr="", stdout=page(
                [{"displayName": "lake", "catalogType": "STANDARD"}], token))
        if method == "GET" and path.endswith("/schemas"):
            return types.SimpleNamespace(returncode=0, stderr="", stdout=page(
                [{"key": k, "lifecycleState": "ACTIVE"}
                 for k in state["schemas"]], token))
        if method == "POST" and path.endswith("/schemas"):
            key = f'{body["catalogKey"]}.{body["displayName"]}'.lower()
            state["schemas"][key] = True
            return types.SimpleNamespace(returncode=0, stderr="",
                                         stdout=json.dumps({"data": {"key": key}}))
        if method == "POST" and path.endswith("/tables"):
            key = f'{body["schemaKey"]}.{body["displayName"]}'.lower()
            state["tables"][key] = body["tableFields"]
            return types.SimpleNamespace(returncode=0, stderr="",
                                         stdout=json.dumps({"data": {"key": key}}))
        if method == "GET" and path.endswith("/tables"):
            return types.SimpleNamespace(returncode=0, stderr="", stdout=page(
                [{"key": k} for k in sorted(state["tables"])], token))
        if method == "GET" and "/tables/" in path:
            key = path.rsplit("/tables/", 1)[1].lower()
            return types.SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
                {"data": {"key": key, "tableFields": state["tables"][key]}}))
        raise AssertionError((method, uri))

    plan = {"statements": [
        {"source_identifier": f"DB.PUBLIC.T{i}", "object_type": "TABLE",
         "target_fqn": f"lake.DB.T{i}", "sql": "",
         "expected_columns": [{"name": "A", "type": "STRING"}]}
        for i in range(3)], "blocked": []}
    call = make_call(_target(), backend="oci_raw", run_process=fake)
    out = deploy_catalog(plan, target=_target(), execute=True, call=call,
                         retry_delays=(), verify_delays=(), schema_wait=())
    assert out["verified"] == 3, out["failed"]
    assert out["failed_targets"] == []
    assert out["poisoned_names"] == [] and out["diagnosis_probes"] == []
