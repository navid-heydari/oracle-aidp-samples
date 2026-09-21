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
