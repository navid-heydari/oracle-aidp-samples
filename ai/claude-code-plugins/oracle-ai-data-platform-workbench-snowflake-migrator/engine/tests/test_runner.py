"""run_sql over the CLI backends. The subprocess call is injected."""
import pytest

from target.coords import resolve_target
from target.runner import BackendError, make_run_sql

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
