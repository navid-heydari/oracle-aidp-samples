"""`snowmig.py run` end to end against a fake transport: the exit code and
RUN_*.md are the contract a chained shell and an operator read.

Exit 0 means the job SUCCEEDED, or the poll budget ran out with the job
genuinely still running (STILL RUNNING is never rounded to a verdict). A run
that ended in any other terminal state, a state this plugin does not
recognise, or a cold-start cancel that could not be confirmed all exit 1.
"""
import argparse
import functools

import pytest

import snowmig
from target import jobs, provisioning


OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    # cmd_run takes watch_job's default `sleep=time.sleep`, bound at import,
    # so the poll and cancel delays are real seconds unless injected here.
    monkeypatch.setattr(jobs, "watch_job",
                        functools.partial(jobs.watch_job,
                                          sleep=lambda _s: None))


def _args(tmp_path, **over):
    base = dict(out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws",
                cluster_id=None, catalog=None, backend=None, config=None,
                job="snowmig_01_structure", job_key="job-k", param=None,
                poll_seconds=0, max_polls=2, cold_start_seconds=60,
                cold_start_restarts=1)
    base.update(over)
    return argparse.Namespace(**base)


class Runs:
    """A job whose every run reads `status`; `started` says whether the
    cluster picked the task up; `cancel` is what cancel_job_run does."""

    def __init__(self, status, *, started=True, cancel=None):
        self.status = status
        self.started = started
        self.cancel = cancel
        self.ops: list[str] = []
        self.submitted = 0

    def __call__(self, op, **kw):
        self.ops.append(op)
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            self.submitted += 1
            return {"key": f"run-{self.submitted}"}
        if op == "get_job_run":
            return {"state": {"status": self.status, "stateMessage": "msg"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1",
                               "startTime": 1789854291517 if self.started
                               else None}]}
        if op == "cancel_job_run":
            if isinstance(self.cancel, Exception):
                raise self.cancel
            return {}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)


def _install(monkeypatch, fake):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)


def _run_md(tmp_path):
    return (tmp_path / "RUN_snowmig_01_structure.md").read_text(
        encoding="utf-8")


@pytest.mark.parametrize("state", ["INTERNAL_ERROR", "SKIPPED",
                                   "UPSTREAM_CANCELED", "EXCLUDED", "FAILED"])
def test_a_run_that_ended_badly_exits_1_and_names_the_state(
        tmp_path, monkeypatch, state):
    fake = Runs(state, started=False)
    _install(monkeypatch, fake)
    rc = snowmig.cmd_run(_args(tmp_path, max_polls=6, poll_seconds=30))
    assert rc == 1
    md = _run_md(tmp_path)
    assert f"**{state}**" in md
    assert "STILL RUNNING" not in md
    assert fake.submitted == 1, "a dead run is not a cold start"
    assert "cancel_job_run" not in fake.ops


def test_a_spent_budget_on_a_running_job_keeps_exit_0_and_says_so(
        tmp_path, monkeypatch, capsys):
    _install(monkeypatch, Runs("RUNNING"))
    rc = snowmig.cmd_run(_args(tmp_path, max_polls=2))
    assert rc == 0
    md = _run_md(tmp_path)
    assert "STILL RUNNING" in md
    assert "2 poll" in md, "the report says the budget was spent"
    assert "STILL RUNNING" in capsys.readouterr().out


def test_an_unrecognised_state_exits_1_rather_than_still_running(
        tmp_path, monkeypatch):
    fake = Runs("SOME_FUTURE_STATE", started=False)
    _install(monkeypatch, fake)
    rc = snowmig.cmd_run(_args(tmp_path, max_polls=4, poll_seconds=30))
    assert rc == 1
    md = _run_md(tmp_path)
    assert "SOME_FUTURE_STATE" in md
    assert "STILL RUNNING" not in md
    assert "unrecognised" in md.lower()
    assert fake.submitted == 1


def test_an_unconfirmed_cold_start_cancel_exits_1_and_says_so(
        tmp_path, monkeypatch, capsys):
    """Only the `oci` CLI on this machine: the cancel (the one job operation
    routed through `aidp`) raises. Nothing may be resubmitted into the slot
    run-1 still holds, and the report must say what happened."""
    fake = Runs("RUNNING", started=False,
                cancel=FileNotFoundError(2, "aidp not found"))
    _install(monkeypatch, fake)
    rc = snowmig.cmd_run(_args(tmp_path, max_polls=6, poll_seconds=30))
    assert rc == 1
    assert fake.submitted == 1
    md = _run_md(tmp_path)
    assert "cold start suspected" in md and "cancel unconfirmed" in md
    assert "FileNotFoundError" in md
    assert "resubmitted as" not in md.lower()
    assert "`None`" not in md
    assert "cancel unconfirmed" in capsys.readouterr().out


def test_a_confirmed_restart_still_renders_the_resubmitted_run():
    md = snowmig._render_run({
        "job": "j", "job_key": "k", "workspace": "ws", "run_key": "run-2",
        "status": "SUCCESS", "message": "", "output": "ok", "terminal": True,
        "ok": True, "polls": 3, "unrecognised": False,
        "cancel_unconfirmed": False,
        "restarts": [{"abandoned_run": "run-1", "cancel_state": "CANCELED",
                      "cancel_error": None, "new_run": "run-2",
                      "after_seconds": 60.0}]})
    assert "**SUCCESS**" in md
    assert "`run-1`" in md and "`run-2`" in md
    assert "CANCELED" in md


def test_the_param_refusal_names_the_flag_that_really_rewrites_params(tmp_path):
    """`--reuse-existing` alone now KEEPS an existing stage notebook, so the
    text that sends the operator to provision to set PARAMS must name
    `--refresh-notebooks`, or it sends them to a command that changes
    nothing."""
    with pytest.raises(snowmig.MissingTarget) as exc:
        snowmig.cmd_run(_args(tmp_path, param=["schema=SALES"]))
    assert "--refresh-notebooks" in str(exc.value)
