"""Driving an AIDP job from outside: run, poll, fetch the output.

Every shape here came from a live run, including the awkward ones: a task
run's output is an executed NOTEBOOK, and a run that dies before launching a
task has no task runs at all.
"""
import json

import pytest

from target import jobs

from target.jobs import (
    TERMINAL_STATES, extract_notebook_text, fetch_task_output, job_run_status,
    run_job, watch_job)


def _notebook_payload(*chunks, ename=None):
    nb = {"cells": [{"outputs": [{"text": list(chunks)}]
                     + ([{"ename": ename, "evalue": "boom"}] if ename else [])}]}
    return {"data": [{"type": "NOTEBOOK", "value": json.dumps(nb)}]}


class Fake:
    def __init__(self, *, states, output=None, task_runs=1):
        self.states = list(states)
        self.output = output if output is not None else _notebook_payload("ok\n")
        self.task_runs = task_runs
        self.ops: list[str] = []

    def __call__(self, operation, **kw):
        self.ops.append(operation)
        if operation == "run_job":
            return {"key": "run-1"}
        if operation == "get_job_run":
            state = self.states.pop(0) if self.states else "SUCCESS"
            return {"state": {"status": state, "stateMessage": f"in {state}"}}
        if operation == "list_task_runs":
            return {"items": [{"key": f"task-{i}"}
                              for i in range(self.task_runs)]}
        if operation == "fetch_task_output":
            return self.output
        raise AssertionError(operation)


def test_a_run_returns_its_key():
    assert run_job(Fake(states=[]), workspace="ws", job_key="j") == "run-1"


def test_a_run_with_no_key_raises_rather_than_returning_nothing():
    def call(operation, **kw):
        return {}

    with pytest.raises(RuntimeError, match="no key"):
        run_job(call, workspace="ws", job_key="j")


def test_an_absent_status_reads_unknown_not_success():
    def call(operation, **kw):
        return {}

    assert job_run_status(call, workspace="ws",
                          run_key="r")["status"] == "UNKNOWN"
    assert "UNKNOWN" not in TERMINAL_STATES


def test_watch_polls_until_a_terminal_state():
    fake = Fake(states=["PENDING", "RUNNING", "SUCCESS"])
    seen = []
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None,
                       on_poll=lambda status, n: seen.append(status))
    assert seen == ["PENDING", "RUNNING", "SUCCESS"]
    assert result["status"] == "SUCCESS"
    assert result["terminal"] is True and result["ok"] is True
    assert "ok" in result["output"]


def test_a_failure_is_terminal_but_not_ok():
    fake = Fake(states=["RUNNING", "FAILED"])
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["terminal"] is True
    assert result["ok"] is False
    assert result["status"] == "FAILED"


def test_a_budget_that_runs_out_is_reported_as_still_running():
    # Neither verdict: the job is simply not finished, and saying otherwise
    # is the failure mode this whole plugin is built against.
    fake = Fake(states=["RUNNING"] * 10)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=2, sleep=lambda _s: None)
    assert result["terminal"] is False
    assert result["ok"] is False
    assert result["status"] == "RUNNING"


def test_the_output_comes_from_the_task_run_not_the_job_run():
    fake = Fake(states=["SUCCESS"])
    watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
              sleep=lambda _s: None)
    assert "list_task_runs" in fake.ops
    assert "fetch_task_output" in fake.ops


def test_a_run_with_no_task_runs_yields_no_output_rather_than_raising():
    # A job that fails BEFORE launching its task has no task runs; that is
    # itself the diagnosis.
    fake = Fake(states=["FAILED"], task_runs=0)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["output"] == ""
    assert result["status"] == "FAILED"


def test_an_unfetchable_output_does_not_lose_the_verdict():
    class NoOutput(Fake):
        def __call__(self, operation, **kw):
            if operation == "fetch_task_output":
                raise RuntimeError("404")
            return super().__call__(operation, **kw)

    result = watch_job(NoOutput(states=["SUCCESS"]), workspace="ws",
                       job_key="j", poll_seconds=0, sleep=lambda _s: None)
    assert result["ok"] is True
    assert "unavailable" in result["output"]


def test_notebook_text_includes_stdout_and_the_exception():
    text = extract_notebook_text(
        _notebook_payload("[discover] 11 schema(s)\n", ename="RuntimeError"))
    assert "11 schema(s)" in text
    assert "RuntimeError: boom" in text


def test_an_unparseable_output_is_returned_rather_than_swallowed():
    text = extract_notebook_text({"data": [{"value": "not json at all"}]})
    assert "not json at all" in text


def test_a_plain_string_payload_is_passed_through():
    assert extract_notebook_text({"data": "raw log"}) == "raw log"


def test_fetch_task_output_is_two_calls():
    fake = Fake(states=[])
    assert "ok" in fetch_task_output(fake, workspace="ws", run_key="r")
    assert fake.ops == ["list_task_runs", "fetch_task_output"]


def test_a_second_run_is_refused_while_one_is_in_flight():
    """maxConcurrentRuns=1 ACCEPTS a second run and then discards it: created,
    ended instantly, no task output. The console meanwhile streams the OLD
    run's log, so a freshly deployed fix looks like it never took."""
    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": [{"key": "older", "endTime": None},
                              {"key": "done", "endTime": 123}]}
        raise AssertionError(f"must not reach {op}")

    with pytest.raises(jobs.JobRunCollision) as exc:
        jobs.watch_job(call, workspace="ws", job_key="j", sleep=lambda s: None)
    msg = str(exc.value)
    assert "older" in msg and "done" not in msg
    assert "cancel-job-run" in msg


def test_a_finished_previous_run_does_not_block_the_next():
    started = {}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": [{"key": "done", "endTime": 123}]}
        if op == "run_job":
            started["yes"] = True
            return {"key": "new"}
        if op == "get_job_run":
            return {"status": "SUCCESS"}
        return {}

    res = jobs.watch_job(call, workspace="ws", job_key="j",
                         sleep=lambda s: None, max_polls=1)
    assert started.get("yes") is True
    assert res["ok"] is True


def test_a_transport_that_cannot_list_runs_is_a_guard_not_a_gate():
    """The guard must never be the reason a migration cannot run."""
    def call(op, **kw):
        if op == "list_job_runs":
            raise RuntimeError("not supported on this build")
        if op == "run_job":
            return {"key": "new"}
        if op == "get_job_run":
            return {"status": "SUCCESS"}
        return {}

    res = jobs.watch_job(call, workspace="ws", job_key="j",
                         sleep=lambda s: None, max_polls=1)
    assert res["ok"] is True
