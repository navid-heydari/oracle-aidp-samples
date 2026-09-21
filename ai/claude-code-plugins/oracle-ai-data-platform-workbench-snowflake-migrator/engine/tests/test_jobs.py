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
            # A real `startTime`: the cluster took the task. Its absence is
            # what the cold-start watchdog reads as never-picked-up.
            return {"items": [{"key": f"task-{i}", "startTime": 1789854291517}
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


# --- the cold-start watchdog ------------------------------------------------
#
# A cluster sometimes never picks up a job run -- characteristically the first
# run on a newly created workspace. Live 2026-09-19: the run sat at RUNNING
# for 9+ minutes with `startTime: null` on its task run and never failed;
# resubmitting the identical job after a cancel succeeded in 90 seconds. The
# job-run status cannot see this, so the task run's `startTime` is the signal.


class ColdStart:
    """A cluster that ignores the first N runs, then behaves."""

    def __init__(self, *, ignore_runs=1):
        self.ignore_runs = ignore_runs
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def _wedged(self, run_key):
        return self.submitted.index(run_key) < self.ignore_runs

    def __call__(self, operation, **kw):
        if operation == "list_job_runs":
            return {"items": []}
        if operation == "run_job":
            key = f"run-{len(self.submitted) + 1}"
            self.submitted.append(key)
            return {"key": key}
        if operation == "get_job_run":
            key = kw["key"]
            if key in self.cancelled:
                return {"state": {"status": "CANCELED"}}
            # A wedged run reports RUNNING forever -- it never fails, which
            # is exactly why nothing times out and the operator sees nothing.
            return {"state": {"status": "RUNNING" if self._wedged(key)
                              else "SUCCESS"}}
        if operation == "list_task_runs":
            started = None if self._wedged(kw["run_key"]) else 1789854291517
            return {"items": [{"key": "t1", "startTime": started}]}
        if operation == "cancel_job_run":
            self.cancelled.append(kw["run_key"])
            return {}
        if operation == "fetch_task_output":
            return _notebook_payload("ok\n")
        raise AssertionError(operation)


def test_a_run_the_cluster_never_picks_up_is_cancelled_and_resubmitted():
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10,
                    sleep=lambda s: None)
    assert fake.submitted == ["run-1", "run-2"]
    assert fake.cancelled == ["run-1"]
    assert res["ok"] is True
    # The result must point at the run that actually ran, not the first one.
    assert res["run_key"] == "run-2"


def test_the_restart_is_recorded_as_evidence_not_just_retried_quietly():
    """A reader comparing the report against the console has to be able to
    see that the output belongs to a different run key than was submitted."""
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10, sleep=lambda s: None)
    assert len(res["restarts"]) == 1
    entry = res["restarts"][0]
    assert entry["abandoned_run"] == "run-1"
    assert entry["new_run"] == "run-2"
    assert entry["cancel_state"] == "CANCELED"


def test_a_healthy_run_is_never_restarted_however_long_it_takes():
    """The budget measures PICK-UP, not work: a task that started and is
    still going is fine, and cancelling it would destroy real progress."""
    calls = {"n": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            calls["n"] += 1
            return {"key": "run-1"}
        if op == "get_job_run":
            return {"state": {"status": "RUNNING"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": 1789854291517}]}
        if op == "fetch_task_output":
            return _notebook_payload("still going\n")
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=20, sleep=lambda s: None)
    assert calls["n"] == 1           # submitted once, never resubmitted
    assert res["terminal"] is False  # budget ran out; not a verdict
    assert res["restarts"] == []


def test_the_watchdog_gives_up_rather_than_restarting_forever():
    fake = ColdStart(ignore_runs=99)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, cold_start_restarts=2,
                    max_polls=20, sleep=lambda s: None)
    assert len(fake.submitted) == 3   # the original plus two restarts
    assert res["terminal"] is False   # STILL RUNNING, never a verdict


def test_the_watchdog_can_be_switched_off():
    fake = ColdStart(ignore_runs=99)
    watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
              cold_start_seconds=60, cold_start_restarts=0, max_polls=5,
              sleep=lambda s: None)
    assert fake.submitted == ["run-1"]
    assert fake.cancelled == []


def test_a_transport_that_cannot_list_task_runs_never_cancels_a_run():
    """Unknown must not read as wedged: on a transport hiccup the watchdog
    would otherwise kill healthy runs."""
    def call(op, **kw):
        if op == "list_task_runs":
            raise RuntimeError("not supported on this build")
        raise AssertionError(op)

    assert jobs.task_started(call, workspace="ws", run_key="r") is True


def test_a_run_with_no_task_runs_at_all_has_not_started():
    def call(op, **kw):
        return {"items": []}

    assert jobs.task_started(call, workspace="ws", run_key="r") is False


def test_a_cancel_is_polled_to_terminal_because_202_is_not_done():
    """The slot must be free before a resubmit: maxConcurrentRuns=1 accepts a
    second run while the first still holds it, and then discards it."""
    seen = {"n": 0}

    def call(op, **kw):
        if op == "cancel_job_run":
            return {}
        if op == "get_job_run":
            seen["n"] += 1
            return {"state": {"status": "RUNNING" if seen["n"] < 3
                              else "CANCELED"}}
        raise AssertionError(op)

    state = jobs.cancel_run(call, workspace="ws", run_key="r",
                            sleep=lambda s: None)
    assert state == "CANCELED"
    assert seen["n"] == 3


# --- the full State vocabulary ---------------------------------------------
#
# The API's State.status enum is PENDING, QUEUED, RUNNING, SKIPPED,
# INTERNAL_ERROR, BLOCKED, SUCCESS, FAILED, CANCELING, CANCELED,
# UPSTREAM_CANCELED, UPSTREAM_FAILED, EXCLUDED, TIMED_OUT, PAUSED_MAINTENANCE.
# TERMINAL_STATES used to omit INTERNAL_ERROR, SKIPPED, UPSTREAM_CANCELED and
# EXCLUDED, so a run that died that way -- the cluster failing to start is the
# realistic case -- was polled for the whole budget, cancelled and resubmitted
# once by the cold-start watchdog, and then reported STILL RUNNING with exit 0.

_API_STATES = {"PENDING", "QUEUED", "RUNNING", "SKIPPED", "INTERNAL_ERROR",
               "BLOCKED", "SUCCESS", "FAILED", "CANCELING", "CANCELED",
               "UPSTREAM_CANCELED", "UPSTREAM_FAILED", "EXCLUDED", "TIMED_OUT",
               "PAUSED_MAINTENANCE"}


def test_the_state_sets_cover_the_api_vocabulary_and_do_not_overlap():
    assert not set(TERMINAL_STATES) & set(jobs.ACTIVE_STATES)
    assert set(TERMINAL_STATES) | set(jobs.ACTIVE_STATES) >= _API_STATES
    assert "UNKNOWN" in jobs.ACTIVE_STATES, \
        "a transport hiccup must stay non-terminal, and must not restart"


@pytest.mark.parametrize("state", ["INTERNAL_ERROR", "SKIPPED",
                                   "UPSTREAM_CANCELED", "EXCLUDED"])
def test_a_dead_run_ends_the_watch_as_terminal_and_not_ok(state):
    fake = Fake(states=["PENDING", state])
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["terminal"] is True
    assert result["ok"] is False
    assert result["status"] == state
    assert fake.ops.count("run_job") == 1
    assert "cancel_job_run" not in fake.ops


def test_a_dead_run_is_never_cancelled_and_resubmitted_by_the_watchdog():
    """The cluster failed to start: the run ended INTERNAL_ERROR within a
    minute and its task never got a startTime. That is a verdict, not a
    cold start."""
    counts = {"run_job": 0, "cancel_job_run": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op in counts:
            counts[op] += 1
            return {"key": "run-%d" % counts["run_job"]}
        if op == "get_job_run":
            return {"state": {"status": "INTERNAL_ERROR"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": None}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=6, sleep=lambda s: None)
    assert counts == {"run_job": 1, "cancel_job_run": 0}
    assert res["terminal"] is True and res["ok"] is False
    assert res["status"] == "INTERNAL_ERROR"


def test_an_unrecognised_status_is_flagged_and_never_restarted():
    """A status this code does not know is neither a verdict nor "still
    running", and the watchdog must not cancel a run it cannot classify."""
    counts = {"run_job": 0, "cancel_job_run": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op in counts:
            counts[op] += 1
            return {"key": "run-%d" % counts["run_job"]}
        if op == "get_job_run":
            return {"state": {"status": "SOME_FUTURE_STATE"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": None}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=4, sleep=lambda s: None)
    assert res["terminal"] is False and res["ok"] is False
    assert res["unrecognised"] is True
    assert res["status"] == "SOME_FUTURE_STATE"
    assert counts == {"run_job": 1, "cancel_job_run": 0}


def test_a_spent_budget_on_a_running_job_is_not_unrecognised():
    fake = Fake(states=["RUNNING"] * 10)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=3, sleep=lambda _s: None)
    assert result["terminal"] is False
    assert result["unrecognised"] is False
    assert result["polls"] == 3, "the report says how much budget was spent"


@pytest.mark.parametrize("state", TERMINAL_STATES)
def test_every_terminal_state_ends_a_cancel_poll(state):
    seen = {"n": 0}

    def call(op, **kw):
        if op == "cancel_job_run":
            return {}
        if op == "get_job_run":
            seen["n"] += 1
            return {"state": {"status": state}}
        raise AssertionError(op)

    assert jobs.cancel_run(call, workspace="ws", run_key="r",
                           sleep=lambda s: None) == state
    assert seen["n"] == 1
