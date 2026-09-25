"""Drive an AIDP job from the operator's machine: run, poll, fetch output.

Local instrumentation for work that executes inside AIDP. Everything here is
live-verified (2026-09-16) and replaces the ad-hoc shell it grew out of:

  * `POST /workspaces/{ws}/jobRuns` with `{"jobKey": ...}` answers 201 and a
    run key; `GET /jobRuns/{key}` carries `state.status` ∈ PENDING, RUNNING,
    SUCCESS, FAILED, ...
  * a task run's OUTPUT is two calls, not one: list the task runs
    (`--sort-by` is REQUIRED -- omitting it fails "Invalid SortBy: null"),
    then fetch that task run's output, which for a NOTEBOOK_TASK arrives as
    an executed notebook whose cell outputs hold the script's stdout.
  * a notebook cell reports SystemExit as an error, so the generated driver
    turns the exit code into a verdict -- see provision_api.

`call` is injected, so the polling and parsing are unit-tested offline.
"""
from __future__ import annotations

import json
import time
from typing import Callable

__all__ = ["TERMINAL_STATES", "SUCCESS_STATES", "JobRunCollision",
           "in_flight_runs", "run_job", "job_run_status",
           "fetch_task_output", "extract_notebook_text", "watch_job",
           "task_started", "cancel_run"]

TERMINAL_STATES = ("SUCCESS", "FAILED", "CANCELED", "TIMED_OUT",
                   "UPSTREAM_FAILED", "BLOCKED")
SUCCESS_STATES = ("SUCCESS",)


class JobRunCollision(RuntimeError):
    """A run of this job is already in flight. Named so the CLI reports it as
    a message rather than a traceback, and so it is never mistaken for the
    job having failed."""


def in_flight_runs(call: Callable[..., dict], *, workspace: str,
                   job_key: str) -> list[str]:
    """Keys of runs of `job_key` that have not ended.

    A run is finished when it carries an `endTime`; the envelope has no status
    field of its own (the status lives on the task runs), so absence of an end
    is the signal. A transport that cannot list runs must not block a run --
    this is a guard, not a gate -- so any failure here returns nothing.
    """
    try:
        payload = call("list_job_runs", workspace=workspace, job_key=job_key)
    except Exception:
        return []
    items = (payload.get("items") if isinstance(payload, dict) else None) or []
    return [str(i.get("key")) for i in items
            if not i.get("endTime") and i.get("key")]


def run_job(call: Callable[..., dict], *, workspace: str, job_key: str,
            parameters: dict[str, str] | None = None) -> str:
    """Start a run and return its key. Raises if the server returns none."""
    from .provision_api import build_job_run_body
    payload = call("run_job", workspace=workspace,
                   body=build_job_run_body(job_key, parameters))
    key = payload.get("key") or payload.get("id")
    if not key:
        raise RuntimeError(
            f"the run was accepted but carried no key: "
            f"{json.dumps(payload)[:200]}")
    return str(key)


def job_run_status(call: Callable[..., dict], *, workspace: str,
                   run_key: str) -> dict:
    """{status, message} for one run. An absent status reads UNKNOWN, never
    as success."""
    payload = call("get_job_run", workspace=workspace, key=run_key)
    state = payload.get("state") or {}
    return {"status": str(state.get("status") or payload.get("status")
                          or "UNKNOWN"),
            "message": str(state.get("stateMessage") or "")}


def task_started(call: Callable[..., dict], *, workspace: str,
                 run_key: str) -> bool:
    """Has the cluster actually PICKED UP this run's task?

    The distinction the job-run status cannot make. A wedged run and a
    healthy one both report `RUNNING` on the envelope; what separates them is
    one field on the task run:

        wedged  (live 2026-09-19): task run exists, `startTime` is null
        healthy (live 2026-09-19): task run carries a real `startTime`

    So `startTime` is the signal, not the status. A transport that cannot
    list task runs returns True -- unknown must never be read as wedged, or
    the watchdog would cancel healthy runs on a transport hiccup.
    """
    try:
        listed = call("list_task_runs", workspace=workspace, run_key=run_key)
    except Exception:
        return True
    items = (listed.get("items") if isinstance(listed, dict) else None) or []
    if not items:
        return False
    return any(i.get("startTime") for i in items)


def cancel_run(call: Callable[..., dict], *, workspace: str, run_key: str,
               poll_seconds: float = 5.0, max_polls: int = 12,
               sleep: Callable[[float], None] = time.sleep) -> str:
    """Cancel a run and poll it to a terminal state; returns that state.

    The cancel answers 202, which is acceptance and not completion, so the
    run is read back. This is polled to terminal rather than fired and
    forgotten because `maxConcurrentRuns: 1` means a resubmit while the old
    run still holds the slot is accepted and then silently discarded.
    """
    try:
        call("cancel_job_run", workspace=workspace, run_key=run_key)
    except Exception:
        pass  # it may already have ended; the poll below is the real answer
    status = "UNKNOWN"
    for _ in range(max_polls):
        try:
            status = job_run_status(call, workspace=workspace,
                                    run_key=run_key)["status"]
        except Exception:
            status = "UNKNOWN"
        if status in TERMINAL_STATES:
            return status
        sleep(poll_seconds)
    return status


def fetch_task_output(call: Callable[..., dict], *, workspace: str,
                      run_key: str) -> str:
    """The first task run's output text, or '' when there is none.

    Two calls, because a job run holds task runs and only a task run has
    output. A run that failed BEFORE launching a task has no task runs at
    all -- which is itself the diagnosis, and is returned as ''.
    """
    listed = call("list_task_runs", workspace=workspace, run_key=run_key)
    items = listed.get("items") or []
    if not items:
        return ""
    task_key = str(items[0].get("key") or items[0].get("taskRunKey") or "")
    if not task_key:
        return ""
    payload = call("fetch_task_output", workspace=workspace,
                   task_run_key=task_key)
    return extract_notebook_text(payload)


def extract_notebook_text(payload: dict) -> str:
    """Every cell output of an executed notebook, concatenated.

    The envelope is `data: [{type: NOTEBOOK, value: "<nbformat json>"}]`, and
    a cell's text lives under `outputs[].text` or `outputs[].data['text/plain']`.
    Anything unparseable is returned as-is rather than swallowed: a failed
    run's only evidence must not be dropped on a shape surprise.
    """
    blocks = payload.get("data")
    if isinstance(blocks, str):
        return blocks
    text: list[str] = []
    for block in blocks or []:
        value = block.get("value") if isinstance(block, dict) else None
        if not value:
            continue
        try:
            notebook = json.loads(value)
        except (TypeError, ValueError):
            text.append(str(value))
            continue
        for cell in notebook.get("cells") or []:
            for out in cell.get("outputs") or []:
                chunk = (out.get("text")
                         or (out.get("data") or {}).get("text/plain") or "")
                if isinstance(chunk, list):
                    chunk = "".join(chunk)
                if chunk:
                    text.append(chunk)
                if out.get("ename"):
                    text.append(f'{out["ename"]}: {out.get("evalue")}')
    return "".join(text)


def watch_job(call: Callable[..., dict], *, workspace: str, job_key: str,
              parameters: dict[str, str] | None = None,
              poll_seconds: float = 30.0, max_polls: int = 40,
              on_poll: Callable[[str, int], None] | None = None,
              cold_start_seconds: float = 60.0, cold_start_restarts: int = 1,
              on_restart: Callable[[str, str], None] | None = None,
              sleep: Callable[[float], None] = time.sleep) -> dict:
    """Run a job, poll to a terminal state, and bring back its output.

    Returns {run_key, status, message, output, terminal, restarts}.
    `terminal: False` means the budget ran out with the job still going --
    reported as running, never rounded to either verdict.

    THE COLD-START WATCHDOG. A cluster sometimes never picks up a job run --
    characteristically the FIRST run on a freshly created workspace. The run
    sits at `RUNNING` with its task unstarted, indefinitely: it does not fail,
    so nothing times out, and an operator watching a status field sees a job
    that is apparently working. Observed live 2026-09-19: the first run on a
    new workspace sat 9+ minutes untouched, and an identical run submitted
    after cancelling it succeeded in 90 seconds.

    So after `cold_start_seconds` with the task still unstarted, the run is
    cancelled and resubmitted, up to `cold_start_restarts` times. The budget
    is deliberately generous to measure only the pick-up, not the work: it
    checks whether the cluster TOOK the task, which is independent of how
    long the task then runs. Set `cold_start_restarts=0` to disable.
    """
    # A job created with `maxConcurrentRuns: 1` still ACCEPTS a second run
    # while the first is going -- and then never executes it: the run is
    # created, ends the instant it starts, and produces no task output. Polled
    # naively that reads as a terminal run with nothing in it, while the
    # console streams the OLD run's log, so the operator sees stale output and
    # concludes the fix they just deployed did not take. Refuse instead, and
    # name the run holding the slot.
    active = in_flight_runs(call, workspace=workspace, job_key=job_key)
    if active:
        raise JobRunCollision(
            f"job {job_key} already has {len(active)} run(s) in flight: "
            + ", ".join(active)
            + ". A job with maxConcurrentRuns=1 accepts a second run and then "
              "discards it, so starting one now would look like a run that "
              "did nothing. Wait for it, or cancel it "
              "(`aidp workflow cancel-job-run <workspace> <run-key>`) -- and "
              "note that a notebook re-uploaded mid-run does NOT affect the "
              "run already going.")
    run_key = run_job(call, workspace=workspace, job_key=job_key,
                      parameters=parameters)
    status, message = "UNKNOWN", ""
    terminal = False
    restarts: list[dict] = []
    restarts_left = max(0, cold_start_restarts)
    waited = 0.0
    attempt = 0
    polls_left = max_polls
    while polls_left > 0:
        sleep(poll_seconds)
        polls_left -= 1
        waited += poll_seconds
        attempt += 1
        state = job_run_status(call, workspace=workspace, run_key=run_key)
        status, message = state["status"], state["message"]
        if on_poll:
            on_poll(status, attempt)
        if status in TERMINAL_STATES:
            terminal = True
            break
        if (restarts_left and waited >= cold_start_seconds
                and not task_started(call, workspace=workspace,
                                     run_key=run_key)):
            # The cluster has not taken the task. Let this run go and submit
            # another -- polling the cancel to terminal first, because the
            # slot must be free or the resubmit is accepted and discarded.
            ended = cancel_run(call, workspace=workspace, run_key=run_key,
                               sleep=sleep)
            stale = run_key
            run_key = run_job(call, workspace=workspace, job_key=job_key,
                              parameters=parameters)
            restarts.append({"abandoned_run": stale, "cancel_state": ended,
                             "new_run": run_key,
                             "after_seconds": waited})
            if on_restart:
                on_restart(stale, run_key)
            restarts_left -= 1
            waited = 0.0
            status, message = "UNKNOWN", ""
    output = ""
    try:
        output = fetch_task_output(call, workspace=workspace, run_key=run_key)
    except Exception as exc:  # the verdict still stands without the log
        output = f"(output unavailable: {str(exc)[:200]})"
    return {"run_key": run_key, "status": status, "message": message,
            "output": output, "terminal": terminal, "restarts": restarts,
            "ok": terminal and status in SUCCESS_STATES}
