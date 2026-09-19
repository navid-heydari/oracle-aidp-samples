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

__all__ = ["TERMINAL_STATES", "SUCCESS_STATES", "run_job", "job_run_status",
           "fetch_task_output", "extract_notebook_text", "watch_job"]

TERMINAL_STATES = ("SUCCESS", "FAILED", "CANCELED", "TIMED_OUT",
                   "UPSTREAM_FAILED", "BLOCKED")
SUCCESS_STATES = ("SUCCESS",)


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
              sleep: Callable[[float], None] = time.sleep) -> dict:
    """Run a job, poll to a terminal state, and bring back its output.

    Returns {run_key, status, message, output, terminal}. `terminal: False`
    means the budget ran out with the job still going -- reported as running,
    never rounded to either verdict.
    """
    run_key = run_job(call, workspace=workspace, job_key=job_key,
                      parameters=parameters)
    status, message = "UNKNOWN", ""
    terminal = False
    for attempt in range(max_polls):
        sleep(poll_seconds)
        state = job_run_status(call, workspace=workspace, run_key=run_key)
        status, message = state["status"], state["message"]
        if on_poll:
            on_poll(status, attempt + 1)
        if status in TERMINAL_STATES:
            terminal = True
            break
    output = ""
    try:
        output = fetch_task_output(call, workspace=workspace, run_key=run_key)
    except Exception as exc:  # the verdict still stands without the log
        output = f"(output unavailable: {str(exc)[:200]})"
    return {"run_key": run_key, "status": status, "message": message,
            "output": output, "terminal": terminal,
            "ok": terminal and status in SUCCESS_STATES}
