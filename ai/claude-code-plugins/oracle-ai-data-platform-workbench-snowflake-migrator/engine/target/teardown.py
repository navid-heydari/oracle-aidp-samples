"""Terminate the compute a migration allocated -- and nothing else.

Reads provision_result.json, and acts only on the clusters that record
PROVES this migration created (`created: true`, see target/provenance.py):
the migration cluster and any warehouse clusters provision made. A cluster
the record names but did not create -- adopted with --reuse-existing, or
the one `compute.warehouse_clusters: existing` points at -- is listed as
"not this migration's, left alone" and never stopped or deleted. It never
lists the workspace and picks clusters by name, which is how a teardown
takes somebody else's compute with it.

Kept, on purpose: the workspace (scripts, plans, report/output -- the record
of the run), the catalogs (the migration's output) and the jobs (the
registered S11 copy scripts a later data move will run).

`stop` (default) is reversible: a stopped cluster costs nothing and the
registered jobs still point at it. `delete` is final, and the copy jobs then
point at a cluster that no longer exists. Dry run unless execute, and every
cluster is read back until it is stopped or gone -- a 2xx on the action is
not the claim.
"""
from __future__ import annotations

import datetime
import time

from .provenance import CREATED, NOT_CREATED, REQUESTED, cluster_records

__all__ = ["ACTIONS", "RELEASED_ACTIONS", "STOPPED_STATES", "teardown",
           "render_teardown"]

ACTIONS = ("stop", "delete")
# One stopped set, shared with the billing report (report/resources.py).
STOPPED_STATES = frozenset({"STOPPED", "INACTIVE", "TERMINATED", "DELETED"})
_STOPPED = STOPPED_STATES
# A VERIFIED step with one of these actions released its cluster: the
# state it was left in, None meaning "the state the step read back".
RELEASED_ACTIONS = {"stopped": None, "already_stopped": None,
                    "deleted": "DELETED", "already_gone": "DELETED"}


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
KEPT = ["the workspace (scripts, plans, report/output — the run's record)",
        "the catalogs (the migration's output)",
        "the jobs (the registered S11 copy scripts)"]


def _targets(prov: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """(targets, left_alone, unkeyed). A target is a cluster the record
    proves this migration created -- including those an earlier push
    created, each in its own workspace; everything else it names with a key
    is left alone and said so, once per key. `unkeyed` are creates this
    migration asked for and never saw listed: failed steps, never looked up
    by name."""
    records = cluster_records(prov)
    targets, left, seen = [], [], set()
    for rec in records:
        at = (rec["workspace"], rec["cluster"])
        if rec["provenance"] == CREATED and at not in seen:
            seen.add(at)
            targets.append({"cluster": rec["cluster"], "name": rec["name"],
                            "role": rec["role"],
                            "workspace": rec["workspace"],
                            "datalake_ocid": rec["datalake_ocid"]})
    for rec in records:
        at = (rec["workspace"], rec["cluster"])
        if rec["provenance"] == NOT_CREATED and at not in seen:
            seen.add(at)
            left.append({"cluster": rec["cluster"], "name": rec["name"],
                         "role": rec["role"], "why": rec["why"]})
    unkeyed, named = [], set()
    for rec in records:
        at = (rec["workspace"], rec["name"])
        if rec["provenance"] == REQUESTED and at not in named:
            named.add(at)
            unkeyed.append({
                "cluster": None, "name": rec["name"], "role": rec["role"],
                "workspace": rec["workspace"], "action": "key_unknown",
                "verified": False,
                "detail": f'requested by this migration (the create was '
                          f'accepted), but its key was never recorded. Look '
                          f'it up by name `{rec["name"]}` in workspace '
                          f'{rec["workspace"]} in the console and confirm '
                          f'it is this migration\'s before terminating it; '
                          f'teardown never picks a cluster by name'})
    return targets, left, unkeyed


def _state(call, workspace: str, key: str):
    items = call("list_clusters", workspace=workspace).get("items") or []
    for item in items:
        if item.get("key") == key or item.get("id") == key:
            return str(item.get("state") or item.get("lifecycleState")
                       or "").upper()
    return None                      # not listed: gone


def teardown(call, prov: dict, *, action: str, execute: bool,
             delays: tuple[float, ...] = (10.0, 20.0, 30.0, 30.0, 60.0,
                                          60.0),
             datalake_ocid: str | None = None) -> dict:
    """Stop or delete what `prov` proves this migration created.

    `datalake_ocid` is the aiDataPlatform this teardown's transport talks
    to. A cluster recorded in ANOTHER one is not looked for here (its
    workspace key means nothing on this platform, and "not listed" would
    read as gone); it is a failed step naming the platform to re-run
    against."""
    if action not in ACTIONS:
        raise ValueError(f"unknown teardown action {action!r}; expected one "
                         f"of {', '.join(ACTIONS)}")
    workspace = (prov.get("workspace") or {}).get("key")
    base = {"dry_run": not execute, "action": action, "workspace": workspace,
            "kept": KEPT, "steps": []}
    if prov.get("dry_run"):
        return {**base, "verified": 0,
                "note": "provision never ran for real, so this migration "
                        "allocated nothing to terminate"}
    targets, left_alone, unkeyed = _targets(prov)
    base["left_alone"] = left_alone
    if not workspace and not targets and not unkeyed:
        # An EXECUTED record naming no workspace is not evidence of an empty
        # migration: its push halted before a key was recorded, and it
        # cannot show what an earlier push allocated.
        return {**base, "verified": 0, "unknown": True,
                "note": "the executed provision record names no workspace "
                        "key (its push halted before one was recorded), so "
                        "this teardown cannot tell what the migration "
                        "allocated; nothing was touched. Check the console "
                        "and PROVISION.md of the push that created the "
                        "environment."}
    if not execute:
        base["steps"] = [{**t, "action": f"would {action}", "verified": None}
                         for t in targets] + unkeyed
        return {**base, "verified": 0, "note": ""}

    for t in targets:
        step = dict(t)
        where = t.get("workspace") or workspace
        if (datalake_ocid and t.get("datalake_ocid")
                and t["datalake_ocid"] != datalake_ocid):
            step.update(action="not_reached", verified=False,
                        detail=f'recorded in aiDataPlatform '
                               f'{t["datalake_ocid"]}, not the one this '
                               f'teardown targets; re-run teardown with '
                               f'--datalake-ocid {t["datalake_ocid"]}')
            base["steps"].append(step)
            continue
        try:
            before = _state(call, where, t["cluster"])
            if action == "stop" and before in _STOPPED:
                step.update(action="already_stopped", verified=True,
                            state=before, at=_now())
                base["steps"].append(step)
                continue
            if before is None:
                step.update(action="already_gone", verified=True, state=None,
                            at=_now())
                base["steps"].append(step)
                continue
            call(f"{action}_cluster", workspace=where, cluster=t["cluster"])
        except Exception as exc:
            # Refused, or never sent: nothing changed as far as we know.
            step.update(action="failed", verified=False,
                        detail=str(exc)[:200])
            base["steps"].append(step)
            continue
        # From here the action WAS accepted. A read-back that errors is
        # "requested, outcome unknown", never "failed": the record must not
        # lose that the delete (or stop) was sent.
        state = before
        done = False
        try:
            for wait in (0.0, *delays):
                time.sleep(wait)
                state = _state(call, where, t["cluster"])
                done = ((state is None) if action == "delete"
                        else (state in _STOPPED))
                if done:
                    break
        except Exception as exc:
            step.update(action=f"{action}_requested", verified=False,
                        state=None, at=_now(),
                        detail=f"{action} sent and accepted; the read-back "
                               f"failed ({str(exc)[:160]}), so its outcome "
                               f"is unknown -- check the console")
            base["steps"].append(step)
            continue
        verb = {"stop": "stopped", "delete": "deleted"}[action]
        # Stamped when it was read back: the billing report's release
        # time, whatever the exit code of the run as a whole.
        step.update(action=verb if done else f"{action}_requested",
                    verified=done, state=state, at=_now())
        base["steps"].append(step)
    # Counted, so the run cannot report full success over them.
    base["steps"] += unkeyed
    return {**base, "note": "", "at": _now(),
            "verified": sum(1 for s in base["steps"] if s["verified"])}


def render_teardown(res: dict) -> str:
    out = ["# Teardown — the compute this migration allocated", ""]
    if res.get("dry_run"):
        out += [f'**DRY RUN — nothing was changed.** Re-run with `--execute` '
                f'to {res["action"]} the clusters below.', ""]
    if res.get("note"):
        out += [res["note"], ""]
    out += ["| Cluster | Role | Action | Verified | State |",
            "|---|---|---|---|---|"]
    for s in res.get("steps") or []:
        verified = {True: "yes", False: "**no**", None: "—"}[s.get("verified")]
        key = (f'`{s.get("cluster")}`' if s.get("cluster")
               else "key never recorded")
        out.append(f'| `{s.get("name")}` ({key}) | '
                   f'{s.get("role")} | {s.get("action")} | {verified} | '
                   f'{s.get("state") or s.get("detail") or "—"} |')
    if res.get("left_alone"):
        out += ["", "Not this migration's — left alone (named in "
                "provision_result.json, but not created by this migration, so "
                "never stopped or deleted here):", ""]
        out += [f'- `{s.get("name") or s.get("cluster")}` '
                f'(`{s.get("cluster")}`), {s.get("role")}: {s.get("why")}'
                for s in res["left_alone"]]
    out += ["", "Kept:", ""] + [f"- {k}" for k in res.get("kept") or []]
    if res.get("action") == "delete":
        gone = [s for s in res.get("steps") or [] if s.get("verified")
                and s.get("action") in ("deleted", "already_gone")]
        if res.get("dry_run"):
            out += ["", "⚠️ `delete` is final: once run with `--execute`, "
                    "the registered copy jobs will point at a cluster that is "
                    "gone and must be re-bound before a data move. `stop` is "
                    "the reversible choice."]
        elif gone:
            out += ["", "⚠️ `delete` is final: the registered copy jobs bound "
                    "to " + ", ".join(f'`{s.get("name")}`' for s in gone)
                    + " now point at a cluster that no longer exists and "
                    "must be re-bound before a data move."]
    return "\n".join(out) + "\n"
