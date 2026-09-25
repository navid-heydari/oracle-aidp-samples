"""Terminate the compute a migration allocated -- and nothing else.

Reads provision_result.json, so the only clusters it can reach are the ones
this migration created: the migration cluster and any warehouse clusters.
It never lists the workspace and picks clusters by name, which is how a
teardown takes somebody else's compute with it.

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

import time

__all__ = ["ACTIONS", "teardown", "render_teardown"]

ACTIONS = ("stop", "delete")
_STOPPED = {"STOPPED", "INACTIVE", "TERMINATED", "DELETED"}
KEPT = ["the workspace (scripts, plans, report/output — the run's record)",
        "the catalogs (the migration's output)",
        "the jobs (the registered S11 copy scripts)"]


def _targets(prov: dict) -> list[dict]:
    out = []
    cl = prov.get("cluster") or {}
    if cl.get("key"):
        out.append({"cluster": cl["key"], "name": cl.get("name"),
                    "role": "migration cluster"})
    for wc in prov.get("warehouse_clusters") or []:
        # A warehouse mapped to an EXISTING cluster was not created here.
        if wc.get("key") and not wc.get("uses_existing"):
            out.append({"cluster": wc["key"], "name": wc.get("name"),
                        "role": f'warehouse cluster for {wc.get("warehouse")}'})
    return out


def _state(call, workspace: str, key: str):
    items = call("list_clusters", workspace=workspace).get("items") or []
    for item in items:
        if item.get("key") == key or item.get("id") == key:
            return str(item.get("state") or item.get("lifecycleState")
                       or "").upper()
    return None                      # not listed: gone


def teardown(call, prov: dict, *, action: str, execute: bool,
             delays: tuple[float, ...] = (10.0, 20.0, 30.0, 30.0, 60.0,
                                          60.0)) -> dict:
    if action not in ACTIONS:
        raise ValueError(f"unknown teardown action {action!r}; expected one "
                         f"of {', '.join(ACTIONS)}")
    workspace = (prov.get("workspace") or {}).get("key")
    base = {"dry_run": not execute, "action": action, "workspace": workspace,
            "kept": KEPT, "steps": []}
    if prov.get("dry_run") or not workspace:
        return {**base, "verified": 0,
                "note": "provision never ran for real, so this migration "
                        "allocated nothing to terminate"}
    targets = _targets(prov)
    if not execute:
        base["steps"] = [{**t, "action": f"would {action}", "verified": None}
                         for t in targets]
        return {**base, "verified": 0, "note": ""}

    for t in targets:
        step = dict(t)
        try:
            before = _state(call, workspace, t["cluster"])
            if action == "stop" and before in _STOPPED:
                step.update(action="already_stopped", verified=True,
                            state=before)
                base["steps"].append(step)
                continue
            if before is None:
                step.update(action="already_gone", verified=True, state=None)
                base["steps"].append(step)
                continue
            call(f"{action}_cluster", workspace=workspace, cluster=t["cluster"])
            state = before
            done = False
            for wait in (0.0, *delays):
                time.sleep(wait)
                state = _state(call, workspace, t["cluster"])
                done = (state is None) if action == "delete" \
                    else (state in _STOPPED)
                if done:
                    break
            verb = {"stop": "stopped", "delete": "deleted"}[action]
            step.update(action=verb if done else f"{action}_requested",
                        verified=done, state=state)
        except Exception as exc:
            step.update(action="failed", verified=False,
                        detail=str(exc)[:200])
        base["steps"].append(step)
    return {**base, "note": "",
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
        out.append(f'| `{s.get("name")}` (`{s.get("cluster")}`) | '
                   f'{s.get("role")} | {s.get("action")} | {verified} | '
                   f'{s.get("state") or s.get("detail") or "—"} |')
    out += ["", "Kept:", ""] + [f"- {k}" for k in res.get("kept") or []]
    if res.get("action") == "delete":
        out += ["", "⚠️ `delete` is final: the registered copy jobs now point "
                "at a cluster that no longer exists and must be re-bound "
                "before a data move."]
    return "\n".join(out) + "\n"
