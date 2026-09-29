"""Which clusters a provision record PROVES this migration created.

`teardown` and the billing report act on that proof and on nothing else. A
key in provision_result.json is not proof: provision records one for every
cluster it touches -- created, adopted with --reuse-existing, name_taken,
and the cluster `compute.warehouse_clusters: existing` maps every warehouse
to. Only `created: true`, written on the create path (or carried forward
from an earlier executed record of the same workspace), is.

A record written before provenance was recorded has no `created` field; its
steps are then the evidence, read the same positive way: a `created` step
for that cluster, or nothing.
"""
from __future__ import annotations

__all__ = ["CREATED", "REQUESTED", "NOT_CREATED", "cluster_records"]

CREATED = "created"
REQUESTED = "requested"
NOT_CREATED = "not_created"


def _legacy(prov: dict, step: str, detail: str | None) -> str | None:
    for s in prov.get("steps") or []:
        if s.get("step") != step:
            continue
        text = str(s.get("detail") or "")
        if detail is not None and not (
                text == detail or text.startswith((detail + " ",
                                                   detail + ":"))):
            continue
        if s.get("action") == "created":
            return CREATED
        if s.get("action") == "create_requested":
            return REQUESTED
    return None


def _verdict(rec: dict, legacy) -> tuple[str, str]:
    if "created" in rec:
        if rec.get("created") is True and rec.get("key"):
            return CREATED, ""
        if rec.get("create_requested") and not rec.get("key"):
            return REQUESTED, ""
    else:
        found = legacy()
        if found == CREATED and rec.get("key"):
            return CREATED, ""
        if found == REQUESTED and not rec.get("key"):
            return REQUESTED, ""
    if rec.get("uses_existing"):
        return NOT_CREATED, ("the existing cluster "
                             "`compute.warehouse_clusters: existing` maps "
                             "this warehouse to; this migration did not "
                             "create it")
    return NOT_CREATED, ("already on the workspace when provision looked "
                         "(adopted with --reuse-existing, or name taken); "
                         "this migration did not create it")


def cluster_records(prov: dict) -> list[dict]:
    """Every cluster the record names, each with its provenance:
    {cluster, name, role, workspace, provenance, why, record}.

    `cluster` is the key (None for a create that was accepted but never
    listed). Nothing here is looked up by name."""
    workspace = (prov.get("workspace") or {}).get("key")
    out = []

    def add(rec: dict, role: str, legacy) -> None:
        provenance, why = _verdict(rec, legacy)
        if provenance == NOT_CREATED and not rec.get("key"):
            return                   # nothing to act on, nothing it holds
        out.append({"cluster": rec.get("key"), "name": rec.get("name"),
                    "role": role,
                    "workspace": rec.get("workspace") or workspace,
                    "provenance": provenance, "why": why, "record": rec})

    cl = prov.get("cluster") or {}
    if cl:
        add(cl, "migration cluster", lambda: _legacy(prov, "cluster", None))
    for wc in prov.get("warehouse_clusters") or []:
        detail = f'{wc.get("warehouse")} -> {wc.get("name")}'
        add(wc, f'warehouse cluster for {wc.get("warehouse")}',
            lambda d=detail: _legacy(prov, "warehouse-cluster", d))
    return out
