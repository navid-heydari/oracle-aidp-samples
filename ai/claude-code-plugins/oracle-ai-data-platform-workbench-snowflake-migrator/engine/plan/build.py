"""Assemble plan.json from an inventory and a dependency graph. Pure.

Wave ordering inside a layer is dependency depth, then object size, then name.
Usage-frequency ranking is the documented extension point for when the deferred
compute-calculation work lands: pass a sort_key that consults a usage profile.

A target-name collision raises. Two source objects silently merging into one
target table is a data-loss defect, so it stops the run.
"""
from __future__ import annotations

import datetime

from .medallion import assign_layer, detect_target_collisions, target_name
from .waves import compute_waves

__all__ = ["build_plan", "TargetCollision"]


class TargetCollision(RuntimeError):
    """Two or more source objects map to the same target name."""

    def __init__(self, collisions: dict[str, list[str]]):
        self.collisions = collisions
        detail = "; ".join(f"{t} <- {sorted(s)}" for t, s in collisions.items())
        super().__init__(f"target name collision, refusing to guess: {detail}")


def build_plan(inventory: dict, dependencies: dict, *,
               user_map: dict[str, str] | None = None,
               strategy: str = "layer-catalog") -> dict:
    records = inventory.get("inventory", [])

    assignment: dict[str, str] = {}
    basis: dict[str, str] = {}
    targets: dict[str, str] = {}
    blocked: list[dict] = []
    unsupported: list[dict] = []

    for rec in records:
        ident = rec["source_identifier"]
        db, schema, name = ident.split(".", 2)
        layer, why = assign_layer(db, schema, user_map, source_identifier=ident)
        assignment[ident] = layer
        basis[ident] = why
        targets[ident] = target_name(db, schema, name, layer, strategy)

        if rec.get("compatibility_status") == "blocked":
            reason = "; ".join(rec.get("blocked_reasons") or ["unspecified"])
            blocked.append({"source_identifier": ident, "reason": reason})
        if rec.get("object_type") == "VIEW":
            unsupported.append({
                "source_identifier": ident, "feature": "VIEW",
                "resolution": "View translation is out of MVP-1 scope. Source SQL is "
                              "captured verbatim in the inventory; deploy views "
                              "topologically in a later phase."})

    collisions = detect_target_collisions(targets)
    if collisions:
        raise TargetCollision(collisions)

    blocked_ids = {b["source_identifier"] for b in blocked}
    planned = [r for r in records if r["source_identifier"] not in blocked_ids]
    sizes = {r["source_identifier"]: (r.get("row_count_exact") or 0) for r in planned}
    waved = compute_waves(
        [r["source_identifier"] for r in planned],
        dependencies.get("edges", []),
        sort_key=lambda n: (sizes.get(n, 0), n))

    clone_targets = [r["source_identifier"] for r in planned
                     if r.get("object_type") == "TABLE"]

    return {
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "namespace_strategy": strategy,
        "waves": waved["waves"],
        "cycles": waved["cycles"],
        "medallion_assignment": assignment,
        "medallion_assignment_basis": basis,
        "fallback_assignments": sorted(k for k, v in basis.items() if v == "fallback"),
        "target_names": targets,
        "clone_targets": clone_targets,
        "blocked": blocked,
        "unsupported": unsupported,
        "dependency_source": dependencies.get("source_used"),
        "dependency_coverage_note": dependencies.get("coverage_note"),
    }
