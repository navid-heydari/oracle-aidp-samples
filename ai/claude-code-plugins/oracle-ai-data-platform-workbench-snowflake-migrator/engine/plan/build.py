"""Assemble plan.json: what can move, what cannot and why, and in what order.

The plan is the product. Its two jobs are to say **which objects are planned to
move** and **which cannot, with a brief reason** -- so every object in the
inventory lands in exactly one of `can_migrate` or `cannot_migrate`, and every
`cannot` carries a category and a sentence.

Bronze is a 1:1 structural mirror (database -> Standard Catalog, schema ->
schema, table -> table, view -> view). Silver and Gold are requirement-driven, so
this module emits disabled job stubs for them rather than inventing
transformation logic nobody specified.

An object that depends on one that is not migrating cannot migrate either --
a view over a blocked or excluded table would be created over nothing. The
cascade follows the dependency edges transitively and names the missing object.

A target-name collision raises: two source objects merging into one target table
is a data-loss defect, not something to resolve by picking a winner.
"""
from __future__ import annotations

import collections
import datetime
import json

from snowflake_source.dialect.views import (
    detect_unsupported_constructs, extract_view_body,
)

from .medallion import bronze_target, detect_target_collisions, layer_jobs
from .restrictions import apply_restrictions
from .waves import compute_waves

__all__ = ["build_plan", "TargetCollision"]


class TargetCollision(RuntimeError):
    """Two or more source objects map to the same target name."""

    def __init__(self, collisions: dict[str, list[str]]):
        self.collisions = collisions
        detail = "; ".join(f"{t} <- {sorted(s)}" for t, s in collisions.items())
        remedy = ""
        if collisions:
            # The one in-tool remedy, in the form the restrictions JSON takes:
            # a quoted part of an exclude_objects entry matches case-sensitively.
            twin = sorted(next(iter(collisions.values())))[-1]
            example = json.dumps(".".join(f'"{p}"' for p in twin.split(".")))
            remedy = (" -- to migrate one twin now, list the other in "
                      "exclude_objects with its exact double-quoted spelling, "
                      f'e.g. "exclude_objects": [{example}]')
        super().__init__(f"target name collision, refusing to guess: {detail}{remedy}")


def _view_verdict(rec: dict) -> tuple[bool, str, str]:
    """(can_migrate, category, reason) for one view."""
    meta = rec.get("source_metadata") or {}
    if str(meta.get("is_secure", "")).lower() in ("true", "y", "yes"):
        return False, "unsupported_object", (
            "Snowflake secure view: its definition and row-visibility rules have "
            "no Delta equivalent")
    if str(meta.get("is_materialized", "")).lower() in ("true", "y", "yes"):
        return False, "unsupported_object", (
            "Snowflake materialized view: no AIDP equivalent; rebuild as a table "
            "plus a refresh job")
    ddl = rec.get("view_ddl_get_ddl") or rec.get("view_text_show")
    if not ddl:
        return False, "no_definition", (
            "no view SQL was captured during extraction, so it cannot be recreated")
    try:
        body = extract_view_body(ddl)
    except ValueError as exc:
        return False, "unparseable_sql", str(exc)
    unsupported = detect_unsupported_constructs(body)
    if unsupported:
        return False, "snowflake_only_sql", "uses " + "; ".join(
            f'{u["construct"]} ({u["reason"]})' for u in unsupported)
    return True, "", ""


def _cascade_dependency_exclusions(can: list[dict], cannot: list[dict],
                                   edges: list[dict]) -> tuple[list[dict], list[dict]]:
    """Move every dependent of a `cannot` object into `cannot`, transitively.

    Without this only the planned ids reached compute_waves, which drops an
    edge whose other end is not in the set: the view lost its only edge, sat
    at indegree 0, sorted first (rows=None -> size 0) and got a CREATE VIEW
    over a table that will never exist, under a report line promising that
    views follow their base tables.
    """
    can_ids = {c["source_identifier"] for c in can}
    kinds = {c["source_identifier"]: c["object_type"] for c in can}
    dependents: dict[str, set[str]] = collections.defaultdict(set)
    for edge in edges:
        if edge["from"] != edge["to"]:
            dependents[edge["to"]].add(edge["from"])

    why = {c["source_identifier"]: c for c in cannot}
    queue = collections.deque(sorted(why))
    while queue:
        missing = queue.popleft()
        for dependent in sorted(dependents.get(missing, ())):
            if dependent not in can_ids:
                continue
            can_ids.discard(dependent)
            state = ("excluded" if why[missing]["category"] == "restriction"
                     else "blocked")
            entry = {"source_identifier": dependent,
                     "object_type": kinds[dependent],
                     "category": "dependency_not_migrated",
                     "reason": f"depends on {missing}, which is {state} "
                               f'({why[missing]["category"]})'}
            cannot.append(entry)
            why[dependent] = entry
            queue.append(dependent)
    return [c for c in can if c["source_identifier"] in can_ids], cannot


def build_plan(inventory: dict, dependencies: dict, *,
               restrictions: dict | None = None,
               bronze_catalog_prefix: str | None = None,
               bronze_schema_style: str = "db_schema",
               architecture_choice: dict | None = None) -> dict:
    records = inventory.get("inventory", [])

    kept, restricted = apply_restrictions(records, restrictions)

    can: list[dict] = []
    cannot: list[dict] = [
        {"source_identifier": e["source_identifier"],
         "object_type": e.get("object_type"),
         "category": "restriction",
         "reason": e["reason"] + f' (restriction: {e["restriction"]})'}
        for e in restricted]

    targets: dict[str, str] = {}
    for rec in kept:
        ident = rec["source_identifier"]
        db, schema, name = ident.split(".", 2)
        targets[ident] = bronze_target(db, schema, name,
                                       catalog_prefix=bronze_catalog_prefix,
                                       schema_style=bronze_schema_style)

        if rec.get("compatibility_status") == "blocked":
            cannot.append({
                "source_identifier": ident, "object_type": rec.get("object_type"),
                "category": "unmapped_type",
                "reason": "column types with no Delta equivalent: "
                          + "; ".join(rec.get("blocked_reasons") or ["unspecified"])})
            continue

        if rec.get("object_type") == "VIEW":
            ok, category, reason = _view_verdict(rec)
            if not ok:
                cannot.append({
                    "source_identifier": ident, "object_type": "VIEW",
                    "category": category, "reason": reason})
                continue

        can.append({"source_identifier": ident,
                    "object_type": rec.get("object_type"),
                    "target": targets[ident],
                    "rows": rec.get("row_count_exact"),
                    "columns": len(rec.get("columns") or [])})

    can, cannot = _cascade_dependency_exclusions(
        can, cannot, dependencies.get("edges", []))

    collisions = detect_target_collisions(
        {c["source_identifier"]: targets[c["source_identifier"]] for c in can})
    if collisions:
        raise TargetCollision(collisions)

    can_ids = {c["source_identifier"] for c in can}
    migratable = [r for r in kept if r["source_identifier"] in can_ids]
    sizes = {r["source_identifier"]: (r.get("row_count_exact") or 0)
             for r in migratable}
    waved = compute_waves(sorted(can_ids), dependencies.get("edges", []),
                          sort_key=lambda n: (sizes.get(n, 0), n))

    catalogs = sorted({targets[i].split(".", 1)[0] for i in can_ids})
    schemas = sorted({tuple(targets[i].split(".")[:2]) for i in can_ids})
    scopes = sorted({(r["source_database"], r["source_schema"])
                     for r in migratable})

    return {
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bronze_catalog_prefix": bronze_catalog_prefix,
        "bronze_schema_style": bronze_schema_style,
        "bronze_mapping": ("Snowflake database -> AIDP Standard Catalog, "
                           "schema -> schema, table -> table, view -> view"),
        "waves": waved["waves"],
        "cycles": waved["cycles"],
        "target_names": targets,
        "clone_targets": sorted(can_ids),
        "can_migrate": sorted(can, key=lambda c: c["source_identifier"]),
        "cannot_migrate": sorted(cannot, key=lambda c: c["source_identifier"]),
        "restrictions_applied": restrictions or {},
        "catalogs_to_create": catalogs,
        "schemas_to_create": [list(s) for s in schemas],
        "silver_gold_jobs": layer_jobs(scopes),
        "dependency_source": dependencies.get("source_used"),
        "dependency_coverage_note": dependencies.get("coverage_note"),
        # Carried so every report can state the architecture decision. None means
        # undecided, which the reports say out loud rather than defaulting.
        "architecture_choice": architecture_choice,
        "summary": {
            "objects_inventoried": len(records),
            "can_migrate": len(can),
            "cannot_migrate": len(cannot),
            "tables": sum(1 for c in can if c["object_type"] == "TABLE"),
            "views": sum(1 for c in can if c["object_type"] == "VIEW"),
            "catalogs": len(catalogs),
            "schemas": len(schemas),
            "silver_gold_jobs": len(layer_jobs(scopes)),
            "cannot_by_category": dict(collections.Counter(
                c["category"] for c in cannot)),
        },
    }
