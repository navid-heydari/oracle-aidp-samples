"""Assemble plan.json: what can move, what cannot and why, and in what order.

The plan is the product. Its two jobs are to say **which objects are planned to
move** and **which cannot, with a brief reason** -- so every object in the
inventory lands in exactly one of `can_migrate` or `cannot_migrate`, and every
`cannot` carries a category and a sentence.

Bronze is a 1:1 structural mirror (database -> Standard Catalog, schema ->
schema, table -> table, view -> view). Silver and Gold are requirement-driven, so
this module emits disabled job stubs for them rather than inventing
transformation logic nobody specified.

A target-name collision raises: two source objects merging into one target table
is a data-loss defect, not something to resolve by picking a winner.
"""
from __future__ import annotations

import collections
import datetime

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
        super().__init__(f"target name collision, refusing to guess: {detail}")


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


def build_plan(inventory: dict, dependencies: dict, *,
               restrictions: dict | None = None,
               bronze_catalog_prefix: str | None = None) -> dict:
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
                                       catalog_prefix=bronze_catalog_prefix)

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
