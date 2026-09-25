"""Assemble plan.json: what can move, what cannot and why, and in what order.

The plan is the product. Its two jobs are to say **which objects are planned to
move** and **which cannot, with a brief reason** -- so every object in the
inventory lands in exactly one of `can_migrate` or `cannot_migrate`, and every
`cannot` carries a category and a sentence.

Bronze is a 1:1 structural mirror (database -> Standard Catalog, schema ->
schema, table -> table, view -> view). Silver and Gold are requirement-driven, so
this module emits disabled job stubs for them rather than inventing
transformation logic nobody specified.

SHOW TABLES lists dynamic, external, Iceberg, event and hybrid tables next to
standard ones, and the extractor keeps the is_* flags. None of those is a table
this plugin can copy: each lands in `cannot_migrate` with a reason specific to
its kind, so the plan agrees with CENSUS.md instead of contradicting it.
TRANSIENT and TEMPORARY tables do migrate, as permanent Delta tables, and the
plan carries a warning per object saying so, which PLANNED_OBJECTS.md lists and
SUMMARY.md scores MEDIUM.

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
from target.ddl import (
    DEFERRED_EQUIVALENT_PROPERTIES, SCRUBBED_PROPERTIES,
    uncarried_column_facts,
)

from .medallion import (TARGET_KEY_MAX, TARGET_NAME_RULE_TEXT, bronze_target,
                        detect_target_collisions, layer_jobs,
                        target_key_overage, unacceptable_target_names)
from .restrictions import apply_restrictions, restriction_matches
from .waves import compute_waves

__all__ = ["build_plan", "TargetCollision", "object_kind_block"]

# Values that mean "this property is not set"; the same list ddl.py skips.
_UNSET = (None, "", "false", "FALSE", "N", "OFF", "null", "NULL")


def _maintenance_facts(rec: dict) -> tuple[list[dict], list[str]]:
    """(deferred_properties, omitted_properties), as `ddl` will report them.

    SUMMARY.md scores risk from the plan entry alone, and it once saw only
    the object type and the row count -- so a clustered table whose DDL plan
    listed cluster_by, change_tracking and retention_time as deferred read
    LOW with "no properties dropped". The tables are ddl.py's own, so the
    two reports name the same settings.
    """
    deferred: list[dict] = []
    omitted: list[str] = []
    for prop, value in (rec.get("source_metadata") or {}).items():
        if value in _UNSET:
            continue
        if prop in DEFERRED_EQUIVALENT_PROPERTIES:
            deferred.append({"property": prop, "value": value,
                             "aidp_equivalent": DEFERRED_EQUIVALENT_PROPERTIES[prop]})
        elif prop in SCRUBBED_PROPERTIES:
            omitted.append(f"{prop}={value}")
    return deferred, omitted


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


def _is_set(value) -> bool:
    return str(value if value is not None else "").strip().lower() in (
        "true", "y", "yes", "1")


# SHOW TABLES flag -> (short label, why a plain Delta copy is not that
# object). First match wins; a table carrying several flags is still one
# refusal. The label is what INVENTORY.md prints in its Compatibility column,
# so the inventory and the plan read one table and cannot disagree.
_TABLE_KIND_BLOCKS = (
    ("is_dynamic", "dynamic table",
     "Snowflake dynamic table: refreshed by Snowflake from its "
     "defining query; the census lists them; no equivalent is "
     "generated -- a copy would be a snapshot that never refreshes"),
    ("is_external", "external table",
     "Snowflake external table: its data lives in the stage's "
     "object storage, not in Snowflake; point AIDP at that "
     "location rather than copying a materialisation of it"),
    ("is_iceberg", "Iceberg table",
     "Snowflake Iceberg table: already open-format in object "
     "storage; register that Iceberg location in AIDP rather "
     "than copying it into Delta"),
    ("is_event", "event table",
     "Snowflake event table: a log and trace sink written by "
     "Snowflake itself; AIDP has no equivalent object"),
    ("is_hybrid", "hybrid table",
     "Snowflake hybrid (Unistore) table: row-store OLTP "
     "semantics do not carry to Delta"),
)

# The same for views, from SHOW VIEWS.
_VIEW_KIND_BLOCKS = (
    ("is_secure", "secure view",
     "Snowflake secure view: its definition and row-visibility rules have "
     "no Delta equivalent"),
    ("is_materialized", "materialized view",
     "Snowflake materialized view: no AIDP equivalent; rebuild as a table "
     "plus a refresh job"),
)


def object_kind_block(rec: dict) -> tuple[str, str] | None:
    """(label, reason) when the object's KIND has no plain-Delta equivalent.

    Independent of the column types, which `compatibility_status` covers.
    Read by the planner, which refuses the object, and by render_inventory,
    which once said `supported` for a dynamic table this refuses.
    """
    blocks = (_VIEW_KIND_BLOCKS if rec.get("object_type") == "VIEW"
              else _TABLE_KIND_BLOCKS)
    meta = rec.get("source_metadata") or {}
    for flag, label, reason in blocks:
        if _is_set(meta.get(flag)):
            return label, reason
    return None

# SHOW TABLES `kind` values that migrate, as permanent tables, with a warning.
_TABLE_KIND_WARNINGS = {
    "TRANSIENT": "TRANSIENT table in Snowflake (no Fail-safe, short Time "
                 "Travel); it is planned as a permanent Delta table, so confirm "
                 "it is meant to persist",
    "TEMPORARY": "TEMPORARY table in Snowflake (session-scoped, dropped when "
                 "the session ends); it is planned as a permanent Delta table, "
                 "so confirm it is meant to persist at all",
}


def _table_verdict(rec: dict) -> tuple[bool, str, str]:
    """(can_migrate, category, reason) for one table, from its SHOW flags."""
    block = object_kind_block(rec)
    if block:
        return False, "unsupported_object", block[1]
    return True, "", ""


def _table_kind_warning(rec: dict) -> dict | None:
    kind = str((rec.get("source_metadata") or {}).get("kind") or "").upper()
    if kind in _TABLE_KIND_WARNINGS:
        return {"source_identifier": rec["source_identifier"], "kind": kind,
                "warning": _TABLE_KIND_WARNINGS[kind]}
    return None


def _view_verdict(rec: dict) -> tuple[bool, str, str]:
    """(can_migrate, category, reason) for one view."""
    block = object_kind_block(rec)
    if block:
        return False, "unsupported_object", block[1]
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
                                   edges: list[dict], inventoried: set[str]
                                   ) -> tuple[list[dict], list[dict]]:
    """Move every dependent of a `cannot` object into `cannot`, transitively.

    Without this only the planned ids reached compute_waves, which drops an
    edge whose other end is not in the set: the view lost its only edge, sat
    at indegree 0, sorted first (rows=None -> size 0) and got a CREATE VIEW
    over a table that will never exist, under a report line promising that
    views follow their base tables.

    An edge to an object outside `inventoried` is a dependency on something
    this migration does not carry at all -- another database, usually. A
    view joining D.S.T with OTHERDB.S.FACTS was planned into wave 2, the
    report said views follow their base tables, and the create failed with
    a bare 500. It is refused here, naming the outside object, and its own
    dependents cascade from it.
    """
    can_ids = {c["source_identifier"] for c in can}
    kinds = {c["source_identifier"]: c["object_type"] for c in can}
    dependents: dict[str, set[str]] = collections.defaultdict(set)
    outside: dict[str, set[str]] = collections.defaultdict(set)
    for edge in edges:
        if edge["from"] == edge["to"]:
            continue
        dependents[edge["to"]].add(edge["from"])
        if edge["to"] not in inventoried and edge["from"] in can_ids:
            outside[edge["from"]].add(edge["to"])

    why = {c["source_identifier"]: c for c in cannot}
    for dependent in sorted(outside):
        names = ", ".join(sorted(outside[dependent]))
        can_ids.discard(dependent)
        entry = {"source_identifier": dependent,
                 "object_type": kinds[dependent],
                 "category": "dependency_not_migrated",
                 "reason": f"depends on {names}, which is outside the "
                           f"assessed scope (not in this inventory), so it "
                           f"is not migrating with it -- migrate or "
                           f"federate it first, or exclude this object"}
        cannot.append(entry)
        why[dependent] = entry
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


def _loads_into(census: dict | None) -> dict[str, list[dict]]:
    """Migrating-table candidates -> the pipes and tasks that write them.

    From the census's own reading of each body (`writes`). A load whose
    target could not be read is simply absent here: no entry is not a claim
    that nothing loads the table, which is why the census says what it read.
    """
    loads: dict[str, list[dict]] = collections.defaultdict(list)
    for obj in (census or {}).get("objects") or []:
        for table in obj.get("writes") or []:
            loads[table].append({"kind": obj["kind"],
                                 "source_identifier": obj["source_identifier"]})
    return loads


def _load_warning(load: dict) -> str:
    return (f'loaded in Snowflake by {load["kind"]} '
            f'{load["source_identifier"]}, which does not migrate: after '
            f"cutover this table stops receiving rows unless that load is "
            f"rebuilt on AIDP (a Job, or a streaming or scheduled load)")


def _name_parts(rec: dict) -> tuple[str, str, str]:
    """(database, schema, name) from the record's own fields.

    Not `source_identifier.split(".", 2)`: Snowflake allows a dot inside a
    quoted name, so `MYDB.PUBLIC.orders.v2` split that way gave a table
    `orders.v2` and a four-part target the name check passed fragment by
    fragment -- and `ddl` then refused the whole estate on it.
    """
    ident = rec["source_identifier"]
    db, schema = rec.get("source_database"), rec.get("source_schema")
    if db is not None and schema is not None and ident.startswith(f"{db}.{schema}."):
        return db, schema, ident[len(db) + len(schema) + 2:]
    db, schema, name = ident.split(".", 2)
    return db, schema, name


def _target_catalog_note(catalogs: list[str], prefix: str | None,
                         style: str) -> str:
    """Say whose catalog name the Target column carries.

    The in-AIDP structure job (01_create_structure, S10) creates each
    approved `target_fqn` as it stands, and refuses a run whose
    `--target-catalog` is not the plan's catalog. This note once described
    the job before that fix ("does not read this column", keeps the source
    schema), so the approval artifact named a schema S10 does not create,
    and with no prefix it steered the operator to a --target-catalog the
    job refuses.
    """
    job = ("The in-AIDP structure job (01_create_structure, S10) creates the "
           "Target column as it stands -- catalog, schema and table -- and "
           "refuses a run whose `provision --target-catalog` is not the "
           "plan's catalog.")
    if prefix is None:
        mirrored = ", ".join(catalogs) or "the source database"
        return (f"The catalog part of the Target column is the source database "
                f"name mirrored 1:1 ({mirrored}); no --bronze-catalog-prefix was "
                f"given. {job} In the runbook the catalog named after the "
                f"source database is the read-only EXTERNAL pointer at "
                f"Snowflake, not a target, so this plan cannot go through the "
                f"structure job as it stands: re-run `plan "
                f"--bronze-catalog-prefix <the INTERNAL catalog created at S4>` "
                f"and `ddl`, and pass that same catalog to `provision "
                f"--target-catalog`.")
    return (f"The catalog part of the Target column is the --bronze-catalog-prefix "
            f"{prefix!r}, with the schema part in the {style!r} style. {job} "
            f"Pass {prefix!r} to `provision --target-catalog`; the schemas "
            f"created are the ones listed here.")


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
    kind_warnings: list[dict] = []
    for rec in kept:
        ident = rec["source_identifier"]
        db, schema, name = _name_parts(rec)
        targets[ident] = bronze_target(db, schema, name,
                                       catalog_prefix=bronze_catalog_prefix,
                                       schema_style=bronze_schema_style)

        # A dot inside a quoted part makes the joined target more than
        # three parts. Checked on the parts, because the joined FQN splits
        # into fragments that each pass the name rule.
        dotted = [p for p in (db, schema, name) if "." in str(p)]
        if dotted:
            cannot.append({
                "source_identifier": ident,
                "object_type": rec.get("object_type"),
                "category": "unacceptable_target_name",
                "reason": (
                    "the source name part(s) "
                    + ", ".join(repr(p) for p in dotted)
                    + f" contain a '.', so the target {targets[ident]!r} is "
                      f"not a three-part catalog.schema.name -- "
                      f"{TARGET_NAME_RULE_TEXT}. Snowflake allows it "
                      f"because the source name is double-quoted. Rename it "
                      f"in Snowflake, or exclude it, and re-run: this plugin "
                      f"does not rewrite an object name.")})
            continue

        # A name the destination will refuse is refused here, not at the
        # create. Planning it means generating DDL for it, attempting it, and
        # burning the name on a 400 -- which is how it was found.
        # A key the destination cannot hold, refused here rather than at
        # the create -- where it returns 202, never appears, and burns the
        # name in that schema.
        over = target_key_overage(targets[ident])
        if over:
            target = targets[ident]
            catalog, schema, _, = (target.split(".", 2) + ["", ""])[:3]
            cannot.append({
                "source_identifier": ident,
                "object_type": rec.get("object_type"),
                "category": "target_key_too_long",
                "reason": (
                    f"the target key {target!r} is {len(target)} characters; "
                    f"the destination stores at most {TARGET_KEY_MAX} and "
                    f"answers a longer one with 202 Accepted, creates "
                    f"nothing, and burns the name. It is over by {over}. "
                    f"The limit is on the WHOLE key: catalog {catalog!r} "
                    f"({len(catalog)}) + schema {schema!r} ({len(schema)}) "
                    f"leave {max(0, TARGET_KEY_MAX - len(catalog) - len(schema) - 2)} "
                    f"characters for the object name. Shorten "
                    f"--bronze-catalog-prefix, or use "
                    f"--bronze-schema-style db, before renaming anything in "
                    f"Snowflake.")})
            continue

        bad = unacceptable_target_names(targets[ident])
        if bad:
            cannot.append({
                "source_identifier": ident,
                "object_type": rec.get("object_type"),
                "category": "unacceptable_target_name",
                "reason": (
                    f'the target name {targets[ident]!r} is not one the '
                    f'destination accepts: '
                    + ", ".join(repr(b) for b in bad)
                    + f' -- {TARGET_NAME_RULE_TEXT}. Snowflake allows it '
                      f'because the source name is double-quoted. Rename it '
                      f'in Snowflake, or exclude it, and re-run: this plugin '
                      f'does not rewrite an object name, because a renamed '
                      f'table is a different table to everything that reads '
                      f'it.')})
            continue

        if rec.get("compatibility_status") == "blocked":
            cannot.append({
                "source_identifier": ident, "object_type": rec.get("object_type"),
                "category": "unmapped_type",
                "reason": "column types with no Delta equivalent: "
                          + "; ".join(rec.get("blocked_reasons") or ["unspecified"])})
            continue

        if rec.get("compatibility_status") == "unassessed":
            # The schema's INFORMATION_SCHEMA.COLUMNS read failed, so the
            # empty column list is a missing fact, not a table with nothing
            # to map. Planned, it read `supported` / LOW / "clones cleanly"
            # everywhere but DDL_PLAN.md, which blamed a privilege for what
            # was a timeout. The reason is the error the read got.
            error = rec.get("columns_read_error") or "no error text was recorded"
            cannot.append({
                "source_identifier": ident, "object_type": rec.get("object_type"),
                "category": "columns_unread",
                "reason": ("its columns could not be read, so no type was "
                           "assessed and no DDL can be generated: the "
                           f"INFORMATION_SCHEMA.COLUMNS read failed ({error}). "
                           "Re-run `assess` once that read succeeds.")})
            continue

        verdict = _view_verdict if rec.get("object_type") == "VIEW" else _table_verdict
        ok, category, reason = verdict(rec)
        if not ok:
            cannot.append({
                "source_identifier": ident, "object_type": rec.get("object_type"),
                "category": category, "reason": reason})
            continue
        warning = _table_kind_warning(rec)
        if warning:
            kind_warnings.append(warning)

        # ddl reports maintenance settings for tables only (build_create_view
        # reads is_secure/is_materialized alone); the plan mirrors that split
        # so SUMMARY.md and DDL_PLAN.md name the same settings.
        deferred, omitted = (([], []) if rec.get("object_type") == "VIEW"
                             else _maintenance_facts(rec))
        # A column DEFAULT or an IDENTITY that does not travel changes what
        # an INSERT DOES after cutover -- NULL, or a failure, where Snowflake
        # supplied a value -- so it goes in `warnings`, which assess_risk
        # counts and PLANNED_OBJECTS.md prints. The sentences come from
        # target.ddl so this and DDL_PLAN.md cannot say different things.
        column_facts = ([] if rec.get("object_type") == "VIEW"
                        else uncarried_column_facts(rec))
        # Constraints are kept OUT of `warnings` on purpose: PK/UNIQUE/FK are
        # unenforced metadata on BOTH sides, so nothing behaves differently
        # after cutover, and raising every table with a primary key to MEDIUM
        # would drown the settings that do change behaviour. They are carried
        # as a fact per object instead, and named in DDL_PLAN.md (R20).
        constraints = ([] if rec.get("object_type") == "VIEW"
                       else list(rec.get("constraints") or []))
        can.append({"source_identifier": ident,
                    "object_type": rec.get("object_type"),
                    "target": targets[ident],
                    "rows": rec.get("row_count_exact"),
                    "columns": len(rec.get("columns") or []),
                    # The facts assess_risk reads. Column warnings (timezone,
                    # semi-structured-as-string, declared lengths) come from
                    # the type mapper; the maintenance settings from SHOW;
                    # the DEFAULT/IDENTITY sentences from target.ddl.
                    "warnings": list(rec.get("warnings") or []) + column_facts,
                    # Declared on the source, created on neither target path.
                    "constraints_not_created": constraints,
                    # Kept apart from the column warnings: assess_risk counts
                    # those, but a TRANSIENT/TEMPORARY table planned as a
                    # permanent one is a sentence about the object itself.
                    "kind_warning": warning["warning"] if warning else None,
                    "deferred_properties": deferred,
                    "omitted_properties": omitted})

    can, cannot = _cascade_dependency_exclusions(
        can, cannot, dependencies.get("edges", []),
        {r["source_identifier"] for r in records})

    # A pipe or task the census read as writing a table that migrates. The
    # census TASK verdict says such a table stops being populated at cutover;
    # this is where it gets named. Kept out of `warnings`, which assess_risk
    # reports as column warnings -- this is a sentence about the table.
    loads = _loads_into(inventory.get("census"))
    loads_that_stop: list[dict] = []
    for c in can:
        fed = loads.get(c["source_identifier"]) or []
        c["load_warnings"] = [_load_warning(f) for f in fed]
        loads_that_stop += [{"table": c["source_identifier"], **f} for f in fed]

    collisions = detect_target_collisions(
        {c["source_identifier"]: targets[c["source_identifier"]] for c in can})
    if collisions:
        raise TargetCollision(collisions)

    can_ids = {c["source_identifier"] for c in can}
    migratable = [r for r in kept if r["source_identifier"] in can_ids]
    sizes = {r["source_identifier"]: (r.get("row_count_exact") or 0)
             for r in migratable}
    edges = dependencies.get("edges", [])
    waved = compute_waves(sorted(can_ids), edges,
                          sort_key=lambda n: (sizes.get(n, 0), n))
    # A planned view with no edge from either source sits at indegree 0 and
    # sorts by size, so it can land ahead of its base table. Derived from the
    # planned views against the edges rather than from the producer's label,
    # so account_usage_empty, account_usage+parsed_ddl, parsed_ddl (ACCOUNT_
    # USAGE denied) and not_extracted are all covered alike.
    ordered = {e["from"] for e in edges}
    unordered_views = sorted(c["source_identifier"] for c in can
                             if c["object_type"] == "VIEW"
                             and c["source_identifier"] not in ordered)

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
        "target_catalog_note": _target_catalog_note(
            catalogs, bronze_catalog_prefix, bronze_schema_style),
        "waves": waved["waves"],
        "cycles": waved["cycles"],
        "target_names": targets,
        "clone_targets": sorted(can_ids),
        "can_migrate": sorted(can, key=lambda c: c["source_identifier"]),
        "cannot_migrate": sorted(cannot, key=lambda c: c["source_identifier"]),
        # Planned, but not as what they were: TRANSIENT/TEMPORARY tables
        # become permanent Delta tables. One entry per affected object.
        "table_kind_warnings": sorted(
            (w for w in kind_warnings
             if w["source_identifier"] in {c["source_identifier"] for c in can}),
            key=lambda w: w["source_identifier"]),
        # Migrating tables loaded by a pipe or task that does not migrate.
        "loads_that_stop": sorted(
            loads_that_stop,
            key=lambda x: (x["table"], x["kind"], x["source_identifier"])),
        "restrictions_applied": restrictions or {},
        # What each list entry matched in this inventory. A zero is a typo
        # until shown otherwise, and the report says so.
        "restriction_matches": restriction_matches(records, restrictions),
        "catalogs_to_create": catalogs,
        "schemas_to_create": [list(s) for s in schemas],
        "silver_gold_jobs": layer_jobs(scopes),
        "dependency_source": dependencies.get("source_used"),
        "dependency_coverage_note": dependencies.get("coverage_note"),
        "dependency_warning": dependencies.get("warning"),
        "dependency_edge_count": len(edges),
        # Ordered by size only; NOT guaranteed to follow their base tables.
        "views_without_dependency_edge": unordered_views,
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
