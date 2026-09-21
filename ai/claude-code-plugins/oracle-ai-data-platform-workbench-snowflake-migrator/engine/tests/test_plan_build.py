"""plan.json assembly: bronze mirror, can/cannot with reasons, restrictions."""
import pytest

from plan.build import TargetCollision, build_plan

PORTABLE_VIEW = ("create view V as select a from D.PUBLIC.T")
SNOWFLAKE_VIEW = ("create view V2 as select * from D.PUBLIC.T "
                  "qualify row_number() over (order by a) = 1")


def rec(ident, kind="TABLE", rows=10, status="supported", blocked=(), ddl=None):
    db, schema, name = ident.split(".")
    r = {"source_identifier": ident, "object_type": kind,
         "source_database": db, "source_schema": schema,
         "compatibility_status": status, "blocked_reasons": list(blocked),
         "row_count_exact": rows, "source_metadata": {"bytes": rows * 10}}
    if kind == "VIEW":
        r["view_ddl_get_ddl"] = ddl or PORTABLE_VIEW
    return r


# --- bronze mirror --------------------------------------------------------

def test_bronze_target_mirrors_the_source_three_part_name():
    plan = build_plan({"inventory": [rec("MYDB.SALES.ORDERS")]}, {"edges": []})
    # AIDP folds identifiers, so the PLANNED target is the folded name.
    assert plan["target_names"]["MYDB.SALES.ORDERS"] == "mydb.sales.orders"


def test_catalogs_and_schemas_to_create_are_derived():
    inv = {"inventory": [rec("D1.S1.A"), rec("D1.S2.B"), rec("D2.S1.C")]}
    plan = build_plan(inv, {"edges": []})
    assert plan["catalogs_to_create"] == ["d1", "d2"]
    assert plan["schemas_to_create"] == [["d1", "s1"], ["d1", "s2"], ["d2", "s1"]]


def test_catalog_prefix_mode_recorded():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []},
                      bronze_catalog_prefix="bronze")
    assert plan["target_names"]["D.S.T"] == "bronze.d_s.t"
    assert plan["bronze_catalog_prefix"] == "bronze"


# --- views are in scope now -----------------------------------------------

def test_portable_view_can_migrate():
    inv = {"inventory": [rec("D.PUBLIC.T"), rec("D.PUBLIC.V", kind="VIEW")]}
    plan = build_plan(inv, {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.T"}]})
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    assert can == {"D.PUBLIC.T", "D.PUBLIC.V"}
    assert "D.PUBLIC.V" in plan["clone_targets"], "views are cloned now"


def test_view_with_snowflake_only_sql_cannot_migrate_with_the_construct_named():
    inv = {"inventory": [rec("D.PUBLIC.V2", kind="VIEW", ddl=SNOWFLAKE_VIEW)]}
    plan = build_plan(inv, {"edges": []})
    cannot = plan["cannot_migrate"]
    assert [c["source_identifier"] for c in cannot] == ["D.PUBLIC.V2"]
    assert "QUALIFY" in cannot[0]["reason"]
    assert cannot[0]["category"] == "snowflake_only_sql"


def test_views_ordered_after_their_base_tables():
    inv = {"inventory": [rec("D.PUBLIC.T"), rec("D.PUBLIC.V", kind="VIEW")]}
    plan = build_plan(inv, {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.T"}]})
    assert plan["waves"] == [["D.PUBLIC.T"], ["D.PUBLIC.V"]]


# --- can / cannot with reasons -------------------------------------------

def test_unmapped_column_type_cannot_migrate():
    inv = {"inventory": [rec("D.S.J", status="blocked",
                             blocked=["PAYLOAD: VARIANT semi-structured"])]}
    plan = build_plan(inv, {"edges": []})
    c = plan["cannot_migrate"][0]
    assert c["category"] == "unmapped_type"
    assert "VARIANT" in c["reason"]


def test_can_migrate_entries_carry_the_target_and_type():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []})
    c = plan["can_migrate"][0]
    assert c["target"] == "d.s.t" and c["object_type"] == "TABLE"


def test_every_object_appears_in_exactly_one_of_can_or_cannot():
    inv = {"inventory": [rec("D.S.OK"), rec("D.S.BAD", status="blocked",
                                            blocked=["x: VARIANT"]),
                         rec("D.S.V2", kind="VIEW", ddl=SNOWFLAKE_VIEW)]}
    plan = build_plan(inv, {"edges": []})
    ids = ([c["source_identifier"] for c in plan["can_migrate"]]
           + [c["source_identifier"] for c in plan["cannot_migrate"]])
    assert sorted(ids) == ["D.S.BAD", "D.S.OK", "D.S.V2"]
    assert len(ids) == len(set(ids))


# --- restrictions ---------------------------------------------------------

def test_restriction_exclusions_appear_in_cannot_migrate():
    inv = {"inventory": [rec("D1.S.A"), rec("D2.S.B")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_databases": ["D2"]})
    c = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D2.S.B")
    assert c["category"] == "restriction"
    assert "D2 excluded" in c["reason"]
    assert plan["restrictions_applied"] == {"exclude_databases": ["D2"]}


def test_restricted_objects_are_not_in_waves_or_clone_targets():
    inv = {"inventory": [rec("D1.S.A"), rec("D2.S.B")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_databases": ["D2"]})
    assert plan["waves"] == [["D1.S.A"]]
    assert plan["clone_targets"] == ["D1.S.A"]


# --- silver / gold jobs ---------------------------------------------------

def test_silver_and_gold_jobs_created_per_schema_and_never_triggered():
    inv = {"inventory": [rec("D.S1.A"), rec("D.S2.B")]}
    plan = build_plan(inv, {"edges": []})
    jobs = plan["silver_gold_jobs"]
    assert len(jobs) == 4
    assert all(j["enabled"] is False for j in jobs)
    assert all(j["trigger"] == "MANUAL_NEVER_TRIGGERED" for j in jobs)


def test_no_jobs_for_schemas_with_nothing_migratable():
    inv = {"inventory": [rec("D.S.BAD", status="blocked", blocked=["x: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    assert plan["silver_gold_jobs"] == []


# --- summary + safety -----------------------------------------------------

def test_summary_counts_match_the_lists():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW"),
                         rec("D.S.BAD", status="blocked", blocked=["x: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    s = plan["summary"]
    assert s["can_migrate"] == 2 and s["cannot_migrate"] == 1
    assert s["tables"] == 1 and s["views"] == 1
    assert s["catalogs"] == 1 and s["schemas"] == 1


def test_target_collision_still_halts():
    inv = {"inventory": [rec("D.S.T"), rec("D.s.T")]}
    with pytest.raises(TargetCollision) as exc:
        build_plan(inv, {"edges": []})
    # The HALT names its one in-tool remedy in the form the JSON file takes:
    # the twin to defer, spelled exactly, double-quoted.
    msg = str(exc.value)
    assert "exclude_objects" in msg
    assert '\\"D\\".\\"s\\".\\"T\\"' in msg


def test_dependency_provenance_carried():
    plan = build_plan({"inventory": [rec("D.S.T")]},
                      {"edges": [], "source_used": "parsed_ddl",
                       "coverage_note": "view edges ONLY"})
    assert plan["dependency_source"] == "parsed_ddl"
    assert "ONLY" in plan["dependency_coverage_note"]


def test_collision_resolved_by_excluding_the_quoted_twin():
    # The HALT above has exactly one in-tool remedy: name the twin to defer
    # with its exact, double-quoted spelling. The other twin is then planned.
    inv = {"inventory": [rec("D.S.T"), rec("D.S.t")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_objects": ['"D"."S"."t"']})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.T"]
    assert [(c["source_identifier"], c["category"])
            for c in plan["cannot_migrate"]] == [("D.S.t", "restriction")]


def test_unknown_count_under_a_cap_lands_in_cannot_migrate_with_the_reason():
    # Views carry no count under the default --row-counts metadata; a cap
    # that cannot be evaluated excludes rather than silently admitting.
    uncounted = rec("D.S.T")
    uncounted["row_count_exact"] = None
    inv = {"inventory": [uncounted, rec("D.S.SMALL", rows=3)]}
    plan = build_plan(inv, {"edges": []}, restrictions={"max_rows": 10})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.SMALL"]
    c = plan["cannot_migrate"][0]
    assert c["category"] == "restriction"
    assert "cannot be evaluated" in c["reason"] and "max_rows" in c["reason"]


# --- dependency cascade ---------------------------------------------------
#
# A view whose base object is not migrating cannot migrate either. Before,
# only the planned ids reached compute_waves, so the edge to a blocked or
# excluded base was dropped, the view had indegree 0, sorted FIRST in wave 1
# (rows=None -> size 0) and its CREATE VIEW was emitted over a table that
# will never exist -- while the report said "views follow their base tables".

def _edge(view, base):
    return {"from": view, "to": base}


def test_view_over_a_blocked_table_cannot_migrate_and_is_not_waved_or_emitted():
    from target.ddl import build_ddl_payload
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V", kind="VIEW", ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]})
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert cannot["D.S.V"]["category"] == "dependency_not_migrated"
    assert "depends on D.S.T, which is blocked" in cannot["D.S.V"]["reason"]
    assert cannot["D.S.V"]["object_type"] == "VIEW"
    assert plan["can_migrate"] == [] and plan["waves"] == []
    assert plan["clone_targets"] == []
    assert plan["summary"]["can_migrate"] == 0
    assert plan["summary"]["cannot_migrate"] == 2
    assert plan["summary"]["cannot_by_category"]["dependency_not_migrated"] == 1
    assert build_ddl_payload(inv, plan)["statements"] == []


def test_view_over_a_restricted_table_cannot_migrate():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW",
                                           ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]},
                      restrictions={"exclude_objects": ["D.S.T"]})
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats == {"D.S.T": "restriction", "D.S.V": "dependency_not_migrated"}
    v = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D.S.V")
    assert "depends on D.S.T, which is excluded" in v["reason"]


def test_dependency_exclusion_cascades_through_a_view_chain():
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V1", kind="VIEW", ddl="create view V1 as select a from D.S.T"),
                         rec("D.S.V2", kind="VIEW", ddl="create view V2 as select a from D.S.V1"),
                         rec("D.S.OK")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V1", "D.S.T"),
                                      _edge("D.S.V2", "D.S.V1")]})
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats["D.S.V1"] == cats["D.S.V2"] == "dependency_not_migrated"
    v2 = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D.S.V2")
    assert "depends on D.S.V1, which is blocked" in v2["reason"]
    assert plan["waves"] == [["D.S.OK"]]


def test_dependency_exclusion_through_a_diamond_lists_each_view_once():
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V1", kind="VIEW", ddl="create view V1 as select a from D.S.T"),
                         rec("D.S.V2", kind="VIEW", ddl="create view V2 as select a from D.S.T"),
                         rec("D.S.V3", kind="VIEW",
                             ddl="create view V3 as select a from D.S.V1 join D.S.V2 on 1=1")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V1", "D.S.T"), _edge("D.S.V2", "D.S.T"),
                                      _edge("D.S.V3", "D.S.V1"), _edge("D.S.V3", "D.S.V2")]})
    ids = ([c["source_identifier"] for c in plan["can_migrate"]]
           + [c["source_identifier"] for c in plan["cannot_migrate"]])
    assert sorted(ids) == ["D.S.T", "D.S.V1", "D.S.V2", "D.S.V3"]
    assert len(ids) == len(set(ids)), "every object in exactly one list, once"
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats == {"D.S.T": "unmapped_type", "D.S.V1": "dependency_not_migrated",
                    "D.S.V2": "dependency_not_migrated",
                    "D.S.V3": "dependency_not_migrated"}


def test_views_whose_bases_all_migrate_are_unaffected_by_the_cascade():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW",
                                           ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]})
    assert plan["waves"] == [["D.S.T"], ["D.S.V"]]
    assert plan["cannot_migrate"] == []
