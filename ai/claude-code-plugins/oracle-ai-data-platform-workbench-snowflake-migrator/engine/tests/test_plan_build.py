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
    assert plan["target_names"]["MYDB.SALES.ORDERS"] == "MYDB.SALES.ORDERS"


def test_catalogs_and_schemas_to_create_are_derived():
    inv = {"inventory": [rec("D1.S1.A"), rec("D1.S2.B"), rec("D2.S1.C")]}
    plan = build_plan(inv, {"edges": []})
    assert plan["catalogs_to_create"] == ["D1", "D2"]
    assert plan["schemas_to_create"] == [["D1", "S1"], ["D1", "S2"], ["D2", "S1"]]


def test_catalog_prefix_mode_recorded():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []},
                      bronze_catalog_prefix="bronze")
    assert plan["target_names"]["D.S.T"] == "bronze.D_S.T"
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
    assert c["target"] == "D.S.T" and c["object_type"] == "TABLE"


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
    with pytest.raises(TargetCollision):
        build_plan(inv, {"edges": []})


def test_dependency_provenance_carried():
    plan = build_plan({"inventory": [rec("D.S.T")]},
                      {"edges": [], "source_used": "parsed_ddl",
                       "coverage_note": "view edges ONLY"})
    assert plan["dependency_source"] == "parsed_ddl"
    assert "ONLY" in plan["dependency_coverage_note"]
