"""plan.json assembly. Pure."""
import pytest

from plan.build import TargetCollision, build_plan


def rec(ident, kind="TABLE", rows=10, status="supported", blocked=()):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "compatibility_status": status, "blocked_reasons": list(blocked),
            "row_count_exact": rows, "source_metadata": {"bytes": rows * 10}}


def test_single_table_gets_a_wave_and_a_target():
    plan = build_plan({"inventory": [rec("BRONZE_PROD.PUBLIC.ORDERS")]},
                      {"edges": []})
    assert plan["waves"] == [["BRONZE_PROD.PUBLIC.ORDERS"]]
    assert plan["target_names"]["BRONZE_PROD.PUBLIC.ORDERS"] == "bronze.PUBLIC.ORDERS"
    assert plan["medallion_assignment"]["BRONZE_PROD.PUBLIC.ORDERS"] == "BRONZE"
    assert plan["medallion_assignment_basis"]["BRONZE_PROD.PUBLIC.ORDERS"] == "matched_rule"


def test_strategy_recorded_in_the_plan():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []},
                      strategy="preserve-source")
    assert plan["namespace_strategy"] == "preserve-source"
    assert plan["target_names"]["D.S.T"] == "D.S.T"


def test_fallback_assignments_are_listed_for_review():
    plan = build_plan({"inventory": [rec("ANALYTICS.PUBLIC.T")]}, {"edges": []})
    assert plan["fallback_assignments"] == ["ANALYTICS.PUBLIC.T"]


def test_blocked_objects_excluded_from_waves_and_listed():
    inv = {"inventory": [rec("D.S.OK"),
                         rec("D.S.BAD", status="blocked", blocked=["PAYLOAD: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    assert plan["waves"] == [["D.S.OK"]]
    assert plan["blocked"] == [{"source_identifier": "D.S.BAD",
                                "reason": "PAYLOAD: VARIANT"}]


def test_views_are_planned_but_marked_unsupported_for_mvp1():
    inv = {"inventory": [rec("D.S.T"),
                         rec("D.S.V", kind="VIEW", status="requires_manual_design")]}
    plan = build_plan(inv, {"edges": [{"from": "D.S.V", "to": "D.S.T"}]})
    assert plan["waves"] == [["D.S.T"], ["D.S.V"]]
    assert any(u["source_identifier"] == "D.S.V" and u["feature"] == "VIEW"
               for u in plan["unsupported"])
    assert "D.S.V" not in plan["clone_targets"], "MVP-1 clones tables only"
    assert plan["clone_targets"] == ["D.S.T"]


def test_smaller_tables_ordered_first_within_a_wave():
    inv = {"inventory": [rec("D.S.BIG", rows=1000), rec("D.S.SMALL", rows=5)]}
    plan = build_plan(inv, {"edges": []})
    assert plan["waves"] == [["D.S.SMALL", "D.S.BIG"]]


def test_cycles_surfaced_from_waves():
    inv = {"inventory": [rec("D.S.A", kind="VIEW", status="requires_manual_design"),
                         rec("D.S.B", kind="VIEW", status="requires_manual_design")]}
    plan = build_plan(inv, {"edges": [{"from": "D.S.A", "to": "D.S.B"},
                                      {"from": "D.S.B", "to": "D.S.A"}]})
    assert sorted(plan["cycles"][0]) == ["D.S.A", "D.S.B"]


def test_target_collision_halts_rather_than_guessing():
    inv = {"inventory": [rec("DB1.PUBLIC.ORDERS"), rec("DB2.PUBLIC.ORDERS")]}
    with pytest.raises(TargetCollision) as exc:
        build_plan(inv, {"edges": []})
    assert "bronze.PUBLIC.ORDERS" in exc.value.collisions


def test_preserve_source_avoids_that_collision():
    inv = {"inventory": [rec("DB1.PUBLIC.ORDERS"), rec("DB2.PUBLIC.ORDERS")]}
    plan = build_plan(inv, {"edges": []}, strategy="preserve-source")
    assert len(plan["target_names"]) == 2


def test_dependency_source_carried_into_the_plan():
    plan = build_plan({"inventory": [rec("D.S.T")]},
                      {"edges": [], "source_used": "parsed_ddl",
                       "coverage_note": "view->object edges ONLY"})
    assert plan["dependency_source"] == "parsed_ddl"
    assert "ONLY" in plan["dependency_coverage_note"]
