"""Artifact -> markdown. Pure."""
from report.render import render_ddl_plan, render_inventory

INV = {
    "probed_at": "2026-09-09T00:00:00+00:00",
    "session": {"A": "DU58131", "R": "AWS_US_EAST_2", "ROLE": "ACCOUNTADMIN"},
    "databases_in_scope": ["MYDB"], "object_count": 2,
    "counts_by_type": {"TABLE": 1, "VIEW": 1},
    "identifier_case_collisions": {}, "extraction_notes": [],
    "inventory": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS", "object_type": "TABLE",
         "row_count_exact": 100, "source_metadata": {"bytes": 9728},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "supported", "columns": [1, 2, 3]},
        {"source_identifier": "MYDB.PUBLIC.ORDERS_VW", "object_type": "VIEW",
         "row_count_exact": 100, "source_metadata": {},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "requires_manual_design", "columns": [1]},
    ],
}


def test_inventory_lists_objects_with_exact_row_counts():
    md = render_inventory(INV)
    assert "MYDB.PUBLIC.ORDERS" in md
    assert "100" in md
    assert "exact" in md.lower(), "must label counts as exact, not estimated"


def test_inventory_shows_the_type_breakdown():
    md = render_inventory(INV)
    assert "TABLE" in md and "VIEW" in md


def test_collisions_rendered_as_a_halt_not_a_footnote():
    md = render_inventory({**INV, "identifier_case_collisions":
                           {"A.B.C": ["A.B.C", "A.B.c"]}})
    assert "HALT" in md.upper()
    assert "A.B.c" in md


def test_extraction_notes_surfaced_when_present():
    md = render_inventory({**INV, "extraction_notes": ["MYDB.X: denied"]})
    assert "denied" in md


def test_ddl_plan_shows_sql_rules_and_omissions():
    md = render_ddl_plan({"statements": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS",
         "target_fqn": "bronze.PUBLIC.ORDERS",
         "sql": "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (`A` STRING)",
         "rules_applied": [{"rule_id": "R03_TYPE_MAP", "detail": "A: TEXT -> STRING"}],
         "warnings": [], "omitted_properties": ["cluster_by=X"]}],
        "blocked": [{"source_identifier": "MYDB.PUBLIC.J", "reason": "VARIANT"}]})
    assert "CREATE TABLE IF NOT EXISTS" in md
    assert "R03_TYPE_MAP" in md
    assert "cluster_by=X" in md
    assert "VARIANT" in md


