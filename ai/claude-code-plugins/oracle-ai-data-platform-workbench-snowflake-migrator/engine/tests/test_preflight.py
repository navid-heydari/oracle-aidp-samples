"""The pre-flight summary: what WOULD happen, shown before anything is created.

Required behaviour: once source and destination are both known and before the
first write, the plugin states every source -> destination mapping, what it
will create, and what it will not do. A migration that starts without the user
having seen this is a migration they did not approve.
"""
import pytest

from report.render import render_preflight

PLAN = {
    "bronze_catalog_prefix": "lake",
    "bronze_schema_style": "db",
    "summary": {"can_migrate": 2, "cannot_migrate": 1, "tables": 1, "views": 1,
                "objects_inventoried": 3},
    "can_migrate": [
        {"source_identifier": "TEST_DB.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "lake.test_db.orders", "rows": 100, "columns": 41},
        {"source_identifier": "TEST_DB.PUBLIC.V_ORDERS", "object_type": "VIEW",
         "target": "lake.test_db.v_orders", "rows": None, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "TEST_DB.PUBLIC.BAD", "object_type": "TABLE",
         "reason": "VARIANT column"}],
    "catalogs_to_create": [], "schemas_to_create": [["lake", "test_db"]],
    "silver_gold_jobs": [{"name": "silver_x", "layer": "SILVER"}],
}
SOURCE = {"account": "DU58131", "region": "AWS_US_EAST_2", "role": "ACCOUNTADMIN",
          "databases": ["TEST_DB"]}
TARGET = {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.aaa",
          "workspace": "ws-key", "cluster_id": "cl-key", "catalog": "lake"}


def test_it_states_both_ends_before_anything_happens():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "DU58131" in md and "ocid1.aidataplatform" in md
    assert "lake" in md


def test_every_object_shows_source_to_destination():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "TEST_DB.PUBLIC.ORDERS" in md
    assert "lake.test_db.orders" in md
    assert "→" in md or "->" in md


def test_it_says_the_destination_names_are_lower_cased_and_why():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "lower" in md.lower()
    assert "fold" in md.lower() or "AIDP" in md


def test_it_lists_what_will_be_created():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "lake.test_db" in md
    assert "1 schema" in md or "schema(s)" in md


def test_it_states_plainly_that_no_data_moves():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET).lower()
    assert "no data" in md or "zero rows" in md
    assert "read-only" in md


def test_it_names_what_will_not_be_created():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "TEST_DB.PUBLIC.BAD" in md
    assert "VARIANT column" in md


def test_it_says_jobs_are_not_triggered():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET).lower()
    assert "never triggered" in md or "not triggered" in md


def test_a_missing_target_is_stated_not_faked():
    md = render_preflight(PLAN, source=SOURCE, target=None)
    assert "not supplied" in md.lower()
    assert "nothing will be created" in md.lower()
