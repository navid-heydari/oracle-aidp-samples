"""Medallion mapping. Bronze mirrors Snowflake 1:1; Silver/Gold are job stubs."""
import pytest

from plan.medallion import (
    LAYERS, UnknownStrategy, bronze_target, layer_jobs, detect_target_collisions,
)


# --- Bronze: a structural mirror, not a layer assignment -------------------

def test_database_becomes_the_catalog():
    # Database -> Standard Catalog, Schemas -> Schemas, Tables -> tables.
    assert bronze_target("MYDB", "SALES", "ORDERS") == "MYDB.SALES.ORDERS"


def test_bronze_preserves_case_exactly():
    # Snowflake unquoted folds to UPPER; the mirror must not re-fold it.
    assert bronze_target("MyDb", "Sales", "Orders") == "MyDb.Sales.Orders"


def test_bronze_is_identity_so_it_cannot_collide():
    mapping = {
        "DB1.PUBLIC.ORDERS": bronze_target("DB1", "PUBLIC", "ORDERS"),
        "DB2.PUBLIC.ORDERS": bronze_target("DB2", "PUBLIC", "ORDERS"),
    }
    assert detect_target_collisions(mapping) == {}


def test_collision_detector_still_catches_case_variants():
    assert detect_target_collisions({"A": "db.s.t", "B": "DB.S.T"})


def test_optional_catalog_prefix_for_a_shared_bronze_catalog():
    # Some deployments want one bronze catalog rather than catalog-per-database.
    assert bronze_target("MYDB", "SALES", "ORDERS",
                         catalog_prefix="bronze") == "bronze.MYDB_SALES.ORDERS"


def test_prefix_mode_can_collide_and_is_detected():
    mapping = {
        "DB1.PUBLIC.T": bronze_target("DB1", "PUBLIC", "T", catalog_prefix="bronze"),
        "DB2.PUBLIC.T": bronze_target("DB2", "PUBLIC", "T", catalog_prefix="bronze"),
    }
    assert detect_target_collisions(mapping) == {}, "db is folded in, so distinct"


def test_bad_identifier_rejected():
    with pytest.raises(UnknownStrategy):
        bronze_target("DB", "S", "T", catalog_prefix="not a catalog")


# --- Silver / Gold: jobs created, never triggered --------------------------

def test_silver_and_gold_jobs_are_created_for_each_source_schema():
    jobs = layer_jobs([("MYDB", "SALES"), ("MYDB", "OPS")])
    names = sorted(j["name"] for j in jobs)
    assert names == ["gold_MYDB_OPS", "gold_MYDB_SALES",
                     "silver_MYDB_OPS", "silver_MYDB_SALES"]


def test_jobs_are_never_scheduled_or_triggered():
    for job in layer_jobs([("D", "S")]):
        assert job["trigger"] == "MANUAL_NEVER_TRIGGERED"
        assert job["schedule"] is None
        assert job["enabled"] is False


def test_jobs_declare_their_layer_and_source_scope():
    jobs = layer_jobs([("D", "S")])
    silver = next(j for j in jobs if j["layer"] == "SILVER")
    assert silver["source_database"] == "D"
    assert silver["source_schema"] == "S"
    assert silver["reads_from"] == "D.S"


def test_job_body_is_an_explicit_placeholder_not_fabricated_logic():
    # Silver/Gold transformations are requirement-driven. Inventing SQL here
    # would ship logic nobody specified.
    silver = next(j for j in layer_jobs([("D", "S")]) if j["layer"] == "SILVER")
    assert silver["body_status"] == "placeholder"
    assert "requirement" in silver["body_note"].lower()


def test_no_jobs_for_an_empty_scope():
    assert layer_jobs([]) == []


def test_layers_constant():
    assert LAYERS == ("BRONZE", "SILVER", "GOLD")
