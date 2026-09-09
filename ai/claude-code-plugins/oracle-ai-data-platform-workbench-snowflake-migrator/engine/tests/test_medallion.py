"""Layer assignment and target naming. Pure."""
import pytest

from plan.medallion import (
    LAYERS, STRATEGIES, UnknownStrategy, assign_layer, detect_target_collisions,
    target_name,
)


@pytest.mark.parametrize("name,expected", [
    ("BRONZE_PROD", "BRONZE"), ("raw_events", "BRONZE"), ("STG_ORDERS", "BRONZE"),
    ("landing", "BRONZE"), ("SILVER_PROD", "SILVER"), ("curated", "SILVER"),
    ("conformed_x", "SILVER"), ("GOLD_PROD", "GOLD"), ("datamart", "GOLD"),
    ("dm_finance", "GOLD"), ("reporting", "GOLD"),
])
def test_heuristic_matches_layer_names(name, expected):
    layer, basis = assign_layer(name, "PUBLIC")
    assert (layer, basis) == (expected, "matched_rule")


def test_schema_name_matches_when_database_does_not():
    assert assign_layer("ANALYTICS", "gold_marts")[0] == "GOLD"


def test_database_wins_over_schema():
    assert assign_layer("GOLD_PROD", "staging")[0] == "GOLD"


def test_unmatched_falls_back_to_bronze_and_says_so():
    layer, basis = assign_layer("ANALYTICS", "PUBLIC")
    assert (layer, basis) == ("BRONZE", "fallback")


def test_user_map_overrides_the_heuristic():
    layer, basis = assign_layer("GOLD_PROD", "PUBLIC",
                                {"GOLD_PROD.PUBLIC.T": "SILVER"},
                                source_identifier="GOLD_PROD.PUBLIC.T")
    assert (layer, basis) == ("SILVER", "user_provided")


def test_user_map_accepts_a_database_level_key():
    layer, basis = assign_layer("ANALYTICS", "PUBLIC", {"ANALYTICS": "GOLD"},
                                source_identifier="ANALYTICS.PUBLIC.T")
    assert (layer, basis) == ("GOLD", "user_provided")


def test_user_map_value_is_validated():
    with pytest.raises(ValueError, match="PLATINUM"):
        assign_layer("D", "S", {"D": "PLATINUM"}, source_identifier="D.S.T")


def test_layer_catalog_is_the_default_shape():
    assert target_name("MYDB", "SALES", "ORDERS", "BRONZE",
                       "layer-catalog") == "bronze.SALES.ORDERS"


def test_preserve_source_keeps_all_three_parts():
    assert target_name("MYDB", "SALES", "ORDERS", "GOLD",
                       "preserve-source") == "MYDB.SALES.ORDERS"


def test_layer_flattened_folds_db_into_schema():
    assert target_name("MYDB", "SALES", "ORDERS", "SILVER",
                       "layer-flattened") == "silver.MYDB_SALES.ORDERS"


def test_unknown_strategy_rejected():
    with pytest.raises(UnknownStrategy):
        target_name("D", "S", "T", "BRONZE", "whatever")


def test_layer_catalog_collides_across_databases():
    # This is exactly why the strategy needs a collision check: layer-catalog
    # drops the source database, so two DBs sharing schema.table merge.
    mapping = {
        "DB1.PUBLIC.ORDERS": target_name("DB1", "PUBLIC", "ORDERS", "BRONZE", "layer-catalog"),
        "DB2.PUBLIC.ORDERS": target_name("DB2", "PUBLIC", "ORDERS", "BRONZE", "layer-catalog"),
    }
    got = detect_target_collisions(mapping)
    assert got == {"bronze.PUBLIC.ORDERS": ["DB1.PUBLIC.ORDERS", "DB2.PUBLIC.ORDERS"]}


def test_preserve_source_never_collides():
    mapping = {
        "DB1.PUBLIC.ORDERS": target_name("DB1", "PUBLIC", "ORDERS", "BRONZE", "preserve-source"),
        "DB2.PUBLIC.ORDERS": target_name("DB2", "PUBLIC", "ORDERS", "BRONZE", "preserve-source"),
    }
    assert detect_target_collisions(mapping) == {}


def test_target_collision_check_is_case_insensitive():
    assert detect_target_collisions({"A": "bronze.S.T", "B": "BRONZE.S.T"})


def test_exported_constants():
    assert LAYERS == ("BRONZE", "SILVER", "GOLD")
    assert "layer-catalog" in STRATEGIES
