"""The in-AIDP manifest -> inventory bridge (runbook S6 -> S7).

The property that matters is AGREEMENT: a column planned from a manifest and
the same column planned from a live `assess` must reach the same verdict. Two
mappers that agree today diverge after the first change to either, so the
bridge is required to call the shared one.
"""
import pytest

from snowflake_source.extract.manifest import (ManifestShapeError,
                                               inventory_from_manifest)
from snowflake_source.dialect.types import map_type


def _col(name, data_type, *, precision=None, scale=None, length=None,
         nullable=True):
    return {"name": name, "type": data_type, "nullable": nullable,
            "data_type": data_type, "numeric_precision": precision,
            "numeric_scale": scale, "character_maximum_length": length}


def _manifest(**kw):
    table = {"name": "ORDERS", "source_rows": 10, "source_bytes": 100,
             "columns": [_col("ID", "NUMBER", precision=38, scale=0)]}
    table.update(kw)
    return {"schemas": [{"name": "SALES", "tables": [table], "views": [],
                         "errors": []}]}


def test_the_bridge_uses_the_same_mapper_as_a_live_assess():
    """Not "a mapping that looks right" -- the same function, same verdict."""
    inv = inventory_from_manifest(
        _manifest(columns=[_col("AMOUNT", "NUMBER", precision=14, scale=2)]),
        database="DB")
    got = inv["inventory"][0]["columns"][0]["target_type"]
    assert got == map_type("NUMBER", precision=14, scale=2).spark_type


def test_a_variant_column_blocks_its_table_exactly_as_it_does_live():
    inv = inventory_from_manifest(
        _manifest(columns=[_col("PAYLOAD", "VARIANT")]), database="DB")
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "blocked"
    assert "PAYLOAD" in rec["blocked_reasons"][0]


def test_the_semi_structured_mode_is_honoured():
    inv = inventory_from_manifest(
        _manifest(columns=[_col("PAYLOAD", "VARIANT")]), database="DB",
        semi_structured="string")
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "supported"
    assert any("PAYLOAD" in w for w in rec["warnings"]), \
        "carrying VARIANT as text is a deferral and must warn"


def test_a_manifest_row_count_is_metadata_and_never_called_verified():
    inv = inventory_from_manifest(_manifest(), database="DB")
    rec = inv["inventory"][0]
    assert rec["row_count_exact"] == 10
    assert rec["row_count_source"] == "show_metadata"
    assert "not a count(*)" in rec["row_count_note"].lower()


def test_a_view_arrives_without_its_sql_and_says_so():
    """A manifest carries columns, not definitions.

    Emitting a view record with no text and no complaint would read as
    translatable, and the planner would refuse it with no reason to show.
    """
    m = {"schemas": [{"name": "S", "tables": [], "errors": [],
                      "views": [{"name": "V", "columns": [_col("A", "TEXT")]}]}]}
    inv = inventory_from_manifest(m, database="DB")
    rec = inv["inventory"][0]
    assert rec["object_type"] == "VIEW"
    assert rec["view_text_show"] is None
    assert "cannot be dialect-translated" in rec["view_ddl_error"]
    assert any("view SQL absent" in n for n in inv["extraction_notes"])


def test_an_old_manifest_without_raw_type_fields_is_refused_not_parsed():
    """Re-parsing "NUMBER(38,0)" would be a second, lossier mapper."""
    m = {"schemas": [{"name": "S", "errors": [], "views": [], "tables": [
        {"name": "T", "columns": [{"name": "C", "type": "NUMBER(38,0)",
                                   "nullable": True}]}]}]}
    with pytest.raises(ManifestShapeError, match="raw-type fields"):
        inventory_from_manifest(m, database="DB")


def test_the_database_is_required_and_never_guessed():
    with pytest.raises(ManifestShapeError, match="needs `database`"):
        inventory_from_manifest(_manifest(), database="")


def test_a_shape_that_is_not_a_manifest_is_refused():
    with pytest.raises(ManifestShapeError, match="top-level `schemas`"):
        inventory_from_manifest({"inventory": []}, database="DB")


def test_discovery_errors_become_extraction_notes():
    """An object discovery could not read is ABSENT from the inventory, and
    absence must never read as "it does not exist"."""
    m = {"schemas": [{"name": "S", "tables": [], "views": [], "errors": [
        {"object": "BROKEN", "kind": "BASE TABLE",
         "error": "INFORMATION_SCHEMA.COLUMNS returned no column"}]}]}
    inv = inventory_from_manifest(m, database="DB")
    assert inv["inventory"] == []
    assert any("BROKEN" in n for n in inv["extraction_notes"])


def test_case_collisions_are_detected_on_the_manifest_path_too():
    m = {"schemas": [{"name": "S", "views": [], "errors": [], "tables": [
        {"name": "ORDERS", "columns": [_col("A", "TEXT")]},
        {"name": "orders", "columns": [_col("A", "TEXT")]}]}]}
    inv = inventory_from_manifest(m, database="DB")
    assert inv["identifier_case_collisions"], \
        "Spark folds to lower and would merge these two, losing data"


def test_the_inventory_shape_matches_what_the_planning_stages_read():
    inv = inventory_from_manifest(_manifest(), database="DB")
    for key in ("probed_at", "databases_in_scope", "row_count_mode",
                "semi_structured_mode", "geospatial_mode",
                "timestamp_ntz_mode", "object_count", "counts_by_type",
                "identifier_case_collisions", "extraction_notes",
                "inventory"):
        assert key in inv, key
    rec = inv["inventory"][0]
    for key in ("source_identifier", "object_type", "source_database",
                "source_schema", "identifier_case_form", "migration_status",
                "compatibility_status", "blocked_reasons", "warnings",
                "columns"):
        assert key in rec, key
    assert rec["source_identifier"] == "DB.SALES.ORDERS"
