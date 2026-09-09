"""Inventory extraction with injected I/O. No Snowflake connection."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.catalog import SYSTEM_DBS, build_inventory

SESSION = [{"U": "NHEYDARI", "A": "DU58131", "R": "AWS_US_EAST_2",
            "ROLE": "ACCOUNTADMIN", "WH": "COMPUTE_WH", "V": "10.32.102"}]

COLUMNS = [
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 1,
     "COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
     "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 2,
     "COLUMN_NAME": "PAID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": 18, "NUMERIC_SCALE": 2,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 3,
     "COLUMN_NAME": "PAYLOAD", "DATA_TYPE": "VARIANT", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
]


def base_responses(**over):
    r = {
        "current_user()": SESSION,
        "show databases": [{"name": "MYDB"}, {"name": "SNOWFLAKE"},
                           {"name": "SNOWFLAKE_SAMPLE_DATA"}],
        "show schemas": [{"name": "PUBLIC"}, {"name": "INFORMATION_SCHEMA"}],
        "information_schema.columns": COLUMNS,
        "show tables": [{"name": "ORDERS", "rows": 99, "bytes": 4096,
                         "created_on": "2026-09-08", "comment": None,
                         "cluster_by": None}],
        "show views": [{"name": "ORDERS_VW", "text": "select * from ORDERS",
                        "created_on": "2026-09-08", "comment": None,
                        "is_secure": "false"}],
        "count(*)": [{"N": 100}],
        "get_ddl": [{"D": "create or replace view ORDERS_VW as select * from ORDERS;"}],
    }
    r.update(over)
    return r


def test_system_databases_are_excluded():
    inv = build_inventory(FakeSql(base_responses()))
    assert inv["databases_in_scope"] == ["MYDB"]
    assert "SNOWFLAKE" in SYSTEM_DBS


def test_information_schema_is_not_walked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["source_schema"] == "PUBLIC" for r in inv["inventory"])


def test_finds_one_table_and_one_view():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert inv["counts_by_type"] == {"TABLE": 1, "VIEW": 1}
    assert inv["object_count"] == 2


def test_row_count_uses_exact_count_not_show_estimate():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] == 100, "must be count(*), not SHOW's 99"
    assert table["source_metadata"]["bytes"] == 4096


def test_precision_and_scale_carried_from_information_schema():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    paid = next(c for c in table["columns"] if c["COLUMN_NAME"] == "PAID")
    assert (paid["NUMERIC_PRECISION"], paid["NUMERIC_SCALE"]) == (18, 2)


def test_type_mapping_recorded_and_variant_blocked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    m = {c["COLUMN_NAME"]: c["target_type"] for c in table["columns"]}
    assert m["ORDER_ID"] == "DECIMAL(38,0)"
    assert m["PAID"] == "DECIMAL(18,2)"
    assert m["PAYLOAD"] is None
    assert table["compatibility_status"] == "blocked"
    assert any("VARIANT" in b for b in table["blocked_reasons"])


def test_table_with_all_types_mapped_is_supported():
    cols = [c for c in COLUMNS if c["COLUMN_NAME"] != "PAYLOAD"]
    inv = build_inventory(
        FakeSql(base_responses(**{"information_schema.columns": cols})),
        databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["compatibility_status"] == "supported"


def test_view_keeps_both_ddl_forms_verbatim():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    view = next(r for r in inv["inventory"] if r["object_type"] == "VIEW")
    assert view["view_text_show"] == "select * from ORDERS"
    assert "create or replace view" in view["view_ddl_get_ddl"]
    assert view["compatibility_status"] == "requires_manual_design"


def test_case_form_captured_per_object():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["identifier_case_form"] == "UPPER_UNQUOTED" for r in inv["inventory"])


def test_case_collision_is_surfaced():
    r = base_responses(**{"show tables": [
        {"name": "ORDERS", "rows": 1, "bytes": 1},
        {"name": "orders", "rows": 1, "bytes": 1}]})
    inv = build_inventory(FakeSql(r), databases=["MYDB"])
    assert inv["identifier_case_collisions"], "must report, not silently merge"


def test_count_failure_is_recorded_not_fatal():
    class Flaky(FakeSql):
        def __call__(self, sql, params=None):
            if "count(*)" in sql.lower():
                raise RuntimeError("warehouse suspended")
            return super().__call__(sql, params)

    inv = build_inventory(Flaky(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] is None
    assert "warehouse suspended" in table["row_count_error"]


def test_schema_listing_failure_is_noted_and_extraction_continues():
    class NoSchemas(FakeSql):
        def __call__(self, sql, params=None):
            if "show schemas" in sql.lower():
                raise RuntimeError("insufficient privileges")
            return super().__call__(sql, params)

    inv = build_inventory(NoSchemas(base_responses()), databases=["MYDB"])
    assert inv["inventory"] == []
    assert any("insufficient privileges" in n for n in inv["extraction_notes"])


def test_migration_status_starts_at_discovered():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["migration_status"] == "discovered" for r in inv["inventory"])
