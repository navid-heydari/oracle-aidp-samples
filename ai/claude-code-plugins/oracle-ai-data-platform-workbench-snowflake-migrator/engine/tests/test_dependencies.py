"""Dependency extraction: ACCOUNT_USAGE preferred, parsed DDL as fallback."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.dependencies import (
    extract_dependencies, parse_view_references,
)

# The real view from the test estate: 4 fully-qualified names, 3 LEFT JOINs.
REAL_VIEW = """
create or replace view RAPPI_ORDER_360_VW(ORDER_ID, ITEM_COUNT) as
  SELECT o.ORDER_ID, COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM TEST_DB_20260908_1529.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.CUSTOMER_DIMENSIONS c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.STORE_DIMENSIONS s
    ON o.STORE_ID = s.STORE_ID
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.ORDER_ITEMS_FACT i
    ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;
"""


def test_parses_all_four_qualified_references():
    got = parse_view_references(REAL_VIEW, default_db="D", default_schema="S")
    assert got == [
        "TEST_DB_20260908_1529.PUBLIC.CUSTOMER_DIMENSIONS",
        "TEST_DB_20260908_1529.PUBLIC.ORDER_DIMENSIONS",
        "TEST_DB_20260908_1529.PUBLIC.ORDER_ITEMS_FACT",
        "TEST_DB_20260908_1529.PUBLIC.STORE_DIMENSIONS",
    ]


def test_bare_name_qualified_with_defaults():
    assert parse_view_references("select * from orders",
                                 default_db="D", default_schema="S") == ["D.S.ORDERS"]


def test_two_part_name_qualified_with_default_db():
    assert parse_view_references("select * from sales.orders",
                                 default_db="D", default_schema="S") == ["D.SALES.ORDERS"]


def test_subquery_after_from_is_not_a_reference():
    got = parse_view_references("select * from (select 1) t",
                                default_db="D", default_schema="S")
    assert got == []


def test_duplicate_references_deduplicated():
    sql = "select * from D.S.A join D.S.A b on 1=1"
    assert parse_view_references(sql, default_db="D", default_schema="S") == ["D.S.A"]


def test_account_usage_is_preferred_when_readable():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "D.S.V", "REFERENCED": "D.S.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, inv)
    assert out["source_used"] == "account_usage"
    assert out["edges"] == [{"from": "D.S.V", "to": "D.S.T",
                             "kind": "VIEW->TABLE", "source": "account_usage"}]


def test_falls_back_to_parsed_ddl_when_account_usage_denied():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "object_dependencies" in sql.lower():
                raise RuntimeError("Object does not exist or not authorized")
            return super().__call__(sql, params)

    out = extract_dependencies(Denied({}), inv)
    assert out["source_used"] == "parsed_ddl"
    assert out["edges"][0]["source"] == "parsed_ddl"
    assert "not authorized" in out["coverage_note"]


def test_fallback_drops_edges_to_objects_outside_the_inventory():
    # A reference we never inventoried cannot be planned, so it must not become
    # a phantom node in the wave graph.
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from OTHER.X.Y"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    out = extract_dependencies(Denied({}), inv)
    assert out["edges"] == []
    assert any("OTHER.X.Y" in n for n in out["unresolved_references"])


def test_tables_produce_no_edges_in_fallback_mode():
    inv = {"inventory": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                          "source_database": "D", "source_schema": "S"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    assert extract_dependencies(Denied({}), inv)["edges"] == []
