"""Dependency extraction: ACCOUNT_USAGE preferred, parsed DDL as fallback."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.dependencies import (
    extract_dependencies, parse_view_references,
)

# The real view from the test estate: 4 fully-qualified names, 3 LEFT JOINs.
REAL_VIEW = """
create or replace view ACME_ORDER_360_VW(ORDER_ID, ITEM_COUNT) as
  SELECT o.ORDER_ID, COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM SNOWMIG_TESTDB.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.CUSTOMER_DIMENSIONS c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.STORE_DIMENSIONS s
    ON o.STORE_ID = s.STORE_ID
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.ORDER_ITEMS_FACT i
    ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;
"""


def test_parses_all_four_qualified_references():
    got = parse_view_references(REAL_VIEW, default_db="D", default_schema="S")
    assert got == [
        "SNOWMIG_TESTDB.PUBLIC.CUSTOMER_DIMENSIONS",
        "SNOWMIG_TESTDB.PUBLIC.ORDER_DIMENSIONS",
        "SNOWMIG_TESTDB.PUBLIC.ORDER_ITEMS_FACT",
        "SNOWMIG_TESTDB.PUBLIC.STORE_DIMENSIONS",
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


# --- quoted, case-sensitive identifiers ------------------------------------
#
# Snowflake keeps a quoted identifier's case and treats it as case-SENSITIVE,
# so a view created as "SalesView" over "Orders" is inventoried as
# DB.S.SalesView / DB.S.Orders, and that exact spelling is the plan's node.
# An edge whose endpoints are spelled any other way is silently dropped by
# the wave computation, and the view is then created before its base table.

def _mixed_case_inventory():
    return {"inventory": [
        {"source_identifier": "DB.S.Orders", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "S",
         "row_count_exact": 1000},
        {"source_identifier": "DB.S.SalesView", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": 'create view "SalesView" as '
                             'select * from "DB"."S"."Orders"'},
        {"source_identifier": "DB.S.PLAIN", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "S",
         "row_count_exact": 10},
        {"source_identifier": "DB.S.PLAIN_V", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": "create view PLAIN_V as select * from DB.S.PLAIN"}]}


def test_account_usage_keeps_edges_for_quoted_mixed_case_objects():
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.SalesView", "REFERENCED": "DB.S.Orders",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        {"REFERENCING": "DB.S.PLAIN_V", "REFERENCED": "DB.S.PLAIN",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, _mixed_case_inventory())
    assert out["source_used"] == "account_usage"
    assert sorted((e["from"], e["to"]) for e in out["edges"]) == [
        ("DB.S.PLAIN_V", "DB.S.PLAIN"), ("DB.S.SalesView", "DB.S.Orders")]


def test_parsed_ddl_fallback_emits_exact_inventory_identifiers():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    out = extract_dependencies(Denied({}), _mixed_case_inventory())
    assert out["source_used"] == "parsed_ddl"
    assert sorted((e["from"], e["to"]) for e in out["edges"]) == [
        ("DB.S.PLAIN_V", "DB.S.PLAIN"), ("DB.S.SalesView", "DB.S.Orders")], \
        "endpoints are the inventory's exact spelling, not an upper-cased copy"
    assert out["unresolved_references"] == []


def test_a_mixed_case_view_lands_in_a_later_wave_than_its_base_table():
    # End to end through the planner: the edge has to survive into the waves.
    from plan.build import build_plan
    inv = _mixed_case_inventory()
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.SalesView", "REFERENCED": "DB.S.Orders",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    plan = build_plan(inv, extract_dependencies(run, inv))
    wave_of = {n: i for i, wave in enumerate(plan["waves"]) for n in wave}
    assert wave_of["DB.S.Orders"] < wave_of["DB.S.SalesView"], plan["waves"]
