"""Spark/Delta DDL generation with a per-rule audit trail. Pure."""
import pytest

from target.ddl import (
    RewriteResult, SCRUBBED_PROPERTIES, build_create_schema, build_create_table,
    quote_spark_string,
)


def col(name, dt, target, *, nullable="YES", pos=1, comment=None,
        precision=None, scale=None):
    return {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
            "IS_NULLABLE": nullable, "ORDINAL_POSITION": pos, "COMMENT": comment,
            "NUMERIC_PRECISION": precision, "NUMERIC_SCALE": scale}


def record(columns, **over):
    r = {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "source_database": "D", "source_schema": "PUBLIC",
         "compatibility_status": "supported", "blocked_reasons": [],
         "columns": columns, "source_metadata": {}}
    r.update(over)
    return r


def test_minimal_table_sql():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO"),
                col("PAID", "NUMBER", "DECIMAL(18,2)", pos=2)]),
        "bronze.PUBLIC.ORDERS")
    assert isinstance(res, RewriteResult)
    assert res.sql == (
        "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (\n"
        "  `ORDER_ID` DECIMAL(38,0) NOT NULL,\n"
        "  `PAID` DECIMAL(18,2)\n"
        ")\nUSING DELTA")
    assert res.blocked is False


def test_never_emits_create_or_replace():
    res = build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.S.T")
    assert "OR REPLACE" not in res.sql
    assert "IF NOT EXISTS" in res.sql


def test_columns_ordered_by_ordinal_position():
    res = build_create_table(
        record([col("SECOND", "TEXT", "STRING", pos=2),
                col("FIRST", "TEXT", "STRING", pos=1)]), "bronze.S.T")
    assert res.sql.index("`FIRST`") < res.sql.index("`SECOND`")


def test_column_comment_emitted_and_escaped():
    # Backslash, not doubling. This test previously asserted `'it''s fine'`,
    # which Spark reads as two adjacent literals and concatenates -- so the
    # comment silently became "its fine".
    res = build_create_table(
        record([col("A", "TEXT", "STRING", comment="it's fine")]), "bronze.S.T")
    assert r"COMMENT 'it\'s fine'" in res.sql


def test_rules_recorded_with_ids():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(5,2)", precision=5, scale=2)]),
        "bronze.S.T")
    ids = {r.rule_id for r in res.rules_applied}
    assert {"R01_TARGET_NAME", "R02_QUOTE_BACKTICK", "R30_USING_DELTA"} <= ids
    assert any(r.rule_id == "R03_TYPE_MAP" and "DECIMAL(5,2)" in r.detail
               for r in res.rules_applied)


@pytest.mark.parametrize("prop", ["cluster_by", "retention_time", "change_tracking"])
def test_snowflake_properties_scrubbed_and_recorded(prop):
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: "something"}),
        "bronze.S.T")
    assert any(prop in o for o in res.omitted_properties)
    assert prop not in res.sql
    assert prop in SCRUBBED_PROPERTIES


def test_blocked_record_produces_no_sql():
    res = build_create_table(
        record([col("P", "VARIANT", None)], compatibility_status="blocked",
               blocked_reasons=["P: VARIANT semi-structured"]),
        "bronze.S.T")
    assert res.blocked is True
    assert res.sql is None
    assert "VARIANT" in res.blocked_reason


def test_column_with_no_target_type_blocks_the_table():
    res = build_create_table(record([col("A", "TEXT", "STRING"),
                                     col("P", "GEOGRAPHY", None, pos=2)]),
                             "bronze.S.T")
    assert res.blocked is True
    assert "GEOGRAPHY" in res.blocked_reason


def test_table_with_no_columns_is_blocked():
    res = build_create_table(record([]), "bronze.S.T")
    assert res.blocked is True
    assert "no columns" in res.blocked_reason.lower()


def test_constraints_are_reported_not_emitted():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(38,0)", nullable="NO")],
               constraints=[{"type": "PRIMARY KEY", "columns": ["A"]}]),
        "bronze.S.T")
    assert "PRIMARY KEY" not in res.sql
    assert any(r.rule_id == "R20_CONSTRAINTS_NOT_EMITTED" for r in res.rules_applied)


def test_create_schema_never_emits_comment():
    # AIDP silently fails to persist CREATE SCHEMA ... COMMENT, and ISO-timestamp
    # colons in the comment are the specific trigger. Never emit it.
    sql = build_create_schema("bronze", "PUBLIC")
    assert sql == "CREATE SCHEMA IF NOT EXISTS `bronze`.`PUBLIC`"
    assert "COMMENT" not in sql


def test_target_fqn_must_be_three_part():
    with pytest.raises(ValueError, match="three-part"):
        build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.ORDERS")


@pytest.mark.parametrize("prop,value", [
    ("rows", 42), ("bytes", 2048), ("created_on", "2026-09-09"),
    ("owner", "ACCOUNTADMIN"),
])
def test_informational_metadata_is_not_reported_as_a_dropped_property(prop, value):
    # These are SHOW observations, not source-side settings. Listing them as
    # "dropped properties with no Delta equivalent" is misleading noise.
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: value}),
        "bronze.S.T")
    assert res.omitted_properties == []


@pytest.mark.parametrize("value", [None, "", "N", "OFF", "false"])
def test_unset_properties_are_not_reported(value):
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"change_tracking": value}),
        "bronze.S.T")
    assert res.omitted_properties == []


def test_a_set_property_is_still_reported():
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"cluster_by": "(COUNTRY_CODE)", "rows": 5}),
        "bronze.S.T")
    assert res.omitted_properties == ["cluster_by=(COUNTRY_CODE)"]


# ==========================================================================
# Spark string literals escape with a BACKSLASH, not by doubling.
#
# Found by parsing generated DDL with a real Spark parser. `'it''s fine'` is
# not an escaped quote in Spark -- it is two adjacent literals, which Spark
# concatenates. A column comment of "Customer's orders" therefore became
# "Customers orders", silently, or failed to parse depending on position.
# ==========================================================================

def test_spark_string_escapes_with_a_backslash():
    assert quote_spark_string("it's fine") == r"'it\'s fine'"


def test_spark_string_escapes_backslashes_first():
    assert quote_spark_string(r"a\b") == r"'a\\b'"


def test_spark_string_leaves_plain_text_alone():
    assert quote_spark_string("plain") == "'plain'"


def test_a_column_comment_with_an_apostrophe_is_backslash_escaped():
    rec = {"source_identifier": "DB.SC.T", "object_type": "TABLE",
           "source_metadata": {},
           "columns": [{"COLUMN_NAME": "A", "target_type": "STRING",
                        "ORDINAL_POSITION": 1, "IS_NULLABLE": "YES",
                        "DATA_TYPE": "TEXT", "COMMENT": "Customer's orders"}]}
    sql = build_create_table(rec, "CAT.SC.T").sql
    assert r"\'" in sql
    assert "''" not in sql, "doubling is two literals in Spark, not an escape"
