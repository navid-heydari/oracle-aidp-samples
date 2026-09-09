"""Spark/Delta DDL generation with a per-rule audit trail. Pure."""
import pytest

from target.ddl import (
    RewriteResult, SCRUBBED_PROPERTIES, build_create_schema, build_create_table,
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
    res = build_create_table(
        record([col("A", "TEXT", "STRING", comment="it's fine")]), "bronze.S.T")
    assert "COMMENT 'it''s fine'" in res.sql


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
