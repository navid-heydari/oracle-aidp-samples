"""View DDL generation. Bronze mirrors source, so reference rewriting is identity;
the real work is refusing to ship a wrong dialect translation."""
import pytest

from snowflake_source.dialect.views import (
    UNSUPPORTED_CONSTRUCTS, detect_unsupported_constructs, extract_view_body,
)
from target.ddl import build_create_view

REAL = """create or replace view RAPPI_ORDER_360_VW(
\tORDER_ID,
\tITEM_COUNT
) as
  SELECT
    o.ORDER_ID,
    COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM TEST_DB.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN TEST_DB.PUBLIC.ORDER_ITEMS_FACT i ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;"""


def view_record(ddl=REAL, **over):
    r = {"source_identifier": "TEST_DB.PUBLIC.RAPPI_ORDER_360_VW",
         "object_type": "VIEW", "source_database": "TEST_DB",
         "source_schema": "PUBLIC", "view_ddl_get_ddl": ddl,
         "source_metadata": {}, "columns": [], "compatibility_status": "supported"}
    r.update(over)
    return r


# --- body extraction ------------------------------------------------------

def test_body_extracted_after_the_column_list():
    body = extract_view_body(REAL)
    assert body.startswith("SELECT")
    assert "GROUP BY o.ORDER_ID" in body
    assert not body.endswith(";")


def test_body_extracted_without_a_column_list():
    body = extract_view_body("create view V as select 1")
    assert body == "select 1"


def test_secure_view_header_still_parses():
    assert extract_view_body("create or replace secure view V as select 1") == "select 1"


def test_unparseable_ddl_raises():
    with pytest.raises(ValueError, match="could not locate"):
        extract_view_body("this is not a view definition")


# --- dialect detection ----------------------------------------------------

def test_portable_view_has_no_unsupported_constructs():
    assert detect_unsupported_constructs(extract_view_body(REAL)) == []


@pytest.mark.parametrize("sql,construct", [
    ("select * from t qualify row_number() over (order by a) = 1", "QUALIFY"),
    ("select f.value from t, lateral flatten(input => t.j) f", "LATERAL FLATTEN"),
    ("select iff(a > 1, 'y', 'n') from t", "IFF"),
    ("select decode(a, 1, 'x') from t", "DECODE"),
    ("select nvl2(a, 1, 2) from t", "NVL2"),
    ("select a::string from t", ":: CAST SHORTHAND"),
    ("select listagg(a, ',') from t", "LISTAGG"),
    ("select seq4() from table(generator(rowcount => 5))", "GENERATOR"),
    ("select array_construct(1, 2) from t", "ARRAY_CONSTRUCT"),
    ("select object_construct('a', 1) from t", "OBJECT_CONSTRUCT"),
    ("select system$current_user() from t", "SYSTEM$ FUNCTION"),
    ("select * from t at(timestamp => x)", "TIME TRAVEL"),
    ("select j:field from t", "VARIANT PATH"),
])
def test_snowflake_only_constructs_detected(sql, construct):
    found = [c["construct"] for c in detect_unsupported_constructs(sql)]
    assert construct in found, found


def test_dateadd_flagged_for_argument_order():
    found = detect_unsupported_constructs("select dateadd(day, 7, d) from t")
    assert any(c["construct"] == "DATEADD/DATEDIFF" for c in found)
    assert any("argument order" in c["reason"].lower() for c in found)


def test_every_detection_carries_a_brief_reason():
    for c in detect_unsupported_constructs("select iff(a,1,2), b::int from t"):
        assert c["reason"] and len(c["reason"]) > 10


def test_max_by_is_not_flagged_spark_supports_it():
    assert detect_unsupported_constructs("select max_by(a, b) from t") == []


def test_constructs_table_is_exported():
    assert "QUALIFY" in UNSUPPORTED_CONSTRUCTS


# --- view generation ------------------------------------------------------

def test_portable_view_generates_create_view():
    res = build_create_view(view_record(), "TEST_DB.PUBLIC.RAPPI_ORDER_360_VW")
    assert res.blocked is False
    assert res.sql.startswith(
        "CREATE VIEW IF NOT EXISTS `TEST_DB`.`PUBLIC`.`RAPPI_ORDER_360_VW` AS")
    assert "SELECT" in res.sql
    assert "OR REPLACE" not in res.sql


def test_bronze_mirror_means_references_are_unchanged():
    res = build_create_view(view_record(), "TEST_DB.PUBLIC.RAPPI_ORDER_360_VW")
    assert "TEST_DB.PUBLIC.ORDER_DIMENSIONS" in res.sql
    assert any(r.rule_id == "R40_VIEW_REFS_IDENTITY" for r in res.rules_applied)


def test_reference_rewrite_applied_when_names_change():
    res = build_create_view(
        view_record(), "bronze.TEST_DB_PUBLIC.RAPPI_ORDER_360_VW",
        name_map={"TEST_DB.PUBLIC.ORDER_DIMENSIONS": "bronze.TEST_DB_PUBLIC.ORDER_DIMENSIONS"})
    assert "bronze.TEST_DB_PUBLIC.ORDER_DIMENSIONS" in res.sql
    assert any(r.rule_id == "R41_VIEW_REFS_REWRITTEN" for r in res.rules_applied)


def test_view_with_qualify_is_blocked_naming_the_construct():
    ddl = "create view V as select * from t qualify row_number() over (order by a) = 1"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert res.sql is None
    assert "QUALIFY" in res.blocked_reason


def test_secure_view_is_blocked():
    res = build_create_view(
        view_record(source_metadata={"is_secure": "true"}), "D.S.V")
    assert res.blocked is True
    assert "secure" in res.blocked_reason.lower()


def test_materialized_view_is_blocked():
    res = build_create_view(
        view_record(source_metadata={"is_materialized": "true"}), "D.S.V")
    assert res.blocked is True
    assert "materiali" in res.blocked_reason.lower()


def test_missing_ddl_is_blocked_not_silently_skipped():
    res = build_create_view(view_record(view_ddl_get_ddl=None), "D.S.V")
    assert res.blocked is True
    assert "no view SQL" in res.blocked_reason


def test_unparseable_ddl_blocks_with_a_reason():
    res = build_create_view(view_record(ddl="garbage"), "D.S.V")
    assert res.blocked is True
    assert "could not locate" in res.blocked_reason
