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
    ("select decode(a, 1, 'x') from t", "DECODE"),
    ("select nvl2(a, 1, 2) from t", "NVL2"),
    ("select seq4() from table(generator(rowcount => 5))", "GENERATOR / SEQ4"),
    ("select system$current_user() from t", "SYSTEM$"),
    ("select * from t at(timestamp => x)", "Time Travel"),
    ("select j:field from t", "VARIANT path"),
])
def test_snowflake_only_constructs_detected(sql, construct):
    found = [c["construct"] for c in detect_unsupported_constructs(sql)]
    assert construct in found, found


@pytest.mark.parametrize("sql", [
    "select iff(a > 1, 'y', 'n') from t",
    "select a::string from t",
    "select listagg(a, ',') from t",
    "select array_construct(1, 2) from t",
    "select object_construct('a', 1) from t",
    "select dateadd(day, 7, d) from t",
])
def test_translatable_constructs_no_longer_block(sql):
    # These have exact rewrites, so they are translated rather than refused.
    assert detect_unsupported_constructs(sql) == []


def test_every_detection_carries_a_brief_reason():
    for c in detect_unsupported_constructs("select decode(a,1,2), nvl2(a,1,2) from t"):
        assert c["reason"] and len(c["reason"]) > 10


def test_max_by_is_not_flagged_spark_supports_it():
    assert detect_unsupported_constructs("select max_by(a, b) from t") == []


def test_constructs_table_is_exported():
    assert "QUALIFY" in UNSUPPORTED_CONSTRUCTS


def test_a_view_using_only_translatable_sql_migrates():
    ddl = "create view V as select IFF(a > 1, 'y', 'n') AS f, b::int AS n from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    assert "IF(a > 1, 'y', 'n')" in res.sql and "IFF(" not in res.sql
    assert "CAST(b AS int)" in res.sql
    assert any(r.rule_id == "R43_VIEW_DIALECT_TRANSLATED" for r in res.rules_applied)
    assert any(r.rule_id == "T01_IFF" for r in res.rules_applied)


def test_a_view_mixing_translatable_and_structural_sql_is_still_blocked():
    ddl = ("create view V as select IFF(a,1,2) from D.S.T "
           "qualify row_number() over (order by a) = 1")
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "QUALIFY" in res.blocked_reason


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


# --- regression: header/body boundary ------------------------------------

def test_body_is_not_truncated_by_an_as_inside_the_select():
    # REGRESSION: the header pattern used to match the FIRST `) as` anywhere, so
    # `IFF(a, 'y', 'n') AS f` was read as the end of the column list and the body
    # was silently truncated. Truncated SQL that still runs is the worst outcome.
    ddl = "create view V as select IFF(a > 1, 'y', 'n') AS f, b from D.S.T"
    body = extract_view_body(ddl)
    assert body.startswith("select IFF(")
    assert body.endswith("from D.S.T")


def test_column_list_header_still_parses_with_an_as_in_the_body():
    ddl = ("create or replace view V(\n\tF,\n\tB\n) as\n"
           "  select IFF(a, 1, 2) AS F, b AS B from D.S.T;")
    body = extract_view_body(ddl)
    assert body.startswith("select IFF(")
    assert "AS B" in body


def test_function_call_in_the_body_does_not_look_like_a_column_list():
    ddl = "create view V as select coalesce(sum(x), 0) as total from D.S.T"
    assert extract_view_body(ddl).startswith("select coalesce(")


def test_quoted_column_list_still_parses():
    ddl = 'create view V("A", "B") as select a, b from t'
    assert extract_view_body(ddl) == "select a, b from t"


# --------------------------------------------------------------------------
# Header location by scanner, not regex (issue #18).
# --------------------------------------------------------------------------

def test_body_found_when_the_view_name_is_quoted_and_contains_a_space():
    # The old header regex allowed [\w$".]+ for the name, so a legal quoted
    # name with a space failed to match and the whole view was unreadable.
    body = extract_view_body('create view "my view" as select 1 as a')
    assert body == "select 1 as a"


def test_body_found_when_the_name_contains_a_doubled_quote():
    assert extract_view_body('create view "we""ird" as select 1') == "select 1"


def test_as_inside_a_string_literal_in_the_header_is_not_the_header_end():
    # COMMENT = 'x as y' -- a scan for ` as ` finds this first.
    body = extract_view_body(
        "create view v comment = 'defined as a rollup' as select 1 as a")
    assert body == "select 1 as a"


def test_as_inside_a_comment_in_the_header_is_not_the_header_end():
    body = extract_view_body("create view v /* used as a stub */ as select 1")
    assert body == "select 1"


def test_column_alias_as_is_not_mistaken_for_the_header_end():
    body = extract_view_body(
        "create view v (f) as select IFF(a, 'y', 'n') AS f from t")
    assert body == "select IFF(a, 'y', 'n') AS f from t"


def test_quoted_column_list_with_spaces_is_handled():
    body = extract_view_body('create view v ("col one", "col two") as select 1, 2')
    assert body == "select 1, 2"


def test_secure_recursive_and_or_replace_prefixes_are_all_accepted():
    for head in ("create view v",
                 "create or replace view v",
                 "create secure view v",
                 "create or replace secure recursive view v"):
        assert extract_view_body(f"{head} as select 1") == "select 1"


def test_a_statement_that_is_not_a_create_view_is_refused():
    with pytest.raises(ValueError):
        extract_view_body("select 1")


def test_a_create_view_with_no_as_is_refused():
    with pytest.raises(ValueError):
        extract_view_body("create view v")


def test_trailing_semicolon_and_whitespace_are_stripped():
    assert extract_view_body("create view v as select 1 ;  ") == "select 1"


def test_unterminated_literal_in_the_ddl_is_reported():
    with pytest.raises(ValueError):
        extract_view_body("create view v as select 'oops")
