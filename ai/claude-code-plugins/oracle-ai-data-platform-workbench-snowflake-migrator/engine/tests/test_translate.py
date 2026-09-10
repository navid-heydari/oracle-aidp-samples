"""Snowflake -> AIDP (Spark SQL) dialect translation skeleton."""
import pytest

from snowflake_source.dialect.translate import (
    RULES, TranslationResult, coverage, translate_sql,
)


def t(sql):
    return translate_sql(sql)


# --- framework -----------------------------------------------------------

def test_registry_declares_every_known_construct():
    ids = {r.rule_id for r in RULES}
    for expected in ("T01_IFF", "T02_CAST_SHORTHAND", "T03_ARRAY_CONSTRUCT",
                     "T05_DATEADD", "T06_LISTAGG", "T10_QUALIFY",
                     "T11_LATERAL_FLATTEN", "T12_GENERATOR"):
        assert expected in ids, expected


def test_every_rule_has_an_id_description_and_status():
    for r in RULES:
        assert r.rule_id and r.description
        assert r.status in ("implemented", "declared")


def test_coverage_reports_the_skeletons_own_gaps():
    c = coverage()
    assert c["total"] == len(RULES)
    assert c["implemented"] + c["declared"] == c["total"]
    assert c["implemented"] > 0, "a skeleton with nothing working is a stub"
    assert c["declared"] > 0, "and one claiming everything works is a lie"
    assert set(c["declared_rule_ids"]) & {"T10_QUALIFY"}


def test_result_shape():
    r = t("select 1")
    assert isinstance(r, TranslationResult)
    assert r.sql == "select 1"
    assert r.applied == [] and r.unsupported == []
    assert r.fully_translated is True


# --- implemented rules ---------------------------------------------------

def test_iff_becomes_if():
    r = t("select IFF(a > 1, 'y', 'n') from t")
    assert "IF(a > 1, 'y', 'n')" in r.sql
    assert "IFF(" not in r.sql
    assert "T01_IFF" in [a["rule_id"] for a in r.applied]
    assert r.fully_translated is True


def test_iff_is_case_insensitive_and_repeated():
    r = t("select iff(a,1,2), IFF(b,3,4) from t")
    assert r.sql.count("IF(") == 2 and "IFF(" not in r.sql


def test_cast_shorthand_on_a_simple_operand():
    r = t("select a::string from t")
    assert "CAST(a AS string)" in r.sql
    assert "::" not in r.sql


def test_cast_shorthand_refused_on_a_complex_operand():
    # (x+y)::int cannot be rewritten by a token rule without risking scope.
    r = t("select (a + b)::int from t")
    assert "T02_CAST_SHORTHAND" in [u["rule_id"] for u in r.unsupported]
    assert r.fully_translated is False
    assert "::" in r.sql, "left untouched rather than mangled"


def test_array_construct_becomes_array():
    r = t("select ARRAY_CONSTRUCT(1, 2) from t")
    assert "array(1, 2)" in r.sql


def test_object_construct_becomes_named_struct():
    r = t("select OBJECT_CONSTRUCT('a', 1) from t")
    assert "named_struct('a', 1)" in r.sql


@pytest.mark.parametrize("unit,expected", [
    ("day", "date_add(d, 7)"),
    ("month", "add_months(d, 7)"),
    ("year", "add_months(d, 7 * 12)"),
])
def test_dateadd_maps_per_unit(unit, expected):
    r = t(f"select DATEADD({unit}, 7, d) from t")
    assert expected in r.sql
    assert "T05_DATEADD" in [a["rule_id"] for a in r.applied]


@pytest.mark.parametrize("unit", ["hour", "minute", "second"])
def test_dateadd_time_units_use_an_interval(unit):
    r = t(f"select DATEADD({unit}, 3, ts) from t")
    assert f"INTERVAL 3 {unit.upper()}" in r.sql


def test_dateadd_unknown_unit_is_refused_not_guessed():
    r = t("select DATEADD(fortnight, 1, d) from t")
    assert "T05_DATEADD" in [u["rule_id"] for u in r.unsupported]
    assert "fortnight" in r.unsupported[0]["detail"]


def test_dateadd_abbreviated_units_normalised():
    assert "date_add(d, 1)" in t("select DATEADD(dd, 1, d) from t").sql


def test_listagg_basic_form():
    r = t("select LISTAGG(name, ',') from t")
    assert "concat_ws(',', collect_list(name))" in r.sql


def test_listagg_with_within_group_is_refused():
    r = t("select LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) from t")
    assert "T06_LISTAGG" in [u["rule_id"] for u in r.unsupported]
    assert "ordering" in r.unsupported[0]["detail"].lower()


# --- declared but not implemented ---------------------------------------

@pytest.mark.parametrize("sql,rule", [
    ("select * from t qualify row_number() over (order by a) = 1", "T10_QUALIFY"),
    ("select f.value from t, lateral flatten(input => t.j) f", "T11_LATERAL_FLATTEN"),
    ("select seq4() from table(generator(rowcount => 5))", "T12_GENERATOR"),
    ("select * from t pivot(sum(a) for b in ('x'))", "T13_PIVOT"),
    ("select system$current_user()", "T14_SYSTEM_FUNCTION"),
    ("select * from t at(timestamp => x)", "T15_TIME_TRAVEL"),
])
def test_structural_constructs_are_reported_never_rewritten(sql, rule):
    r = t(sql)
    ids = [u["rule_id"] for u in r.unsupported]
    assert rule in ids, ids
    assert r.sql == sql, "declared rules must not alter the SQL"
    assert r.fully_translated is False


def test_every_unsupported_entry_explains_what_is_needed():
    r = t("select * from t qualify row_number() over (order by a) = 1")
    u = r.unsupported[0]
    assert u["detail"] and len(u["detail"]) > 20
    assert u["rule_id"] and u["construct"]


# --- safety --------------------------------------------------------------

def test_translation_never_introduces_a_data_movement_verb():
    for sql in ("select IFF(a,1,2) from t", "select a::int from t",
                "select LISTAGG(a, ',') from t"):
        out = t(sql).sql.upper()
        for verb in ("INSERT", "DELETE", "UPDATE", "MERGE", "DROP", "TRUNCATE"):
            assert verb not in out


def test_a_clean_statement_is_returned_byte_identical():
    sql = "SELECT a, b FROM d.s.t WHERE a > 1 GROUP BY a, b"
    assert t(sql).sql == sql


def test_partial_translation_is_flagged_not_claimed_complete():
    r = t("select IFF(a,1,2) from t qualify row_number() over (order by a) = 1")
    assert r.applied and r.unsupported
    assert r.fully_translated is False
