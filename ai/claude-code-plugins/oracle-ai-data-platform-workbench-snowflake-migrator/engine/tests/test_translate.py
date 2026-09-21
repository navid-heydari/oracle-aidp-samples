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
    assert "CAST(a AS STRING)" in r.sql, "the type goes through the mapper"
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


# --------------------------------------------------------------------------
# Literal awareness (issue #18). Every rule used to run re.sub / re.search
# over raw SQL, so text inside a string literal was treated as code.
# --------------------------------------------------------------------------

def test_iff_inside_a_literal_is_not_rewritten():
    # Rewriting here would change the DATA the view returns, not the SQL.
    out = translate_sql("select 'IFF(x)' as label, IFF(a, 1, 2) as v")
    assert out.sql == "select 'IFF(x)' as label, IF(a, 1, 2) as v"


def test_array_construct_inside_a_literal_is_not_rewritten():
    out = translate_sql("select 'ARRAY_CONSTRUCT(1)' as doc")
    assert out.sql == "select 'ARRAY_CONSTRUCT(1)' as doc"


def test_qualify_inside_a_literal_does_not_block_the_view():
    # The word appears in a projected string, not as a clause.
    out = translate_sql("select 'QUALIFY' as reason from t")
    assert [u["rule_id"] for u in out.unsupported] == []
    assert out.fully_translated


def test_variant_path_rule_does_not_fire_on_a_json_literal():
    # T16's detector is `word : word`, which matches inside any JSON-ish
    # literal. That false positive blocked views with no VARIANT access at all.
    out = translate_sql("""select '{"a": 1}' as payload from t""")
    assert [u["rule_id"] for u in out.unsupported] == []


def test_variant_path_rule_still_fires_on_real_variant_access():
    out = translate_sql("select payload:customer from t")
    assert "T16_VARIANT_PATH" in [u["rule_id"] for u in out.unsupported]


def test_cast_shorthand_still_rewrites_a_literal_operand():
    # The operand is a literal on purpose; only `::` has to be code.
    out = translate_sql("select 'x'::varchar as v")
    assert out.sql == "select CAST('x' AS STRING) as v"


def test_cast_shorthand_inside_a_literal_is_not_rewritten():
    out = translate_sql("select 'a::int' as doc from t")
    assert out.sql == "select 'a::int' as doc from t"
    assert out.fully_translated


def test_listagg_separator_literal_survives_translation():
    out = translate_sql("select LISTAGG(name, ', ') from t")
    assert out.sql == "select concat_ws(', ', collect_list(name)) from t"


def test_dateadd_inside_a_literal_is_not_rewritten():
    out = translate_sql("select 'DATEADD(day, 1, d)' as doc")
    assert out.sql == "select 'DATEADD(day, 1, d)' as doc"


def test_comment_text_does_not_trigger_a_declared_rule():
    out = translate_sql("-- QUALIFY is discussed here\nselect 1")
    assert [u["rule_id"] for u in out.unsupported] == []


# --------------------------------------------------------------------------
# A cast operand that is a literal with an escaped quote. The rule's literal
# pattern used to stop at the first `'`, so `'don\\'t'::string` was spliced in
# the middle of the literal and the re-lex raised UnterminatedLiteral -- out of
# translate_sql, out of the planner, out of the CLI: exit 1 for the whole
# estate with no view named.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql,expected_operand", [
    ("select 'it\\'s'::varchar as v from t", "'it\\'s'"),
    ("select 'don\\'t'::string as w, a from DB.SC.T", "'don\\'t'"),
    ("select '\\''::varchar as v from t", "'\\''"),
    ("select 'a\\\\'::varchar as v from t", "'a\\\\'"),
])
def test_cast_shorthand_honours_backslash_escaped_quote_in_literal(sql, expected_operand):
    r = t(sql)
    assert f"CAST({expected_operand} AS" in r.sql, r.sql
    assert "::" not in r.sql
    assert r.fully_translated is True


def test_cast_shorthand_honours_doubled_quote_in_literal():
    r = t("select 'it''s'::varchar as v from t")
    assert "::" not in r.sql
    assert r.fully_translated is True
    assert r.sql.startswith("select CAST('it")


def test_a_rule_that_raises_unterminated_literal_blocks_only_that_construct(monkeypatch):
    from snowflake_source.dialect import lexer, translate

    def boom(sql):
        raise lexer.UnterminatedLiteral("boom")

    fake = translate.TranslationRule(
        "T99_FAKE", "FAKE", "fake rule for the guard", "implemented",
        r"\bselect\b", boom)
    monkeypatch.setattr(translate, "RULES", (fake,))
    r = translate.translate_sql("select x from t")
    assert r.sql == "select x from t"
    assert [u["rule_id"] for u in r.unsupported] == ["T99_FAKE"]
    assert "boom" in r.unsupported[0]["detail"]


# --------------------------------------------------------------------------
# T02 used to copy the Snowflake type name into CAST verbatim. Spark FLOAT is
# single precision, bare DECIMAL is DECIMAL(10,0), INT is 32-bit, and NUMBER /
# TEXT / TIME / VARIANT are not Spark types at all -- so the "exact" rewrite
# silently narrowed values or failed at first query. The type now goes through
# the same mapper as table DDL.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("src,expected", [
    ("a::FLOAT", "DOUBLE"),
    ("a::REAL", "DOUBLE"),
    ("a::NUMBER(10,2)", "DECIMAL(10,2)"),
    ("a::DECIMAL", "DECIMAL(38,0)"),
    ("a::INT", "DECIMAL(38,0)"),
    ("a::NUMBER", "DECIMAL(38,0)"),
    ("a::BIGINT", "DECIMAL(38,0)"),
    ("a::TEXT", "STRING"),
    ("a::varchar(20)", "STRING"),
    ("a::DATE", "DATE"),
    ("a::BOOLEAN", "BOOLEAN"),
    ("a::DOUBLE PRECISION", "DOUBLE"),
])
def test_cast_shorthand_maps_the_type_through_the_type_mapper(src, expected):
    r = t(f"select {src} from t")
    assert f"CAST(a AS {expected}) from t" in r.sql, r.sql
    assert "::" not in r.sql
    assert r.fully_translated is True


def test_cast_shorthand_never_emits_a_snowflake_type_name():
    r = t("select a::FLOAT, b::NUMBER(10,2), c::DECIMAL, d::INT, e::TEXT from t")
    for banned in ("AS FLOAT", "AS NUMBER", "AS TEXT", "AS INT)", "AS DECIMAL)"):
        assert banned not in r.sql, r.sql


@pytest.mark.parametrize("src,reason_word", [
    ("a::VARIANT", "semi-structured"),
    ("a::OBJECT", "semi-structured"),
    ("a::ARRAY", "semi-structured"),
    ("a::GEOGRAPHY", "no Spark"),
    ("a::FOOBAR", "unmapped"),
])
def test_cast_shorthand_refuses_unmappable_types(src, reason_word):
    sql = f"select {src} from t"
    r = t(sql)
    assert "T02_CAST_SHORTHAND" in [u["rule_id"] for u in r.unsupported]
    assert reason_word in r.unsupported[0]["detail"], r.unsupported
    assert r.sql == sql, "the statement is left untouched, not half-cast"


def test_cast_shorthand_carries_type_mapper_warnings():
    r = t("select a::TIMESTAMP, b::TIME from t")
    assert r.fully_translated is True
    assert any("timezone semantics differ" in w for w in r.warnings), r.warnings
    assert any("Spark has no TIME type" in w for w in r.warnings), r.warnings


def test_cast_shorthand_uses_the_same_mapping_as_table_ddl():
    # Guards against the two code paths drifting apart again.
    from snowflake_source.dialect.types import _DIRECT, map_type
    for name in list(_DIRECT) + ["NUMBER"]:
        if name == "NUMBER":
            src, expected = "a::NUMBER(12,3)", map_type(name, precision=12, scale=3)
        else:
            src, expected = f"a::{name}", map_type(name)
        r = t(f"select {src} from t")
        assert f"CAST(a AS {expected.spark_type})" in r.sql, (name, r.sql)


# --------------------------------------------------------------------------
# DATEADD: only the exact forms are rewritten. The amount used to be
# interpolated unparenthesised (`a + b * 7`), a column amount was put into an
# INTERVAL literal (a Spark parse error), and any form the regex could not
# match -- quoted unit, nested call -- fell through with no report at all, so
# the view was stamped portable with Snowflake DATEADD still in it.
# --------------------------------------------------------------------------

def test_dateadd_compound_amount_is_refused_not_misparenthesised():
    sql = "select DATEADD(week, a + b, d), DATEADD(year, n - 1, d) from t"
    r = t(sql)
    assert r.sql == sql, "left untouched rather than `a + b * 7`"
    assert "T05_DATEADD" in [u["rule_id"] for u in r.unsupported]
    assert "amount" in r.unsupported[0]["detail"]


@pytest.mark.parametrize("sql,expected", [
    ("select DATEADD(day, n, d) from t", "date_add(d, n)"),
    ("select DATEADD(week, n, d) from t", "date_add(d, (n) * 7)"),
    ("select DATEADD(year, n, d) from t", "add_months(d, (n) * 12)"),
    ("select DATEADD(month, t.n, d) from t", "add_months(d, t.n)"),
])
def test_dateadd_column_amount_is_parenthesised_where_it_is_multiplied(sql, expected):
    r = t(sql)
    assert expected in r.sql, r.sql
    assert r.fully_translated is True


@pytest.mark.parametrize("sql,expected", [
    ("select DATEADD(year, 7, d) from t", "add_months(d, 7 * 12)"),
    ("select DATEADD(day, -3, d) from t", "date_add(d, -3)"),
    ("select DATEADD(week, -2, d) from t", "date_add(d, -2 * 7)"),
    ("select DATEADD(hour, -1, ts) from t", "(ts + INTERVAL -1 HOUR)"),
])
def test_dateadd_literal_amount_keeps_the_bare_form(sql, expected):
    r = t(sql)
    assert expected in r.sql, r.sql
    assert r.fully_translated is True


def test_dateadd_time_unit_with_a_column_amount_is_refused():
    sql = "select DATEADD(hour, n_hours, ts) from t"
    r = t(sql)
    assert r.sql == sql
    assert "T05_DATEADD" in [u["rule_id"] for u in r.unsupported]
    assert "INTERVAL" in r.unsupported[0]["detail"]


@pytest.mark.parametrize("body", [
    "select DATEADD(day, -30, CURRENT_DATE()) as cutoff from t",
    "select DATEADD('day', 1, d) as d2 from t",
    "select DATEADD(day, abs(n), d) as d2 from t",
    "select DATEADD(day, 1, d) as d1, DATEADD(day, -30, CURRENT_DATE()) as c from t",
])
def test_dateadd_forms_the_rule_cannot_rewrite_are_refused_not_carried_over(body):
    r = t(body)
    assert r.sql == body, "refusal is total: no half-translated body"
    assert r.applied == []
    assert "T05_DATEADD" in [u["rule_id"] for u in r.unsupported]


def test_dateadd_applied_record_carries_the_date_return_caveat():
    r = t("select DATEADD(day, 1, d) from t")
    caveat = r.applied[0].get("caveat", "")
    assert "TIMESTAMP" in caveat and "DATE" in caveat, r.applied


def test_a_rule_whose_output_still_matches_its_own_detector_is_reported(monkeypatch):
    # The generic guard: an implemented rule that leaves its construct in
    # place is a refusal, never a silent pass-through stamped portable.
    from snowflake_source.dialect import translate

    fake = translate.TranslationRule(
        "T99_FAKE", "FAKE", "fake rule", "implemented", r"\bFAKE\s*\(",
        lambda sql: (sql.replace("x", "y"), None))
    monkeypatch.setattr(translate, "RULES", (fake,))
    r = translate.translate_sql("select FAKE(x) from t")
    assert r.applied == []
    assert [u["rule_id"] for u in r.unsupported] == ["T99_FAKE"]
