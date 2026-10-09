import pytest

from ocidi2aidp.compile.expressions import (FUNCTIONS, ParamSpec, Scope, Udf, Untranslatable,
                                            function_table_markdown, spark_type, translate)


def scope(**kw):
    base = dict(aliases={"SOURCE_1": "", "EXPRESSION_1": ""}, entities={"CUSTOMERS": ""},
                params={"P_MIN": ParamSpec("P_MIN", "INTEGER", 1)}, node="N")
    base.update(kw)
    return Scope(**base)


def join_scope():
    return Scope(aliases={"SOURCE_1": "l", "SOURCE_2": "r", "SHARED": "?"},
                 entities={"ORDERS": "l", "CUSTOMERS": "r"}, params={}, node="J")


def sql(text, sc=None):
    return translate(text, sc or scope()).sql


def test_operator_entity_attr_resolves_to_column():
    assert sql("SOURCE_1.CUSTOMERS.NAME = 'x'") == "`NAME` = 'x'"
    assert sql("EXPRESSION_1.NAME IS NULL") == "`NAME` IS NULL"
    assert sql("CUSTOMERS.NAME") == "`NAME`"


def test_join_sides_get_aliases():
    assert sql("SOURCE_1.ORDERS.CID = SOURCE_2.CUSTOMERS.ID", join_scope()) == "l.`CID` = r.`ID`"


def test_reference_through_both_join_inputs_is_refused():
    with pytest.raises(Untranslatable, match="both inputs"):
        translate("SHARED.X.Y = 1", join_scope())


def test_parameters_become_placeholders_never_inline_values():
    t = translate("SOURCE_1.X.AMOUNT > $P_MIN AND SOURCE_1.X.B < ${P_MIN}", scope())
    assert t.sql == "`AMOUNT` > ${P_MIN} AND `B` < ${P_MIN}"
    assert t.params == {"P_MIN"}


def test_undeclared_parameter_is_untranslatable():
    with pytest.raises(Untranslatable, match="not declared"):
        translate("X > $NOPE", scope())


def test_system_parameters():
    t = translate("SOURCE_1.X.TS > SYS.LAST_LOAD_DATE", scope())
    assert t.sql == "`TS` > ${SYS_LAST_LOAD_DATE}"
    assert "SYS_LAST_LOAD_DATE" in t.params


def test_decode_is_null_safe_case_not_spark_decode():
    out = sql("DECODE(SOURCE_1.X.S, 'A', 1, NULL, 0, -1)")
    assert out == "CASE WHEN (`S`) <=> ('A') THEN 1 WHEN (`S`) <=> (NULL) THEN 0 ELSE -1 END"
    assert sql("DECODE(SOURCE_1.X.S, 'A', 1)").endswith("ELSE NULL END")


def test_to_char_classifies_date_and_number_formats():
    assert sql("TO_CHAR(SOURCE_1.X.D, 'yyyy-MM-dd')") == "date_format(`D`, 'yyyy-MM-dd')"
    assert sql("TO_CHAR(SOURCE_1.X.N, '999,990.00')") == "to_char(`N`, '999,990.00')"
    assert sql("TO_CHAR(SOURCE_1.X.N)") == "CAST(`N` AS STRING)"
    with pytest.raises(Untranslatable):
        sql("TO_CHAR(SOURCE_1.X.D, 'yyyy', 'fr')")


def test_cast_to_oracle_types_becomes_spark_types():
    assert sql("CAST(SOURCE_1.X.A AS VARCHAR2(10))") == "CAST(`A` AS STRING)"
    assert sql("CAST(SOURCE_1.X.A AS NUMBER(12,2))") == "CAST(`A` AS DECIMAL(12,2))"
    assert sql("CAST(SOURCE_1.X.A AS NUMBER)") == "CAST(`A` AS DECIMAL(38,10))"
    assert sql("CAST(SOURCE_1.X.A AS DECIMAL(5,1))") == "CAST(`A` AS DECIMAL(5,1))"


def test_whitespace_before_a_call_is_kept():
    assert sql("1 + CAST('2' AS INT)") == "1 + CAST('2' AS INT)"


def test_window_lambda_case_and_extract_pass_through():
    assert sql("ROW_NUMBER() OVER (PARTITION BY SOURCE_1.X.K ORDER BY SOURCE_1.X.T DESC)") == \
        "ROW_NUMBER() OVER (PARTITION BY `K` ORDER BY `T` DESC)"
    assert sql("TRANSFORM(SOURCE_1.X.ITEMS, x -> x.qty * 2)") == "TRANSFORM(`ITEMS`, x -> x.qty * 2)"
    assert sql("EXTRACT(YEAR FROM SOURCE_1.X.T)") == "EXTRACT(YEAR FROM `T`)"
    assert sql("CASE WHEN SOURCE_1.X.A IS NULL THEN 0 ELSE 1 END") == \
        "CASE WHEN `A` IS NULL THEN 0 ELSE 1 END"


def test_udf_is_inlined_with_parenthesised_arguments():
    sc = scope(udfs={"FULLNAME": Udf("FULLNAME", ["A", "B"], "CONCAT(A, ' ', B)"),
                     "LIB.FULLNAME": Udf("FULLNAME", ["A", "B"], "CONCAT(A, ' ', B)")})
    assert sql("FULLNAME(SOURCE_1.X.F, SOURCE_1.X.L)", sc) == "(CONCAT((`F`), ' ', (`L`)))"
    assert sql("LIB.FULLNAME(SOURCE_1.X.F, 'b')", sc) == "(CONCAT((`F`), ' ', ('b')))"


def test_unknown_function_and_ora_hash_go_to_fallback():
    with pytest.raises(Untranslatable, match="not in the translation table"):
        sql("MYSTERY(1)")
    with pytest.raises(Untranslatable, match="Oracle's hash"):
        sql("ORA_HASH(SOURCE_1.X.A)")


def test_numeric_trunc_is_refused_date_trunc_is_not():
    assert sql("TRUNC(SOURCE_1.X.D, 'MM')") == "trunc(`D`, 'MM')"
    assert sql("TRUNC(SOURCE_1.X.D, 'DD')") == "date_trunc('DD', `D`)"
    with pytest.raises(Untranslatable):
        sql("TRUNC(SOURCE_1.X.N, 2)")


def test_sysdate_and_generated_ids_are_reported():
    t = translate("SYSDATE", scope())
    assert t.sql == "current_timestamp()"
    t = translate("NUMERIC_ID()", scope())
    assert t.sql == "monotonically_increasing_id()"
    assert any(f.severity == "review" for f in t.findings)


def test_unbalanced_and_bad_characters():
    with pytest.raises(Untranslatable):
        sql("UPPER(SOURCE_1.X.A")
    with pytest.raises(Untranslatable):
        sql("A ¤ B")


@pytest.mark.parametrize("di,spark", [("VARCHAR2(200)", "STRING"), ("NUMBER(10)", "DECIMAL(10,0)"),
                                      ("NUMBER(12,2)", "DECIMAL(12,2)"), ("NUMBER", "DECIMAL(38,10)"),
                                      ("DATE", "DATE"), ("TIMESTAMP(6)", "TIMESTAMP"),
                                      ("INTEGER", "INT"), ("BINARY_DOUBLE", "DOUBLE")])
def test_type_mapping(di, spark):
    assert spark_type(di)[0] == spark


def test_reference_doc_matches_function_table():
    from pathlib import Path
    doc = (Path(__file__).parents[1] / "references" / "expression-functions.md").read_text()
    assert function_table_markdown() in doc, "regenerate: python -m ocidi2aidp.docs"
    assert len(FUNCTIONS) > 150
