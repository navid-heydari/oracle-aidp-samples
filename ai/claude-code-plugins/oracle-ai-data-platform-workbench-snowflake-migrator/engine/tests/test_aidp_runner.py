"""The pure halves of the AIDP adapter. The live connection is not tested here."""
import json

import pytest

from target.aidp_runner import unwrap_outputs, wrap_sql


def test_wrap_single_statement_calls_spark_sql():
    code = wrap_sql("SHOW TABLES IN `bronze`.`S`")
    assert "spark.sql(" in code
    assert "SHOW TABLES IN `bronze`.`S`" in code
    assert "print(" in code


def test_wrap_batch_keeps_all_statements_in_one_execution():
    # AIDP discards per-statement DDL on session close, so the chunk must share
    # a single kernel execution.
    code = wrap_sql("CREATE TABLE a (x INT);\nCREATE TABLE b (y INT)")
    assert code.count("spark.sql(") == 2
    assert code.count("print(") == 1


def test_wrap_ignores_trailing_semicolons_and_blank_statements():
    assert wrap_sql("SELECT 1;;\n;").count("spark.sql(") == 1


def test_wrap_rejects_empty_sql():
    with pytest.raises(ValueError, match="no SQL"):
        wrap_sql("   ;  ")


def test_wrap_quotes_sql_safely():
    # A statement containing quotes must not break the generated Python.
    code = wrap_sql("SELECT 'it''s'")
    compile(code, "<wrapped>", "exec")


def ok(rows):
    return {"status": "ok", "outputs": [
        {"type": "TEXT_PLAIN",
         "value": "__SNOWMIG_ROWS__" + json.dumps(rows) + "\n"}]}


def test_unwrap_returns_the_printed_rows():
    assert unwrap_outputs(ok([{"tableName": "T"}])) == [{"tableName": "T"}]


def test_unwrap_handles_no_rows():
    assert unwrap_outputs(ok([])) == []


def test_unwrap_ignores_unrelated_stdout():
    r = ok([{"a": 1}])
    r["outputs"].insert(0, {"type": "TEXT_PLAIN", "value": "warning: whatever\n"})
    assert unwrap_outputs(r) == [{"a": 1}]


def test_unwrap_raises_on_kernel_error_status():
    with pytest.raises(RuntimeError, match="kernel error"):
        unwrap_outputs({"status": "error", "outputs": []})


def test_unwrap_raises_on_an_error_output():
    # A silent [] here would make a failed DDL batch look like a success that
    # simply returned no rows.
    with pytest.raises(RuntimeError, match="NameError"):
        unwrap_outputs({"status": "ok", "outputs": [
            {"type": "error", "ename": "NameError", "evalue": "spark undefined"}]})


def test_unwrap_rejects_a_non_dict_result():
    with pytest.raises(RuntimeError, match="unexpected"):
        unwrap_outputs(["nope"])


def test_unwrap_without_the_sentinel_returns_empty():
    assert unwrap_outputs({"status": "ok", "outputs": [
        {"type": "TEXT_PLAIN", "value": "nothing useful\n"}]}) == []
