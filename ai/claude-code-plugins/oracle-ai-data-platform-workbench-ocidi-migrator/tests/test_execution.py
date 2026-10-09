"""Run the generated notebooks on local Spark + Delta and check the ROWS.

Skipped when pyspark / delta-spark are not installed (pip install -e ".[spark]").
These catch what reading the emitted code cannot: a translation that looks
right and computes something else.
"""
import datetime as dt
from decimal import Decimal

import pytest

from spark_harness import make_spark, run_notebook, seed_sales, spark_available

pytestmark = pytest.mark.skipif(not spark_available(), reason="pyspark/delta-spark not installed")


@pytest.fixture(scope="module")
def env(migrated, tmp_path_factory):
    out, _ = migrated
    root = tmp_path_factory.mktemp("spark")
    spark = make_spark(root)
    params = seed_sales(spark, root)
    run_notebook(out / "ddl/00_setup.ipynb", spark, params)
    yield spark, out, params
    spark.stop()


def nb(out, rel):
    return out / "notebooks" / f"{rel}.ipynb"


def rows(spark, table):
    return [r.asDict() for r in spark.table(table).collect()]


def test_load_customers_expression_filter_merge(env):
    spark, out, params = env
    run_notebook(nb(out, "SALES/Ingest/IT_LOAD_CUSTOMERS"), spark, params)
    got = sorted(rows(spark, "sales_dw.dim_customer"), key=lambda r: r["CUSTOMER_ID"])
    assert [r["CUSTOMER_ID"] for r in got] == [10, 11]       # 12: no email; 13: before P_SINCE
    assert got[0]["FULL_NAME"] == "Ada Lovelace" and got[0]["EMAIL"] == "ada@example.com"
    assert [r["SEGMENT"] for r in got] == ["DOMESTIC", "DOMESTIC"]
    assert isinstance(got[0]["CUSTOMER_ID"], Decimal)        # cast to the DDL type
    run_notebook(nb(out, "SALES/Ingest/IT_LOAD_CUSTOMERS"), spark, params)
    assert spark.table("sales_dw.dim_customer").count() == 2  # MERGE is idempotent


def test_parameter_override_from_job_task(env):
    spark, out, params = env
    run_notebook(nb(out, "SALES/Ingest/IT_LOAD_CUSTOMERS"), spark,
                 dict(params, P_SINCE="2018-01-01 00:00:00"))
    assert spark.table("sales_dw.dim_customer").count() == 3  # 13 is now included


def test_data_loader_overwrite(env):
    spark, out, params = env
    run_notebook(nb(out, "SALES/Ingest/DL_PRODUCTS"), spark, params)
    run_notebook(nb(out, "SALES/Ingest/DL_PRODUCTS"), spark, params)
    assert spark.table("sales_dw.dim_product").count() == 3


def test_orders_join_lookup_split_aggregate_incremental(env):
    spark, out, params = env
    run_notebook(nb(out, "SALES/Marts/IT_ORDERS_DAILY"), spark, params)
    large = sorted(rows(spark, "sales_dw.large_orders"), key=lambda r: r["ORDER_ID"])
    assert [(r["ORDER_ID"], r["NET_AMOUNT"]) for r in large] == [(1, Decimal("1080.00")),
                                                                  (5, Decimal("1800.00"))]
    daily = {(r["ORDER_DATE"], r["COUNTRY"], r["CATEGORY"]): r
             for r in rows(spark, "sales_dw.fact_orders_daily")}
    # 4 non-cancelled orders; lookup RETURN_ANY keeps one row per order
    assert sum(r["ORDER_COUNT"] for r in daily.values()) == 4
    unknown = daily[(dt.date(2026, 9, 2), "FR", "UNKNOWN")]           # no product 999: null fill
    assert unknown["TOTAL_NET"] == Decimal("0.00")                     # NVL(NULL amount, 0)
    assert daily[(dt.date(2026, 9, 1), "US", "TOOLS")]["TOTAL_NET"] == Decimal("1080.00")
    wm = rows(spark, "ocidi_control.watermarks")
    assert wm[0]["task"] == "IT_ORDERS_DAILY" and wm[0]["value"] == dt.datetime(2026, 9, 2, 13)
    # second run: nothing new past the watermark -> append adds nothing
    run_notebook(nb(out, "SALES/Marts/IT_ORDERS_DAILY"), spark, params)
    assert spark.table("sales_dw.large_orders").count() == 2


def test_type_overflow_refuses_instead_of_writing_null(env):
    spark, out, params = env
    spark.sql("CREATE TABLE IF NOT EXISTS sales_dw.tiny (V DECIMAL(3,0)) USING DELTA")
    ns = run_notebook(out / "ddl/00_setup.ipynb", spark, params)
    df = spark.createDataFrame([(12345,)], "V INT")
    with pytest.raises(ValueError, match="does not fit"):
        ns["_write_delta"](df, "`spark_catalog`.`sales_dw`.`tiny`", "append")


def test_stub_raises_loudly(env):
    spark, out, params = env
    with pytest.raises(NotImplementedError, match="was not converted"):
        run_notebook(nb(out, "SALES/Ingest/IT_BICC_GL"), spark, params)


def test_reconcile_notebook_runs(env):
    spark, out, params = env
    spark.sql("CREATE DATABASE IF NOT EXISTS SALES_DW")
    ns = run_notebook(out / "reconcile/reconcile.ipynb", spark, params)
    by_table = {r["table"]: r for r in ns["_results"]}
    # legacy == new here (same catalog), so the table compares equal to itself
    assert by_table["sales_dw.dim_product"]["status"] == "MATCH"
