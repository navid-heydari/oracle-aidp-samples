"""Run generated notebooks on a local Spark + Delta session.

The notebooks are executed cell by cell exactly as written. AIDP's injected
``oidlUtils`` is replaced by a stub whose ``getParameter`` returns test
values, which is also how the tests point ``SRC_*`` / ``TARGET_CATALOG`` at
local schemas and directories instead of AIDP catalogs and ``oci://`` paths.
"""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path


def spark_available() -> bool:
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


def make_spark(warehouse: Path):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.master("local[2]").appName("ocidi2aidp-tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", str(warehouse / "warehouse"))
               .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


class _Parameters:
    def __init__(self, values: dict):
        self._values = values

    def getParameter(self, name, default):  # noqa: N802 - AIDP's spelling
        return self._values.get(name, default)


class OidlUtilsStub:
    def __init__(self, values: dict):
        self.parameters = _Parameters({k: str(v) for k, v in values.items()})


def run_notebook(path: Path, spark, params: dict) -> dict:
    nb = json.loads(Path(path).read_text(encoding="utf-8"))
    ns = {"spark": spark, "oidlUtils": OidlUtilsStub(params), "__name__": "__notebook__"}
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
        exec(compile(src, f"{Path(path).name}#cell{i}", "exec"), ns)
    return ns


def seed_sales(spark, root: Path) -> dict:
    """Source data for the snapshot_sales fixture. Returns notebook parameters."""
    spark.sql("CREATE DATABASE IF NOT EXISTS SALES")
    ts = dt.datetime
    spark.createDataFrame([
        (1, 10, 100, dt.date(2026, 9, 1), Decimal("1200.00"), "SHIPPED", ts(2026, 9, 1, 10)),
        (2, 10, 101, dt.date(2026, 9, 1), Decimal("50.00"), "SHIPPED", ts(2026, 9, 1, 11)),
        (3, 11, 100, dt.date(2026, 9, 2), Decimal("300.00"), "CANCELLED", ts(2026, 9, 2, 9)),
        (4, 12, 999, dt.date(2026, 9, 2), None, "NEW", ts(2026, 9, 2, 12)),
        (5, 11, 101, dt.date(2026, 9, 2), Decimal("2000.00"), "SHIPPED", ts(2026, 9, 2, 13)),
    ], "ORDER_ID INT, CUSTOMER_ID INT, PRODUCT_ID INT, ORDER_DATE DATE, AMOUNT DECIMAL(12,2), "
       "STATUS STRING, UPDATED_AT TIMESTAMP").write.format("delta").mode("overwrite") \
        .saveAsTable("SALES.ORDERS")
    spark.createDataFrame([(10, "US"), (11, "CA"), (12, "FR")],
                          "CUSTOMER_ID INT, COUNTRY STRING").write.format("delta") \
        .mode("overwrite").saveAsTable("SALES.CUSTOMERS")
    spark.createDataFrame([(100, "TOOLS", Decimal("10.00")), (101, "TOYS", Decimal("5.00")),
                           (101, "TOYS-DUP", Decimal("5.00"))],
                          "PRODUCT_ID INT, CATEGORY STRING, LIST_PRICE DECIMAL(12,2)") \
        .write.format("delta").mode("overwrite").saveAsTable("SALES.PRODUCTS")
    landing = root / "buckets" / "landing" / "customers"
    landing.mkdir(parents=True, exist_ok=True)
    (landing / "customers.csv").write_text(
        "CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,COUNTRY,UPDATED_AT\n"
        "10,ada,lovelace, Ada@Example.COM ,US,2021-06-01 00:00:00\n"
        "11,alan,turing,alan@example.com,CA,2022-01-01 00:00:00\n"
        "12,grace,hopper,,FR,2023-01-01 00:00:00\n"
        "13,old,record,old@example.com,DE,2019-01-01 00:00:00\n", encoding="utf-8")
    return {"TARGET_CATALOG": "spark_catalog", "SRC_ADW_SALES": "spark_catalog",
            "SRC_OS_LANDING": f"file://{root / 'buckets'}/{{bucket}}/"}
