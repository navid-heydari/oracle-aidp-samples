#!/usr/bin/env python3
"""Validate the Rappi test corpus. PASS/FAIL per check, exit 0/1.

Mirrors MAXWELL/scripts/demo-cecl-banking/snowflake/validate.py: row-count floors
PLUS business invariants, because a corpus that only satisfies counts cannot detect
a "right-shaped but wrong-answer" migration -- one that runs clean and returns
the wrong numbers.

Snowflake does NOT enforce PK/FK -- only NOT NULL. So referential integrity has to be
asserted here, not assumed from the DDL. Delta will not enforce it either, which is
exactly why these checks have to survive the migration as tests.

Usage:
  export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PRIVATE_KEY_PATH=...
  python3 validate.py
"""
from __future__ import annotations
import os, sys, decimal
from cryptography.hazmat.primitives import serialization
import snowflake.connector

DB, SCHEMA = os.environ.get("CORPUS_DB", "TEST_DB_20260908_1529"), "PUBLIC"
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  --  {detail}" if detail else ""))


def main() -> int:
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
        k = serialization.load_pem_private_key(fh.read(), password=None)
    der = k.private_bytes(encoding=serialization.Encoding.DER,
                          format=serialization.PrivateFormat.PKCS8,
                          encryption_algorithm=serialization.NoEncryption())
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        private_key=der, warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=DB, schema=SCHEMA, login_timeout=60)
    cur = conn.cursor()

    def one(sql: str):
        cur.execute(sql)
        return cur.fetchone()

    print("\n-- row-count floors ------------------------------------------------")
    counts = {}
    for t in ("ORDER_DIMENSIONS", "CUSTOMER_DIMENSIONS", "STORE_DIMENSIONS",
              "COURIER_DIMENSIONS", "ORDER_ITEMS_FACT"):
        counts[t] = one(f"select count(*) from {t}")[0]
    check("ORDER_DIMENSIONS preserved at 100", counts["ORDER_DIMENSIONS"] == 100,
          f"{counts['ORDER_DIMENSIONS']} rows")
    for t, floor in (("CUSTOMER_DIMENSIONS", 1), ("STORE_DIMENSIONS", 1),
                     ("COURIER_DIMENSIONS", 1), ("ORDER_ITEMS_FACT", 100)):
        check(f"{t} non-trivially populated", counts[t] >= floor, f"{counts[t]} rows")

    print("\n-- primary-key uniqueness (Snowflake does NOT enforce this) ---------")
    for t, pk in (("ORDER_DIMENSIONS", "ORDER_ID"),
                  ("CUSTOMER_DIMENSIONS", "CUSTOMER_ID"),
                  ("STORE_DIMENSIONS", "STORE_ID"),
                  ("COURIER_DIMENSIONS", "COURIER_ID"),
                  ("ORDER_ITEMS_FACT", "ORDER_ITEM_ID")):
        n, d, nulls = one(f"select count(*), count(distinct {pk}), "
                          f"count_if({pk} is null) from {t}")
        check(f"{t}.{pk} unique and not null", n == d and nulls == 0,
              f"{n} rows / {d} distinct / {nulls} null")

    print("\n-- referential integrity: no orphans --------------------------------")
    for label, sql in (
        ("ORDER -> CUSTOMER", "select count(*) from ORDER_DIMENSIONS o "
         "left join CUSTOMER_DIMENSIONS c on o.CUSTOMER_ID=c.CUSTOMER_ID "
         "where o.CUSTOMER_ID is not null and c.CUSTOMER_ID is null"),
        ("ORDER -> STORE", "select count(*) from ORDER_DIMENSIONS o "
         "left join STORE_DIMENSIONS s on o.STORE_ID=s.STORE_ID "
         "where o.STORE_ID is not null and s.STORE_ID is null"),
        ("ORDER -> COURIER", "select count(*) from ORDER_DIMENSIONS o "
         "left join COURIER_DIMENSIONS k on o.COURIER_ID=k.COURIER_ID "
         "where o.COURIER_ID is not null and k.COURIER_ID is null"),
        ("ITEM -> ORDER", "select count(*) from ORDER_ITEMS_FACT i "
         "left join ORDER_DIMENSIONS o on i.ORDER_ID=o.ORDER_ID "
         "where o.ORDER_ID is null"),
    ):
        orphans = one(sql)[0]
        check(f"{label}: zero orphans", orphans == 0, f"{orphans} orphan(s)")

    print("\n-- join fan-out: the LEFT JOINs must actually vary -------------------")
    delivered_no_items = one(
        "select count(*) from ORDER_DIMENSIONS o left join ("
        " select ORDER_ID, count(*) N from ORDER_ITEMS_FACT group by 1) i"
        " on o.ORDER_ID=i.ORDER_ID"
        " where o.ORDER_STATUS='DELIVERED' and coalesce(i.N,0)=0")[0]
    check("every DELIVERED order has >=1 item", delivered_no_items == 0,
          f"{delivered_no_items} delivered order(s) with no items")
    cancelled_with_items = one(
        "select count(*) from ORDER_DIMENSIONS o join ORDER_ITEMS_FACT i"
        " on o.ORDER_ID=i.ORDER_ID where o.ORDER_STATUS='CANCELLED'")[0]
    check("no CANCELLED order has items", cancelled_with_items == 0,
          f"{cancelled_with_items} item(s) on cancelled orders")
    lo, hi, distinct_n = one(
        "select min(N), max(N), count(distinct N) from ("
        " select ORDER_ID, count(*) N from ORDER_ITEMS_FACT group by 1)")
    check("item fan-out varies (>=3 distinct counts)", distinct_n >= 3,
          f"items/order min={lo} max={hi} distinct={distinct_n}")

    print("\n-- exact business invariants ----------------------------------------")
    # I1  sum(item line totals) per order == order PRODUCT_SUBTOTAL, exactly
    bad_i1, worst_i1 = one("""
        select count(*), coalesce(max(abs(DELTA)),0) from (
          select o.ORDER_ID, o.PRODUCT_SUBTOTAL - sum(i.LINE_TOTAL_AMOUNT) as DELTA
          from ORDER_DIMENSIONS o join ORDER_ITEMS_FACT i on o.ORDER_ID=i.ORDER_ID
          group by o.ORDER_ID, o.PRODUCT_SUBTOTAL
          having o.PRODUCT_SUBTOTAL - sum(i.LINE_TOTAL_AMOUNT) <> 0)""")
    check("I1 sum(LINE_TOTAL_AMOUNT) == PRODUCT_SUBTOTAL exactly",
          bad_i1 == 0, f"{bad_i1} order(s) off, worst delta={worst_i1}")
    # I2  unit_price*qty - discount == line_total, exactly
    bad_i2, worst_i2 = one("""
        select count(*), coalesce(max(abs(UNIT_PRICE*QUANTITY-DISCOUNT_AMOUNT
                                         -LINE_TOTAL_AMOUNT)),0)
        from ORDER_ITEMS_FACT
        where UNIT_PRICE*QUANTITY-DISCOUNT_AMOUNT-LINE_TOTAL_AMOUNT <> 0""")
    check("I2 UNIT_PRICE*QUANTITY - DISCOUNT == LINE_TOTAL exactly",
          bad_i2 == 0, f"{bad_i2} item(s) off, worst delta={worst_i2}")
    neg = one("select count(*) from ORDER_ITEMS_FACT where DISCOUNT_AMOUNT < 0")[0]
    check("no negative DISCOUNT_AMOUNT", neg == 0, f"{neg} negative")
    # dimension attributes agree with the denormalised copy on the order
    mismatch = one("""
        select count(*) from ORDER_DIMENSIONS o
        join CUSTOMER_DIMENSIONS c on o.CUSTOMER_ID=c.CUSTOMER_ID
        where o.COUNTRY_CODE <> c.COUNTRY_CODE""")[0]
    check("customer COUNTRY_CODE agrees with order", mismatch == 0,
          f"{mismatch} disagreement(s)")
    # Country-based views are the partition/redaction boundary in all three AIDP
    # reference architectures, so every entity must belong to exactly one country.
    for label, tbl, key in (("customer", "CUSTOMER_DIMENSIONS", "CUSTOMER_ID"),
                            ("store", "STORE_DIMENSIONS", "STORE_ID"),
                            ("courier", "COURIER_DIMENSIONS", "COURIER_ID")):
        spanning = one(f"select count(*) from (select {key} from ORDER_DIMENSIONS "
                       f"where {key} is not null group by {key} "
                       f"having count(distinct COUNTRY_CODE) > 1)")[0]
        check(f"no {label} spans multiple countries", spanning == 0,
              f"{spanning} {label}(s) in >1 country")
    cour_mismatch = one("""
        select count(*) from ORDER_DIMENSIONS o
        join COURIER_DIMENSIONS k on o.COURIER_ID=k.COURIER_ID
        where o.COUNTRY_CODE <> k.COUNTRY_CODE""")[0]
    check("courier COUNTRY_CODE agrees with order", cour_mismatch == 0,
          f"{cour_mismatch} disagreement(s)")
    # repeat business: customers must have >1 order or the customer join is trivial
    repeat = one("select count(*) from (select CUSTOMER_ID from ORDER_DIMENSIONS "
                 "group by 1 having count(*) > 1)")[0]
    check("customers show repeat business", repeat > 0,
          f"{repeat} customer(s) with >1 order")

    print("\n-- the view is no longer vacuous (the original defect) --------------")
    n, orders, items, qty, amt, cust, store = one("""
        select count(*), count(distinct ORDER_ID), sum(ITEM_COUNT),
               sum(TOTAL_ITEM_QUANTITY), sum(ITEM_TOTAL_AMOUNT),
               count(CUSTOMER_NAME), count(STORE_NAME)
        from RAPPI_ORDER_360_VW""")
    check("view row count == 100", n == 100, f"{n} rows")
    check("view CUSTOMER_NAME resolves", cust == 100, f"{cust}/100 matched")
    check("view STORE_NAME resolves", store == 100, f"{store}/100 matched")
    check("view ITEM_COUNT is non-zero", (items or 0) > 0, f"sum(ITEM_COUNT)={items}")
    check("view TOTAL_ITEM_QUANTITY is non-zero", (qty or 0) > 0, f"sum={qty}")
    check("view ITEM_TOTAL_AMOUNT is non-zero",
          decimal.Decimal(amt or 0) > 0, f"sum={amt}")
    # the view's aggregate must reconcile to the source fact table
    view_amt, fact_amt = one("""
        select (select sum(ITEM_TOTAL_AMOUNT) from RAPPI_ORDER_360_VW),
               (select sum(LINE_TOTAL_AMOUNT) from ORDER_ITEMS_FACT)""")
    check("view ITEM_TOTAL_AMOUNT reconciles to ORDER_ITEMS_FACT",
          decimal.Decimal(view_amt) == decimal.Decimal(fact_amt),
          f"view={view_amt} fact={fact_amt}")

    conn.close()
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{'='*68}\n{len(results)-len(failed)}/{len(results)} checks passed")
    if failed:
        print("FAILED: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
