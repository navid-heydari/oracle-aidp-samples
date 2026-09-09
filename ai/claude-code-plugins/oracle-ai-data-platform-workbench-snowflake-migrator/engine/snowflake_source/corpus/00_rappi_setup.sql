-- =====================================================================
-- 00_rappi_setup.sql  --  Rappi-shaped Snowflake test corpus
-- Target: TEST_DB_20260908_1529.PUBLIC   Account: npxbexe-op03637 (AWS_US_EAST_2)
-- Revised 2026-09-09. Supersedes the version in the Snowflake workspace.
--
-- WHY THIS REVISION
--   The original left CUSTOMER_DIMENSIONS, STORE_DIMENSIONS and ORDER_ITEMS_FACT
--   empty, so all three LEFT JOINs in RAPPI_ORDER_360_VW matched nothing. The view
--   returned 100 rows of NULLs and zeros and therefore could NOT detect a wrong join
--   key, a wrong join type, a dropped join or a broken GROUP BY -- the
--   "right-shaped but wrong-answer" failure class. It also declared no PK/FK at all,
--   and COURIER_ID pointed at no table.
--
-- TYPE SURFACE THIS CORPUS EXERCISES (for the migrator's type mapper)
--   NUMBER(38,0) · NUMBER(18,2) · NUMBER(10,2) · NUMBER(5,2) · TEXT(2|3|30|50|100|255)
--   · TIMESTAMP_NTZ x11 · BOOLEAN · DATE.
--   The trap is TIMESTAMP_NTZ: Spark's default TIMESTAMP is session-timezone-dependent,
--   so it must map to Spark TIMESTAMP_NTZ, NOT TIMESTAMP. 11 of 41 columns on
--   ORDER_DIMENSIONS are affected.
--   Also: currency is a PER-ROW attribute (6 countries, 6 currencies) with NO FX column
--   anywhere, so any cross-country monetary aggregate is summing six currencies.
--
-- DESIGN DECISIONS
--   1. ORDER_DIMENSIONS rows are PRESERVED, never regenerated. The dimensions are
--      DERIVED FROM the 100 existing orders, so FK coverage is 100% by construction
--      rather than by coincidence.
--   2. Referential integrity is COMPLETE for every declared FK. LEFT JOIN semantics
--      are exercised through ORDER_ITEMS_FACT fan-out instead: DELIVERED orders get
--      1..5 items, CANCELLED orders get none. Legal under the FK (items -> orders),
--      and it makes the LEFT JOIN + COALESCE in the view actually mean something.
--   3. Two exact business invariants are built in, so reconciliation can assert
--      values and not just row counts:
--        I1  SUM(items.LINE_TOTAL_AMOUNT) per order = orders.PRODUCT_SUBTOTAL   (exact)
--        I2  item.UNIT_PRICE * QUANTITY - DISCOUNT_AMOUNT = LINE_TOTAL_AMOUNT   (exact)
--      I2 holds because UNIT_PRICE rounds UP and DISCOUNT absorbs the difference.
--   4. Deterministic: every value derives from ORDER_ID / CUSTOMER_ID / STORE_ID via
--      MOD, so re-running reproduces the corpus byte-for-byte. No RANDOM().
--
-- NOTE ON SNOWFLAKE CONSTRAINTS: only NOT NULL is enforced. PRIMARY KEY / FOREIGN KEY
-- / UNIQUE are metadata only. They are declared here because the migrator must extract
-- and translate that metadata -- and because Delta does not enforce them either, so a
-- migration cannot rely on the engine to catch a violation.
--
-- Idempotent. Re-runnable. Touches ONLY the derived tables.
-- =====================================================================

USE DATABASE TEST_DB_20260908_1529;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ---------------------------------------------------------------------
-- 1. COURIER_DIMENSIONS -- did not exist; ORDER_DIMENSIONS.COURIER_ID was a
--    dangling reference to nothing.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS COURIER_DIMENSIONS (
    COURIER_ID          NUMBER(38,0) NOT NULL,
    COURIER_EXTERNAL_ID VARCHAR(100),
    COURIER_NAME        VARCHAR(150),
    COUNTRY_CODE        VARCHAR(2),
    CITY_NAME           VARCHAR(100),
    VEHICLE_TYPE        VARCHAR(30),
    IS_ACTIVE           BOOLEAN,
    RECORD_CREATED_AT   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT   TIMESTAMP_NTZ(9)
);

-- ---------------------------------------------------------------------
-- 1b. Bind CUSTOMER_ID and COURIER_ID to a single country.
--
--     DEFECT FOUND 2026-09-09: in the original data all 35 customers and all 40
--     couriers appeared in up to 3 different countries, while stores were correctly
--     country-bound (0 multi-country). The generator assigned CUSTOMER_ID and
--     COURIER_ID independently of COUNTRY_CODE.
--
--     WHY IT MATTERS: a delivery customer belongs to one country and a courier is
--     physically in one city. More importantly, all three AIDP reference
--     architectures use COUNTRY-BASED VIEWS as the partition and redaction boundary.
--     An entity spanning countries makes country-scoped row-level security ambiguous,
--     would duplicate or mis-route rows in a per-country ADW/ALH split, and lets a
--     cross-country aggregate silently mix six currencies (there is no FX column).
--
--     FIX: reassign both FKs from a country-scoped id block. The per-country offset
--     modulus MUST be coprime to 6: COUNTRY_CODE is itself a function of
--     MOD(ORDER_ID, 6), so a modulus of 6 collapses to ONE customer per country.
--     5 customers/country and 7 couriers/country give repeat business. Deterministic, so
--     re-running is a no-op. This is the ONLY statement that writes to
--     ORDER_DIMENSIONS, and it touches only these two FK columns.
--     Original ranges before the fix: CUSTOMER_ID 9000000-9000034 (35 distinct),
--     COURIER_ID 70000-70039 (40 distinct).
-- ---------------------------------------------------------------------
UPDATE ORDER_DIMENSIONS
SET CUSTOMER_ID = 9000000
                + CASE COUNTRY_CODE WHEN 'AR' THEN 0 WHEN 'BR' THEN 1 WHEN 'CL' THEN 2
                                    WHEN 'CO' THEN 3 WHEN 'MX' THEN 4 WHEN 'PE' THEN 5
                                    ELSE 9 END * 100
                + MOD(ORDER_ID, 5),   -- 5 is coprime to the country period of 6
    COURIER_ID  = 70000
                + CASE COUNTRY_CODE WHEN 'AR' THEN 0 WHEN 'BR' THEN 1 WHEN 'CL' THEN 2
                                    WHEN 'CO' THEN 3 WHEN 'MX' THEN 4 WHEN 'PE' THEN 5
                                    ELSE 9 END * 100
                + MOD(ORDER_ID, 7)
WHERE COUNTRY_CODE IS NOT NULL;

-- ---------------------------------------------------------------------
-- 2. Reload the derived tables. ORDER_DIMENSIONS is deliberately absent here.
-- ---------------------------------------------------------------------
TRUNCATE TABLE CUSTOMER_DIMENSIONS;
TRUNCATE TABLE STORE_DIMENSIONS;
TRUNCATE TABLE COURIER_DIMENSIONS;
TRUNCATE TABLE ORDER_ITEMS_FACT;

-- ---------------------------------------------------------------------
-- 3. CUSTOMER_DIMENSIONS -- one row per distinct CUSTOMER_ID in the orders.
--    Attributes taken from that customer's most recent order via MAX_BY, so the
--    denormalised values on ORDER_DIMENSIONS and the dimension row agree.
-- ---------------------------------------------------------------------
INSERT INTO CUSTOMER_DIMENSIONS
    (CUSTOMER_ID, CUSTOMER_EXTERNAL_ID, CUSTOMER_NAME, COUNTRY_CODE, CITY_NAME,
     SIGNUP_DATE, CUSTOMER_SEGMENT, IS_PRIME_MEMBER, CRM_LIFECYCLE_STAGE,
     RECORD_CREATED_AT, RECORD_UPDATED_AT)
SELECT
    o.CUSTOMER_ID,
    'CUST-' || TO_VARCHAR(o.CUSTOMER_ID),
    -- Name off a DENSE index, not off CUSTOMER_ID: the country block offset is a
    -- multiple of 100, so MOD(CUSTOMER_ID, 10) only ever yields 5 values and the
    -- names collapse to 2 distinct pairs -- which reads as a broken join on review.
    GET(ARRAY_CONSTRUCT('Ana','Bruno','Carla','Diego','Elena',
                        'Felipe','Gabriela','Hugo','Isabela','Javier'),
        MOD(FLOOR(MOD(o.CUSTOMER_ID, 10000) / 100) * 5
            + MOD(o.CUSTOMER_ID, 100), 10))::STRING
      || ' ' ||
    GET(ARRAY_CONSTRUCT('Silva','Gomez','Rojas','Martinez','Pereira',
                        'Castro','Vargas','Nunes'),
        MOD(FLOOR(MOD(o.CUSTOMER_ID, 10000) / 100) * 5
            + MOD(o.CUSTOMER_ID, 100), 8))::STRING,
    MAX_BY(o.COUNTRY_CODE, o.CREATED_AT),
    MAX_BY(o.CITY_NAME,    o.CREATED_AT),
    -- DATEADD(unit, n, col) -- Snowflake's argument order, a documented gotcha
    DATEADD(day, -MOD(o.CUSTOMER_ID, 900), MIN(o.CREATED_AT)::DATE),
    MAX_BY(o.CUSTOMER_SEGMENT, o.CREATED_AT),
    MAX_BY(o.IS_PRIME_MEMBER,  o.CREATED_AT),
    CASE
        WHEN COUNT(*) >= 4 THEN 'ACTIVE_LOYAL'
        WHEN COUNT(*) >= 2 THEN 'ACTIVE'
        ELSE 'NEW'
    END,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM ORDER_DIMENSIONS o
WHERE o.CUSTOMER_ID IS NOT NULL
GROUP BY o.CUSTOMER_ID;

-- ---------------------------------------------------------------------
-- 4. STORE_DIMENSIONS -- STORE_NAME already exists denormalised on the order.
--    PARTNER_TIER is a real business rule over observed order volume.
-- ---------------------------------------------------------------------
INSERT INTO STORE_DIMENSIONS
    (STORE_ID, STORE_EXTERNAL_ID, STORE_NAME, COUNTRY_CODE, CITY_NAME, ZONE_NAME,
     VERTICAL, SUBVERTICAL, PARTNER_TIER, IS_ACTIVE,
     RECORD_CREATED_AT, RECORD_UPDATED_AT)
SELECT
    o.STORE_ID,
    'STORE-' || TO_VARCHAR(o.STORE_ID),
    COALESCE(MAX_BY(o.STORE_NAME, o.CREATED_AT),
             'Store ' || TO_VARCHAR(o.STORE_ID)),
    MAX_BY(o.COUNTRY_CODE, o.CREATED_AT),
    MAX_BY(o.CITY_NAME,    o.CREATED_AT),
    MAX_BY(o.ZONE_NAME,    o.CREATED_AT),
    MAX_BY(o.VERTICAL,     o.CREATED_AT),
    MAX_BY(o.SUBVERTICAL,  o.CREATED_AT),
    CASE
        WHEN COUNT(*) >= 8 THEN 'PLATINUM'
        WHEN COUNT(*) >= 5 THEN 'GOLD'
        WHEN COUNT(*) >= 3 THEN 'SILVER'
        ELSE 'BRONZE'
    END,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM ORDER_DIMENSIONS o
WHERE o.STORE_ID IS NOT NULL
GROUP BY o.STORE_ID;

-- ---------------------------------------------------------------------
-- 5. COURIER_DIMENSIONS
-- ---------------------------------------------------------------------
INSERT INTO COURIER_DIMENSIONS
    (COURIER_ID, COURIER_EXTERNAL_ID, COURIER_NAME, COUNTRY_CODE, CITY_NAME,
     VEHICLE_TYPE, IS_ACTIVE, RECORD_CREATED_AT, RECORD_UPDATED_AT)
SELECT
    o.COURIER_ID,
    'COUR-' || TO_VARCHAR(o.COURIER_ID),
    'Courier ' || TO_VARCHAR(o.COURIER_ID - 70000 + 1),
    MAX_BY(o.COUNTRY_CODE, o.CREATED_AT),
    MAX_BY(o.CITY_NAME,    o.CREATED_AT),
    GET(ARRAY_CONSTRUCT('MOTORCYCLE','BICYCLE','CAR','WALKER'),
        MOD(o.COURIER_ID, 4))::STRING,
    MOD(o.COURIER_ID, 11) <> 0,          -- ~9% inactive, deliberately
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM ORDER_DIMENSIONS o
WHERE o.COURIER_ID IS NOT NULL
GROUP BY o.COURIER_ID;

-- ---------------------------------------------------------------------
-- 6. ORDER_ITEMS_FACT -- the fan-out that makes the view's LEFT JOIN meaningful.
--    DELIVERED -> 1..5 items;  CANCELLED -> 0 items.
--    I1: the last item absorbs the rounding remainder so the per-order sum equals
--        PRODUCT_SUBTOTAL exactly.
--    I2: UNIT_PRICE rounds UP and DISCOUNT_AMOUNT absorbs the difference, so
--        UNIT_PRICE * QUANTITY - DISCOUNT_AMOUNT = LINE_TOTAL_AMOUNT exactly.
-- ---------------------------------------------------------------------
INSERT INTO ORDER_ITEMS_FACT
    (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY,
     QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, LINE_TOTAL_AMOUNT, ITEM_STATUS, CREATED_AT)
WITH seq AS (
    SELECT SEQ4() AS i FROM TABLE(GENERATOR(ROWCOUNT => 5))
),
ord AS (
    SELECT
        ORDER_ID,
        COALESCE(PRODUCT_SUBTOTAL, 0)          AS SUBTOTAL,
        1 + MOD(ORDER_ID, 5)                   AS ITEM_N,
        CREATED_AT,
        VERTICAL
    FROM ORDER_DIMENSIONS
    WHERE ORDER_STATUS = 'DELIVERED'
),
line AS (
    SELECT
        o.ORDER_ID, o.ITEM_N, o.CREATED_AT, o.VERTICAL, s.i,
        ROUND(o.SUBTOTAL / o.ITEM_N, 2)        AS EVEN_SHARE,
        CASE WHEN s.i = o.ITEM_N - 1
             THEN o.SUBTOTAL - ROUND(o.SUBTOTAL / o.ITEM_N, 2) * (o.ITEM_N - 1)
             ELSE ROUND(o.SUBTOTAL / o.ITEM_N, 2)
        END                                    AS LINE_TOTAL,
        1 + MOD(o.ORDER_ID + s.i, 3)           AS QTY
    FROM ord o
    JOIN seq s ON s.i < o.ITEM_N
)
SELECT
    l.ORDER_ID * 100 + l.i                     AS ORDER_ITEM_ID,
    l.ORDER_ID,
    800000 + MOD(l.ORDER_ID * 7 + l.i, 500)    AS PRODUCT_ID,
    'Product ' || TO_VARCHAR(800000 + MOD(l.ORDER_ID * 7 + l.i, 500)),
    GET(ARRAY_CONSTRUCT('GROCERY','PREPARED_FOOD','PHARMACY','ELECTRONICS',
                        'BEVERAGES','HOUSEHOLD'),
        MOD(l.ORDER_ID + l.i, 6))::STRING      AS PRODUCT_CATEGORY,
    l.QTY,
    CEIL(l.LINE_TOTAL / l.QTY * 100) / 100     AS UNIT_PRICE,
    (CEIL(l.LINE_TOTAL / l.QTY * 100) / 100) * l.QTY - l.LINE_TOTAL
                                               AS DISCOUNT_AMOUNT,
    l.LINE_TOTAL                               AS LINE_TOTAL_AMOUNT,
    'FULFILLED'                                AS ITEM_STATUS,
    l.CREATED_AT
FROM line l;

-- ---------------------------------------------------------------------
-- 7. PK / FK / UNIQUE declarations. Metadata only in Snowflake -- see header.
-- ---------------------------------------------------------------------
ALTER TABLE CUSTOMER_DIMENSIONS ADD CONSTRAINT PK_CUSTOMER_DIMENSIONS PRIMARY KEY (CUSTOMER_ID);
ALTER TABLE STORE_DIMENSIONS    ADD CONSTRAINT PK_STORE_DIMENSIONS    PRIMARY KEY (STORE_ID);
ALTER TABLE COURIER_DIMENSIONS  ADD CONSTRAINT PK_COURIER_DIMENSIONS  PRIMARY KEY (COURIER_ID);
ALTER TABLE ORDER_DIMENSIONS    ADD CONSTRAINT PK_ORDER_DIMENSIONS    PRIMARY KEY (ORDER_ID);
ALTER TABLE ORDER_ITEMS_FACT    ADD CONSTRAINT PK_ORDER_ITEMS_FACT    PRIMARY KEY (ORDER_ITEM_ID);

ALTER TABLE ORDER_DIMENSIONS ADD CONSTRAINT FK_ORDER_CUSTOMER
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER_DIMENSIONS (CUSTOMER_ID);
ALTER TABLE ORDER_DIMENSIONS ADD CONSTRAINT FK_ORDER_STORE
    FOREIGN KEY (STORE_ID)    REFERENCES STORE_DIMENSIONS (STORE_ID);
ALTER TABLE ORDER_DIMENSIONS ADD CONSTRAINT FK_ORDER_COURIER
    FOREIGN KEY (COURIER_ID)  REFERENCES COURIER_DIMENSIONS (COURIER_ID);
ALTER TABLE ORDER_ITEMS_FACT ADD CONSTRAINT FK_ITEM_ORDER
    FOREIGN KEY (ORDER_ID)    REFERENCES ORDER_DIMENSIONS (ORDER_ID);

ALTER TABLE ORDER_DIMENSIONS    ADD CONSTRAINT UK_ORDER_EXTERNAL_ID    UNIQUE (ORDER_EXTERNAL_ID);
ALTER TABLE CUSTOMER_DIMENSIONS ADD CONSTRAINT UK_CUSTOMER_EXTERNAL_ID UNIQUE (CUSTOMER_EXTERNAL_ID);
ALTER TABLE STORE_DIMENSIONS    ADD CONSTRAINT UK_STORE_EXTERNAL_ID    UNIQUE (STORE_EXTERNAL_ID);
