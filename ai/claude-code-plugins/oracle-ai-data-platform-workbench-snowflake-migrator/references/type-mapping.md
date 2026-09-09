# Snowflake → Spark/Delta type mapping

Precision and scale always come from `INFORMATION_SCHEMA.COLUMNS`. They are never
inferred from sampled data: `NUMBER` is Snowflake's default numeric type, and
getting its scale wrong does not raise — it silently changes values.

| Snowflake | Spark / Delta | Note |
|---|---|---|
| `NUMBER(p,s)`, `DECIMAL`, `NUMERIC` | `DECIMAL(p,s)` | Highest-consequence mapping. `NUMBER(38,0)` stays `DECIMAL(38,0)` — 38 digits do not fit in a `BIGINT` |
| `NUMBER` with no precision | **blocked** | Refuses to guess |
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ` | **Not `TIMESTAMP`.** Spark's bare `TIMESTAMP` is session-timezone-dependent |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP` | `TIMESTAMP` | Timezone semantics differ; recorded as a warning |
| `TEXT`, `VARCHAR(n)`, `CHAR` | `STRING` | Declared length is not enforced by Delta; recorded |
| `BOOLEAN`, `DATE`, `BINARY` | `BOOLEAN`, `DATE`, `BINARY` | Direct |
| `FLOAT`, `DOUBLE`, `REAL` | `DOUBLE` | |
| `TIME` | `STRING` | No direct Spark equivalent |
| `VARIANT`, `OBJECT`, `ARRAY` | **blocked** | Semi-structured; needs an explicit struct/map/array design |
| `GEOGRAPHY`, `GEOMETRY` | **blocked** | No target type |
| anything else | **blocked** | Unmapped types are never approximated |

## Properties dropped

Recorded in `omitted_properties`, never emitted: `CLUSTER BY` ·
`DATA_RETENTION_TIME_IN_DAYS` · `CHANGE_TRACKING` ·
`MAX_DATA_EXTENSION_TIME_IN_DAYS` · tags · masking policies · row-access policies.

## Constraints

Snowflake `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` are unenforced metadata — only
`NOT NULL` is enforced. Delta does not enforce them either. They are captured in
the inventory and reported, not emitted as DDL.

---

# View SQL portability

A view migrates only if its body contains no Snowflake-only construct. Detected
constructs **block** the view with the construct named — they are never rewritten
on a guess, because a view that ships a subtly wrong translation returns numbers.

Because Bronze mirrors the source 1:1, object references inside a view body need
no rewriting; the only question is dialect.

| Construct | Why it blocks |
|---|---|
| `QUALIFY` | No Spark equivalent; needs a subquery with `WHERE` on the window result |
| `LATERAL FLATTEN` / `FLATTEN(` | Maps to `explode` / `LATERAL VIEW`, but the mapping depends on the VARIANT shape |
| `IFF(` | Spark uses `IF()`; a rename is safe but is not applied automatically |
| `DECODE(` | Must become `CASE` |
| `NVL2(` | No Spark equivalent; must become `CASE` |
| `::` cast shorthand | Spark requires `CAST(x AS t)` |
| `LISTAGG(` | Becomes `collect_list` + `concat_ws` |
| `GENERATOR(` / `SEQ4(` / `SEQ8(` | Spark uses `range()` |
| `ARRAY_CONSTRUCT(` | Spark uses `array()` |
| `OBJECT_CONSTRUCT(` | Spark uses `named_struct()` or `map()` |
| `SYSTEM$…` | Snowflake-internal, no target |
| `AT(TIMESTAMP…)` / `BEFORE(` | Time Travel has no Delta equivalent in this form |
| `col:field` | VARIANT path access; needs an explicit struct design |
| `DATEADD` / `DATEDIFF` | Argument order and unit strings differ; needs a signature mapping, not a rename |
| `PIVOT` / `UNPIVOT` | Spark syntax differs materially |

`MAX_BY` / `MIN_BY` are **not** blocked — Spark supports them.

Also blocked, regardless of body: **secure views** (row-visibility rules have no
equivalent) and **materialized views** (rebuild as a table plus a refresh job).

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | Standard Catalog |
| Schema | Schema |
| Table | Table (managed Delta) |
| View | View |
| Warehouse | Spark compute cluster — see the compute proposal |
