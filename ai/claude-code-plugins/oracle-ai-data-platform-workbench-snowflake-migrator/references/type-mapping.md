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
| `TIME` | `STRING` | No Spark TIME type. **Warned**, not silent: ordering, comparison and time arithmetic become string operations |
| `VARIANT`, `OBJECT`, `ARRAY` | **blocked** by default; `STRING` with `--semi-structured string` | Semi-structured; needs an explicit struct/map/array design |
| `GEOGRAPHY`, `GEOMETRY` | **blocked** by default; `STRING` with `--geospatial string` | No spatial target type |
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


## Integer columns become `DECIMAL(38,0)`

Every Snowflake integer alias — `INT`, `INTEGER`, `BIGINT`, `SMALLINT`,
`TINYINT`, `BYTEINT` — *is* `NUMBER(38,0)`, so `DECIMAL(38,0)` is the faithful
mapping and `BIGINT` would silently narrow the declared range. Fidelity was
chosen over familiarity.

Expect it to surprise people: Spark schemas normally show `BIGINT` for an ID
column, and downstream casts and joins will see `DECIMAL`. It is reported as an
informational **note**, not a warning, precisely so it does not inflate every
table's risk level — a warning on every integer column would drown the warnings
that matter.

## The two escape hatches

Default-deny is right for a type whose target shape is a design decision. But
default-deny with *no alternative* is not a usable tool: one `VARIANT` column
blocks its entire table, and a real Snowflake estate — order payloads, event
bodies, API responses — is full of them.

| Flag | Default | What the non-default does |
|---|---|---|
| `--semi-structured` | `block` | `string`: carry the JSON as text, with a warning on every affected column |
| `--geospatial` | `block` | `string`: carry the value as text, with no spatial type, index or predicate support |

They are **separate flags on purpose**. Deciding to carry JSON as text is not
the same decision as carrying a geography as text, and one switch for both would
force a customer to accept a call they were not asked about.

Neither hatch solves anything — both defer. With `--semi-structured string`
nothing on the target can address a field inside the value, and any query using
Snowflake path syntax stops working until a struct/map design is agreed. Say
that when you use it.
