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

A view migrates only if every Snowflake-only construct in its body has an exact
rewrite. A construct with one is **translated** and the rule id is recorded; a
construct without one **blocks** the view with the construct named — it is never
rewritten on a guess, because a view that ships a subtly wrong translation
returns numbers. The authoritative list is `translate.RULES`, documented in
[dialect-translation.md](dialect-translation.md); the table below summarises it.

Because Bronze mirrors the source 1:1, object references inside a view body need
no rewriting; the only question is dialect.

| Construct | What happens |
|---|---|
| `IFF(` | **Translated** to `IF()` (`T01`) |
| `::` cast shorthand | **Translated** to `CAST(x AS <mapped type>)` through the type mapper (`T02`); an unmappable type blocks with the mapper's reason |
| `ARRAY_CONSTRUCT(` | **Translated** to `array()` (`T03`) |
| `OBJECT_CONSTRUCT(` | **Translated** to `named_struct()` (`T04`) |
| `DATEADD(unit, n, col)` | **Translated** per unit (`T05`) when `n` is an integer literal or a column; any other form blocks. Exact for `DATE` operands only, and the plan says so |
| `LISTAGG(x, sep)` | **Translated** to `concat_ws(sep, collect_list(x))` (`T06`); `WITHIN GROUP` blocks |
| `"quoted identifier"` | **Translated** to `` `backticked` `` (`T07`): Spark reads `"..."` as a string literal. Case is kept |
| `'it''s'` doubled quote in a literal | **Translated** to `'it\'s'` (`T08`): Spark reads `''` as two adjacent literals and concatenates them |
| `$$...$$` dollar-quoted string | Blocks: Spark has no dollar quoting (`T09`) |
| `QUALIFY` | Blocks: no Spark equivalent; needs a subquery with `WHERE` on the window result |
| `LATERAL FLATTEN` / `FLATTEN(` | Blocks: maps to `explode` / `LATERAL VIEW`, but the mapping depends on the VARIANT shape |
| `GENERATOR(` / `SEQ4(` / `SEQ8(` | Blocks: Spark uses `range()`, and `SEQ4()` has no gapless equivalent |
| `PIVOT` / `UNPIVOT` | Blocks: Spark syntax differs materially |
| `SYSTEM$…` | Blocks: Snowflake-internal, no target |
| `AT(TIMESTAMP…)` / `BEFORE(` | Blocks: Time Travel has no Delta equivalent in this form |
| `col:field` | Blocks: VARIANT path access; needs an explicit struct design |
| `DECODE(` | Blocks: must become `CASE` |
| `NVL2(` | Blocks: no Spark equivalent; must become `CASE` |
| `DATEDIFF` / `TIMESTAMPDIFF` | Blocks: Snowflake counts unit-boundary crossings, Spark truncates, and `dd`/`yy`/`mm` are not Spark units |
| `TIMESTAMPADD` / `TIMEADD` | Blocks: `DATEADD` aliases whose operands are `TIMESTAMP`/`TIME` by construction, where the `DATEADD` rewrite is not exact |

`MAX_BY` / `MIN_BY` are **not** blocked — Spark supports them.

Also blocked, regardless of body: **secure views** (row-visibility rules have no
equivalent) and **materialized views** (rebuild as a table plus a refresh job).

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | EXTERNAL catalog (source type SNOWFLAKE) by default; Standard catalog on explicit request |
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
