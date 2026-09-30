# Snowflake → Spark/Delta type mapping

Precision and scale always come from `INFORMATION_SCHEMA.COLUMNS`. They are never
inferred from sampled data: `NUMBER` is Snowflake's default numeric type, and
getting its scale wrong does not raise — it silently changes values.

| Snowflake | Spark / Delta | Note |
|---|---|---|
| `NUMBER(p,s)`, `DECIMAL`, `NUMERIC` | `DECIMAL(p,s)` | Highest-consequence mapping. `NUMBER(38,0)` stays `DECIMAL(38,0)` — 38 digits do not fit in a `BIGINT` |
| `NUMBER` with no precision | **blocked** | Refuses to guess |
| `TIMESTAMP_NTZ` | `TIMESTAMP` by default; `TIMESTAMP_NTZ` with `--timestamp-ntz preserve` | The target refuses `TIMESTAMP_NTZ` at CREATE TABLE, so the default carries it as `TIMESTAMP` and records the timezone caveat on every affected column: Spark's `TIMESTAMP` is read through the session timezone, so keep sessions on UTC. With `preserve`, `ddl` halts (exit 3) |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP` | `TIMESTAMP` | Timezone semantics differ; recorded as a warning |
| `TEXT`, `VARCHAR(n)`, `CHAR` | `STRING` | Declared length is not enforced by Delta; recorded |
| `BOOLEAN`, `DATE`, `BINARY` | `BOOLEAN`, `DATE`, `BINARY` | Direct |
| `FLOAT`, `DOUBLE`, `REAL` | `DOUBLE` | |
| `TIME` | `STRING` | No Spark TIME type. **Warned**, not silent: ordering, comparison and time arithmetic become string operations |
| `VARIANT`, `OBJECT`, `ARRAY` | `STRING` (JSON text) by default; **blocked** with `--semi-structured block` | Semi-structured; warned on every affected column. A typed struct/map/array design is a separate decision |
| `GEOGRAPHY`, `GEOMETRY` | **blocked** by default; `STRING` with `--geospatial string` | No spatial target type |
| anything else | **blocked** | Unmapped types are never approximated |

## Table properties

No table property is emitted as DDL:

- Settings with an AIDP equivalent — `CLUSTER BY`,
  `DATA_RETENTION_TIME_IN_DAYS`, `CHANGE_TRACKING` — are listed per object in
  the DDL plan under *"Maintenance and layout — decisions, NOT applied"*, with
  the equivalent named ([maintenance-and-layout.md](maintenance-and-layout.md)).
- `MAX_DATA_EXTENSION_TIME_IN_DAYS`, which has no equivalent, is recorded in
  `omitted_properties`.
- Tags, masking policies and row-access policies are not carried;
  `snowmig security` reports them in `SECURITY.md`.

## Constraints

Snowflake `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` are unenforced metadata — only
`NOT NULL` is enforced. Delta does not enforce them either. They are captured in
the inventory and reported, not emitted as DDL.

---

# View SQL portability

A view migrates only if every Snowflake-only construct in its body has an exact
rewrite. A construct with one is **translated** and the rule id is recorded; a
construct without one **blocks** the view with the construct named — it is never
rewritten on a guess, because a subtly wrong translation still returns numbers,
just not the right ones. The authoritative list is `translate.RULES`, documented in
[dialect-translation.md](dialect-translation.md); the table below summarises it.

Object references inside a view body are identity in the default Bronze mirror
(`R40_VIEW_REFS_IDENTITY`) and are rewritten to the planned target names under
`--bronze-catalog-prefix` or a schema-style option (`R41_VIEW_REFS_REWRITTEN`).
The rewrite matches whole three-part names on code and identifier segments only
— never inside a string literal (that is data the view returns) or a comment —
a quoted part must match exactly, an unquoted one case-insensitively, and the
plan lists only the references that were actually rewritten.

One- and two-part names are qualified too, where Snowflake resolves them: a
bare `NAME` against the view's own schema, `SCHEMA.NAME` against its own
database. They are read only where a relation stands -- every item of a FROM
list (`from ORDERS o, CUSTOMERS c` has two) and after JOIN -- never the
column in `EXTRACT(YEAR FROM o.D)`, `TRIM(... FROM c.X)`, `SUBSTRING(s FROM
n)` or `a IS DISTINCT FROM c.Y`. A quoted part keeps its case (`"Orders"` is
not `ORDERS`). One that names nothing in the migration is left as written and
reported (`R42_VIEW_REFS_UNRESOLVED` for a bare name,
`R45_VIEW_REFS_UNRESOLVED` for every one- or two-part name).

A view's header column list (`create view V(CUSTOMER, TOTAL) as select
CUST_ID, SUM(AMT) ...`) renames the body's output columns, so it is carried
(`R44_VIEW_COLUMN_LIST`): `CREATE VIEW <fqn> (CUSTOMER, TOTAL) AS ...` in the
SQL, and `SELECT * FROM (<body>) AS named_columns(CUSTOMER, TOTAL)` as the
catalog API's viewText, which has no column-list field. A list that cannot be
read blocks the view.

| Construct | What happens |
|---|---|
| `IFF(` | **Translated** to `IF()` (`T01`) |
| `::` cast shorthand | **Translated** to `CAST(x AS <mapped type>)` through the type mapper (`T02`); an unmappable type blocks with the mapper's reason. `::TIME` blocks (the result depends on the operand's type); a `::TIMESTAMP` cast carries the timezone warning as an `R43` caveat |
| `ARRAY_CONSTRUCT(` | **Translated** to `array()` (`T03`) |
| `OBJECT_CONSTRUCT(` | **Translated** to `named_struct()` (`T04`) |
| `DATEADD(unit, n, col)` | **Translated** per unit (`T05`) when `n` is an integer literal or a column; any other form blocks. Exact for `DATE` operands only, and the plan says so |
| `LISTAGG(x, sep)` | **Translated** to `concat_ws(sep, collect_list(x))` (`T06`); `WITHIN GROUP` or a window `OVER (…)` blocks |
| `"quoted identifier"` | **Translated** to `` `backticked` `` (`T07`): Spark reads `"..."` as a string literal. Case is kept |
| `'it''s'` doubled quote in a literal | **Translated** to `'it\'s'` (`T08`): Spark reads `''` as two adjacent literals and concatenates them |
| `// note` line comment | **Translated** to `-- note` (`T21`): Spark has no `//` comment |
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
| Database | In the S1–S12 runbook: an INTERNAL target catalog for the migrated tables, plus an EXTERNAL catalog (source type SNOWFLAKE) registered over the live source. The stand-alone catalog commands register the EXTERNAL catalog by default and create a Standard catalog on explicit request |
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

## Semi-structured, geospatial and timestamp modes

| Flag | Config key | Default | The other mode |
|---|---|---|---|
| `--semi-structured` | `mapping.semi_structured` | `string`: carry the JSON as text, with a warning on every affected column | `block`: the table is blocked until a typed design exists |
| `--geospatial` | — | `block`: the table is blocked | `string`: carry the value as text, with no spatial type, index or predicate support |
| `--timestamp-ntz` | `mapping.timestamp_ntz` | `timestamp`: carry `TIMESTAMP_NTZ` as `TIMESTAMP`, with the timezone caveat recorded | `preserve`: keep `TIMESTAMP_NTZ`; `ddl` halts on it (exit 3) |

`--mapping-defaults off` (or `mapping.enabled: false` in the config) restores
the strict modes for a run — `VARIANT` blocks and `TIMESTAMP_NTZ` is preserved
— and an explicit flag always wins.

`--semi-structured` and `--geospatial` are **separate flags on purpose**.
Carrying JSON as text is not the same decision as carrying a geography as
text, and one switch for both would make a customer accept a call they were
not asked about.

Text defers the design rather than completing it. With JSON carried as text,
nothing on the target can address a field inside the value (`from_json` /
`get_json_object` read it), and a view using Snowflake path syntax is blocked
until a struct/map design is agreed. Tell the consumers of those tables.
