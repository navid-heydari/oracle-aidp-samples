# Snowflake → AIDP dialect translation

`engine/snowflake_source/dialect/translate.py`. **Skeleton**: the framework is
real, and coverage is reported honestly rather than implied.

Two rule states:

- **implemented** — a provably *exact* rewrite, applied automatically
- **declared** — recognised, described, and deliberately *not* rewritten

The governing rule: **never approximate.** A rule either produces SQL that means
the same thing, or it reports what a real implementation needs and leaves the
input untouched. SQL that "mostly works" returns numbers, and wrong numbers are
worse than a blocked object.

## Implemented (6)

| Rule | Snowflake | AIDP / Spark |
|---|---|---|
| `T01_IFF` | `IFF(c, a, b)` | `IF(c, a, b)` |
| `T02_CAST_SHORTHAND` | `x::TYPE` | `CAST(x AS <mapped type>)` — the type goes through the same Snowflake → Spark table as column DDL (`FLOAT` → `DOUBLE`, `TEXT`/`VARCHAR(n)` → `STRING`, `NUMBER(p,s)` → `DECIMAL(p,s)`, bare `NUMBER`/`DECIMAL`/`INT` → `DECIMAL(38,0)`, Snowflake's documented default). `VARIANT`/`OBJECT`/`ARRAY`/`GEOGRAPHY` and unknown spellings are refused with the mapper's reason; `TIMESTAMP`/`TIME` carry the mapper's warning. Bare column or literal operand only; refused when the left operand is an expression, because a token rule cannot find its boundary |
| `T03_ARRAY_CONSTRUCT` | `ARRAY_CONSTRUCT(…)` | `array(…)` |
| `T04_OBJECT_CONSTRUCT` | `OBJECT_CONSTRUCT('k', v)` | `named_struct('k', v)` |
| `T05_DATEADD` | `DATEADD(unit, n, col)` | `date_add` / `add_months` / `+ INTERVAL`, chosen by unit. Argument order differs and the unit decides the function, so a rename would be wrong. Only the exact forms are rewritten: `n` an integer literal or a column (a column is parenthesised where it meets the `* 7` / `* 12` multiplier; for hour/minute/second only a literal, because Spark's `INTERVAL` takes a constant). An expression amount, an unrecognised unit, a quoted unit or a nested call such as `CURRENT_DATE()` is refused, never guessed or carried over. **Caveat, recorded on every application:** exact for `DATE` operands only — Spark `date_add`/`add_months` return `DATE`, so a `TIMESTAMP` operand is truncated to `DATE`; the DDL plan says so instead of calling the view exact |
| `T06_LISTAGG` | `LISTAGG(x, sep)` | `concat_ws(sep, collect_list(x))`. Refused with `WITHIN GROUP (ORDER BY …)`, because `collect_list` does not guarantee ordering and the semantics would be lost silently |

## Declared — recognised, not rewritten (11)

| Rule | Why a substitution is not safe |
|---|---|
| `T10_QUALIFY` | Needs statement restructuring: project the window expression into a subquery and move the predicate to an outer `WHERE`. Changes the select list |
| `T11_LATERAL_FLATTEN` | Target shape depends on the VARIANT structure and on which of value/index/key is read |
| `T12_GENERATOR` | Replaces a FROM-clause table function, and `SEQ4()` has no gapless Spark equivalent, so row identity would change |
| `T13_PIVOT` | Spark's syntax and aggregate placement differ; a mechanical rewrite risks changing the grouping |
| `T14_SYSTEM_FUNCTION` | Snowflake-internal, no equivalent. Each needs an explicit decision |
| `T15_TIME_TRAVEL` | Delta uses `VERSION AS OF` / `TIMESTAMP AS OF` with different retention; not interchangeable |
| `T16_VARIANT_PATH` | Needs the VARIANT column given a concrete struct type first |
| `T17_DECODE` | Variable argument count with a positional default; needs the argument list parsed |
| `T18_NVL2` | Simple in shape, but operands may contain commas, so it needs argument parsing |
| `T19_DATEDIFF` | `DATEDIFF` / `TIMESTAMPDIFF`: Snowflake counts unit-boundary crossings while Spark's `datediff` / `months_between` truncate, and the Snowflake unit abbreviations (`dd`, `yy`, `mm`, `hh`, `mi`, `ss`) are not Spark datetime units. Needs a per-unit expression and a decision for sub-day units |
| `T20_TIMESTAMPADD` | `TIMESTAMPADD` / `TIMEADD`: aliases of `DATEADD`, but their operands are `TIMESTAMP` or `TIME` by construction, exactly where the `DATEADD` rewrite is not exact. Refused until decided |

## Where it is used

`build_create_view` runs the translator. A view whose body uses only
**implemented** constructs is translated and migrates, recording each rule id. A
view touching any **declared** construct is blocked, naming it. A mixed view is
blocked — partial translation is never emitted.

## Adding a rule

Append to `RULES` with `status="implemented"` and a `translate` callable that
returns `(sql, None)` on success or `(original_sql, reason)` when it cannot act
safely. Or `status="declared"`, `translate=None`, and a `detail` explaining what
a real implementation would have to do. `coverage()` reports the split.
