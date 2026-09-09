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
