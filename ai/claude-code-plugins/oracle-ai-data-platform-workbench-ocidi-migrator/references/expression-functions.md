# OCI-DI expression functions and their Spark SQL translation

Generated from `ocidi2aidp/compile/expressions.py` by `python -m ocidi2aidp.docs`;
`tests/test_expressions.py` fails if this file and the table drift apart.

How to read the **Translation** column:

| Value | Meaning |
|---|---|
| same | Emitted unchanged. OCI-DI runs data flows on Spark, and for these the DI function is the Spark function. |
| rename | Emitted as the Spark function named in the note. |
| rewrite | Rewritten (shape-dependent; the note says how). Rewrites that change semantics at the edges produce a review finding in the migration. |
| fallback | Never translated deterministically. The operator becomes an LLM work order. |

A function that is **not in this table** is never passed through on the
assumption that Spark has the same function: the operator becomes an LLM
work order. `DECODE` is why -- in OCI-DI it is Oracle's CASE shorthand, in
Spark it is a charset decoder.

Also rewritten, outside function calls:

- `OPERATOR.ENTITY.ATTR` / `OPERATOR.ATTR` / `ENTITY.ATTR` -> the column, aliased `l.`/`r.` inside joins and lookups.
- `$P` / `${P}` -> a `${P}` placeholder the notebook fills with a quoted SQL literal at run time.
- `SYS.TASK_START_TIME`, `SYS.LAST_LOAD_DATE`, ... -> runtime-cell values (`SYS.LAST_LOAD_DATE` comes from the watermark table).
- `SYSDATE`, `SYSTIMESTAMP` -> `current_timestamp()`.
- `CAST(x AS VARCHAR2(n))` / `NUMBER(p,s)` -> `STRING` / `DECIMAL(p,s)` (Spark 3 rejects CHAR/VARCHAR in a CAST).
- User-defined functions -> inlined at the call site.

| Function | Category | Translation | Note |
|---|---|---|---|
| `ABS` | numeric | same | Spark function of the same name |
| `ACOS` | numeric | same | Spark function of the same name |
| `ADD_MONTHS` | date | same | Spark function of the same name |
| `AGGREGATE` | higher_order | same | Spark function of the same name |
| `ANY_VALUE` | aggregate | same | Spark function of the same name |
| `APPROX_COUNT_DISTINCT` | aggregate | same | Spark function of the same name |
| `ARRAY` | array_map | same | Spark function of the same name |
| `ARRAYS_OVERLAP` | array_map | same | Spark function of the same name |
| `ARRAYS_ZIP` | array_map | same | Spark function of the same name |
| `ARRAY_CONTAINS` | array_map | same | Spark function of the same name |
| `ARRAY_DISTINCT` | array_map | same | Spark function of the same name |
| `ARRAY_EXCEPT` | array_map | same | Spark function of the same name |
| `ARRAY_INTERSECT` | array_map | same | Spark function of the same name |
| `ARRAY_JOIN` | array_map | same | Spark function of the same name |
| `ARRAY_MAX` | array_map | same | Spark function of the same name |
| `ARRAY_MIN` | array_map | same | Spark function of the same name |
| `ARRAY_POSITION` | array_map | same | Spark function of the same name |
| `ARRAY_REMOVE` | array_map | same | Spark function of the same name |
| `ARRAY_REPEAT` | array_map | same | Spark function of the same name |
| `ARRAY_SORT` | array_map | same | Spark function of the same name |
| `ARRAY_UNION` | array_map | same | Spark function of the same name |
| `ASCII` | string | same | Spark function of the same name |
| `ASIN` | numeric | same | Spark function of the same name |
| `ASSERT_TRUE` | conditional | same | Spark function of the same name |
| `ATAN` | numeric | same | Spark function of the same name |
| `ATAN2` | numeric | same | Spark function of the same name |
| `AVG` | aggregate | same | Spark function of the same name |
| `BASE64` | string | same | Spark function of the same name |
| `BIGINT` | conversion | same | Spark function of the same name |
| `BIN` | numeric | same | Spark function of the same name |
| `BINARY` | conversion | same | Spark function of the same name |
| `BIT_LENGTH` | string | same | Spark function of the same name |
| `BOOLEAN` | conversion | same | Spark function of the same name |
| `BOOL_AND` | aggregate | same | Spark function of the same name |
| `BOOL_OR` | aggregate | same | Spark function of the same name |
| `BROUND` | numeric | same | Spark function of the same name |
| `CARDINALITY` | array_map | same | Spark function of the same name |
| `CAST` | conversion | same | Spark function of the same name |
| `CBRT` | numeric | same | Spark function of the same name |
| `CEIL` | numeric | same | Spark function of the same name |
| `CEILING` | numeric | same | Spark function of the same name |
| `CHARACTER_LENGTH` | string | same | Spark function of the same name |
| `CHAR_LENGTH` | string | same | Spark function of the same name |
| `CHR` | string | same | Spark function of the same name |
| `COALESCE` | conditional | same | Spark function of the same name |
| `COLLECT_LIST` | aggregate | same | Spark function of the same name |
| `COLLECT_SET` | aggregate | same | Spark function of the same name |
| `CONCAT` | string | same | Spark function of the same name |
| `CONCAT_WS` | string | same | Spark function of the same name |
| `CONTAINS` | string | same | Spark function of the same name |
| `CONV` | numeric | same | Spark function of the same name |
| `CORR` | aggregate | same | Spark function of the same name |
| `COS` | numeric | same | Spark function of the same name |
| `COUNT` | aggregate | same | Spark function of the same name |
| `COVAR_POP` | aggregate | same | Spark function of the same name |
| `COVAR_SAMP` | aggregate | same | Spark function of the same name |
| `CRC32` | hash | same | Spark function of the same name |
| `CUME_DIST` | window | same | Spark function of the same name |
| `CURRENT_DATE` | date | same | Spark function of the same name |
| `CURRENT_TIMESTAMP` | date | same | Spark function of the same name |
| `CURRENT_TIMEZONE` | date | same | Spark function of the same name |
| `DATE` | conversion | same | Spark function of the same name |
| `DATEADD` | date | same | Spark function of the same name |
| `DATEDIFF` | date | same | Spark function of the same name |
| `DATE_ADD` | date | same | Spark function of the same name |
| `DATE_DIFF` | date | same | Spark function of the same name |
| `DATE_FORMAT` | date | same | Spark function of the same name |
| `DATE_PART` | date | same | Spark function of the same name |
| `DATE_SUB` | date | same | Spark function of the same name |
| `DATE_TRUNC` | date | same | Spark function of the same name |
| `DAY` | date | same | Spark function of the same name |
| `DAYOFMONTH` | date | same | Spark function of the same name |
| `DAYOFWEEK` | date | same | Spark function of the same name |
| `DAYOFYEAR` | date | same | Spark function of the same name |
| `DECODE` | conditional | rewrite | Oracle-style DECODE -> CASE WHEN x <=> s THEN r ... (Spark's decode() is a charset decoder) |
| `DEGREES` | numeric | same | Spark function of the same name |
| `DENSE_RANK` | window | same | Spark function of the same name |
| `DOUBLE` | conversion | same | Spark function of the same name |
| `E` | numeric | same | Spark function of the same name |
| `ELEMENT_AT` | array_map | same | Spark function of the same name |
| `ENCODE` | string | same | Spark function of the same name |
| `ENDSWITH` | string | same | Spark function of the same name |
| `EXISTS` | higher_order | same | Spark function of the same name |
| `EXP` | numeric | same | Spark function of the same name |
| `EXPLODE` | array_map | same | Spark function of the same name |
| `EXTRACT` | date | same | Spark function of the same name |
| `FACTORIAL` | numeric | same | Spark function of the same name |
| `FILTER` | higher_order | same | Spark function of the same name |
| `FIRST` | window | same | Spark function of the same name |
| `FIRST_VALUE` | window | same | Spark function of the same name |
| `FLATTEN` | array_map | same | Spark function of the same name |
| `FLOAT` | conversion | same | Spark function of the same name |
| `FLOOR` | numeric | same | Spark function of the same name |
| `FORALL` | higher_order | same | Spark function of the same name |
| `FORMAT_NUMBER` | string | same | Spark function of the same name |
| `FORMAT_STRING` | string | same | Spark function of the same name |
| `FROM_JSON` | json | same | Spark function of the same name |
| `FROM_UNIXTIME` | date | same | Spark function of the same name |
| `FROM_UTC_TIMESTAMP` | date | same | Spark function of the same name |
| `GET_JSON_OBJECT` | json | same | Spark function of the same name |
| `GREATEST` | numeric | same | Spark function of the same name |
| `HASH` | hash | same | Spark function of the same name |
| `HEX` | numeric | same | Spark function of the same name |
| `HOUR` | date | same | Spark function of the same name |
| `IF` | conditional | same | Spark function of the same name |
| `IFNULL` | conditional | same | Spark function of the same name |
| `INITCAP` | string | same | Spark function of the same name |
| `INSTR` | string | rewrite | two arguments -> instr(); position/occurrence -> fallback |
| `INT` | conversion | same | Spark function of the same name |
| `ISNAN` | conditional | same | Spark function of the same name |
| `ISNOTNULL` | conditional | same | Spark function of the same name |
| `ISNULL` | conditional | same | Spark function of the same name |
| `JSON_ARRAY_LENGTH` | json | same | Spark function of the same name |
| `JSON_OBJECT_KEYS` | json | same | Spark function of the same name |
| `JSON_TUPLE` | json | same | Spark function of the same name |
| `LAG` | window | same | Spark function of the same name |
| `LAST` | window | same | Spark function of the same name |
| `LAST_DAY` | date | same | Spark function of the same name |
| `LAST_VALUE` | window | same | Spark function of the same name |
| `LCASE` | string | same | Spark function of the same name |
| `LEAD` | window | same | Spark function of the same name |
| `LEAST` | numeric | same | Spark function of the same name |
| `LEFT` | string | same | Spark function of the same name |
| `LENGTH` | string | same | Spark function of the same name |
| `LEVENSHTEIN` | string | same | Spark function of the same name |
| `LISTAGG` | aggregate | fallback | Spark 3.5 has no LISTAGG; needs array_join(collect_list) with ordering |
| `LN` | numeric | same | Spark function of the same name |
| `LOCATE` | string | same | Spark function of the same name |
| `LOG` | numeric | same | Spark function of the same name |
| `LOG10` | numeric | same | Spark function of the same name |
| `LOG2` | numeric | same | Spark function of the same name |
| `LOWER` | string | same | Spark function of the same name |
| `LPAD` | string | same | Spark function of the same name |
| `LTRIM` | string | same | Spark function of the same name |
| `MAKE_DATE` | date | same | Spark function of the same name |
| `MAKE_TIMESTAMP` | date | same | Spark function of the same name |
| `MAP` | array_map | same | Spark function of the same name |
| `MAP_CONCAT` | array_map | same | Spark function of the same name |
| `MAP_ENTRIES` | array_map | same | Spark function of the same name |
| `MAP_FILTER` | higher_order | same | Spark function of the same name |
| `MAP_FROM_ARRAYS` | array_map | same | Spark function of the same name |
| `MAP_FROM_ENTRIES` | array_map | same | Spark function of the same name |
| `MAP_KEYS` | array_map | same | Spark function of the same name |
| `MAP_VALUES` | array_map | same | Spark function of the same name |
| `MAP_ZIP_WITH` | higher_order | same | Spark function of the same name |
| `MASK` | string | same | Spark function of the same name |
| `MAX` | aggregate | same | Spark function of the same name |
| `MD5` | hash | same | Spark function of the same name |
| `MEDIAN` | aggregate | same | Spark function of the same name |
| `MIN` | aggregate | same | Spark function of the same name |
| `MINUTE` | date | same | Spark function of the same name |
| `MOD` | numeric | same | Spark function of the same name |
| `MONOTONICALLY_INCREASING_ID` | generated | same | Spark function of the same name |
| `MONTH` | date | same | Spark function of the same name |
| `MONTHS_BETWEEN` | date | same | Spark function of the same name |
| `NAMED_STRUCT` | array_map | same | Spark function of the same name |
| `NANVL` | conditional | same | Spark function of the same name |
| `NEXT_DAY` | date | same | Spark function of the same name |
| `NOW` | date | same | Spark function of the same name |
| `NTH_VALUE` | window | same | Spark function of the same name |
| `NTILE` | window | same | Spark function of the same name |
| `NULLIF` | conditional | same | Spark function of the same name |
| `NUMERIC_ID` | generated | rewrite | -> monotonically_increasing_id() (review: values differ) |
| `NVL` | conditional | same | Spark function of the same name |
| `NVL2` | conditional | same | Spark function of the same name |
| `OCTET_LENGTH` | string | same | Spark function of the same name |
| `ORA_HASH` | hash | fallback | no Spark function returns Oracle's hash values |
| `OVERLAY` | string | same | Spark function of the same name |
| `PERCENTILE` | aggregate | same | Spark function of the same name |
| `PERCENTILE_APPROX` | aggregate | same | Spark function of the same name |
| `PERCENT_RANK` | window | same | Spark function of the same name |
| `PI` | numeric | same | Spark function of the same name |
| `PMOD` | numeric | same | Spark function of the same name |
| `POSITION` | string | same | Spark function of the same name |
| `POW` | numeric | same | Spark function of the same name |
| `POWER` | numeric | same | Spark function of the same name |
| `QUARTER` | date | same | Spark function of the same name |
| `RADIANS` | numeric | same | Spark function of the same name |
| `RAND` | numeric | same | Spark function of the same name |
| `RANDN` | numeric | same | Spark function of the same name |
| `RANK` | window | same | Spark function of the same name |
| `REDUCE` | higher_order | same | Spark function of the same name |
| `REGEXP_COUNT` | string | same | Spark function of the same name |
| `REGEXP_EXTRACT` | string | same | Spark function of the same name |
| `REGEXP_EXTRACT_ALL` | string | same | Spark function of the same name |
| `REGEXP_INSTR` | string | same | Spark function of the same name |
| `REGEXP_LIKE` | string | same | Spark function of the same name |
| `REGEXP_REPLACE` | string | same | Spark function of the same name |
| `REGEXP_SUBSTR` | string | same | Spark function of the same name |
| `REPEAT` | string | same | Spark function of the same name |
| `REPLACE` | string | same | Spark function of the same name |
| `REVERSE` | string | same | Spark function of the same name |
| `RIGHT` | string | same | Spark function of the same name |
| `ROUND` | numeric | same | Spark function of the same name |
| `ROWID` | generated | rewrite | -> monotonically_increasing_id() (review: values differ) |
| `ROW_NUMBER` | window | same | Spark function of the same name |
| `RPAD` | string | same | Spark function of the same name |
| `RTRIM` | string | same | Spark function of the same name |
| `SCHEMA_OF_JSON` | json | same | Spark function of the same name |
| `SECOND` | date | same | Spark function of the same name |
| `SEQUENCE` | array_map | same | Spark function of the same name |
| `SHA` | hash | same | Spark function of the same name |
| `SHA1` | hash | same | Spark function of the same name |
| `SHA2` | hash | same | Spark function of the same name |
| `SHIFTLEFT` | numeric | same | Spark function of the same name |
| `SHIFTRIGHT` | numeric | same | Spark function of the same name |
| `SHUFFLE` | array_map | same | Spark function of the same name |
| `SIGN` | numeric | same | Spark function of the same name |
| `SIGNUM` | numeric | same | Spark function of the same name |
| `SIN` | numeric | same | Spark function of the same name |
| `SIZE` | array_map | same | Spark function of the same name |
| `SLICE` | array_map | same | Spark function of the same name |
| `SMALLINT` | conversion | same | Spark function of the same name |
| `SORT_ARRAY` | array_map | same | Spark function of the same name |
| `SOUNDEX` | string | same | Spark function of the same name |
| `SPACE` | string | same | Spark function of the same name |
| `SPARK_PARTITION_ID` | generated | same | Spark function of the same name |
| `SPLIT` | string | same | Spark function of the same name |
| `SPLIT_PART` | string | same | Spark function of the same name |
| `SQRT` | numeric | same | Spark function of the same name |
| `STARTSWITH` | string | same | Spark function of the same name |
| `STDDEV` | aggregate | same | Spark function of the same name |
| `STDDEV_POP` | aggregate | same | Spark function of the same name |
| `STDDEV_SAMP` | aggregate | same | Spark function of the same name |
| `STRING` | conversion | same | Spark function of the same name |
| `STRUCT` | array_map | same | Spark function of the same name |
| `SUBSTR` | string | same | Spark function of the same name |
| `SUBSTRING` | string | same | Spark function of the same name |
| `SUBSTRING_INDEX` | string | same | Spark function of the same name |
| `SUM` | aggregate | same | Spark function of the same name |
| `SYS_GUID` | generated | rename | -> uuid() |
| `TAN` | numeric | same | Spark function of the same name |
| `TIMESTAMP` | conversion | same | Spark function of the same name |
| `TIMESTAMP_MILLIS` | date | same | Spark function of the same name |
| `TIMESTAMP_SECONDS` | date | same | Spark function of the same name |
| `TINYINT` | conversion | same | Spark function of the same name |
| `TO_CHAR` | conversion | rewrite | date format -> date_format(); number format -> to_char(); one argument -> CAST AS STRING; locale -> fallback |
| `TO_DATE` | conversion | same | to_date(str[, fmt]); pattern checked for Spark 3 differences |
| `TO_JSON` | json | same | Spark function of the same name |
| `TO_NUMBER` | conversion | rewrite | one argument -> CAST AS DECIMAL(38,10) (assumption reported); with format -> to_number() |
| `TO_TIMESTAMP` | conversion | same | to_timestamp(str[, fmt]); pattern checked |
| `TO_UNIX_TIMESTAMP` | date | same | Spark function of the same name |
| `TO_UTC_TIMESTAMP` | date | same | Spark function of the same name |
| `TRANSFORM` | higher_order | same | Spark function of the same name |
| `TRANSFORM_KEYS` | higher_order | same | Spark function of the same name |
| `TRANSFORM_VALUES` | higher_order | same | Spark function of the same name |
| `TRANSLATE` | string | same | Spark function of the same name |
| `TRIM` | string | same | Spark function of the same name |
| `TRUNC` | date | rewrite | date unit -> trunc() or date_trunc(); numeric truncation -> fallback |
| `TRY_ADD` | numeric | same | Spark function of the same name |
| `TRY_CAST` | conversion | same | Spark function of the same name |
| `TRY_DIVIDE` | numeric | same | Spark function of the same name |
| `TRY_ELEMENT_AT` | array_map | same | Spark function of the same name |
| `TRY_MULTIPLY` | numeric | same | Spark function of the same name |
| `TRY_SUBTRACT` | numeric | same | Spark function of the same name |
| `UCASE` | string | same | Spark function of the same name |
| `UNBASE64` | string | same | Spark function of the same name |
| `UNHEX` | numeric | same | Spark function of the same name |
| `UNIX_MILLIS` | date | same | Spark function of the same name |
| `UNIX_SECONDS` | date | same | Spark function of the same name |
| `UNIX_TIMESTAMP` | date | same | Spark function of the same name |
| `UPPER` | string | same | Spark function of the same name |
| `UUID` | generated | same | Spark function of the same name |
| `VARIANCE` | aggregate | same | Spark function of the same name |
| `VAR_POP` | aggregate | same | Spark function of the same name |
| `VAR_SAMP` | aggregate | same | Spark function of the same name |
| `WEEKDAY` | date | same | Spark function of the same name |
| `WEEKOFYEAR` | date | same | Spark function of the same name |
| `WIDTH_BUCKET` | numeric | same | Spark function of the same name |
| `XXHASH64` | hash | same | Spark function of the same name |
| `YEAR` | date | same | Spark function of the same name |
| `ZIP_WITH` | higher_order | same | Spark function of the same name |
