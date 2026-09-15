# Zero-copy clone and table metadata on AIDP Delta

Demonstrates two Delta capabilities on AIDP, with explicit expected counts at each step:

| Topic | What the notebook shows |
|---|---|
| Zero-copy clone | `SHALLOW CLONE` creates a table referencing the source's data files; `DESCRIBE DETAIL` and `DESCRIBE HISTORY` confirm the zero-copy property and the `CLONE` commit; writing to the clone diverges copy-on-write while the source stays unchanged. |
| Structure without data | `CREATE TABLE ... LIKE` and `CREATE TABLE ... AS SELECT ... WHERE 1=0` both produce an empty table with the same columns. |
| Metadata in SQL | Attaching and reading back metadata at table scope (`TBLPROPERTIES`), column scope (column `COMMENT`), and via a registry table that scales across the lakehouse. |

## Running it

Attach to an AIDP cluster with Delta and run top to bottom. Set `CATALOG` in the configuration cell
to a catalog you can create schemas in; the notebook creates the scratch schema
`clone_metadata_demo` and drops it in the final cell.

Statements are executed directly rather than through a try/except wrapper, so anything unsupported on
your build fails at that cell instead of being silently recorded.

## Things worth knowing before you use clones

- **`VACUUM` can break a shallow clone.** The clone references the *source's* parquet files. Vacuuming
  the source may delete files the clone still needs, and Delta does not track that dependency. Avoid
  vacuuming a cloned source, or raise its retention window.
- **`DEEP CLONE` availability varies by build.** The open-source Delta 3.2 grammar rejects it (the OSS
  docs cover shallow clone only); Oracle's `3.2.0-oci` build may accept it. The notebook explains it
  but does not run it, so a build without it does not halt the run.
- **Some `DESCRIBE` forms are not subqueryable.** Spark 3.5 does not parse
  `SELECT ... FROM (DESCRIBE DETAIL t)` — that is a parser error, not a missing feature. Run
  `DESCRIBE DETAIL` / `DESCRIBE HISTORY` / `DESCRIBE` as top-level statements and project the result
  with the DataFrame API, as the notebook does.
- **`CREATE TABLE ... LIKE` takes at most a two-part name** in the Spark 3.5 grammar, which is why the
  notebook sets the current catalog and schema with `USE` instead of fully qualifying every reference.
- **`owner` is a reserved table property.** Use a distinct key such as `data_owner`.

## Metadata is attached, not enforced

Delta and Spark SQL store the metadata above but do not act on it: there is no policy engine that
reads a tag and redacts a column at query time. Enforcement is something you build — typically a view
that redacts whichever columns your registry marks sensitive. Writing that generically needs care
around per-type mask shapes and identifier handling, so it is deliberately left out of this sample
rather than sketched unsafely.

## Environment as tested

Spark 3.5.0 · Delta 3.2.0-oci-1.0.0 · `spark.sql.sources.default=delta` · catalog impl `hive`
