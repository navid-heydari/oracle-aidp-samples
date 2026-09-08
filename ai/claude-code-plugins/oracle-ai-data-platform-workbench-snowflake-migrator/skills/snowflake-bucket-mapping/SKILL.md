---
name: snowflake-bucket-mapping
description: SCAFFOLD - NOT YET PORTED TO SNOWFLAKE, do not invoke for Snowflake work. (Original Databricks description follows.) Configure the s3:// → oci:// bucket / namespace mapping the migrator uses to rewrite paths during notebook + catalog migration. Use when (a) the user has external tables / files at s3:// paths that need to land on OCI Object Storage, OR (b) check_data_availability reports "S3 bucket X not found in OCI bucket mapping", OR (c) DDL rewriter logs a missing-bucket warning.
---

> ⚠️ **SCAFFOLD — CONTENT NOT YET PORTED.** This file was forked verbatim from the
> Databricks→AIDP migrator and renamed. Its instructions still describe **Databricks**
> sources (Unity Catalog, `dbutils`, `%run`, notebook cells). Do **not** follow them for a
> Snowflake estate. See `PORTING-STATUS.md` at the plugin root for this file’s
> keep / adapt / rewrite disposition and its owner.
# `snowflake-bucket-mapping` — wire up `s3://` ↔ `oci://`

Several places in the migrator look up `s3://<bucket>/<path>` and rewrite to `oci://<bucket>@<namespace>/<path>`:
- `snowflake-migrate-catalog` rewrites external-table `LOCATION` clauses.
- `snowflake-migrate-job` Pass-1 rewrites `spark.read.parquet("s3://...")` literals in notebook cells.
- `snowflake-check-data` probes paths via the same translation.

All consult the same bucket-mapping config.

## When to use

- Setting up the migrator on a new workstation / new tenancy combo.
- Any time a tool reports `S3 bucket "<name>" not found in OCI bucket mapping. Known buckets: [...]`.
- After provisioning a new OCI Object Storage bucket that mirrors a Databricks-side S3 bucket.

## The config file

The migrator loads bucket mappings via the `load_bucket_mapping()` helper. The customer supplies a JSON file with this shape (file path is configurable via `--bucket-mapping`):

```json
{
  "buckets": {
    "<s3-bucket-name>": {
      "oci_bucket": "<oci-bucket-name>",
      "oci_namespace": "<oci-namespace>",
      "notes": "optional human note"
    },
    "<another-s3-bucket>": {
      "oci_bucket": "<another-oci-bucket>",
      "oci_namespace": "<oci-namespace>"
    }
  },
  "default_namespace": "<oci-namespace>",
  "default_region": "<oci-region>"
}
```

| Field | Meaning |
|---|---|
| `buckets.<s3-name>.oci_bucket` | Target OCI Object Storage bucket. |
| `buckets.<s3-name>.oci_namespace` | OCI tenancy namespace (NOT the DataLake namespace — these can differ). |
| `default_namespace` | Used when a path references an `oci://` URL without an explicit `@<ns>`. |
| `default_region` | Used to construct the OCI client. |

Save the file to a path the user controls (gitignored — it contains tenancy-specific identifiers). Pass via `--bucket-mapping <path>` to every migrator entrypoint.

## Building the mapping for a new tenancy

If the user is doing this for the first time:

1. **List the source S3 buckets referenced in the Databricks workspace.** Quick way:
   ```bash
   # On the migrator repo:
   grep -roE 's3://[a-z0-9.-]+' <source-databricks-checkout>/ | sort -u
   ```
   Or use the migrator's prep helper if available.

2. **For each, identify the target OCI bucket + namespace.** Either:
   - The user provisions matching OCI buckets (recommended for big migrations — preserves bucket names).
   - The user routes everything into a single shared OCI bucket with prefix isolation.

3. **Find the OCI namespace.** Each OCI tenancy has ONE namespace per region — find it via:
   ```bash
   oci os ns get --profile <profile>
   # returns: {"data": "<your-namespace>"}
   ```

4. **Write the JSON** and place it at `config/bucket_mapping.json` (or wherever your team stores secrets). Make sure it's gitignored.

5. **Test the mapping** by re-running [`snowflake-check-data`](../snowflake-check-data/SKILL.md) with `--bucket-mapping <path>`. Any `MISSING` entries with `s3://...` paths now resolve to `oci://...` and probe correctly.

## When the mapping should fail FAST (vs warn)

Behavior contract:
- `migrate_catalog.py` REJECTS unknown buckets when `--catalog-manifest` is explicit (data-correctness gate).
- `job_migrate.py` Pass-1 surfaces unknown buckets in the cell-fix Claude context so the model can either route to a synthetic stub OR ask the user.
- `check_data_availability.py` reports unknown-bucket as a hard `MISSING` row.

If you see WARNINGS but the migration continues, that's usually safe — the rewriter passed the path through unchanged. Confirm the consumer notebook either no longer reads that path OR has been adapted.

## Common mistakes

| Mistake | Fix |
|---|---|
| Confusing DataLake namespace vs OCI tenancy namespace | These can differ. `oci_namespace` in the mapping is the TENANCY namespace (`oci os ns get`), not the DataLake's internal namespace. |
| Hardcoding bucket names that include `s3://` prefix | Don't include `s3://` — just the bucket name. |
| Forgetting to pass `--bucket-mapping <path>` to subsequent invocations | The path is per-run, not persisted. Add it to your `snowflake-migrate-job` / `snowflake-migrate-catalog` invocations. |
| Listing buckets the user doesn't actually have read access to | The mapping resolves the name; access errors surface at first read. Don't pre-mock buckets the user can't touch. |

## After this

- Re-run [`snowflake-check-data`](../snowflake-check-data/SKILL.md) — any `MISSING` rows that were due to bucket-map issues should now be `OK`.
- Proceed to [`snowflake-migrate-job`](../snowflake-migrate-job/SKILL.md) / [`snowflake-migrate-catalog`](../snowflake-migrate-catalog/SKILL.md).
