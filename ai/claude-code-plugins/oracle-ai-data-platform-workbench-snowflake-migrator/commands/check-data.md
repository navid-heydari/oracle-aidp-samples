---
description: SCAFFOLD - NOT YET PORTED TO SNOWFLAKE. Pre-migration data-availability scan. Reads a manifest, probes every spark.read.* / saveAsTable target on the AIDP cluster, reports OK / MISSING / EMPTY.
---

> ⚠️ **SCAFFOLD — CONTENT NOT YET PORTED.** This file was forked verbatim from the
> Databricks→AIDP migrator and renamed. Its instructions still describe **Databricks**
> sources (Unity Catalog, `dbutils`, `%run`, notebook cells). Do **not** follow them for a
> Snowflake estate. See `PORTING-STATUS.md` at the plugin root for this file’s
> keep / adapt / rewrite disposition and its owner.
# `/check-data` — pre-migration scan

Light wrapper over [`snowflake-check-data`](../skills/snowflake-check-data/SKILL.md). Use before any [`snowflake-migrate-job`](../skills/snowflake-migrate-job/SKILL.md) run.

## Workflow

1. Find an existing manifest at `reports/<job>_manifest.json`. If none, ask the user to build one via [`/migrate-job`](./migrate-job.md) Phase 1, OR run [`snowflake-build-dag`](../skills/snowflake-build-dag/SKILL.md).
2. Invoke `${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py` (or `_for_workflow.py` if the manifest came from a Databricks Job ID).
3. Output a 3-section summary: TABLES (OK / MISSING / EMPTY), PATHS (same), and a remediation tip per category.

## Args

$ARGUMENTS

If `$ARGUMENTS` names a manifest file or job name, use it; else infer from the most recent `reports/<job>_manifest.json`.

## Output template

```
== Data availability for <MyJob> ==

TABLES — 23 total
  OK     21
  MISSING  1   → '<catalog>.<schema>.<table_b>' — run /migrate-catalog or create manually
  EMPTY    1   → '<catalog>.<schema>.<table_c>' (0 rows; data backfill needed)

PATHS — 8 total
  OK      7
  MISSING 1   → 'oci://<bucket>@<ns>/path' — confirm bucket-mapping config

VERDICT: 2 issues. Safe to proceed? (y / N / fix-first)
```

## When to STOP and remediate first

If MISSING tables > 0, do NOT proceed to [`/migrate-job`](./migrate-job.md) without resolving. Options surfaced to the user:
- "These schemas missing — run /migrate-catalog first?"
- "These S3 buckets unmapped — open snowflake-bucket-mapping skill?"
- "These specific tables out of scope — exclude in manifest?"
- "Proceed anyway, accept Pass-2 failures at these reads?"
