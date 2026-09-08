---
description: SCAFFOLD - NOT YET PORTED TO SNOWFLAKE. Guided full-job migration flow. Walks the user from "I want to migrate this Databricks job" through DAG → data check → migrate → status, asking before each phase.
---

> ⚠️ **SCAFFOLD — CONTENT NOT YET PORTED.** This file was forked verbatim from the
> Databricks→AIDP migrator and renamed. Its instructions still describe **Databricks**
> sources (Unity Catalog, `dbutils`, `%run`, notebook cells). Do **not** follow them for a
> Snowflake estate. See `PORTING-STATUS.md` at the plugin root for this file’s
> keep / adapt / rewrite disposition and its owner.
# `/migrate-job` — guided full migration

Drive the migrator end-to-end with checkpoints between phases. Asks the user before running each long-running step so they can abort or adjust.

## Workflow

1. **Confirm prerequisites** (invoke [`snowflake-migrator-bootstrap`](../skills/snowflake-migrator-bootstrap/SKILL.md) silently — surface any gaps).
2. **Ask for the source** — either a Databricks workspace path OR a Databricks Job ID. Don't proceed without it.
3. **Build the DAG** — invoke [`snowflake-build-dag`](../skills/snowflake-build-dag/SKILL.md). Show the resulting task count + dep count. Ask "looks right?" before next step.
4. **Data check** — invoke [`snowflake-check-data`](../skills/snowflake-check-data/SKILL.md). Show the OK/MISSING/EMPTY counts.
   - If any MISSING / EMPTY: ask the user how to handle. Options: (a) migrate catalog first via [`snowflake-migrate-catalog`](../skills/snowflake-migrate-catalog/SKILL.md), (b) configure [`snowflake-bucket-mapping`](../skills/snowflake-bucket-mapping/SKILL.md), (c) proceed anyway (with a clear warning).
5. **Migrate** — invoke [`snowflake-migrate-job`](../skills/snowflake-migrate-job/SKILL.md). Surface live log tail.
6. **Status** — when the run finishes, invoke [`/migration-status`](./migration-status.md) on the resulting `JOB_REPORT.md`.

## Args (optional — if user provided them upfront)

$ARGUMENTS

If `$ARGUMENTS` contains a Databricks Job ID or workspace path, jump straight to step 3 (skip the question).

## Checkpoints to surface to the user

Between phases, give the user a 1-line summary + the cost estimate of the next step:

```
[Phase 1/5] DAG built: 7 tasks, 18 dep notebooks, output reports/<MyJob>_manifest.json
[Phase 2/5] About to scan source data on cluster — ~2 min, no token cost. Proceed? (y/N)
...
[Phase 4/5] About to start Pass-2 migration — est. 30-90 min, $10-30 in Claude tokens. Proceed? (y/N)
```

Do not auto-proceed past the migrate step without explicit user confirmation. Pass-2 is expensive.

## When to stop

- User aborts at any checkpoint.
- Data check shows >50% MISSING — likely the catalog hasn't been migrated yet; route to [`/migrate-catalog`](./migrate-catalog.md) instead.
- Cluster is not Active — instruct the user to start it before continuing.
