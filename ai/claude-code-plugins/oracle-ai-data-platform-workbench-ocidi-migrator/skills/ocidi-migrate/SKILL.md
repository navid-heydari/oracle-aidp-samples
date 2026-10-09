---
name: ocidi-migrate
description: Compile an OCI Data Integration snapshot into AIDP artifacts - one PySpark notebook per Integration / Data Loader / SQL / REST / OCI Data Flow task, one AIDP job per pipeline and per published task (with PAUSED Quartz schedules from DI task schedules), target-table DDL, a watermark seed, a reconciliation notebook, LLM work orders for anything not deterministically translatable, report.json and REVIEW.md. Offline. Use when the user wants to migrate or convert OCI-DI data flows, tasks or pipelines, or generate the AIDP notebooks and jobs for them, after ocidi-analyze has been reviewed.
---

# `ocidi-migrate` — snapshot -> notebooks, jobs, DDL, work orders

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp migrate <snapshot-or-export> -o out/ \
  [--catalog ocidi_migrated] [--job-prefix <p>] [--timezone <IANA>] [--only TASK1,TASK2]
```

Offline and deterministic: the same snapshot gives the same output, and
work-order ids are stable, so answers already written in `out/fallback/*.py`
survive a re-run (re-run `ocidi-fallback` apply afterwards to splice them in).

## Output

| Path | What |
|---|---|
| `REVIEW.md` | **Read this to the user.** Every finding, worst first. |
| `report.json` | One row per object: status, findings, artifacts, work orders |
| `notebooks/<project>/<folder>/<task>.ipynb` | Runtime cell + `parameters` cell + one cell per operator |
| `jobs/<name>.job.json` / `.review.md` | AIDP job body (POSTed verbatim by publish) + its notes |
| `ddl/00_setup.ipynb`, `ddl/setup.sql` | Target schemas, typed Delta tables, the watermark table |
| `watermarks/seed.sql` | Last DI watermark per incremental source |
| `reconcile/reconcile.ipynb` | Legacy DI target vs new Delta table, per table |
| `sources.md` | Every source parameter and what must exist on AIDP |
| `fallback/<id>.json` | LLM work orders |

Statuses: `ok`, `needs_review`, `fallback_pending` (contains a stub that
raises), `llm_assisted`, `manual`, `failed`.

## How the notebooks are parameterised

Every location is a notebook parameter read through
`oidlUtils.parameters.getParameter` (default when not on AIDP):
`TARGET_CATALOG`, `CONTROL_SCHEMA`, one `SRC_<ASSET>` per DI data asset, and
every DI parameter by name in `PARAMS`. A job task can therefore point the
same notebook at a test catalog without editing it.

## Semantics to tell the user about

- **Write modes**: APPEND → append, OVERWRITE/TRUNCATE → overwrite, MERGE →
  `MERGE INTO` on the merge key, IGNORE → `mode("ignore")` (assumption).
  Writes are cast to the target table's types; a value that would not fit
  **stops the write** rather than becoming NULL (Spark's default cast would
  silently null it).
- **Incremental sources** keep a watermark in
  `<catalog>.ocidi_control.watermarks`; seed it (`ocidi-reconcile`) before
  the first run or the first run is a full load.
- **Schedules**: only exact conversions are emitted; everything is PAUSED.
  OCI-DI custom cron numbers weekdays 1–7 from **Monday**; the job uses names.

Next: [`ocidi-fallback`](../ocidi-fallback/SKILL.md) if there are work
orders, then [`ocidi-verify`](../ocidi-verify/SKILL.md).
