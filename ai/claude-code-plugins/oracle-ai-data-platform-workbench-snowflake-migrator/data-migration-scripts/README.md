# Data-plane notebooks — run INSIDE AIDP, schema by schema

These four **notebooks** are the **data plane** of the migrator. The Claude
Code plugin (assessment, plan, DDL, catalog registration) is the control
plane; it uploads these to the workspace folder
`backup-snowflake-migration/scripts/` and wires each into a parametrised AIDP
Job. They are equally runnable by hand: open one in the console, edit the
`PARAMS` cell at the top, and run it.

## Everything here is `.ipynb`, and that is not a style choice

AIDP types a workspace object by its **extension**: a `.py` is stored as a
`FILE` even when uploaded with `--type NOTEBOOK`, and only an `.ipynb`
becomes a `NOTEBOOK`. A job task is a `NOTEBOOK_TASK` pointing at a NOTEBOOK.
So a `.py` on the workspace could never be run as a job at all. Verified live
2026-09-19 by uploading both shapes and reading the listing back.

Each notebook is **self-contained** — parameters, shared source helpers and
stage logic in one object. There is no driver wrapper and nothing is imported
off the `/Workspace` mount, so the code you open is exactly the code that
runs.

## They are GENERATED — edit the source, not the notebook

The canonical Python lives once in `engine/dataplane/`. These notebooks are
assembled from it:

```bash
bin/snowmig build-notebooks
```

Generated so five copies of the shared helpers cannot drift; committed so
what ships is reviewable. **A hand edit here is overwritten on the next
build.**

**Precondition:** a Snowflake connection config on the workspace mount (the
plugin's `provision --source-config` puts it there), and an INTERNAL target
catalog. Registering the account as an EXTERNAL catalog is optional — useful,
but only required for `--source-mode external-catalog`.

**Nothing here can write to Snowflake.** The connector is read-only in AIDP
4.0 by Oracle's statement, an external catalog refuses DDL by contract, and
every statement these notebooks issue against the source is a SELECT.

| # | Notebook | Reads | Writes | Purpose |
|---|---|---|---|---|
| — | `diagnose_environment.ipynb` | everything | nothing | **run this first**: mount, egress, credentials, catalog |
| 0 | `00_discover_snowflake.ipynb` | Snowflake | reports dir | inventory every schema/table/column into `discovery_manifest.json` |
| 1 | `01_create_structure.ipynb` | `ddl_plan.json` (or source) | target catalog | create target schemas + empty Delta tables, read back against the plan (views listed, not created) |
| 2 | `02_copy_schema.ipynb` | Snowflake | target catalog + reports | copy ONE schema's tables, verify counts (and exact decimal sums) |
| 3 | `03_reconcile.ipynb` | reports + target catalog | reports dir | plan-vs-reality report: what landed, what did not, and why |

The shared helper (`how the source is read, in either mode`) is inlined into
every notebook that needs it; it is no longer a separate upload.

## Two source modes — use `connector`

**Discover schemas and tables by running the workflow in `connector` mode.
Do not enumerate them with three-part names against the EXTERNAL catalog.**
Both can be made to return an answer; only one of them finishes on a real
estate.

| Mode | How it reads Snowflake | Use it? |
|---|---|---|
| `connector` *(default)* | the AIDP Snowflake connector, from the cluster: `spark.read.format("aidataplatform").option("type","SNOWFLAKE")` | **yes** — live-verified, needs no extra cluster library and no catalog crawl |
| `external-catalog` | three-part names against a registered EXTERNAL catalog | only on explicit request — needs a completed crawl, and costs a `DESCRIBE` per object |

Why it is not a close call:

- **Cost.** Connector discovery is **two `INFORMATION_SCHEMA` queries for the
  whole database** — live: 1065 relations and 9935 columns in a single run.
  The three-part-name route is `SHOW` plus a `DESCRIBE` per object, so its
  cost scales with the object count and does not finish at estate scale.
- **Fewer preconditions.** The connector needs the credentials that smoke
  already proved. The external-catalog route additionally needs the catalog
  crawl to have completed, which is one more thing that has to be true
  before discovery can even start.
- **Evidence.** A workflow leaves a job run, its task output and a manifest
  on the platform. That is the record of the migration; a read that happens
  somewhere else leaves nothing behind.

The practical point is **time**: an agent that starts down the three-part
route spends a long while discovering that it does not scale. Start with
`connector` and stay there unless a user asks otherwise.

## Structure modes

`01_create_structure` `--mode`:

| Mode | Types come from | Cost |
|---|---|---|
| `ddl-plan` *(default)* | `plan/ddl_plan.json` — the migrator's own mapper, which refuses what it cannot map exactly, reviewed and signed off before the run | no source read; a table absent from the plan is reported `not_in_plan` and NOT created, and a run in which **every** table is `not_in_plan` exits 1 — the plan and the requested schema do not overlap |
| `ctas` | Spark, derived through the connector | one Snowflake round trip per table — measured slow, and the mapping is the connector's, not an audited one |
| `manifest` | `discovery_manifest.json` verbatim | only valid for an external-catalog manifest (Spark types); a connector manifest carries Snowflake types and is refused rather than mistranslated |

In every mode the CREATE returning is not the claim: the table is `DESCRIBE`d
afterwards and compared with the plan, column by column and in order.
`CREATE TABLE IF NOT EXISTS` is a silent no-op on a table that is already
there, so without the read-back a stale layout would be certified as created
from the plan — and the copy is a positional `INSERT ... SELECT *`.

## Statuses and verdicts

What each report records per table. Anything under **problem** exits 1 in
the stage that records it and is a problem verdict in `MIGRATION_REPORT.md`.

**`structure_report_<schema>.json`** (`objects`, one per table)

| Status | Meaning | Problem? |
|---|---|---|
| `created` | not there before; reads back as planned | no |
| `already_existed` | there before, and it matches the plan (in `ctas` mode there is no plan: the layout is NOT compared, and the record's reason says so) | no |
| `type_drift` | there before with a layout the plan did not produce; left as found, differing columns listed; excluded from the copy's default scope | **yes** |
| `not_in_plan` | the approved plan carries no columns for it; NOT created | no (but a run of nothing else exits 1) |
| `failed` | the CREATE raised; the error is the reason | **yes** |
| `dry_run` | `--dry-run`; nothing was issued | no |

Views sit under a separate `views` key, every one `not_created_by_this_path`
(with `in_plan` when the plan was read): these jobs create **tables only**;
create views with `snowmig deploy --execute` and verify them against the
source. They are listed so a view the plan promised is never absent from
every report with exit 0.

**`copy_report_<schema>.json`** (`tables`, one per table)

| Status | Meaning | Problem? |
|---|---|---|
| `verified` | counts equal after the copy (and decimal sums, with `counts+sums`) | no |
| `skipped_nonempty` | `skip-existing` found rows already there, **equal** to the source count; not re-verified | no |
| `count_mismatch` | counts differ — after a copy, or on a `skip-existing` target that already held a different number of rows (nothing copied) | **yes** |
| `sum_mismatch` | counts equal, a decimal column does not sum equal | **yes** |
| `type_drift` | a source DECIMAL column is not DECIMAL, or narrower, on the target; NOT copied — an INSERT would round or truncate with the count intact | **yes** |
| `failed` | the copy raised; `insert_completed: true` means the rows landed before verification failed, so re-copy with `--mode overwrite`, never `append` | **yes** |
| `target_missing` | no table to copy into (usually `not_in_plan` upstream) | no |

A re-run never softens a recorded failure: `count_mismatch`, `sum_mismatch`,
`type_drift` and `failed` stand until a real re-copy verifies the table.
`--force` re-copies verified tables and needs `--mode overwrite` or
`append` — `skip-existing` cannot re-copy a table that holds rows.

**`MIGRATION_REPORT.md` verdicts** (per table, from the two reports plus the
live catalog)

| Verdict | Meaning | Problem? |
|---|---|---|
| `MIGRATED_VERIFIED` | copy verified, table present (and, with `--counts`, still at the verified row count) | no |
| `PRESENT_NOT_REVERIFIED` | rows were already there at the source's count; sums not re-checked | no |
| `STRUCTURE_ONLY` | table present, no copy yet | no |
| `NOT_MIGRATED` | never attempted, or intentionally not in the plan | no |
| `VIEW_NOT_CREATED_BY_THIS_PATH` | a manifest view; the job path creates tables only | no |
| `MISSING_DESPITE_REPORT` | a report says created or verified; the catalog lacks it | **yes** |
| `STRUCTURE_FAILED` | the CREATE raised | **yes** |
| `STRUCTURE_TYPE_DRIFT` | the table's layout is not the plan's — outranks a verified copy, since counts match when rows land in the wrong columns | **yes** |
| `STRUCTURE_ONLY_COPY_FAILED` | the copy ended in a mismatch, drift or failure | **yes** |
| `COUNT_DRIFT` | `--counts` only: verified at N rows, the target now holds a different number — changed since the copy, not by it | **yes** |
| `TARGET_UNREADABLE` | `SHOW TABLES` failed; not the same as empty | **yes** |

A report written for a **different target catalog** is ignored by reconcile
(and named in the report), never applied to this one.

## The intended run, per schema

The plugin's `provision` stage uploads these notebooks and wires one AIDP job
per notebook, pointing each job straight at it — no driver wrapper. This
run's coordinates are written into the notebook's own `PARAMS` cell at upload
time, because job `parameters` reach a notebook neither as argv nor as
environment (probed live).

To run one by hand, open it in the console and edit the `PARAMS` cell:

```python
# ── PARAMETERS ──
PARAMS = {
    'source-mode': 'connector',
    'source-config': '/Workspace/backup-snowflake-migration/plan/snowmig-config.json',
    'target-catalog': 'snowdemo',   # REQUIRED
    'schema': 'SALES',              # REQUIRED for 02_copy_schema
    'verify': 'counts+sums',
    'reports-dir': '/Workspace/backup-snowflake-migration/reports',
}
```

`None` omits a flag entirely; `True` passes a bare switch. The notebook turns
`PARAMS` into the same argument list the stage has always taken, so behaviour
is unchanged — only the way you supply it is.

Run order: `00_discover_snowflake` once, then `01_create_structure` and
`02_copy_schema` per schema in the order the plan's waves say, then
`03_reconcile` at the end (or any time — it only reads).

**Scope and mode are INPUTS.** To migrate less, change a `PARAMS` value —
never edit the stage logic to make it cover less.

**A schema is the operating unit, never a table.** Two measured costs drive
that:

- a job run costs ~5–6 minutes of startup before it does anything, so
  per-table runs are the wrong shape;
- in connector mode every source read opens its own Snowflake session, so the
  copy batches all of a schema's source counts into **one** round trip per 50
  tables instead of two per table. `--verify counts+sums` still costs a read
  per table for the sums, which is why it is opt-in.

Every script is **resumable**: re-running skips work its report already
records as done (`--force` overrides; for the copy it needs `--mode overwrite`
or `append`), because a 200k-table estate will not finish in one sitting and
must never restart from zero. A re-run never softens a recorded failure.

## Safety properties (hold for all four)

- The source is read-only twice over: by the connector's own contract (or the
  external catalog's), and by the read-only grant on the service user.
- Nothing is dropped, ever. `--mode overwrite` rewrites a table's **rows**
  (`INSERT OVERWRITE`); it never drops the table, and the default mode
  (`skip-existing`) touches nothing that already has rows — it compares
  their count with the source's and records `count_mismatch` when they differ.
- A per-table failure is recorded and the run continues; the report — not the
  exit code alone — is the deliverable.
- Verification is explicit: row counts by default, `counts+sums` adds an
  exact `SUM` over every decimal column **of the source** (cast to
  `DECIMAL(38,s)` with the source's scale on both sides); float tolerance is
  wrong for money, so floats are never summed for equality. A target column
  that cannot hold a source decimal without loss stops the copy as
  `type_drift` before any row moves, in both verify modes.

## Consistency warning — read before a production cutover

Each table is copied at a different moment. If the source keeps changing
during the copy, the target is internally consistent per table but NOT across
tables. For a real cutover: freeze writers, or copy from a Snowflake
zero-copy `CLONE` taken at a single point in time, or plan an incremental
re-sync. The reconcile report tells you what drifted since the copy.

## Cluster libraries (`requirements-aidp.txt`)

**Nothing needs installing.** Both source modes use surfaces already on the
cluster — the `aidataplatform` format is built in. `requirements-aidp.txt`
exists for fallbacks only and ships empty of active entries; the plugin's
provisioning step installs whatever it does contain via the documented
cluster-libraries API (`PATCH .../clusters/{key}/libraries`, types `PYPI` /
`WORKSPACE_FILE` / `MAVEN`), and a cluster restart follows.

## What is proven, and what is not

Canonical per-stage status: `../GAPS.md` → "What is actually proven". Its
sentence, repeated here so this file cannot drift from it:

**What has run live:** the discovery job (`snowmig_00_discover`) ran to
SUCCESS on a migration cluster, reading 1065 relations and 9935 columns in
two `INFORMATION_SCHEMA` queries; the structure job (`snowmig_01_structure`)
ran on a cluster from the approved plan, a healthy 23-minute run left alone
by the cold-start guard (2026-09-19); the copy (`snowmig_02_copy_schema`)
and reconcile (`snowmig_03_reconcile`) jobs are **not yet confirmed by the
authors**.

Also live-verified (2026-09-16, a real DataLake and a real Snowflake
account): the connector read and pushdown; the structure step's refusal of
tables absent from the approved plan; the upload/job/run/output loop; and the
`/Workspace` mount these paths assume.

An earlier version of this file described a five-table copy verified
row-for-row with exact decimal sums; the 0.25.0 changelog, three days later,
said the copy had not executed. Until the person who ran the cluster jobs
confirms which is right, the copy is unproven here. Two rules did come out
of an early copy attempt — a table with no target is recorded as
`target_missing` and skipped rather than ending the run, and the copy's
default scope is **what the structure step created for this target**, not the
whole manifest — and they are pinned by tests regardless.

**Not yet proven at scale**: the largest run was one schema. Before a real
estate, run `diagnose_environment.ipynb`, then one small schema end to end,
then read `MIGRATION_REPORT.md` against the console.
