# Table maintenance and data layout — Snowflake vs AIDP

A customer who runs `OPTIMIZE`/`VACUUM`-shaped maintenance on Snowflake and
wants "the same thing on AIDP" is asking a question with a surprising answer:
**Snowflake has no such commands, and AIDP does.** The gap is not a missing
feature on either side — it is a difference in *who runs maintenance*.

- **Snowflake** maintains layout and reclaims storage **automatically, in the
  background, un-asked.** There is no `VACUUM` and no `OPTIMIZE` statement.
- **AIDP** (Spark 3.5 / Delta Lake 3.2.0 OSS) has `OPTIMIZE`, `VACUUM`,
  `ZORDER BY` and liquid clustering, and **runs none of them for you.** Each is
  an explicit statement that has to be scheduled.

So the migration does not lose the capability. It moves the responsibility.
That is the thing to tell the customer, because a lift-and-shift that ignores
it produces tables that quietly fragment and storage that quietly grows.

## What each side actually offers

### Snowflake

| Concern | Mechanism | Who runs it |
|---|---|---|
| Data layout / clustering | `CLUSTER BY (cols)` + **Automatic Clustering** | Snowflake, background, serverless credits |
| Manual recluster | `ALTER TABLE … RECLUSTER` (deprecated) | — |
| Storage reclamation | Automatic once Time Travel + Fail-safe expire | Snowflake |
| Time travel | `DATA_RETENTION_TIME_IN_DAYS` (0–1 Standard, 0–90 Enterprise+) | Snowflake |
| Retention extension | `MAX_DATA_EXTENSION_TIME_IN_DAYS` | Snowflake |
| Disaster safety net | **Fail-safe: 7 days, not user-controllable** | Snowflake |
| Point lookups on non-cluster keys | Search Optimization Service | Snowflake |
| Change capture | `CHANGE_TRACKING` + streams | Snowflake |

There is **no `VACUUM` and no `OPTIMIZE`** in Snowflake SQL. Micro-partition
maintenance is not exposed as a user command.

*Observed on the test account:* every table reports `retention_time=1`,
`cluster_by=''`, `automatic_clustering='OFF'`, and
`ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY` shows **0 reclustering events and
0 credits** — so nothing on that account exercises clustering today. A real
Rappi-scale estate is the opposite case and must be re-measured.

### AIDP — Delta Lake 3.2.0 (OSS) on Spark 3.5

| Concern | Statement / setting |
|---|---|
| Compaction | `OPTIMIZE c.s.t [WHERE <partition pred>]` — bin-packing, idempotent |
| Ordered layout | `OPTIMIZE c.s.t ZORDER BY (cols)` — **opt-in**; bin-packing alone does not order |
| Liquid clustering | `CREATE TABLE … CLUSTER BY (col)` / `ALTER TABLE … CLUSTER BY` |
| Storage reclamation | `VACUUM c.s.t RETAIN 168 HOURS` — **destructive, explicit** |
| Prevent small files | `optimizeWrite` (pre-write shuffle), AQE coalesce / `REBALANCE` hint |
| Compact after write | `autoCompact` — bin-packing only, **never z-orders** |
| Time travel | `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE` |
| Time-travel bound | `delta.deletedFileRetentionDuration`, `delta.logRetentionDuration` |
| Change capture | Change Data Feed (`delta.enableChangeDataFeed`) |

`OPTIMIZE`, `VACUUM`, `DESCRIBE HISTORY` and `VERSION AS OF` are confirmed
working on AIDP by the sibling AIDP workbench plugin, which live-verified them.
This plugin has **not** executed them itself.

## The mapping

| Snowflake | AIDP equivalent | Same behaviour? |
|---|---|---|
| `CLUSTER BY` + Automatic Clustering | `CLUSTER BY` (liquid) or `OPTIMIZE … ZORDER BY` | **No — not automatic.** Needs a scheduled job |
| (automatic storage reclamation) | `VACUUM … RETAIN n HOURS` | **No — explicit and destructive** |
| `DATA_RETENTION_TIME_IN_DAYS` | `delta.deletedFileRetentionDuration` + `delta.logRetentionDuration` | Close, but coupled to VACUUM — see below |
| `MAX_DATA_EXTENSION_TIME_IN_DAYS` | — | No equivalent; Delta retention is one duration |
| Fail-safe (7 days) | — | **No equivalent. A safety net the customer loses** |
| Search Optimization Service | Data skipping + ZORDER (partial) | **No equivalent** for arbitrary point lookups |
| `CHANGE_TRACKING` / streams | Change Data Feed | Broadly equivalent |
| (no `OPTIMIZE`) | `OPTIMIZE` | AIDP gains a capability Snowflake never exposed |

## Four traps worth stating before anyone commits

**1. On Delta, `VACUUM` is what bounds time travel.** In Snowflake, retention
and storage reclamation are separate concerns the customer never runs by hand.
On Delta they are the same knob: vacuuming to a short retention *destroys* the
ability to `VERSION AS OF` past it. A customer used to "reclaim storage
aggressively, it's free" will silently delete their own recovery window.
`RETAIN` below 168 hours is blocked unless
`spark.databricks.delta.retentionDurationCheck.enabled=false` — that guard
exists for exactly this reason and should not be casually disabled.

**2. `OPTIMIZE` *increases* storage until `VACUUM` runs.** It writes new
compacted files and leaves the old ones as tombstones until retention expires.
A field case study measured active files 200 → 1 after `OPTIMIZE`, but physical
files only 206 → 6 **after `VACUUM`**. Scheduling `OPTIMIZE` without `VACUUM`
is a cost regression, not a win.

**3. Nothing runs itself.** AIDP has no managed predictive-optimization
service. Every `OPTIMIZE`/`VACUUM` is a scheduled AIDP Job. Maintenance
becomes a pipeline the customer owns and monitors — new operational surface
that did not exist for them on Snowflake.

**4. Prefer preventing small files over compacting them.** OCI Object Storage
rate-limits requests and can return **HTTP 429** under a small-file write
burst, and `OPTIMIZE`/`autoCompact` make that worse — they write *more* files.
`optimizeWrite` and AQE coalescing cut the files actually written. Note
`delta.targetFileSize` is a vendor extension and is NOT in OSS Delta 3.2.0;
use
`spark.databricks.delta.optimize.maxFileSize` (default 1 GiB) and
`spark.databricks.delta.optimizeWrite.binSize` (default 512 MiB).

*(Those two config keys really are spelled with that legacy vendor prefix;
OSS Delta honours it. They are Spark settings, not a foreign platform.)*

## What this plugin does today

**Reports the gap; applies nothing.** Source settings with a real AIDP
equivalent — `cluster_by`, `retention_time`, `data_retention_time_in_days`,
`change_tracking` — are surfaced per object in the DDL plan under
*"Maintenance and layout — decisions, NOT applied"*, with the equivalent named,
and they raise the object's risk to MEDIUM.

They were previously reported as *"dropped, no Delta equivalent"*, which was
false for all four and false in the expensive direction: it invited a customer
to accept a silent performance regression on their largest tables.

**No maintenance DDL is generated or executed** — no `OPTIMIZE`, no `VACUUM`,
no `CLUSTER BY`, no `TBLPROPERTIES`, enforced by test. Choosing a cadence and a
retention needs the customer's recovery requirements and their query patterns,
neither of which this plugin knows.

See `../ACTION-ITEMS.md` for the planned work to close it.
