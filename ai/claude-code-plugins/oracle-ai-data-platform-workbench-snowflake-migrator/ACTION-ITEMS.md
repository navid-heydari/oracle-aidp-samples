# Action items — maintenance and layout parity

Opened 2026-09-10, after verifying how Snowflake and AIDP differ on
`OPTIMIZE`/`VACUUM`-shaped maintenance. Background and the full mapping:
`references/maintenance-and-layout.md`.

**The finding in one line.** Snowflake maintains layout and reclaims storage
automatically and exposes no `OPTIMIZE`/`VACUUM`; AIDP has both plus `ZORDER`
and liquid clustering and runs none of them for you. The capability survives
the migration — the *responsibility* moves to the customer, and nothing in this
plugin tells them what to schedule.

| # | Item | Effort | Blocked by |
|---|---|---|---|
| M1 | Report the gap instead of mislabelling it | — | **done** |
| M2 | Extract maintenance state as first-class inventory | S | **done** |
| M3 | Propose a per-table maintenance plan | M | M2 |
| M4 | Emit a disabled maintenance job | M | M3 |
| M5 | Time-travel parity statement | S | M2 done — unblocked |
| M6 | Cost comparison: clustering credits vs OPTIMIZE job | M | M2, live AIDP |
| M7 | Name the capabilities with no equivalent | S | **done** |
| M8 | Verify the maintenance DDL on a live AIDP | S | AIDP environment |

---

## M1 — Report the gap instead of mislabelling it ✅ done

`cluster_by`, `retention_time`, `data_retention_time_in_days` and
`change_tracking` were reported as *"dropped Snowflake properties with no Delta
equivalent"*. All four **do** have AIDP equivalents. Now split into
`SCRUBBED_PROPERTIES` (genuinely nowhere to go) and
`DEFERRED_EQUIVALENT_PROPERTIES` (equivalent named, not applied), surfaced in
the DDL plan and raising risk to MEDIUM.

## M2 — Extract maintenance state as first-class inventory ✅ done

**Why:** the plan cannot propose a cadence it cannot see, and today the
extractor only picks these up incidentally from `SHOW TABLES`.

**Capture, read-only:** clustering keys and `automatic_clustering` per table ·
`DATA_RETENTION_TIME_IN_DAYS` and `MAX_DATA_EXTENSION_TIME_IN_DAYS` at account,
database, schema and table level (they cascade — record which level set it) ·
per-table reclustering credits from
`ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY` · whether Search Optimization is
enabled · `CHANGE_TRACKING` · DML churn per table from
`ACCOUNT_USAGE.TABLE_DML_HISTORY`, which is what determines how often
compaction is actually needed.

**Where:** a new `engine/snowflake_source/extract/maintenance.py`, same
injected-`run_sql` shape as `catalog.py`. Note `ACCOUNT_USAGE` needs a grant
the plugin should not assume — degrade to "unknown, not measured", never to
zero.

**Done when:** a `maintenance.json` artifact records the above per table, with
the source level for each cascading setting, and says explicitly which parts
were unreadable.

**Delivered.** `engine/snowflake_source/extract/maintenance.py`, the
`snowmig maintenance` stage, `maintenance.json` + `MAINTENANCE.md`. Captures
clustering keys and `automatic_clustering`, Search Optimization (with bytes),
`change_tracking`, the retention cascade at account/database/schema level with
per-table effective values, reclustering credits and DML churn from
`ACCOUNT_USAGE`, and a per-table list of *signals* naming what each will
require on AIDP.

Two decisions worth keeping:

- **"Not measured" is never rendered as zero.** An unreadable `ACCOUNT_USAGE`
  reports `measured: False` with null counts, because "0 reclustering credits"
  and "we could not look" lead to opposite decisions. Verified by test.
- **The retention level is inferred, not probed.** `SHOW TABLES` already
  carries each table's effective retention, so the cascade costs one account
  query plus one per database and schema — not one `SHOW PARAMETERS` per
  table, which would be thousands of round trips on a real estate.
  `--probe-table-parameters` opts into the exact path.

Live on the test account: 0 of 6 tables flagged (nothing clustered, no Search
Optimization, retention inherited at 1 day), `ACCOUNT_USAGE` readable, real DML
churn measured. A clean estate now says so explicitly rather than rendering an
empty section.

## M3 — Propose a per-table maintenance plan

**Why:** this is what the customer is actually asking for when they say "we
want the same thing on AIDP".

**Shape:** for each table, propose `OPTIMIZE` cadence (from DML churn and file
count, not from table size alone) · `ZORDER BY` / `CLUSTER BY` keys **seeded
from the source clustering key** where one exists · a `VACUUM` retention
**derived from the source `DATA_RETENTION_TIME_IN_DAYS`, never shorter** ·
whether `optimizeWrite` is the better answer than a compaction job at all.

**Rules to hold:** propose, never apply. Where the source has no clustering
key, say so rather than inventing one — Snowflake customers frequently rely on
natural insert order, and a guessed `ZORDER` key is a cost with no benefit.
Never propose a `VACUUM` retention that shortens the customer's recovery window
without flagging it in terms of lost time travel.

**Done when:** `MAINTENANCE_PLAN.md` presents one row per table with the
proposal and its evidence, and every number traces to something measured.

## M4 — Emit a disabled maintenance job

**Why:** the same reasoning as the Silver/Gold stubs — the artifact should
exist and be reviewable, and must not run untriggered.

**Shape:** an AIDP Job (cron, per `aidp-pipelines`) whose tasks are the
`OPTIMIZE` and `VACUUM` statements from M3, `enabled=False`,
`trigger="MANUAL_NEVER_TRIGGERED"`. `VACUUM` must be a separate, separately
approved task from `OPTIMIZE` — it is the destructive one.

**Done when:** the job is generated, a test asserts it is disabled and that no
`VACUUM` runs without explicit opt-in, and the notebook/report says plainly
that enabling it is the customer's action.

## M5 — Time-travel parity statement

*(M2 delivered its input: the retention cascade and per-table effective values
are now captured, and a table-level override is already reported as a signal.
What is still missing is the explicit source-window vs target-window
comparison.)*

**Why:** the trap most likely to cause data loss. On Snowflake, retention and
storage reclamation are independent and automatic; on Delta, `VACUUM` is what
bounds time travel. A customer used to reclaiming storage freely will delete
their own recovery window.

**Deliver:** a per-table statement of the source recovery window
(`DATA_RETENTION_TIME_IN_DAYS` + 7 days Fail-safe) against the proposed Delta
retention, with any *reduction* called out explicitly. Include that **Fail-safe
has no equivalent** — that is 7 days of Snowflake-operated recovery the
customer silently loses, and no Delta setting restores it.

## M6 — Cost comparison

**Why:** maintenance moves from Snowflake serverless credits to AIDP cluster
time, and nobody has priced the swap.

**Deliver:** observed Automatic Clustering credits per table from
`ACCOUNT_USAGE` versus the estimated AIDP cost of the M3 cadence. Fold into the
existing compute proposal rather than a separate report. Needs a live AIDP run
to calibrate, so keep the estimate labelled as an estimate.

## M7 — Name the capabilities with no equivalent ✅ done

Short and worth doing early, because both are things a customer discovers at
the worst moment:

- **Fail-safe** — 7 days, Snowflake-operated, no user control, **no AIDP
  equivalent**.
- **Search Optimization Service** — point lookups on non-clustered columns.
  Delta data skipping plus `ZORDER` covers some of it; arbitrary
  high-cardinality point lookups it does not.

**Done when:** both appear in the assessment output as named gaps, not as
absences the reader has to notice.

**Delivered.** `NO_AIDP_EQUIVALENT` in the maintenance extractor, rendered in
`MAINTENANCE.md` under *"Capabilities with no AIDP equivalent"*. Three, not
two — `MAX_DATA_EXTENSION_TIME_IN_DAYS` belongs with them, since Delta
retention is a single duration with no automatic extension.

## M8 — Verify on a live AIDP

Folded into the broader live-verification session. Specifically: confirm
`OPTIMIZE`, `OPTIMIZE … ZORDER BY`, `VACUUM … RETAIN`, `DESCRIBE HISTORY` and
`ALTER TABLE … CLUSTER BY` all execute through the `aidp`/`oci` backend, and
capture their real response shapes. The sibling AIDP plugin reports these as
live-verified through *its* execution path; this plugin's path is different and
unproven.

---

## Not in scope for this list

Deciding the actual cadences and retentions. That needs the customer's recovery
requirements and query patterns, and it is a conversation, not a default.
