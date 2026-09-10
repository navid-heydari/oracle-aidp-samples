# Action items — maintenance, layout, and estate coverage

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


---

# Estate coverage and security (opened 2026-09-10)

| # | Item | Effort | Status |
|---|---|---|---|
| C1 | Census of non-table/view objects, with language triage | M | **done** |
| C2 | Security posture: policy attachments, secure views, grants | M | **done** |
| C3 | Blast radius: which migrating tables a task/stream feeds | M | open |
| C4 | Capture procedure/UDF bodies for sizing | S | partial — `--capture-definitions` |

## C3 — Blast radius (the one that is still open)

**Why it matters most.** The census answers *what exists*. It does not answer
*what stops working*. A task that populates a migrated table means that table
goes stale after cutover: the clone succeeds and then quietly stops being
correct. That is the failure a customer discovers in production.

**Shape:** join the census against `ACCOUNT_USAGE.OBJECT_DEPENDENCIES` and each
task's / stream's target, then report per migrating table: "populated by task
`X`, which does not migrate — this table will not be refreshed on AIDP." The
lineage extraction already exists (`extract/dependencies.py`); what is missing
is resolving a task or stream to the tables it writes.

**Done when:** every migrating table that is fed by a non-migrating object is
named in `PLANNED_OBJECTS.md` as *will go stale after cutover*, with the object
that feeds it.


---

# Target transport (opened 2026-09-10, from the first live AIDP contact)

| # | Item | Effort | Status |
|---|---|---|---|
| T1 | Choose and implement a working DDL transport | M | **done — catalog REST API** |
| T2 | Check the HTTP status on every backend response | S | **done** |
| T3 | Fix `list_tables` to send a fully-qualified `schemaKey` | S | **done** |
| T4 | `timestamp_ntz` is silently rejected by the catalog API | S | **done — explicit decision** |
| T5 | Ask Oracle whether `timestamp_ntz` support is planned | S | open — needs Oracle |

## T1 — the transport question

`POST /workspaces/<ws>/sql/execute` returns **404**: it does not exist. The
`oci_raw` backend therefore cannot execute DDL, and the `aidp` CLI is not
installed. Three ways forward:

**Catalog REST API instead of SQL (recommended).** Schema/table/view CRUD is GA
at `/catalogs`, `/schemas`, `/tables`, `/views`. For a structure-only clone
this is strictly better than SQL: it needs **no Spark cluster** — which matters,
because a stopped cluster costs nothing and starting one costs money — and it
removes the whole "batch DDL is discarded when the session closes" problem the
deploy module was built around. It also means `expected_columns` can be posted
as a field list rather than rendered into `CREATE TABLE` text.

**The `aidp` CLI.** The plugin's preferred backend, and its flags remain
unverified. Cheapest to try if the CLI can be installed.

**Reuse the sibling plugin's `aidp_sql.py`.** A proven WebSocket kernel-session
transport, but it needs a running cluster and is a much heavier dependency.

**Done when:** one transport executes `CREATE SCHEMA` + `CREATE TABLE` against
a real catalog, the existence and structure probes read back what was created,
and the command shapes are recorded as verified rather than assumed.

## T3 — `schemaKey` must be fully qualified

`GET /tables?catalogKey=lake&schemaKey=default` returns **400
InvalidParameter**; `schemaKey=lake.default` returns 200. `build_command`'s
`list_tables` sends the bare schema, so it would 400 on every call.


---

# What the first live migration taught us (2026-09-10)

Five behaviours, none of them documented, every one of which broke the run.
All are now handled and covered by tests.

**1. `oci raw-request` exits 0 on an HTTP error.** The status is in the response
*body*. The plugin was reading a 404 error object as one row of data, and the
smoke test reported PASS against an endpoint that does not exist. Fixed: any
non-2xx status raises.

**2. AIDP lower-cases identifiers.** A schema created as
`TEST_DB_20260908_1529` is stored as `test_db_20260908_1529`. Every
`schemaKey` and read-back key built from the requested case was wrong, so all
seven objects reported failed while the schema had in fact been created. Fixed
twice over: the PLAN now folds target names so the reports show the name the
destination will really use, and the deploy path additionally RESOLVES keys from
the server rather than assuming them.

This also makes the case-collision detector load-bearing rather than
theoretical: Snowflake keeps `ORDERS` and `"orders"` apart and AIDP cannot, so
two such tables now halt the run instead of silently merging.

**3. A list response is a collection; a create response is one object.**
Collapsing both to `rows[0]` meant key resolution saw a single schema instead
of the list and found no match. Fixed.

**4. Creation is ASYNCHRONOUS and can fail SILENTLY.** `POST /tables` returns
**202 Accepted with an empty body**. The object appears seconds later — or
never, if the async work fails, and when it fails *nothing reports it*. Six
tables once returned 202 and not one of them existed. Fixed: the read-back
polls with a bounded backoff, and an object that never appears is a failure
whose reason says exactly that.

This is the strongest possible argument for the read-back-and-compare design.
A plugin that trusted the create's return code would have reported a clean
seven-object migration into an empty schema.

**5. `timestamp_ntz` is not a valid catalog `fieldType`.** It is the *only*
standard type rejected — `timestamp`, `date`, `boolean`, `binary`, `double`,
`bigint`, `int` and `float` all work. A POST carrying it returns 202 and then
fails silently, which is how it hid. This collides with a deliberate fidelity
choice (Snowflake `TIMESTAMP_NTZ` maps to Spark `TIMESTAMP_NTZ` because bare
`TIMESTAMP` is session-timezone-dependent), so it is now an explicit decision:
`--timestamp-ntz block` (default) refuses the object and names the column;
`--timestamp-ntz timestamp` downgrades it and records the timezone caveat on
the field description.

**T5 is the open question for Oracle:** is `timestamp_ntz` support planned on
the catalog API? Until it is, every Snowflake estate with a timezone-naive
timestamp — which is most of them — has to accept the downgrade or wait.


---

# A failed create POISONS the object name (2026-09-10, verified)

**The most operationally dangerous thing found so far, and it is not a plugin
bug.**

When an asynchronous `POST /tables` fails, the name it used becomes
**permanently unusable in that schema**. Every subsequent create for that name
returns **202 Accepted** and is then silently dropped. Proven by isolating one
variable at a time:

| Attempt | Body | Result |
|---|---|---|
| `test_table` (after earlier failures) | plugin's exact body | 202, **never appears** |
| `fresh_probe_9` | **identical body**, new name | 202, **appears** |
| `test_table` after `DELETE` | same body again | 202, **still never appears** |

`DELETE` returns 202 and does **not** recover the name. So the poisoning
survives an explicit delete.

## Why this matters far beyond this test

**A first failed migration attempt burns every name it touched.** A customer
whose initial run fails for any reason — a bad type, a permissions gap, a
transient error — cannot simply fix the problem and re-run into the same
schema. Every table that failed will keep returning 202 and keep not existing,
and nothing in the API will say why. The obvious diagnosis ("our plugin is
broken") is wrong, and the obvious remedy (delete and retry) does not work.

The only recovery found is **a different schema**.

## What the plugin should do about it

| # | Item | Effort | Status |
|---|---|---|---|
| P1 | Detect the poisoned-name signature and say so | S | open |
| P2 | Offer a `--target-suffix` / fresh-schema retry path | S | open |
| P3 | Ask Oracle whether this is intended, and how to clear a name | S | needs Oracle |

**P1** is the important one. The signature is unmistakable — create returns
202, the object never appears, and a create with a novel name in the same
schema succeeds — so the plugin can distinguish "your body is wrong" from
"this name is burned" and tell the user which. Right now it correctly reports
*"the create returned 202 Accepted but no object ever appeared"*, which is
true but sends the reader hunting for a body problem that is not there.

**P3 is a real question for Oracle:** is a failed create meant to reserve the
name permanently, and if so what clears it? Until that is answered, the
practical guidance for any real migration is: **if a run fails, retry into a
new schema, not the same one.**
