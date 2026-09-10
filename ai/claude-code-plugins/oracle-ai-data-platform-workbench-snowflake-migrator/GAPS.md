# Gaps and next steps

**State:** v0.13.1 · 882 offline tests · 14 live tests · one real migration
completed end to end against a live AIDP DataLake.

This is the single list. `ACTION-ITEMS.md` holds the detail and the reasoning
behind each item; this file is the ranked view of what is left.

---

## What is actually proven

Worth stating first, because "verified" now means something specific here.

| Surface | Status |
|---|---|
| Snowflake extraction (inventory, census, lineage, maintenance, security) | **Live-verified** against a real account |
| Read-only enforcement on the source | Enforced at the transport, tested |
| Plan, waves, restrictions, DDL generation | Unit-tested; generated SQL parsed by a real Spark parser |
| Target **catalog CRUD** transport | **Live-verified** — 7 objects created and read back |
| Case folding, async polling, key resolution | **Live-verified** (each was a real failure first) |
| Target **SQL** transport | **Dead** — `POST …/sql/execute` returns 404 |
| Smoke test, destination half | **Broken** — still uses the dead SQL path |
| Notebook upload / run | **Never executed** |
| Data movement | **Not implemented, by design** |

---

## Ranked: what to fix next

### 1. The smoke test lies about the destination — HIGH, small

`smoke` still calls `make_aidp_run_sql`, which posts to the SQL endpoint that
returns 404. So it reports **FAIL** on a destination that works perfectly.
Anyone running the pipeline in order hits a failing smoke test and stops,
before reaching the transport that does work.

**Fix:** point the destination checks at the catalog API — `GET /catalogs`
proves read, and the write probe becomes create-schema/delete-schema. Roughly
the same size as the transport work already done.

### 2. Recognise a poisoned name and say so — HIGH, small *(P1)*

A failed async create permanently burns that object name in that schema.
Every later create returns 202 and is silently dropped; `DELETE` does not
recover it. The plugin currently reports *"the create returned 202 Accepted
but no object ever appeared"* — true, but it sends the reader hunting for a
body problem that is not there.

The signature is distinguishable: **a novel name in the same schema
succeeds.** So the plugin can probe once and tell the user which situation
they are in, and recommend a fresh schema.

**This is the item most likely to waste a customer's day.**

### 3. Notebook upload and run — MEDIUM, medium *(M8-adjacent)*

`notebook --upload` and `run_notebook` have never executed. The command
shapes are invented, exactly as the SQL path was — and that one turned out to
be a 404. Assume these are wrong until proven.

### 4. Blast radius — MEDIUM, medium *(C3)*

The census says *what exists*; it does not say *what stops working*. A task
that populates a migrated table means that table goes stale after cutover:
the clone succeeds and then quietly stops being correct. Join the census
against `OBJECT_DEPENDENCIES` and report, per migrating table, "populated by
task X, which does not migrate".

### 5. Maintenance proposal and job — MEDIUM, medium *(M3, M4)*

M2 delivered the inputs. Still missing: a per-table `OPTIMIZE` cadence from
measured churn, `ZORDER` keys seeded from the source clustering key, a
`VACUUM` retention never shorter than the source's, and all of it emitted as
a **disabled** job.

### 6. Time-travel parity statement — LOW, small *(M5)*

Source recovery window (`DATA_RETENTION_TIME_IN_DAYS` + 7 days Fail-safe)
against the proposed Delta retention, with any reduction called out and
Fail-safe named as having no equivalent.

### 7. Cost comparison — LOW, medium *(M6)*

Snowflake automatic-clustering credits versus the AIDP cost of the M3 cadence.
Needs a live AIDP run to calibrate.

---

## Open questions that need Oracle, not code

| # | Question | Why it matters |
|---|---|---|
| P3 | Is a failed create *meant* to reserve the name permanently, and what clears it? | Until answered, the only recovery is a new schema |
| T5 | Is `timestamp_ntz` support planned on the catalog API? | Every estate with a timezone-naive timestamp must accept a downgrade |
| — | Is view column-type derivation documented anywhere? | A `decimal(22,0)` → `decimal(20,0)` narrowing is silent today |

## Open questions that need the customer

| Question | Blocking |
|---|---|
| Silver/Gold job body shape | The medallion deliverable is one third stubs |
| JDBC: AIDP reading Snowflake, or the plugin using JDBC? | Asked three times; the Python connector remains |
| `RAPPI-CONTEXT.md` is on the remote — rewrite history, delete the branch, or accept? | Publishing |
| Rappi's region: Ashburn, Oregon or Ohio? | Any cost or transfer estimate |

## Publish blockers

- `RAPPI-CONTEXT.md` (customer-confidential, already pushed)
- Test-account identifiers in eight fixtures (`npxbexe`, `DU58131`, `NHEYDARI`)
- `PLAN-2PERSON-TIMETABLE.md` and `docs/specs/` ship inside the plugin

---

## Known limits, stated rather than hidden

- **No data is moved.** By design; `DATA_CLONE` and `DONE` are unreachable.
- **Scale is untested.** Validated on 7 objects. Pagination, per-schema column
  reads and metadata row counts are all in place, but nothing has run against
  a large estate.
- **`VARIANT`/`OBJECT`/`ARRAY` block their table** unless
  `--semi-structured string`, which defers rather than solves.
- **A view's column types are derived by the target**, not carried over.
  Verified: three aggregate columns changed type, two of them narrowing.
