# Gaps and next steps

**State:** v0.15.0 · 880 offline tests · 14 live tests · one real migration
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
| Smoke test, destination half | **Live-verified** — read + write probe on the catalog API |
| Notebook upload / run | **Never executed** |
| Data movement | **Not implemented, by design** |

---

## Ranked: what to fix next

### ~~1. The smoke test lies about the destination~~ ✅ DONE

`smoke` still calls `make_aidp_run_sql`, which posts to the SQL endpoint that
returns 404. So it reports **FAIL** on a destination that works perfectly.
Anyone running the pipeline in order hits a failing smoke test and stops,
before reaching the transport that does work.

**Done.** The destination half runs on the catalog API: `list_schemas` proves
read, and the write probe creates a schema, confirms it is *visible* (creates
are async and can fail silently) and deletes it again. Live: **PASS**, with the
probe cleaning up after itself. The probe name is unique per run, because a
fixed one would burn itself the first time it failed.

### ~~2. Recognise a poisoned name and say so~~ ✅ DONE *(P1)*

A failed async create permanently burns that object name in that schema.
Every later create returns 202 and is silently dropped; `DELETE` does not
recover it. The plugin currently reports *"the create returned 202 Accepted
but no object ever appeared"* — true, but it sends the reader hunting for a
body problem that is not there.

The signature is distinguishable: **a novel name in the same schema
succeeds.** So the plugin can probe once and tell the user which situation
they are in, and recommend a fresh schema.

**Done.** On the first object that never appears, the plugin creates one
throwaway object with a novel name in the same schema — once per schema, not
per object — and reads the answer:

- **novel name lands** → the planned names are burned. Says so, and says to
  retry into a fresh schema, because re-running into this one cannot work.
- **novel name fails too** → not a burned name; points at the request (an
  unsupported field type is the usual cause) or the catalog permissions.

`--no-diagnose` turns it off, since the probe writes.

### 1. Notebook upload and run — now the top item *(M8-adjacent)*

`notebook --upload` and `run_notebook` have never executed. The command
shapes are invented, exactly as the SQL path was — and that one turned out to
be a 404. Assume these are wrong until proven.

### 2. Blast radius — MEDIUM *(C3)*

The census says *what exists*; it does not say *what stops working*. A task
that populates a migrated table means that table goes stale after cutover:
the clone succeeds and then quietly stops being correct. Join the census
against `OBJECT_DEPENDENCIES` and report, per migrating table, "populated by
task X, which does not migrate".

### 3. Maintenance proposal and job — MEDIUM *(M3, M4)*

M2 delivered the inputs. Still missing: a per-table `OPTIMIZE` cadence from
measured churn, `ZORDER` keys seeded from the source clustering key, a
`VACUUM` retention never shorter than the source's, and all of it emitted as
a **disabled** job.

### 4. Time-travel parity statement — LOW *(M5)*

Source recovery window (`DATA_RETENTION_TIME_IN_DAYS` + 7 days Fail-safe)
against the proposed Delta retention, with any reduction called out and
Fail-safe named as having no equivalent.

### 5. Cost comparison — LOW *(M6)*

Snowflake automatic-clustering credits versus the AIDP cost of the M3 cadence.
Needs a live AIDP run to calibrate.

---

## Parked — TBD, do not re-raise

These are recorded and deliberately **not** blocking. Nothing in the ranked
list above waits on them, and they should not be surfaced in reports or
conversation until the phase that needs them arrives.

| # | Parked item | Revisit when |
|---|---|---|
| **JDBC** | Whether AIDP reads Snowflake over JDBC, or the plugin uses JDBC instead of the Python connector | **The byte-movement phase.** It belongs in the generated notebook, where it is auditable by the user and reviewable afterwards — not in the structure clone |
| **P3** | Is a failed create *meant* to reserve the name permanently, and what clears it? | Oracle answers. Workaround in the meantime: retry into a fresh schema |
| **T5** | Is `timestamp_ntz` support planned on the catalog API? | Oracle answers. Workaround: `--timestamp-ntz timestamp`, which records the caveat |
| **—** | Is view column-type derivation documented? | Oracle answers. The plugin reports the drift per column regardless |

## Open questions that need the customer

| Question | Blocking |
|---|---|
| Silver/Gold job body shape | The medallion deliverable is one third stubs |
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
