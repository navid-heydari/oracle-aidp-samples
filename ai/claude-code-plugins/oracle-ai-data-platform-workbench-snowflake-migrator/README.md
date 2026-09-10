# Oracle AI Data Platform — Snowflake Migrator (Claude Code plugin)

> ⚠️ **Before sharing or publishing this plugin, work through
> [CLEANUP-BEFORE-PUBLISH.md](CLEANUP-BEFORE-PUBLISH.md).** It carries
> customer-confidential context and a developer's test-account details that must
> come out first.

Investigate a Snowflake estate and migrate its **structure** onto Oracle AI Data
Platform (AIDP): inventory → what can and cannot move → medallion layout →
shallow clone of schemas, tables and views.

**Structure only. It copies no data.** Data movement, stored procedures, tasks,
streams and pipes are later phases.

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | **Standard Catalog** |
| Schema | Schema |
| Table | Table (managed Delta, empty) |
| View | View — when its SQL is portable |
| Warehouse | Spark compute cluster (see the compute proposal) |

Bronze mirrors the source 1:1, so target names equal source names. Silver and
Gold are requirement-driven: the plan emits **disabled job stubs** that the
migrator never triggers, because their content is a requirement to define with
the customer rather than logic to invent.

## Safety posture

| | |
|---|---|
| Against Snowflake | **Read-only, always.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL` |
| Against AIDP | **Dry-run by default.** Writing needs `--execute` plus all four target coordinates in the same command |
| Target coordinates | **Never stored and never discovered.** No config file, no environment default, no cache. Supplied per conversation and confirmed in the turn they are used |
| Unmapped types/features | **Blocked with a reason.** Never approximated, never silently defaulted. Semi-structured and geospatial types have an explicit opt-in escape hatch — see below |
| "Verified" | **Means the planned columns are there**, checked by `DESCRIBE`. A name that already belonged to a different structure is reported as a mismatch and left untouched — never counted as cloned |
| Assessment cost | **Free by default.** Row counts come from Snowflake's maintained metadata; a `COUNT(*)` per object, which executes every view, is opt-in |
| Collisions | **Halt (exit 3).** Identifier-case and target-name collisions stop the run rather than picking a winner |

## Pipeline

```
Snowflake ──▶ inventory.json ──▶ dependencies.json ──▶ plan.json ──▶ ddl_plan.json ──▶ [AIDP]
              INVENTORY.md                           PLANNED_OBJECTS.md  DDL_PLAN.md   SOFT_CLONE_SUMMARY.md

Snowflake ──▶ warehouses.json ──▶ compute.json ──▶ COMPUTE_PROPOSAL.md
```

The two reports the plugin exists to produce:

- **`PLANNED_OBJECTS.md`** — what is planned to move, and what cannot with a
  brief reason per object
- **`SOFT_CLONE_SUMMARY.md`** — what the shallow clone actually created

Each stage reads the previous artifact and is independently re-runnable.

| Stage | Skill | Command |
|---|---|---|
| 0 · auth | `snowflake-migrator-bootstrap` | |
| 1 · investigate | `snowflake-assess-estate` | `/snowflake-assess` |
| 2 · plan | `snowflake-migration-plan` | `/snowflake-plan` |
| 3 · medallion + shallow clone | `snowflake-medallion-clone` | `/snowflake-soft-clone` |
| — · compute | `snowflake-compute-proposal` | `/snowflake-compute` |

`snowflake-migrator-overview` routes between them and carries the shared rules.

## Quick start

```bash
python3 -m pip install -r engine/requirements.txt

# 1. investigate (read-only)
python3 engine/snowmig.py assess --out-dir ./out \
  --account <org>-<account> --user <user> --auth keypair --key-path ~/.sf_key.p8
#   [--row-counts metadata|exact|none]        default metadata: free, and exact
#                                             for a settled table. `exact` runs
#                                             COUNT(*) per object and EXECUTES
#                                             every view.
#   [--semi-structured block|string]          VARIANT/OBJECT/ARRAY
#   [--geospatial block|string]               GEOGRAPHY/GEOMETRY

# 2. plan (deps needs Snowflake; plan is offline)
python3 engine/snowmig.py deps --out-dir ./out --account ... --user ... --auth keypair --key-path ...
python3 engine/snowmig.py plan --out-dir ./out

# 3. generate DDL (offline) then deploy (dry-run unless --execute)
python3 engine/snowmig.py ddl    --out-dir ./out
python3 engine/snowmig.py deploy --out-dir ./out

# 4. compute sizing (independent of the above)
python3 engine/snowmig.py compute --out-dir ./out --credit-price 3.0 \
  --account ... --user ... --auth keypair --key-path ...
```

Exit codes: `0` ok · `1` error · `3` halt.

## Restrictions

Narrow the estate before planning with `--restrictions restrictions.json`. Every
exclusion appears in `PLANNED_OBJECTS.md` with the restriction that fired.

```json
{
  "exclude_databases": ["SNOWFLAKE_LEARNING_DB"],
  "exclude_schemas": ["STAGE"],
  "exclude_object_types": ["VIEW"],
  "exclude_name_patterns": ["^TMP_", "_BAK$"],
  "max_rows": 100000000
}
```

`include_*` variants act as allowlists. An unrecognised key is an **error**, not
an ignored line — a typo would otherwise apply nothing while appearing to work.

## Why a view might not migrate

Because Bronze mirrors the source, object references inside a view need no
rewriting; only dialect matters. 15 Snowflake-only constructs — `QUALIFY`,
`LATERAL FLATTEN`, `IFF`, `::`, `LISTAGG`, `DATEADD`, … — **block** the view with
the construct named, rather than being rewritten on a guess. Secure and
materialized views are blocked outright. See
[references/type-mapping.md](references/type-mapping.md).

## Tests

Offline, no credentials, no AIDP:

```bash
cd engine && python3 -m pytest tests -q
```

Live end-to-end against a real estate (read-only, does not deploy):

```bash
cd engine && SNOWMIG_LIVE=1 SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... \
  SNOWFLAKE_PRIVATE_KEY_PATH=... python3 -m pytest tests/test_live_smoke.py -q
```

The test corpus generator is `engine/snowflake_source/corpus/00_rappi_setup.sql`
with `validate.py` asserting referential integrity, join fan-out and two exact
business invariants — row-count floors alone cannot catch a migration that runs
clean and returns the wrong numbers.

## Execution backend

AIDP writes go through the **`aidp` CLI** when installed, otherwise **`oci
raw-request`**. `oci ai-data-platform` covers only the control plane, which is why
the data plane uses `raw-request`. The engine prints which backend it chose and
fails loudly if neither CLI is present.

## Row counts, and what they cost

| Mode | Source | Cost |
|---|---|---|
| `metadata` (default) | Snowflake's maintained count, from `SHOW`. Views get none | Free |
| `exact` | `COUNT(*)` per object | **Executes every view.** Warehouse time per object |
| `none` | — | Free |

The metadata count agrees with `COUNT(*)` for a settled standard table — the
live corpus confirms it on all six — but it can lag very recent DML and is not
maintained for external tables, so the reports label it as metadata and never
call it verified. Every blank in a Rows column carries its reason: not counted,
not requested, or a named error.

## Semi-structured and geospatial types

`VARIANT`, `OBJECT` and `ARRAY` block their table by default, because the
target shape is a design decision rather than something to guess. Because that
would otherwise be a dead end on an estate full of JSON payloads,
`--semi-structured string` carries the value as text with a warning on every
affected column. `--geospatial string` does the same for `GEOGRAPHY` and
`GEOMETRY`. Two flags, not one: they are separate decisions.

Neither hatch solves the problem — both defer it. As text, nothing on the target
can address a field inside the value.

## Table maintenance — a difference worth reading before you migrate

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`**: it maintains layout and
reclaims storage in the background. AIDP has `OPTIMIZE`, `VACUUM`, `ZORDER BY`
and liquid clustering, and **runs none of them for you**. The capability
survives the migration; the *responsibility* moves.

This plugin reports the gap per object and applies nothing — no maintenance
DDL is generated, enforced by test. Source settings with a real equivalent
(`cluster_by`, `retention_time`, `change_tracking`) appear in the DDL plan under
*"Maintenance and layout — decisions, NOT applied"*, with the equivalent named,
and raise the object's risk to MEDIUM.

Two traps are worth knowing up front: on Delta **`VACUUM` is what bounds time
travel** (on Snowflake those are independent and automatic), and **`OPTIMIZE`
increases storage until `VACUUM` runs**. Full mapping and the planned work:
`references/maintenance-and-layout.md`, `ACTION-ITEMS.md`.

## Known limitation

The `deploy --execute` path has **never run against a live AIDP deployment** —
no environment has been available. That covers the `aidp`/`oci` command shapes,
the REST paths, the existence and `DESCRIBE` probes, notebook upload, run-status
polling, and the destination half of the smoke test. Command construction is
pure and tested, and every command is printed before it runs, so a wrong flag
should produce an obvious CLI usage error rather than a silent partial
migration. **Treat the first live run as a shake-out**, and expect the probe
response shapes to need adjusting: the code accepts several spellings of the
name and type columns and reports "unrecognised output" rather than guessing,
but it cannot know which one AIDP actually returns.

The Snowflake side, by contrast, is live-verified: 10 gated end-to-end tests run
against a real account.

## Docs

- [docs/specs/2026-09-09-mvp1-design.md](docs/specs/2026-09-09-mvp1-design.md) — design
- [docs/plans/2026-09-09-snowflake-migrator-mvp1.md](docs/plans/2026-09-09-snowflake-migrator-mvp1.md) — implementation plan
- [references/type-mapping.md](references/type-mapping.md) — the type table
- [PLAN-2PERSON-TIMETABLE.md](PLAN-2PERSON-TIMETABLE.md) — full-programme schedule beyond MVP-1
- [ASSUMPTIONS.md](ASSUMPTIONS.md) — everything this rests on, and what breaks if each is wrong
- [references/data-movement-options.md](references/data-movement-options.md) — the five ways bytes could move later; **none implemented**
- [CLEANUP-BEFORE-PUBLISH.md](CLEANUP-BEFORE-PUBLISH.md) — **do this before sharing**
- `RAPPI-CONTEXT.md` — **customer-confidential and slated for removal.** Engagement
  context that shaped the requirements. Not needed to use the plugin.
