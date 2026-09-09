# Oracle AI Data Platform — Snowflake Migrator (Claude Code plugin)

Investigate a Snowflake estate and migrate its **structure** onto Oracle AI Data
Platform (AIDP): inventory → dependency-aware plan → medallion layout → empty
managed Delta tables.

**Schema only. It moves no data.** Data movement, view translation, stored
procedures, tasks, streams and pipes are later phases — see
[docs/specs/2026-09-09-mvp1-design.md](docs/specs/2026-09-09-mvp1-design.md) §1.2.

## Safety posture

| | |
|---|---|
| Against Snowflake | **Read-only, always.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL` |
| Against AIDP | **Dry-run by default.** Writing needs `--execute` plus all four target coordinates in the same command |
| Target coordinates | **Never stored and never discovered.** No config file, no environment default, no cache. Supplied per conversation and confirmed in the turn they are used |
| Unmapped types/features | **Blocked with a reason.** Never approximated, never silently defaulted |
| Collisions | **Halt (exit 3).** Identifier-case and target-name collisions stop the run rather than picking a winner |

## Pipeline

```
Snowflake ──▶ inventory.json ──▶ dependencies.json ──▶ plan.json ──▶ ddl_plan.json ──▶ [AIDP]
              INVENTORY.md                            MIGRATION_PLAN.md  DDL_PLAN.md    DEPLOY.md
```

Each stage reads the previous artifact and is independently re-runnable.

| Stage | Skill | Command |
|---|---|---|
| 0 · auth | `snowflake-migrator-bootstrap` | |
| 1 · investigate | `snowflake-assess-estate` | `/snowflake-assess` |
| 2 · plan | `snowflake-migration-plan` | `/snowflake-plan` |
| 3 · medallion + soft clone | `snowflake-medallion-clone` | `/snowflake-soft-clone` |

`snowflake-migrator-overview` routes between them and carries the shared rules.

## Quick start

```bash
python3 -m pip install -r engine/requirements.txt

# 1. investigate (read-only)
python3 engine/snowmig.py assess --out-dir ./out \
  --account <org>-<account> --user <user> --auth keypair --key-path ~/.sf_key.p8

# 2. plan (deps needs Snowflake; plan is offline)
python3 engine/snowmig.py deps --out-dir ./out --account ... --user ... --auth keypair --key-path ...
python3 engine/snowmig.py plan --out-dir ./out

# 3. generate DDL (offline) then deploy (dry-run unless --execute)
python3 engine/snowmig.py ddl    --out-dir ./out
python3 engine/snowmig.py deploy --out-dir ./out
```

Exit codes: `0` ok · `1` error · `3` halt.

## Namespace strategies

Generic by default, overridable with `--namespace-strategy` and `--layer-map`.

| Strategy | Target | Trade-off |
|---|---|---|
| `layer-catalog` *(default)* | `bronze.<schema>.<table>` | Matches catalog-per-layer. Drops the source database, so same-named schemas across databases collide → halt |
| `preserve-source` | `<db>.<schema>.<table>` | Exact 1:1, never collides. Layer recorded as metadata |
| `layer-flattened` | `bronze.<db>_<schema>.<table>` | Layers visible and collision-free, mangled schema names |

Layer assignment precedence: explicit `--layer-map` → name heuristic → BRONZE
fallback. Fallbacks are **reported for review**, never silently applied.

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

## Known limitation

`engine/target/aidp_runner.py` — the `deploy --execute` path — has **never run
against a live AIDP cluster**. Its SQL wrapping and output unwrapping are pure
and tested; the connection and event-loop handling are not.

## Docs

- [docs/specs/2026-09-09-mvp1-design.md](docs/specs/2026-09-09-mvp1-design.md) — design
- [docs/plans/2026-09-09-snowflake-migrator-mvp1.md](docs/plans/2026-09-09-snowflake-migrator-mvp1.md) — implementation plan
- [references/type-mapping.md](references/type-mapping.md) — the type table
- [PLAN-2PERSON-TIMETABLE.md](PLAN-2PERSON-TIMETABLE.md) — full-programme schedule beyond MVP-1
- [PORTING-STATUS.md](PORTING-STATUS.md) — what was kept from the Databricks migrator fork and what was deleted
- `RAPPI-CONTEXT.md` — **customer-confidential.** Engagement context that shaped
  the requirements. Not needed to use the plugin; do not publish it.
