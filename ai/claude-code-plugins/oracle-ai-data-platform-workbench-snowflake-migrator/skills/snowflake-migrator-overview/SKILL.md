---
name: snowflake-migrator-overview
description: Router and shared rules for migrating a Snowflake estate onto Oracle AI Data Platform (AIDP). Read this first whenever the user mentions moving, assessing, inventorying, or cloning Snowflake tables, views, schemas, or databases onto AIDP, or asks what a Snowflake migration would involve. Explains the four-stage pipeline and picks the right next skill; adds no API surface of its own.
---

# Snowflake → AIDP migrator — router

Four stages, each producing a reviewable artifact. Run them in order; each is
re-runnable on its own.

| Stage | Skill | Produces |
|---|---|---|
| 0 | `snowflake-migrator-bootstrap` | verified Snowflake auth |
| 1 | `snowflake-assess-estate` | `inventory.json` + `INVENTORY.md` |
| 2 | `snowflake-migration-plan` | `dependencies.json`, `plan.json` + `MIGRATION_PLAN.md` |
| 3 | `snowflake-medallion-clone` | `ddl_plan.json` + `DDL_PLAN.md`, then optional deploy |

## Routing

- "what's in this Snowflake account", "list the tables", "how big are they" → stage 1
- "what order would we migrate in", "what depends on what", "show me a plan" → stage 2
- "create the medallion structure", "clone the schema", "soft clone" → stage 3
- auth or connection errors from any stage → stage 0

## Rules that apply to every stage

1. **Read-only against Snowflake.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL`.
   Never DDL or DML against the source.
2. **MVP-1 moves no data and translates no views.** It creates *empty* Delta
   tables. If the user expects rows to arrive, say so plainly before running.
3. **AIDP target coordinates are never stored and never guessed.** There is no
   config file and no environment default. Ask the user for the DataLake OCID,
   workspace, cluster and catalog in the turn you need them.
4. **Dry-run is the default.** Nothing is created on AIDP without `--execute`
   plus all four coordinates in the same command, and an explicit confirmation
   in that turn. An approval from an earlier turn does not carry.
5. **Never present an approximation as a conversion.** An unmapped type or an
   unrecognised construct is reported as blocked, with the reason. Do not
   substitute a "close enough" type.
6. **A halt is a halt.** Exit code 3 means an identifier-case or target-name
   collision. Show the collisions and stop; do not pick a winner.

## Engine

All stages call one CLI:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage> --out-dir <dir> [...]
```
