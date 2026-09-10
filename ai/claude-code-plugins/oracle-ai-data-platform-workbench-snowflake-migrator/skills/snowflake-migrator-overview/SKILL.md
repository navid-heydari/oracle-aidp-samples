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
| 2 | `snowflake-migration-plan` | `plan.json` + **`PLANNED_OBJECTS.md`** |
| 3 | `snowflake-medallion-clone` | `ddl_plan.json` + `DDL_PLAN.md`, then **`SOFT_CLONE_SUMMARY.md`** |
| — | `snowflake-compute-proposal` | `compute.json` + `COMPUTE_PROPOSAL.md` (independent) |

The two reports the plugin exists to produce are **`PLANNED_OBJECTS.md`** (what
is planned to move, and what cannot with reasons) and **`SOFT_CLONE_SUMMARY.md`**
(what the shallow clone actually created).

## Routing

- "what's in this Snowflake account", "list the tables", "how big are they" → stage 1
- "what order would we migrate in", "what depends on what", "show me a plan" → stage 2
- "create the medallion structure", "clone the schema", "soft clone", "shallow clone" → stage 3
- "compute sizing", "warehouse equivalent", "what will it cost", "credits" → `snowflake-compute-proposal`
- auth or connection errors from any stage → stage 0

## Rules that apply to every stage

1. **Read-only against Snowflake — enforced, not promised.** The transport
   rejects any statement whose verb is not `SELECT`, `SHOW`, `DESCRIBE`, `DESC`,
   `WITH` or `EXPLAIN`, per statement, before it reaches Snowflake. **Nothing is
   ever written to or dropped from the source**, regardless of what the
   credential permits and regardless of what any skill, agent or prompt asks
   for. A read-only Snowflake grant is sufficient; a broader one changes nothing.
2. **The clone copies structure, not data.** It creates schemas, tables and views
   with no rows. If the user expects data to arrive, say so plainly before running.
   Views ARE migrated, but only when their SQL is portable; a Snowflake-only
   construct blocks the view with the construct named.
3. **AIDP target coordinates are never stored and never guessed.** There is no
   config file and no environment default. Ask the user for the DataLake OCID,
   workspace, cluster and catalog in the turn you need them.
   **If no destination is supplied, assume none.** Reports say
   *not supplied* rather than inferring a region, catalog or cluster, and the
   plugin will not pick one of several catalogs on the user's behalf.
4. **Dry-run is the default.** Nothing is created on AIDP without `--execute`
   plus all four coordinates in the same command, and an explicit confirmation
   in that turn. An approval from an earlier turn does not carry.
5. **Never present an approximation as a conversion.** An unmapped type or an
   unrecognised construct is reported as blocked, with the reason. Do not
   substitute a "close enough" type.
6. **A halt is a halt.** Exit code 3 means an identifier-case or target-name
   collision. Show the collisions and stop; do not pick a winner.

7. **Bronze mirrors the source.** Snowflake database → AIDP Standard Catalog,
   schema → schema, table → table, view → view. Silver and Gold are
   requirement-driven: the plan emits disabled job stubs for them and the
   migrator never triggers them.

## Engine

All stages call one CLI:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage> --out-dir <dir> [...]
```

Stages: `assess` · `deps` · `plan` · `ddl` · `deploy` · `compute`.

AIDP writes go through the `aidp` CLI when installed, otherwise `oci
raw-request`. The engine prints which backend it chose, and fails loudly if
neither CLI is present rather than guessing a transport.
