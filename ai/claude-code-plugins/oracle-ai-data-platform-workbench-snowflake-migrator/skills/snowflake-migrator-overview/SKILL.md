---
name: snowflake-migrator-overview
description: Router and shared rules for migrating a Snowflake estate onto Oracle AI Data Platform (AIDP). Read this first whenever the user mentions moving, assessing, inventorying, or cloning Snowflake tables, views, schemas, or databases onto AIDP, or asks what a Snowflake migration would involve. Explains the pipeline and picks the right next skill; adds no API surface of its own.
---

# Snowflake → AIDP migrator — router

Each stage produces a reviewable artifact. Run them in order; each is
re-runnable on its own, and every stage after `assess` reads its input from
`--out-dir`.

| Stage | CLI | Skill | Produces |
|---|---|---|---|
| 0 | — | `snowflake-migrator-bootstrap` | verified Snowflake auth |
| 1 | `assess` | `snowflake-assess-estate` | `inventory.json` · `INVENTORY.md` · **`CENSUS.md`** |
| 2 | `deps` + `plan` | `snowflake-migration-plan` | `plan.json` · **`PLANNED_OBJECTS.md`** |
| 3 | `catalog` (+ `ddl`) | `snowflake-medallion-clone` | **`CATALOG.md`** · `DDL_PLAN.md` |
| 4 | `summary` | `snowflake-migration-plan` | **`SUMMARY.md`** — the per-object roll-up |
| — | `maintenance` | `snowflake-assess-estate` | `MAINTENANCE.md` — clustering, retention, churn, and who inherits `OPTIMIZE`/`VACUUM` |
| — | `security` | `snowflake-assess-estate` | `SECURITY.md` — masking/row-access policies, secure views, grants |
| — | `compute` | `snowflake-compute-proposal` | `COMPUTE_PROPOSAL.md` |
| — | `smoke` | `snowflake-smoke-test` | `SMOKE_TEST.md` |
| — | `notebook` | `snowflake-clone-notebook` | executable `.ipynb` · `NOTEBOOK.md` — the **Standard-catalog** table-creation script, run on AIDP compute |
| — | `deploy` | `snowflake-medallion-clone` | **`SOFT_CLONE_SUMMARY.md`** — the older control-plane clone path |
| — | `data-options` | `snowflake-migration-plan` | `DATA_MOVEMENT_OPTIONS.md` |

The two reports the plugin exists to produce are **`PLANNED_OBJECTS.md`** (what
is planned to move, and what cannot with reasons) and **`SOFT_CLONE_SUMMARY.md`**
(what the shallow clone actually created). **`SUMMARY.md`** is the per-object
roll-up: name, rows, risk, migration status.

## Three stages are easy to forget — do not

- **`summary`** produces `SUMMARY.md`, the per-object table with rows, risk and
  migration status. Run it after `plan` (and again after `deploy`).
- **`maintenance`** answers "we use `OPTIMIZE`/`VACUUM`-style upkeep, what
  happens on AIDP". Snowflake exposes neither and does it in the background;
  AIDP has both and runs neither. Run it after `assess`.
- **`security`** is the only stage with an exposure consequence. A masked
  column arrives **unmasked**. Run it after `assess` on any estate that has
  ever had a masking policy, and never skip it silently.

## Routing

- "what's in this Snowflake account", "list the tables", "how big are they" → stage 1
- "what order would we migrate in", "what depends on what", "show me a plan" → stage 2
- "create the medallion structure", "create the target catalog", "clone the
  schema", "soft clone", "shallow clone" → stage 3 (EXTERNAL catalog by default)
- "I want a Standard catalog", "I need real tables on AIDP" → stage 3 Phase C →
  `snowflake-clone-notebook`, run on compute. Only on an explicit ask.
- "compute sizing", "warehouse equivalent", "what will it cost", "credits" → `snowflake-compute-proposal`
- auth or connection errors from any stage → stage 0
- "what about our stored procedures / tasks / streams / UDFs" → `assess` writes
  `CENSUS.md`; they are inventoried and **none of them migrate**
- "we use OPTIMIZE/VACUUM", "clustering", "time travel", "retention" → the
  `maintenance` stage
- "masking", "row access", "who can see what", "PII", "secure view", "grants"
  → the `security` stage, and read it out rather than summarising it away

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

7. **Always present the data-movement architecture options.** Every plan and
   summary carries them, and you must walk the user through them rather than
   letting the section pass unread — even when they asked for something else,
   even when no destination was supplied, and even when they gave no instruction
   about architecture at all. There are six. `A1`–`A5` cover internal and
   external catalogs, object storage, Fivetran and Kafka, and Iceberg interop.
   **`A6_CUSTOMER_DEFINED` is the open slot**: the customer may want something
   not listed, or may not have decided yet — both are valid answers, and neither
   is forced into one of the others.

   If nothing has been chosen, say so plainly: the architecture is **undecided**,
   and the choice drives cost, wall-clock and whether a later migration can run
   unattended. Record a choice with `snowmig data-options --choose <id>
   --rationale "..."`. **None of the six is implemented** — recording a choice
   executes nothing.

   If the user has no architecture in mind, do not push them to pick from
   `A1`–`A5`. Record `A6` with their reason and move on: that is a deliberate
   deferral and it blocks nothing — the assessment, the plan and the shallow
   clone all proceed. When they later describe a design, including one we never
   listed, record it verbatim with `--custom-name` and
   `--custom-description-file`. **Never paraphrase their design into one of
   ours**: a customer design filed under `A1` reads as an assessed variant of
   `A1`, and it is not.

8. **Bronze mirrors the source.** Snowflake database → AIDP catalog, schema →
   schema, table → table, view → view. Silver and Gold are
   requirement-driven: the plan emits disabled job stubs for them and the
   migrator never triggers them.

8b. **The target catalog is EXTERNAL/SNOWFLAKE by default.** `snowmig.py
   catalog` registers a read-only pointer at the live source, which copies
   nothing and creates no tables. A **Standard** (or Internal) catalog is only
   ever created when the user has explicitly asked for one, and then its tables
   come from the `notebook` script run on AIDP compute — not the control-plane
   CRUD API, where a 202 Accepted can silently create nothing. Never offer or
   assume a Standard catalog.

9. **Never report success ahead of verification.** AIDP creates are
   asynchronous and can settle late or fail silently — `catalog_deploy.py`
   polls for `lifecycleState == ACTIVE` and `deploy` reports `verified/total`
   for exactly this reason. Read that actual result before saying anything
   succeeded. "Pending", "still settling", "not yet confirmed" and "exit code
   nonzero" are not success — say which one it is, not the outcome you expect
   it to reach. This applies to every stage, not only `deploy`: a `smoke` FAIL,
   an unreadable `ACCOUNT_USAGE`, or a schema still `CREATING` all get named as
   what they are, never rounded up.

10. **After running any stage, report where the run stands — before waiting
    on the user.** Don't stop on a bare command result and let the next
    prompt be silence. Say, in the same turn: what this stage's actual result
    was (per rule 9), which stage is next per the stage board's `next_stage`
    (`snowmig stages --out-dir <dir>`, see `snowflake-stage-board`), and the
    concrete command or confirmation that would run it. Do this every time a
    stage finishes during a migration session, unprompted — the user should
    never have to ask "where are we" to find out.

## Engine

All stages call one CLI:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage> --out-dir <dir> [...]
```

Stages: `assess` · `deps` · `plan` · `ddl` · `deploy` · `compute`.

AIDP writes go through the `aidp` CLI when installed, otherwise `oci
raw-request`. The engine prints which backend it chose, and fails loudly if
neither CLI is present rather than guessing a transport.

## One thing to raise even when nobody asks

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`** — it maintains layout and
reclaims storage in the background. AIDP has both, plus `ZORDER BY` and liquid
clustering, and **runs none of them for you**. Nothing is lost in the
migration; the *responsibility* moves.

Run `snowmig maintenance` after `assess` and read `MAINTENANCE.md`. Which
architecture the customer picks decides who inherits that work — federating
leaves it with Snowflake, landing Delta tables transfers it on day one — so it
belongs in the architecture conversation, not after it.
