# Assumptions register

Everything the plugin, the plan and the reports currently rest on. Each entry
says what happens if it is wrong. Reviewed 2026-09-09.

## What was actually touched

| | |
|---|---|
| **Snowflake** | Account `DU58131` (org URL `npxbexe-op03637`), region **`AWS_US_EAST_2`**, user `NHEYDARI`, role `ACCOUNTADMIN`, version `10.32.102`. Scope: `TEST_DB_20260908_1529` — 6 tables + 1 view |
| **AIDP** | **None. Never contacted.** No DataLake OCID, workspace or cluster was ever supplied or used. No real OCID exists anywhere on disk. Every deploy run was a dry run |
| **Writes to Snowflake** | Only by the corpus fixture (`engine/snowflake_source/corpus/00_rappi_setup.sql`), run deliberately to build the test estate. **No plugin skill can write** — the transport refuses non-read verbs |

## A. Assumptions about the source

| # | Assumption | If wrong |
|---|---|---|
| A1 | A read-only Snowflake grant is enough | Nothing changes: the transport refuses writes regardless of the grant |
| A2 | `count(*)` per object is affordable | At 200k objects this costs real warehouse time. Needs sampling, with the trade-off stated |
| A3 | `ACCOUNT_USAGE` will be granted | Lose observed credits and usage-ranked prioritisation; sizing falls back to declared warehouse size, which says nothing about actual load |
| A4 | `OBJECT_DEPENDENCIES` is readable, else view-DDL parsing suffices | The fallback covers **view→object edges only**; the graph is partial and is labelled as such |
| A5 | `GET_DDL()` returns the full view definition | A truncated definition would produce a wrong view. Both `GET_DDL` and `SHOW VIEWS.text` are captured verbatim so they can be diffed |
| A6 | The test estate is representative | It is not. 7 objects, no stored procs, tasks, streams, pipes, masking policies, or `VARIANT` columns. Nothing here is evidence about a 200k-object estate |

## B. Assumptions about the destination

| # | Assumption | If wrong |
|---|---|---|
| B1 | Target coordinates arrive per conversation | Enforced: `coords.py` cannot read env, config or cache. No destination is assumed when none is given |
| B2 | The target catalog exists and is **INTERNAL** | The migrator does not create catalogs. `PLANNED_OBJECTS.md` lists what must exist. An EXTERNAL catalog cannot hold managed Delta and is refused |
| B3 | `CREATE SCHEMA / TABLE / VIEW` in Spark SQL is the right mechanism | If AIDP requires REST table registration instead, `deploy` needs rework. **Never verified — no environment** |
| B4 | The `aidp` CLI flags and `oci raw-request` paths are correct | **Unverified.** `aidp` is not installed; `oci ai-data-platform` is control-plane only. Every command is printed before it runs, so a wrong flag should surface as a CLI usage error, not a silent partial migration |
| B5 | `SHOW TABLES … LIKE` verifies existence on AIDP | If unsupported, verification silently reports 0. **Unverified** |
| B6 | Notebooks belong in the workspace filesystem | Placed at `/Workspace/Shared/…`. Catalogs hold tables and views, not notebooks |

## C. Assumptions in the mapping

| # | Assumption | If wrong |
|---|---|---|
| C1 | Bronze mirrors the source 1:1 (database → Standard Catalog) | Stated by the requirement. `--bronze-catalog-prefix` is the only variation |
| C2 | AIDP supports three-level namespaces | If not, names must flatten and collisions become possible again |
| C3 | `NUMBER(38,0)` → `DECIMAL(38,0)`, not `BIGINT` | 38 digits overflow 64 bits, so `BIGINT` would corrupt values |
| C4 | `TIMESTAMP_NTZ` → Spark `TIMESTAMP_NTZ` | Spark's bare `TIMESTAMP` is session-timezone-dependent; the wrong choice shifts every timestamp |
| C5 | A view body with no Snowflake-only construct runs unchanged on Spark | Plausible but unproven — no view has been executed on AIDP. Every generated view carries a warning to verify against the source |
| C6 | The 6 implemented dialect rules are exact | Reviewed, not executed. `LISTAGG` refuses `WITHIN GROUP` and `::` refuses expression operands rather than guessing |
| C7 | Snowflake PK/FK/UNIQUE need not be emitted | Neither engine enforces them. They are captured and reported |

## D. Assumptions in the plan and reports

| # | Assumption | If wrong |
|---|---|---|
| D1 | Dependency depth then size is a sensible wave order | Usage-frequency ranking is the better input and is a documented extension point |
| D2 | Risk levels are meaningful | Derived from observable facts (view, dropped properties, timezone warnings, row count), not judgement. Adjust the thresholds if they mislead |
| D3 | `DATA_CLONE` and `DONE` stay unreachable | They are, by construction. No code path can report that data moved |
| D4 | One catalog per deploy is safer than fanning out | A multi-database estate spans catalogs; one approval must not authorise all of them |

## E. Assumptions about the engagement

| # | Assumption | If wrong |
|---|---|---|
| E1 | The goal is the plugin, not a migrated customer | Stated explicitly. Rappi supplies the shape of the requirement |
| E2 | Migration Assessment is the current phase | Only Preparation and Executive Demo carry `[DONE]`. Unconfirmed |
| E3 | Rappi's region is unresolved | Three answers exist: the deck's diagram says Ashburn, a note says Oregon, and the test account is Ohio. **No cost or transfer estimate should be produced until this is settled** |
| E4 | Data movement is a later phase | See `references/data-movement-options.md`. Five options are presented; none is implemented |
| E5 | The architecture choice belongs to the customer | The options are surfaced in **every** plan and summary, and no default is applied. If the user expresses no preference, `A2` (federate first) is offered as a stated *recommendation*, never as a silent default |

## Outstanding items that block real use

1. **No AIDP environment has ever been contacted.** B3, B4 and B5 are all unverified, which means the entire write path is unproven.
2. **`NUMBER(p,s)` has never been through an unload round trip.** It survives the Python connector; the Parquet-and-Delta path is untested and is the assumption that invalidates the most if wrong.
3. **Rappi's actual region is unknown** (E3).
4. **The test estate proves nothing about scale** (A6).
