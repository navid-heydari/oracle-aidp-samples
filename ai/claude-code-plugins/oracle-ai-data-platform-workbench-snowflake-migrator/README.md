> ⚠️ **SCAFFOLD — this plugin is not functional.**
> It is a verbatim fork of the **Databricks**→AIDP migrator v0.2.0 with the plugin
> identity and skill slugs renamed for Snowflake. **No content has been ported.** The
> body of this README, and every skill, command, and agent, still describes Databricks
> (Unity Catalog, `dbutils`, `%run`, notebook cells). Do not run it against a Snowflake
> estate.
>
> - **[`PLAN-2PERSON-TIMETABLE.md`](PLAN-2PERSON-TIMETABLE.md)** — the delivery plan:
>   two-person split, week-by-week schedule, gates, risk register, anti-overlap rules.
> - **[`PORTING-STATUS.md`](PORTING-STATUS.md)** — per-file keep / adapt / rewrite
>   disposition, verified facts, and the missing-skill gaps.
> - **[`RAPPI-CONTEXT.md`](RAPPI-CONTEXT.md)** — the customer this is being built for
>   (~200k tables, ~3 PB, 82 warehouses, ~$5 MM/yr) and the 12 design-doc assumptions
>   the engagement deck changes. **Read this before the design doc.**
> - Design source of truth: `~/Workspace/oracle/snowflake_migrator/README.md`
>   + `Rappi_Oracle_AIDP_Engagement_Enriched_v8.pptx`.

---

# Oracle AI Data Platform — Databricks Migrator (Claude Code plugin)

> **Migrate Databricks notebooks, jobs, and catalogs onto Oracle AIDP — in natural language.**
> Drives the *AIDP Databricks Migration Toolkit* (a Claude-with-tool-use migrator that runs cells live on the AIDP cluster, verifies, and self-fixes) end-to-end from Claude Code.

> **Canonical home:** [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-databricks-migrator`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-databricks-migrator).
> End users install via Anthropic's community marketplace (see [Install](#install)).

This plugin is **self-contained** — it ships the full Python migrator engine bundled under `engine/`. After install, skills invoke the engine via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...` — no separate clone required.

> **Status:** **v0.2.0** — self-contained release (engine bundled).

---

## What it does

| Area | Skills |
|---|---|
| **Bootstrap & setup** | `snowflake-migrator-overview`, `snowflake-migrator-bootstrap` |
| **Plan & inventory** | `snowflake-build-dag`, `snowflake-check-data` |
| **Notebook migration** | `snowflake-migrate-job`, `snowflake-fixup-cell`, `snowflake-resume-migration` |
| **Catalog migration** | `snowflake-migrate-catalog`, `snowflake-bucket-mapping` |
| **Verification & quality** | `snowflake-acceptance-contract` |

Each skill is a single-file Markdown SKILL.md with a clear "When to use" trigger so Claude routes correctly without external direction.

### What gets automated end-to-end

```
Databricks workspace                                    AIDP DataLake
─────────────────────                                   ──────────────
Unity Catalog / HMS schemas + tables (DDL)   ──┐
   │                                           ├──→  Catalog migration (batched DDL replay)
External s3:// table locations             ──┘            18 rewrite rules:
                                                          • 3-part → 2-part name flatten
                                                          • s3://→oci:// via bucket-map
                                                          • source format preserved (Delta stays Delta)
                                                          • delta.* / spark.sql.* catch-all scrub
                                                          • MV / streaming rejection
                                                          • CREATE SCHEMA COMMENT-colon strip

Notebooks (.dbc + .ipynb + .py)            ──→   Notebook migration (per workflow / per task)
Jobs + schedules + task DAGs                     Pass 1: %run dep tree, code-only rewrites
                                                 Pass 2: cell-by-cell execute on AIDP cluster,
                                                         4-way verify (status / stderr / Spark
                                                         logs / Opus eval), up to 10 fix attempts,
                                                         fixup_cell rewind for replays.
                                                 Output: per-job .ipynb + JOB_REPORT.md
```

### Signature differentiators

- **Cell-by-cell execute / verify / fix loop** — the migrator runs each Databricks-rewritten cell on a live AIDP cluster, parses the output, and self-corrects via Claude with tool use (14 tools: `explore_path`, `suggest_oci_path`, `search_catalog`, `run_on_cluster`, `describe_table`, `list_schemas_and_tables`, `read_notebook_source`, `inspect_package_source`, `summarize_notebook`, `submit_code`, `make_note`, `get_cell_history`, `get_history_entry`, `fixup_cell`).
- **Write-redirect sandbox schema** — source data is never touched during migration. Every `.saveAsTable(...)` / `INSERT INTO` is silently redirected to a sandbox schema, then verified post-run.
- **Acceptance contract** — for batch / streaming pipelines, declare PASS only after K consecutive empty-pending windows (consecutive-zero convergence).

---

## When to use this plugin

You're moving a Databricks workload onto AIDP and want Claude Code to drive the whole port. Typical asks:

- *"Migrate this Databricks job to AIDP"* / *"port my workflow"*
- *"Build a migration manifest from this Databricks workspace path"*
- *"My migrated notebook fails at cell 23 — fix it"*
- *"Migrate the Unity Catalog DDL into the AIDP default catalog"*
- *"Check whether the source tables are available before I migrate"*
- *"Resume the migration from task X"*
- *"Set up the acceptance contract for the streaming task"*

---

## Install

### Via Anthropic's community marketplace (recommended)

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-databricks-migrator
```

> The plugin is published from this canonical Oracle-samples location. Anthropic's community-marketplace bot picks up new oracle-samples plugins on its weekly cadence, so this install command becomes effective ~1 week after merge.

### From the development mirror (pre-release commits)

```
/plugin marketplace add ahmedawan-oracle/claude-code-plugins
/plugin install oracle-ai-data-platform-workbench-databricks-migrator
```

The `ahmedawan-oracle/claude-code-plugins` umbrella marketplace tracks pre-release commits before they land in the community marketplace.

---

## Prerequisites

This plugin is **self-contained** — the full migrator engine (`scripts/`, `aidp_compat/`, schemas, `requirements.txt`, `setup.py`) ships bundled under `engine/`. After install, skills invoke the engine via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...`. To actually run a migration you still need:

1. **Python deps** — one-time install in any Python 3.10+ environment:
   ```bash
   pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt
   ```
   (Or `cd ${CLAUDE_PLUGIN_ROOT}/engine && pip install -e .`.)
2. **OCI authentication** — `~/.oci/config` with either an `api_key` profile (recommended for unattended runs) or an `oci session authenticate` session-token profile (for interactive notebooks). The engine reads whichever profile you select via `--oci-profile`.
3. **An ACTIVE AIDP cluster** — the engine's Pass-2 execute/verify/fix loop talks to a live cluster via WebSocket. The cluster must be in `Active` state before invoking `job_migrate.py`.
4. **An `ANTHROPIC_API_KEY` env var** — the engine uses Claude with tool use under the hood for each cell rewrite + verify. Each migrated job spends a few minutes of Claude-with-tool-use time.
5. **An `env-coords.md`** — see [references/env-coords.template.md](./references/env-coords.template.md) for the scaffold the skills thread through (DataLake OCID, workspace UUID, cluster ID, AIDP base URL, OCI profile name).

Once those are in place, the plugin's skills know how to invoke each entrypoint — Claude Code will run the right CLI commands in the right order based on your natural-language ask.

---

## Skill index

### Foundation
| Skill | Purpose |
|---|---|
| `snowflake-migrator-overview` | Router. Read this first to get the lay of the toolkit + which skill handles which phase. |
| `snowflake-migrator-bootstrap` | One-shot environment readiness check (Python deps, OCI auth, cluster state, env-coords file). |

### Plan
| Skill | Purpose |
|---|---|
| `snowflake-build-dag` | Build a migration manifest (`reports/<job>_manifest.json`) from a Databricks workspace path. Walks `%run` chains, emits the execution DAG. |
| `snowflake-check-data` | Pre-migration scan: verify source tables / paths exist on the AIDP cluster before kicking off. |

### Execute
| Skill | Purpose |
|---|---|
| `snowflake-migrate-job` | Run the migrator end-to-end against a manifest. Pass-1 deps + Pass-2 cell-by-cell on the live cluster. |
| `snowflake-fixup-cell` | Targeted rewind: re-execute cells from history index N with a `why` reason, through execute+verify+fix. |
| `snowflake-resume-migration` | Resume an interrupted run. Skips already-migrated notebooks via `_migration_cache` + on-cluster `os.path.exists()`. |

### Catalog
| Skill | Purpose |
|---|---|
| `snowflake-migrate-catalog` | Extract Unity Catalog / HMS metadata → 18-rule DDL rewriter → batched replay on AIDP. |
| `snowflake-bucket-mapping` | Configure `s3://` → `oci://` bucket/namespace mappings the rewriter consumes. |

### Verify
| Skill | Purpose |
|---|---|
| `snowflake-acceptance-contract` | YAML-driven consecutive-zero-window acceptance for batch / streaming convergence. |

### Slash commands (interactive flows)
| Command | What it does |
|---|---|
| `/migrate-job` | Guided full-job migration flow (DAG → check-data → migrate → status). |
| `/migrate-catalog` | Guided catalog migration (extract → rewrite preview → batched replay). |
| `/check-data` | Pre-migration data-availability scan. |
| `/migration-status` | Parse + summarize a `JOB_REPORT.md` from a previous run. |

### Specialist agents (for parallel review)
| Agent | What it does |
|---|---|
| `snowflake-object-analyzer` | Reads a Databricks notebook + reports what it does, dependencies, risks (drives the migrator's Pass-1 planning). |
| `migration-reviewer` | Reviews a migrated `.ipynb` for correctness post-Pass-2 (Spark API drift, %run trailing-slash, builtins.sum shadow, etc.). |

### References (loaded on demand)
| Reference | Use |
|---|---|
| [`references/ddl-rewrite-rules.md`](./references/ddl-rewrite-rules.md) | The 18 DDL rewrite rules the catalog migrator applies, with input/output examples. |
| [`references/gotchas.md`](./references/gotchas.md) | 15 Databricks → AIDP gotchas + fix recipes. |
| [`references/env-coords.template.md`](./references/env-coords.template.md) | Scaffold for your environment coordinates (DataLake OCID, workspace UUID, etc.). Fill once, refer to in every skill. |
| [`references/job-report-format.md`](./references/job-report-format.md) | How to parse `JOB_REPORT.md` to extract per-cell pass/fail/fix counts. |
| [`references/cli-map.md`](./references/cli-map.md) | Each migrator CLI entrypoint mapped to its purpose + canonical invocation. |

---

## Engine

Everything flows through the migrator's CLI:

```bash
# Inventory & plan
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag.py --root <workspace-path> --job-name <name> --output reports/<name>_manifest.json
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py --root <workspace-path> --cluster <cluster-id>

# Execute
export ANTHROPIC_API_KEY=sk-ant-...
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py \
  --manifest reports/<name>_manifest.json \
  --cluster <cluster-id> \
  --aidp-base https://aidp.<region>.oci.oraclecloud.com/20240831 \
  --datalake-ocid <your-datalake-ocid> \
  --workspace-id <your-workspace-uuid> \
  --output-base <output-workspace-path> \
  --oci-profile <profile-name>

# Catalog (separate flow)
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py --catalogs <catalog> --schemas-only "<catalog>:<schema>" --out reports/catalog_pack.json
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py --pack reports/catalog_pack.json --cluster <cluster-id> --aidp-base ... --datalake-ocid ...
```

The skills tell Claude when to call each + how to thread args from the env-coords reference into them.

---

## Privacy

This plugin **does not collect, store, transmit, or share any user data**. Everything runs locally against **your own** AIDP tenancy. Full statement: [`PRIVACY.md`](./PRIVACY.md).

---

## License

MIT — see [`LICENSE`](./LICENSE).

## Core developers

- **Sid Rao** (Oracle Agentic AI Development)
- **Ahmed Awan** (Oracle Forward Deployed Engineering)
- **Nishant Patel** (Oracle Forward Deployed Engineering)
