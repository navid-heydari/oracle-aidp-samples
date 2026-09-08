# Porting status — Databricks migrator → Snowflake migrator

**What this directory is.** A verbatim fork of
`oracle-ai-data-platform-workbench-databricks-migrator` v0.2.0, with the plugin
identity and skill slugs renamed for Snowflake. **No content has been ported yet.**
Every `SKILL.md`, command, and agent still contains Databricks instructions and
carries a scaffold banner saying so.

**Do not run this against a customer estate.** It would attempt Unity Catalog calls.

**Design source of truth:** `~/Workspace/oracle/snowflake_migrator/README.md`
(+ `build-plan.html`). **Fork base for the engine split:**
`~/Workspace/oracle/oci-aidp-databricks-validator/aidp_migrator_codex/` — it carries
18 test files and an `openai_model.py` provider seam that this plugin copy does not.

**Owner column:** `A` = Core/Target engineer, `B` = Source/Dialect engineer.
See `PLAN-2PERSON-TIMETABLE.md` for the schedule and the write-ownership rules.

---

## Verified facts (checked against this copy, not the upstream docs)

| Claim | Verified |
|---|---|
| `catalog_ddl_rewriter.py` implements 18 DDL rules | **False — 11.** Docstring line 11: "subset of the 18 from the design spec; rest stub out as warnings" |
| Views are migrated | **False.** `catalog_ddl_rewriter.py:275` raises `UnsupportedDDL("R15_VIEW_DEFERRED")` for every view. MV and streaming tables also rejected (`:269`, `:271`) |
| A `databricks-sdk` dependency must be removed | **False.** `engine/requirements.txt` has none. Deps: `oci`, `requests`, `websocket-client`, `anthropic`, `nbformat`, `pandas`, `numpy` |
| Databricks network calls are widespread | **False.** One file makes real calls: `extract_catalog_databricks.py`. Three others only carry Databricks strings in regex/prompt text |
| The plugin ships tests | **False.** 0 test suites (`scripts/test_jar_functionality.py` is a diagnostic script, not a test) |
| Engine size | 37 scripts + 21 `aidp_compat/` modules; `job_migrate.py` = **9,614 lines**, `agent_migrate.py` = 3,186 |
| The samples repo does not vendor the migrator | **Now false.** Design doc §2.2 is stale — this repo *does* vendor it under `ai/claude-code-plugins/`, which is what made this fork possible |

---

## Disposition by area

### KEEP AS-IS — source-neutral (owner A)

These already work against a live AIDP cluster. Do not rewrite; move into
`aidp_migration_core/` unchanged and let the fork base's tests guard the move.

| Path | Why it survives |
|---|---|
| `engine/scripts/aidp_executor.py` | OCI-signed REST + Jupyter WebSocket transport |
| `engine/scripts/cluster_session.py` | Session singleton + `StatelessPool` |
| `engine/scripts/cluster_lifecycle.py` | Cluster start/stop/wait |
| `engine/scripts/install_cluster_libraries.py` | No outbound internet from cluster; Maven path |
| `engine/scripts/setup_migration_cluster.py` | Cluster provisioning |
| `engine/scripts/fetch_spark_logs.py` | Driver-log scrape — catches silent failures |
| `engine/scripts/throttle_coordinator.py` | Cross-process file-backed token bucket |
| `engine/scripts/migration_429_detector.py` | OCI 429 / circuit-breaker detection |
| `engine/aidp_compat/oci_throttle.py`, `bucket_shard.py` | OCI write throttling / hash-shard |
| `engine/aidp_compat/safe_io.py` | FUSE-safe I/O |
| `engine/scripts/fuse_scanner.py` + `fuse_risk_db.json` | `/Volumes` FUSE hazard scan |
| `engine/scripts/acceptance_contract.py` + `schemas/acceptance_contract.schema.json` | Convergence verification |
| `engine/scripts/context_compactor.py`, `cell_context.py` | LLM context management |
| `engine/scripts/consolidate_reports.py` | JOB_REPORT emitter |
| `references/job-report-format.md` | Format spec **and** parser contract |
| `engine/scripts/clone_workflow.py` | Recreate target workflow |
| write-redirect sandbox — inside `job_migrate.py:1682-2500` | The data-safety gate; must be **extracted**, not rewritten |
| `catalog_ddl_rewriter.py::schema_create_sql()` | AIDP metastore COMMENT workaround — pure target-side |

### ADAPT — same skeleton, new vocabulary

| Path | Change | Owner |
|---|---|---|
| `engine/scripts/catalog_ddl_rewriter.py` | Keep `RuleApplication`/`RewriteResult`/`UnsupportedDDL` audit architecture. Replace property list, type mapper, quoting, unsupported-object list | B |
| `engine/scripts/migrate_catalog.py` | Batched replay + per-statement probe unchanged; bucket-map → Snowflake **stage** semantics | A (replay) / B (stage) |
| `engine/scripts/check_data_availability*.py` | Probe side unchanged; extraction regexes retarget | B |
| `engine/scripts/build_dag.py`, `build_dag_from_workflow.py` | Topo builder + manifest emitter reusable **verbatim**; only dependency *discovery* changes | A (builder) / B (discovery) |
| `engine/scripts/job_migrate.py` | Orchestration shell, retry budgets, circuit breaker keep; all `_rewrite_*`/`_preprocess_*`/`_apply_*` rewriters go | A (shell) / B (rewriters) |
| `engine/scripts/agent_migrate.py` | Tool set ~90% reusable (`run_on_cluster`, `search_catalog`, `describe_table`, `list_schemas_and_tables`, `make_note`, `get_cell_history`, `submit_code`); prompt **structure** keeps, content goes | A (tools) / B (prompt content) |
| `engine/scripts/migrate_validate.py` | `compare_outputs()` becomes the parity harness | A |
| all 10 `skills/*/SKILL.md`, 4 commands, 2 agents | Full content rewrite; slugs already renamed | see timetable |

### REWRITE — no reusable content

| Path | Replaced by | Owner |
|---|---|---|
| `engine/scripts/extract_catalog_databricks.py` | `snowflake_source/extract/catalog.py`. **Keep only `_get()`** — its 6-attempt backoff honoring `Retry-After` was built for 100k-table catalogs | B |
| `engine/aidp_compat/` — 20 of 21 modules | Snowflake has no `dbutils`. Only `path_translator.py` and `safe_io.py` survive | B |
| `engine/scripts/cell_analyzer.py` (`DATABRICKS_PATTERNS`) | Snowflake construct patterns | B |
| `engine/scripts/analyze_notebooks.py`, `deep_analyze_notebook.py`, `job_preflight.py` | SQL-script / proc / dbt analyzers | B |
| `references/ddl-rewrite-rules.md`, `cli-map.md`, `gotchas.md` | Snowflake equivalents. Note both existing reference docs are **factually wrong** about the code | B |
| `engine/scripts/analyze_jars.py`, `test_jar_functionality.py` | No Snowflake analogue — delete | A |
| `engine/scripts/provision_shard_buckets.sh`, `enumerate_and_download.py` | Re-scope to unload staging | A |

### BUILD NEW — no prior art anywhere

| Component | Why it is net-new | Owner |
|---|---|---|
| `snowflake_source/unload/` | **Neither codebase moves a single row.** Upstream docs: "Data movement — use OCI BCP or equivalent; the catalog toolkit migrates METADATA only" | A+B (only sanctioned pairing) |
| `snowflake_source/dialect/` | `QUALIFY`, `DATEADD(unit,n,col)`, `VARIANT`/`LATERAL FLATTEN`, `MERGE`, `NUMBER(p,s)` | B |
| `snowflake_source/translate/view.py` | Migrator skips 100% of views; Snowflake gold layers are often nothing but views | B |
| `snowflake_source/translate/stored_proc.py` | Explicitly out of scope upstream. SQL Scripting / JS / Python / Java. JS is worst | B |
| `snowflake_source/extract/usage.py` | `ACCOUNT_USAGE.QUERY_HISTORY` + `WAREHOUSE_METERING_HISTORY`. Databricks exposes no equivalent — the clearest differentiator | B |
| `snowflake_source/dialect/identifiers.py` | Case-folding policy + **collision detector that halts, never guesses** | B |
| `aidp_migration_core/publish.py` | Port MAXWELL `catalog.ts::publishTable` — durable registration. Fixes a defect the migrator has | A |
| `eval/` | Ground-truth differential diff + fixture replay. Migrator has **no offline test story** | A |
| Warehouse → cluster-shape advisor | Warehouses are not clusters: no `cluster_id`, no libraries API, size/auto-suspend/credits map onto AIDP shapes | A |

### GAP — spec'd skills that do not exist in this fork

The fork gives 10 skills; the design calls for 12. Renamed 1:1, so four are **missing**
and two copied ones have questionable Snowflake value.

| Spec skill | Status |
|---|---|
| `snowflake-assess-estate` | **MISSING** — and it is the Phase-B shippable |
| `snowflake-compat-report` | **MISSING** |
| `snowflake-migrate-views` | **MISSING** — the hardest component has no skill |
| `snowflake-reconcile` | **MISSING** |
| `snowflake-migration-status` | Exists only as a command, not a skill |
| `snowflake-bucket-mapping` (copied) | Re-scope to stage→OCI resolver, or delete |
| `snowflake-fixup-cell` (copied) | "Cell" is not the Snowflake unit of work; re-scope to statement/object |

---

## Adjustment — 2026-09-08, after reading the Rappi engagement deck

The dispositions above are unchanged, but the **priority order and two owners** moved.
See `RAPPI-CONTEXT.md` for the customer facts and `PLAN-2PERSON-TIMETABLE.md` v2 for the
schedule. Deltas that affect this file:

| Item above | Adjustment |
|---|---|
| `clone_workflow.py` — listed **KEEP AS-IS** | **Demote.** Rappi orchestrates with **Airflow (Astronomer)**, not AIDP workflows. Recreating AIDP workflows is the wrong target; DAG emission goes to Airflow. Keep the code, drop it from the critical path |
| `references/gotchas.md` etc. — owner B | Unchanged, but add the Fivetran, Shares, Hightouch, HEX and QA/DEV findings from `RAPPI-CONTEXT.md` §4 |
| "Warehouse → cluster-shape advisor — owner A, W12, **Low**" | Now **High, W2.** 82 warehouses / 491 max clusters / **$5 MM/yr** is the business case, not an advisory surface |
| "`extract/usage.py` — Medium (differentiator)" | Now **Critical.** At **200,000 tables** usage ranking is the only economically viable way in, not a nice-to-have |
| "`unload/` — no prior art" | Still true in code, but the deck supplies the **strategy**: same-region S3 unload (no Snowflake egress) → OCI/AWS Interconnect; historic via object storage, current 1–2 weeks direct. Blocked instead on **which region Rappi is in** — the deck contradicts itself |
| Masking / row-access policies — "no target" | A pattern exists: the Porto deployment uses a **redacted catalog of Spark views**. Work becomes *generating* those views from extracted policy metadata |
| **New areas with no row above** | `ingestion/` (Fivetran → AIDP — no supported destination exists), `serving/` (ADW/ALH/ExaCS tier + redaction), `sizing/` (warehouse → cluster + credit model). All **owner A**, all net-new |
