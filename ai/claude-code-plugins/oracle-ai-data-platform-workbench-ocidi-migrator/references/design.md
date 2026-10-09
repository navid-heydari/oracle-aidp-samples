# Design: OCI Data Integration → AIDP migrator

Status: v0.1. Built without access to a live OCI-DI workspace; the AIDP side (provision, publish) was exercised live on 2026-10-09. Every OCI-DI
JSON shape below comes from the OCI Python SDK models (`oci.data_integration`,
2.165.1). The SDK is generated from the REST API, so it is the closest
available source of truth. Nothing here has been checked against a real
export. See [known-limitations.md](known-limitations.md).

## What "migrate" means here

OCI-DI holds no data. It moves data between data assets. So the job is to
move its **logic** onto AIDP:

| OCI-DI | AIDP |
|---|---|
| Data flow + Integration task | One PySpark notebook (`.ipynb`) |
| Data Loader task | One PySpark notebook (same compiler) |
| Pipeline + Pipeline task | One AIDP job (tasks with `dependsOn` / `runIf`) |
| Published Integration or Data Loader task with no pipeline | One single-task AIDP job |
| Schedule + Task schedule | The job's `schedule` (Quartz cron + IANA `timezoneId`), created **PAUSED** |
| Parameters (data flow → task → task schedule → runtime) | Notebook parameters read with `oidlUtils.parameters.getParameter`; task values become job task `parameters` |
| User-defined functions | Inlined at each call site (macro expansion) |
| Incremental load / `SYS.LAST_LOAD_DATE` | A Delta watermark table, `<catalog>.ocidi_control.watermarks`, seeded from the last successful DI run |
| Target data entity | **A managed Delta table in the AIDP catalog** (default catalog `ocidi_migrated`) |
| Source data asset | An AIDP external catalog (ADW/ATP/Oracle/MySQL/…), an `oci://` path (Object Storage), or a configured override |

Where data lands was settled on 2026-10-09: **targets are retargeted to AIDP
Delta**, and the original DI targets become reconciliation baselines.

The default AIDP workspace is **`ocidi_migrated`**. `provision` creates it
when it is missing; that step is a dry run until `--apply`.

## Approach: deterministic compiler + LLM fallback ("option C")

```
extract ─▶ snapshot/ ─▶ analyze ─▶ migrate ─▶ out/ ─▶ fallback ─▶ verify ─▶ provision ─▶ publish ─▶ reconcile
(live,     (JSON,        (offline   (offline    notebooks  (Claude    (static    (workspace   (dry run   (generated
 read-only) verbatim)    inventory)  compiler)  jobs, DDL   writes the  PASS/      + catalog;  unless     notebook,
                                                 work orders long tail) REVIEW/    dry run)    --apply)   runs on AIDP)
                                                                         FAIL)
```

1. **The deterministic compiler is the default path.**
   - A data flow is a DAG of `FlowNode`s. Each node has an `operator` with
     ports. Edges are `OutputLink.key` → `InputLink.fromLink`.
   - The compiler sorts the graph in dependency order and emits one
     DataFrame per output port.
   - Expressions are tokenised and rewritten into Spark SQL against a curated
     function table ([expression-functions.md](expression-functions.md)).
2. **Anything the compiler can't do safely becomes a work order, not a
   guess.**
   - The notebook gets a stub function `_fb_<id>(spark, inputs, params)` that
     raises `NotImplementedError`, so an unconverted node fails loudly. It
     never produces wrong data.
   - `fallback/<id>.json` records the raw DI node, its input schemas, the
     expected output fields and the contract.
3. **The LLM fills in work orders. By default that is Claude, in the session.**
   - The `ocidi-fallback` skill sends each work order to the
     `ocidi-fallback-author` agent. The agent writes `fallback/<id>.py`.
   - `fallback apply` checks each response before splicing it in. The code
     must parse, define exactly the contracted function, import only from an
     allow-list, and contain no credentials, `exec`, subprocess, filesystem
     or network calls.
   - The spliced cell keeps an `LLM-ASSISTED — REVIEW REQUIRED` banner, and
     the report marks the object `llm_assisted`.
   - The optional `--provider anthropic` mode does the same headless, using
     `ANTHROPIC_API_KEY`.
4. **No credential ever appears in a generated artifact.**
   - Sources are read through catalogs or `oci://` paths.
   - DI connections that reference OCI Vault secrets map to AIDP
     credentials by name. Values are never copied.

## Why the input is a live REST snapshot, not the export zip

- **The export zip layout is undocumented.**
- **The typed SDK drops data.** It deserialises by `modelType`, and the
  Table Function operator has no model type in it. Typed SDK objects would
  therefore lose fields such as the Spark SQL text.

So `extract` calls the REST API (`/20200430/workspaces/{id}/…`), signs
requests with the OCI SDK signer, and writes the **raw JSON verbatim** to a
snapshot directory. Every later step is offline and reads only that snapshot.

`--from-export` is also accepted. It scans a zip or directory for JSON
objects with a `modelType` and classifies them. That path is unverified.

`extract` reads design-time objects (projects, folders, data flows, tasks,
pipelines) **and** runtime objects (applications, published objects,
schedules, task schedules, the last successful task run). What runs in
production is the published version, which can differ from the design-time
copy.

## Snapshot layout

```
snapshot/
  manifest.json          workspace id/name, region, extracted_at, counts, tool version
  workspace.json
  projects/<key>.json    folders/<key>.json
  data_assets/<key>.json connections/<key>.json
  data_flows/<key>.json  tasks/<key>.json      pipelines/<key>.json
  function_libraries/<key>.json  user_defined_functions/<key>.json
  applications/<key>.json
  published_objects/<application-key>/<key>.json
  schedules/<application-key>/<key>.json
  task_schedules/<application-key>/<key>.json
  task_runs/<application-key>/<task-key>.json   last SUCCESS run per published task
```

## Output layout

```
out/
  report.json            one row per migrated object: kind, status, findings, artifacts
  REVIEW.md              everything a human must look at, grouped by object
  notebooks/<project>/<name>.ipynb
  jobs/<name>.job.json   AIDP job body; review notes travel beside it in <name>.review.md
  ddl/00_setup.ipynb     CREATE SCHEMA / CREATE TABLE … USING DELTA, plus the watermark table
  ddl/setup.sql          the same DDL as plain SQL
  watermarks/seed.sql    INSERT of the last-known DI watermark per incremental source
  fallback/<id>.json     work orders (+ <id>.py responses once written)
  reconcile/<name>.ipynb row-count / checksum comparison, legacy DI target vs new Delta table
  sources.md             every source the notebooks read, and how AIDP must expose it
```

Object statuses: `ok`, `needs_review` (converted, with review findings),
`fallback_pending` (contains an unfilled work order), `llm_assisted`
(contains LLM-written code that passed validation), `manual` (no faithful
equivalent; a stub with a rebuild note) and `failed`.

## Rules every component follows

- **Never guess silently.**
  - A value that can't be derived is omitted and becomes a named finding.
  - Examples: a schedule with no exact Quartz form, a pipeline Decision
    node, a join whose duplicate-column handling depends on runtime UI
    state.
- **Jobs are created PAUSED**, even when the schedule converted exactly.
- **`publish` never overwrites.**
  - An existing notebook path or job name is skipped.
  - A job is refused if any of its notebooks was not uploaded by this
    migration, in this run or an earlier one recorded in `publish.json`.
  - Notebooks go first, jobs second.
- **Jobs need a cluster.** AIDP rejects a job task without `cluster` (400
  `tasks[i].cluster must not be null`, live 2026-10-09). Without
  `--cluster-key`, `publish` uploads the notebooks and defers the jobs.
- **Generated notebooks need nothing installed on the cluster.**
  - The few runtime helpers (the parameter shim, SQL literal quoting,
    watermark get/set, Oracle-function shims) are inlined in a "runtime"
    cell.
- **Delta write semantics** follow the DI write mode:

| DI write mode | Delta write |
|---|---|
| APPEND | `mode("append")` |
| OVERWRITE | `mode("overwrite")` |
| MERGE | `DeltaTable.forName(...).merge(...)` on the merge key: update all when matched, insert all when not |
| IGNORE | `mode("ignore")` (an assumption, reported) |

## Testing

- **Unit tests** cover each layer: snapshot loading, graph, expressions,
  every operator emitter, schedules, pipelines, fallback validation and
  splicing, verify, and the publish plan.
- **Execution tests**, when PySpark is installed, run every generated
  fixture notebook end to end on a local Spark with a Delta catalog and
  assert on the **result rows**, not on the emitted text.
- **The fixture snapshot** `tests/fixtures/snapshot_sales` is hand-written
  to the SDK model shapes. It is the stand-in until a real DI workspace is
  available. When one is, run `extract` against it, add the snapshot as a
  second fixture, and fix whatever it breaks.
