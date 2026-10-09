---
name: ocidi-migrator-overview
description: Router skill. Read first whenever the user mentions migrating OCI Data Integration (OCI-DI, "DIS", data flows, integration tasks, data loader tasks, DI pipelines, DI schedules, DI applications) onto Oracle AI Data Platform (AIDP). Lays out the fixed verb sequence (extract -> analyze -> migrate -> fallback -> verify -> provision -> publish -> reconcile), which ocidi-* skill owns each step, the standing rules every step follows, and what the tool honestly cannot do yet. Adds no API surface of its own.
---

# `ocidi-migrator-overview` — router

`ocidi2aidp` moves OCI Data Integration *logic* onto AIDP. OCI-DI stores no
data of its own; it moves data between data assets. So what migrates is:

| OCI-DI | becomes on AIDP |
|---|---|
| Data flow + Integration task, Data Loader task | a PySpark notebook (one per task) |
| Pipeline (Pipeline task) | an AIDP job: tasks with `dependsOn` / `runIf` |
| Published task with no pipeline | a single-task AIDP job |
| Schedule + task schedule | the job's Quartz schedule + IANA timezone, created **PAUSED** |
| Target data entity | a **managed Delta table** in catalog `ocidi_migrated` (default) |
| Source data asset | an AIDP external catalog / `oci://` path / AIDP credential (JDBC) |
| Incremental load, `SYS.LAST_LOAD_DATE` | `ocidi_control.watermarks` Delta table, seeded from the last DI run |
| SQL task / REST task / OCI Data Flow task | a notebook (LLM work order / compiled call / manual stub) |

Default AIDP workspace: **`ocidi_migrated`**, created by `provision` if missing.

## How to run the engine

The Python engine ships inside this plugin. Every skill calls it the same way:

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp <verb> [args]
```

(`pip install -e "${CLAUDE_PLUGIN_ROOT}"` also installs an `ocidi2aidp` command.)

## The fixed order — pick the skill for the step

```
extract ─▶ analyze ─▶ migrate ─▶ fallback ─▶ verify ─▶ provision ─▶ publish ─▶ reconcile
 live,      offline    offline    Claude     offline    AIDP,        AIDP,       AIDP job
 read-only                        in-session            dry run      dry run     the user runs
```

| User says | Skill |
|---|---|
| "set up", "what do I need", first run, auth errors | [`ocidi-migrator-bootstrap`](../ocidi-migrator-bootstrap/SKILL.md) |
| "pull / export / read our OCI-DI workspace" | [`ocidi-extract`](../ocidi-extract/SKILL.md) |
| "what would this involve", "inventory", "how hard" | [`ocidi-analyze`](../ocidi-analyze/SKILL.md) |
| "migrate", "convert", "generate the notebooks/jobs" | [`ocidi-migrate`](../ocidi-migrate/SKILL.md) |
| "fill the work orders", "fix the unconverted parts", REVIEW REQUIRED stubs | [`ocidi-fallback`](../ocidi-fallback/SKILL.md) |
| "is the output OK", "check before publishing" | [`ocidi-verify`](../ocidi-verify/SKILL.md) |
| "create the workspace / catalog", "set up AIDP for this" | [`ocidi-provision`](../ocidi-provision/SKILL.md) |
| "push to AIDP", "upload", "create the jobs" | [`ocidi-publish`](../ocidi-publish/SKILL.md) |
| "does the data match", "cutover", "seed watermarks" | [`ocidi-reconcile`](../ocidi-reconcile/SKILL.md) |

No live OCI-DI? Everything from `analyze` on runs from a snapshot directory;
`${CLAUDE_PLUGIN_ROOT}/tests/fixtures/snapshot_sales` is a complete example workspace to demo with.

## Standing rules (every step)

1. **Deterministic first; the LLM only fills work orders.** `migrate` never
   asks a model anything. What it cannot translate becomes
   `fallback/<id>.json` plus a stub that raises `NotImplementedError` — a
   notebook with an unfilled stub fails loudly, it never writes wrong rows.
2. **Never guess silently.** Anything the compiler had to decide is a
   finding in `REVIEW.md` (`info` / `assume` / `review` / `fallback` /
   `manual`). Read `REVIEW.md` back to the user; don't summarise it as "done".
3. **No credentials in artifacts.** Sources are catalogs, `oci://` paths, or
   AIDP credential *names* read with `aidputils.secrets.get` at run time.
   Never paste a secret into a notebook, config, or chat.
4. **Writes to AIDP are dry runs until `--apply`**, and `--apply` needs the
   user's explicit go-ahead *after* they have seen the dry-run list.
   `publish` never overwrites. Jobs are created PAUSED.
5. **Published beats design-time.** `extract` reads applications' published
   objects; `migrate` compiles the published copy when one exists.

## What it does not do (say so before promising)

- **Unverified against a real OCI-DI workspace.** JSON shapes come from the
  OCI Python SDK models (2.165.1) and docs; the fixture is hand-built. The
  first real `extract` will surface shape differences — fix them, add that
  snapshot as a test fixture. See `${CLAUDE_PLUGIN_ROOT}/references/known-limitations.md`.
- Does not move historical data. It migrates the pipelines; history in the
  old DI targets stays put (reconcile compares against it).
- Pipeline **Decision** operators, **OCI Function** operators, **OCI Data
  Flow** tasks, **Fusion BICC/BIP** sources: no faithful automatic form —
  reported `manual` (or a work order an LLM may decline).
- Does not run jobs. Running and unpausing are the user's steps.
