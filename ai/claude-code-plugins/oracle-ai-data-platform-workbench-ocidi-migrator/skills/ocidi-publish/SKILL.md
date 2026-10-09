---
name: ocidi-publish
description: Upload a finished OCI Data Integration migration into the AIDP workspace (default ocidi_migrated) - the setup DDL notebook, every migrated notebook and the reconcile notebook under /Workspace/ocidi, then the AIDP jobs with their PAUSED schedules. Dry run by default; --apply never overwrites an existing notebook or job, uploads notebooks before jobs, and refuses any job whose notebooks did not upload. Use when the user wants to publish, deploy, push to AIDP, upload or create the jobs for an OCI-DI migration (an ocidi-migrate output directory).
---

# `ocidi-publish`

## 1. Preconditions

- `ocidi-verify` shows no FAIL.
- The workspace exists (`ocidi-provision`).
- **A cluster key to create jobs**: a USER cluster in that workspace
  (`aidp cluster list <workspace-key>`). AIDP rejects a job task without one
  (400 `tasks[i].cluster must not be null`), so without `--cluster-key` the
  notebooks upload and every job is deferred. Re-running later with
  `--cluster-key` skips the notebooks already uploaded and creates the jobs.

## 2. Dry run — show the user the list

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp publish out/ [--prefix <name>]
```

Read back: the notebooks (with their status — `fallback_pending` and
`manual` notebooks raise at their first unconverted step), the jobs and their
schedules. `--prefix` puts everything under `/Workspace/ocidi/<prefix>/` and
prefixes job names, so two people can publish into one workspace.

## 3. Apply — only after an explicit yes

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp publish out/ --apply \
  [--cluster-key <key>] [--prefix <name>]
```

Writes `out/publish.json` (uploaded / skipped / created / refused / errors).
Report every skipped and refused item and why.

## 4. After publishing (the user's steps — say them)

1. Run `<notebook_root>[/<prefix>]/_setup/00_setup.ipynb` (default
   `/Workspace/ocidi/_setup/00_setup.ipynb`) once (schemas, tables,
   watermark table).
2. Seed watermarks from `out/watermarks/seed.sql` if any source is incremental.
3. Run each job once by hand; then [`ocidi-reconcile`](../ocidi-reconcile/SKILL.md).
4. Only then unpause schedules — and pause the OCI-DI task schedules at the
   same time, or both systems will load the same data.
