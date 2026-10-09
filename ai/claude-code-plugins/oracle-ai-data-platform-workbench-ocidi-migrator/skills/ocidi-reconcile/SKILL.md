---
name: ocidi-reconcile
description: Cut an OCI Data Integration migration over safely - seed incremental watermarks from the last successful OCI-DI run, run the generated reconcile notebook that compares every legacy DI target (row counts, numeric column sums, keys present on one side only) with its new Delta table, and walk the parallel-run and schedule-switch checklist. Use when the user asks whether the migrated data matches, how to cut over, how to switch schedules, or about watermarks.
---

# `ocidi-reconcile` — prove it, then cut over

## 1. Seed watermarks (incremental sources only)

`out/watermarks/seed.sql` holds one `MERGE INTO <catalog>.ocidi_control.watermarks`
per incremental source, valued at the start of the last successful OCI-DI
run. Replace `{catalog}` and run it in an AIDP SQL cell **before** the first
migrated run; without it the first run is a full load (and an APPEND target
gets duplicates). Rows a DI run was still reading at that instant are
re-read, never skipped.

## 2. Parallel run and compare

1. Let OCI-DI and the AIDP job process the same input window.
2. Run `<notebook_root>[/<prefix>]/_reconcile/reconcile.ipynb` (default
   `/Workspace/ocidi/_reconcile/reconcile.ipynb`). For each target it
   prints `MATCH`, `DIFF` (row counts, numeric sums, `keys_only_new` /
   `keys_only_legacy`) or `ERROR`. The legacy side is read through the same
   `SRC_<ASSET>` catalog parameter the source reads use — the old DI target
   must be reachable through that external catalog.
3. Only aggregates are computed; no rows leave the cluster.

A `DIFF` on an `llm_assisted` notebook means read its LLM-written cell first.
A `DIFF` on an OVERWRITE target fed by an incremental source (finding
`TG08`) is expected if the windows differ.

## 3. Switch

Unpause the AIDP job schedule and **pause the OCI-DI task schedule in the
same change window**. Keep the DI application until a full cycle reconciles.
