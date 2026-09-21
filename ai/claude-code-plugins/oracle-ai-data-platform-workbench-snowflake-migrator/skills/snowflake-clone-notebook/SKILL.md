---
name: snowflake-clone-notebook
description: Generate the table-creation script for a Standard AIDP catalog as an executable notebook, place it in the workspace Shared directory, and run it on AIDP compute. The script creates schemas, then tables, then views in dependency order, prints per-object progress with elapsed time so a long run stays visible, and verifies each object individually at the end. This script creates structure only and copies no data - every table arrives with zero rows; rows are copied only by the snowmig_02_copy_schema job, when the operator runs it. Use when the user has explicitly asked for a Standard catalog, or wants the migration delivered as a runnable script rather than executed straight from the CLI.
---

# Standard-catalog table-creation script

**This is the Standard-catalog path, and only for a Standard catalog the user
explicitly asked for.** The default target is an EXTERNAL/SNOWFLAKE catalog,
which needs no tables at all — see `snowflake-medallion-clone` Phase A. Do not
reach for this skill just because a migration is in progress.

Why a script on compute rather than the control-plane API: it runs **inside AIDP
compute**, so every statement's success, failure and elapsed time appears in the
cluster's own output. The catalog CRUD API returns 202 Accepted over OCI/HTTP
and then fails silently, which is far harder to track down.

Three steps: generate, upload, run.

**The script creates empty structure and moves no data.** Every table it
creates has its columns and zero rows. A user who hears "clone" may expect rows —
say this before they run it.

## 1. Generate (offline, safe)

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig notebook \
  [--catalog <catalog>]
```

Writes `snowmig_shallow_clone_<catalog>.ipynb` locally plus `NOTEBOOK.md`.
One notebook per catalog — bronze mirrors the source, so a multi-database estate
has several.

## 2. Upload to the AIDP workspace

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig notebook \
  --upload --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>
```

`--upload` is a **dry run** without `--execute`: it prints where the notebook
would land and sends nothing. With `--execute` the upload is **refused**, and
the command says so: its only transport is the Jupyter contents API, which
the validated build answers with a 200 and then cannot read the file back
(GAPS.md 13), so an upload could not be reported honestly. The verified way to
create the structure is `snowmig provision --execute` (which places the stage
notebooks in the workspace) followed by `snowmig run --job
snowmig_01_structure`. Ask the user for the four coordinates in this turn;
nothing is stored.

Lands at `/Workspace/Shared/snowmig_shallow_clone_<catalog>.ipynb` — the
**shared** directory on purpose, so it can be re-run, read and debugged
independently of this plugin and of the conversation that generated it.
**Notebooks live in the workspace filesystem, not in a data catalog** — catalogs
hold tables and views. Say that if the user expects to find it under a catalog.

## 3. Execution is the user's call

**Do not run it for them.** Ask, then let them run it in the AIDP workspace
after they agree.

The notebook is built to be watched: each object prints
`[3/7] D.S.ORDERS ... ok (0.4s)`. If a run is slow, that output is where the
progress is — report the last line rather than guessing. The final cell prints
`verified N/M` plus any missing object.

## What the notebook does and does not do

- Creates schemas → tables → views, in that order, views after the tables they read
- `IF NOT EXISTS` throughout, so re-running is safe
- **No `INSERT`, `COPY INTO`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE` or `DROP`.**
  Its metadata records `moves_data: false`
- Tables arrive **empty**. Say this out loud — a user seeing "clone" may expect rows
- Blocked objects appear in markdown only, never in a code cell, so it cannot
  attempt them
