---
name: snowflake-clone-notebook
description: Generate the shallow-clone migration script as an executable AIDP notebook and place it in the AIDP workspace, ready for the user to run. The notebook creates schemas, then tables, then views in dependency order, prints per-object progress with elapsed time so a long run stays visible, and verifies each object individually at the end. Creates structure only and copies no data - every table arrives with zero rows. Use when the user wants the migration delivered as a runnable notebook rather than executed straight from the CLI.
---

# Shallow-clone notebook

Two steps: generate, then upload. Executing it is the user's action, not yours.

**The notebook creates empty structure and moves no data.** Every table it
creates has its columns and zero rows. A user who hears "clone" may expect rows —
say this before they run it.

## 1. Generate (offline, safe)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py notebook --out-dir ./snowmig_out \
  [--catalog <catalog>]
```

Writes `snowmig_shallow_clone_<catalog>.ipynb` locally plus `NOTEBOOK.md`.
One notebook per catalog — bronze mirrors the source, so a multi-database estate
has several.

## 2. Upload to the AIDP workspace

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py notebook --out-dir ./snowmig_out \
  --upload --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat> \
  [--dry-run]
```

`--dry-run` prints the exact upload command without running it. Ask the user for
the four coordinates in this turn; nothing is stored.

Lands at `/Workspace/Shared/snowmig_shallow_clone_<catalog>.ipynb`. **Notebooks
live in the workspace filesystem, not in a data catalog** — catalogs hold tables
and views. Say that if the user expects to find it under a catalog.

## 3. Execution is the user's call

**Do not run it for them.** Ask, then either let them run it in AIDP or invoke
`aidp notebook run` after they agree.

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
