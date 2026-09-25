---
name: snowflake-provision-environment
description: Provision the migration environment inside AIDP for the prod flow - the workspace (name auto-translated to the simplest safe charset), the migration-assets compute cluster, optional cluster libraries from requirements-aidp.txt, the backup-snowflake-migration/ workspace folder holding the data-migration scripts and the plan artifacts, and four parametrised jobs (discover, structure, copy-schema, reconcile). Use when the user wants to set up AIDP for the migration, upload the migration scripts, create the migration workspace or cluster, or wire the migration jobs. Dry-run by default.
---

# Provision the migration environment

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig provision \
  --workspace-name "<name>" \
  [--cluster-name migration-assets] \
  [--external-catalog <registered EXTERNAL catalog>] \
  [--target-catalog <INTERNAL catalog>] \
  --datalake-ocid <aiDataPlatform OCID> \
  [--subnet-id <ocid>] [--maven <coords>] [--skip-libraries] \
  [--execute]
```

Dry run first, always. Show `PROVISION.md` — it lists every step that would
run, the **translated** workspace/cluster names with the reasons, and the
⚠️ unverified-contract warning — and only then, on the user's go-ahead,
re-run with `--execute`.

## What it does, in order

1. **Workspace** — CREATED, and poll until visible. The name passes through
   the simplest-charset translation (`[a-z0-9_]`, starts with a letter,
   accents folded) so a name the API might reject never reaches it; a rename
   is reported, never silent.

   **A name already in use is a COLLISION and the run STOPS.** Nothing is
   adopted. A migration creates its own environment so that everything it
   touches can be identified, audited and torn down as a unit; inheriting a
   stranger's workspace makes the blast radius unknowable. Report the
   collision and ask the user for another name. `--reuse-existing` opts back
   in, and it is never what you offer first.
2. **Cluster `migration_assets`** — CREATED, default small config; sizing is
   a later, explicit decision (`/snowflake-compute` proposes it). A taken
   name stops the run, exactly as for the workspace.
3. **Libraries** — only what `requirements-aidp.txt` enables (the default
   file enables NOTHING: the external-catalog path needs no extra library).
   Installing forces a cluster restart, per the AIDP doc.
4. **`backup-snowflake-migration/`** — the four data-migration scripts into
   `scripts/`, and whatever plan artifacts exist in `--out-dir`
   (`plan.json`, `ddl_plan.json`, `SUMMARY.md`, …) into `plan/`, each upload
   read back before it is called done.
5. **Four jobs** — `snowmig_00_discover` → `03_reconcile`. Each runs one
   **self-contained stage notebook** whose own `PARAMS` cell carries the
   arguments, because job parameters reach a notebook neither as argv nor as
   environment (established live). No schedule: running one is always the
   user's call, and the PARAMS cell is editable in the console.

   `--stage-param NAME=VALUE` (repeatable) writes a value into the PARAMS
   cell of every stage that declares NAME — the stage flag without `--`,
   e.g. `schema=SALES`, `tables=ORDERS,LINES`, `dry-run=true`. A name no
   stage declares is refused, a switch takes `true`/`false`, and with
   `--reuse-existing` it needs `--refresh-notebooks`, because a notebook
   already on the workspace is otherwise kept as it is.

   A job is a **workflow**: logged, re-runnable, and its task output is
   exportable as evidence. Run one with `snowmig.py run --job <name>`, never
   by executing a notebook interactively — interactive execution leaves
   nothing behind and is not an acceptable record of a migration.

## Source mode — say which one is in play

`--source-mode connector` (the default) has the in-AIDP scripts read
Snowflake through the AIDP connector on the cluster. It is the path proven
live, it needs no extra cluster library, and it does not wait on the external
catalog's crawler — which on at least one deployment fails
(`CONNECTOR_0067, Login has timed out`) with credentials the connector
accepts. It needs `--source-config` so the credential reaches the workspace
mount: its `snowflake:` block is uploaded as JSON to
`plan/<config stem>.json` (the `aidp:` block is not copied), and because that
block carries a secret it is uploaded only when passed explicitly. A `*_path`
secret is refused before anything is uploaded — the path is not on the
cluster.

`--source-mode external-catalog` uses three-part names instead, and needs a
catalog whose crawl has actually succeeded. Check that first — an empty
`SHOW SCHEMAS IN <catalog>` means the crawler never populated it, which is
not the same as an empty database.

## Rules

- The EXTERNAL catalog is registered by `/snowflake-catalog`, not here —
  one writer per concern. Pass its name via `--external-catalog` so the jobs
  are born pointing at it.
- Report `verified` per step, never `executed`. `create_requested` means the
  API accepted and the object never became visible in the poll budget — say
  it is pending and point at the console. `name_taken` is neither: it means
  something of that name was already there and this run did **not** adopt it.
- **Hand-off.** After `--execute`, read `workspace.key` and `cluster.key`
  from `provision_result.json` and have the user put them under `aidp:` in
  `snowmig-config.yaml` (`aidp.workspace`, `aidp.cluster_id`) before
  `/snowflake-catalog`. `PROVISION.md` shows the display names, which are
  not the keys; the catalog step needs the keys, and `provision` does not
  write them back.
- **Never reuse, never "ensure".** Do not list existing workspaces or
  clusters and offer the user a choice among them. The only question is *may
  I create this*.
- Every REST shape here follows the documented 20260430 contract and is not
  yet live-verified; two field families are inferred (library items,
  per-task job fields). On the first live run, validate against a UI-created
  job/library before scaling out — and say so to the user.
- After provisioning, the run order is: `snowmig_00_discover`, then
  `01_structure`, then `02_copy_schema` once per schema, then
  `03_reconcile`. `MIGRATION_REPORT.md` in the reports folder is the
  plan-vs-reality deliverable.
