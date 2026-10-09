---
name: ocidi-provision
description: Make sure the AIDP side of an OCI Data Integration migration exists - resolve the AIDP instance (DataLake) by OCID or display name, then find or create the target workspace (default ocidi_migrated) and the INTERNAL target catalog (default ocidi_migrated). Dry run by default; --apply creates only what is missing. Use when the user asks to create the migration workspace or catalog, set up AIDP for the migration, or before the first publish.
---

# `ocidi-provision`

## 1. Dry run — always first

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp provision \
  [--instance-name <aidp-instance-name> | --instance-id <DataLake OCID>] \
  [--workspace-name ocidi_migrated] [--catalog ocidi_migrated]
```

Prints, per object, `exists` or `would-create`. Nothing is created.
Needs the `aidp` CLI even as a dry run (it lists workspaces and catalogs).
Resolving `--instance-name` uses OCI Search (needs `pip install oci`).

## 2. Apply — only after the user says yes to the dry-run list

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp provision --apply [--skip-catalog] \
  --save aidp-target.json
```

- Creates the workspace (`aidp workspace create`) and waits for ACTIVE (up
  to 15 minutes), and the catalog (`aidp catalog create`, INTERNAL).
- Reuses anything that already exists; never deletes or renames.
- `--save` writes the resolved instance/workspace keys to a local file
  (identifiers, not secrets — still keep it out of git).

## 3. What provision does *not* do

- **Schemas and tables**: created by `ddl/00_setup.ipynb`, which needs a
  cluster. Publish uploads it; run it once before the migrated jobs.
- **Clusters**: compute is billable and a deliberate choice — use the
  engineer-agent `aidp-cluster-ops` skill, then pass `--cluster-key` to
  publish.
- **Source access**: external catalogs for ADW/ATP/Oracle/MySQL sources, bucket
  policies, AIDP credentials for JDBC sources — listed in `out/sources.md`;
  set them up with the `aidp-table-management` / `aidp-credentials` skills.
