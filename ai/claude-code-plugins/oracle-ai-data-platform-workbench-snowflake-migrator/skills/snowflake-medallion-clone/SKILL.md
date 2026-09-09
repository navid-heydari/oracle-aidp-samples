---
name: snowflake-medallion-clone
description: Create the medallion architecture on Oracle AI Data Platform and run the shallow clone - generating Spark SQL for schemas, tables and views from an approved Snowflake plan and, only after explicit confirmation, creating them in an INTERNAL AIDP catalog through the aidp or oci CLI. Structure only; copies no data. Use when the user asks to create the medallion structure, soft clone, shallow clone, or deploy the target schema.
---

# Stage 3 — medallion structure and shallow clone

Two phases, deliberately separate: generate, then deploy.

## Phase A — generate the DDL (offline, safe)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py ddl --out-dir ./snowmig_out
```

Show `DDL_PLAN.md`: the SQL, the rule behind each transformation, dropped
properties, and everything blocked. Statements come out in wave order, so a view
always follows the tables it reads. Nothing has touched AIDP.

## Phase B — deploy (coordinates AND confirmation, in this turn)

**Ask the user for these now. They are stored nowhere and there is no default:**

- DataLake OCID
- workspace
- cluster id (must be ACTIVE)
- **target catalog** — must be **INTERNAL**, and see the scoping rule below

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deploy --out-dir ./snowmig_out \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>
```

Without `--execute` this is a dry run and creates nothing.

## Rules

1. **One catalog per run.** Bronze mirrors the source, so a multi-database estate
   spans catalogs. A run deploys only `--catalog` and reports the rest as out of
   scope. Deploying another catalog is another explicit confirmation.
2. **Catalogs are not created by the migrator.** `PLANNED_OBJECTS.md` lists the
   catalogs needed. Have the user create them, or confirm they exist and are
   INTERNAL. The migrator creates schemas, tables and views only.
3. **Never `--execute` on an earlier approval.** Ask in the turn you run it.
4. **INTERNAL catalogs only.** ADB/ADW/ALH are EXTERNAL read-only JDBC catalogs in
   AIDP and cannot hold managed Delta tables. If the user names one, explain
   rather than trying.
5. **Report `verified`, never `executed`.** A batch can report success while
   statements inside it failed, so every object is probed individually. The honest
   number is `verified/total`.
6. **Say the objects are empty.** This is a structural clone: schemas, tables and
   views with no rows. Data movement is a later phase.
7. **No `CREATE OR REPLACE`, no `DROP`.** Existing objects are left alone; a 409
   means "already exists", not a failure.
8. **Silver/Gold jobs are not created here.** The plan defines them, disabled and
   never triggered. Creating them on AIDP is a separate step the user asks for.

## Backend

The engine uses the `aidp` CLI when installed and falls back to `oci
raw-request`. It prints which one it chose. If neither CLI is present it fails
loudly rather than guessing a transport. `oci ai-data-platform` covers only the
control plane, which is why the data plane goes through `raw-request`.
