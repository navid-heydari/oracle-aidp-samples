---
name: snowflake-medallion-clone
description: Create a medallion architecture on Oracle AI Data Platform and start the soft clone - generating Spark/Delta CREATE TABLE DDL from an approved Snowflake migration plan and, only after explicit confirmation, creating the bronze/silver/gold schemas and empty managed Delta tables in an INTERNAL AIDP catalog. Schema only; moves no data and translates no views. Use when the user asks to create the medallion structure, soft clone, or deploy the target schema.
---

# Stage 3 — medallion structure and soft clone

Two phases, deliberately separate: generate, then deploy.

## Phase A — generate the DDL (offline, safe)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py ddl --out-dir ./snowmig_out
```

Show the user `DDL_PLAN.md`: the SQL, the rule applied to each transformation,
every dropped property, and everything blocked. Nothing has touched AIDP.

## Phase B — deploy (requires coordinates AND confirmation)

**You must ask the user for these in this turn. They are not stored anywhere, and
there is no default to fall back on:**

- DataLake OCID
- workspace
- cluster id (must be ACTIVE)
- target catalog — must be **INTERNAL**

Then confirm explicitly what will be created, and only then:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deploy --out-dir ./snowmig_out \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>
```

Without `--execute` this is a dry run and creates nothing.

## Rules

1. **Never run `--execute` on the strength of an earlier approval.** Ask the user
   in the turn you run it. If they approved a plan yesterday, ask again.
2. **INTERNAL catalogs only.** ADB/ADW/ALH appear in AIDP as EXTERNAL, read-only
   JDBC catalogs and cannot hold a managed Delta table. If the user names one,
   explain that rather than trying.
3. **Report `verified`, not `executed`.** A batch can report success while
   statements inside it failed, so every table is probed individually afterwards.
   The honest number is `verified/total`. Never say "created" on the strength of
   `executed`.
4. **Empty tables.** Say clearly that these have zero rows and that data movement
   is a later phase.
5. **No `CREATE OR REPLACE`, no `DROP`.** Existing tables are left alone; a 409
   means "already exists", not a failure.
6. **The live deploy path is unverified.** `engine/target/aidp_runner.py` has
   never run against a real AIDP cluster. Warn the user the first time, and treat
   an unexpected error there as a bug to report rather than a problem with their
   estate.
