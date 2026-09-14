---
description: Create the target catalog and medallion structure on AIDP from an approved plan. Registers an EXTERNAL/SNOWFLAKE catalog by default; a Standard catalog only if you explicitly ask, and then as a script run on AIDP compute.
---

# `/snowflake-soft-clone`

Thin wrapper over [`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Require an approved `plan.json`.
2. Ask for the DataLake OCID, workspace, cluster id, catalog name and the path
   to the YAML/JSON connection config **in this turn**.
3. Confirm, then register the **EXTERNAL/SNOWFLAKE** catalog with
   `snowmig.py catalog --execute`. It copies nothing and creates no tables.
4. **Only if the user explicitly asked for a Standard catalog:** generate DDL,
   show `DDL_PLAN.md`, then hand over the table-creation script via
   [`snowflake-clone-notebook`](../skills/snowflake-clone-notebook/SKILL.md) —
   it lands in `/Workspace/Shared/` and runs on AIDP compute.
5. Report the registered catalog, or `verified/total` for a Standard clone —
   never `executed`.
