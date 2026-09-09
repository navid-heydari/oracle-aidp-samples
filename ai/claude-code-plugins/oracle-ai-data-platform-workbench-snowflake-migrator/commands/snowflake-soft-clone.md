---
description: Create the medallion structure and empty Delta tables on AIDP from an approved plan. Generates DDL first; deploys only after you supply target coordinates and confirm.
---

# `/snowflake-soft-clone`

Thin wrapper over [`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Require an approved `plan.json`.
2. Generate DDL and show `DDL_PLAN.md`. Nothing has touched AIDP.
3. Ask for the DataLake OCID, workspace, cluster id and INTERNAL catalog **in this turn**.
4. Confirm what will be created, then deploy with `--execute`.
5. Report `verified/total`, never `executed`.
