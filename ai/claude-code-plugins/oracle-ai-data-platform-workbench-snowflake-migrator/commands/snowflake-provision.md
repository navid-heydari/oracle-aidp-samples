---
description: Provision the AIDP migration environment - workspace (name auto-translated), migration-assets cluster, cluster libraries, the backup-snowflake-migration/ folder with scripts and plan, and four parametrised migration jobs. Dry-run by default; --execute only after showing the plan.
---

# `/snowflake-provision`

Thin wrapper over
[`snowflake-provision-environment`](../skills/snowflake-provision-environment/SKILL.md).

1. Ask for the aiDataPlatform OCID and the workspace name **in this turn**;
   the external/target catalog names if already decided.
2. Dry-run `snowmig.py provision` and show `PROVISION.md` — including any
   name translation and the unverified-contract warning.
3. On the user's go-ahead, re-run with `--execute`.
4. Report each step's `verified` from `provision_result.json` — pending is
   pending, never rounded up.
5. Hand-off: read `workspace.key` and `cluster.key` from
   `provision_result.json` (`PROVISION.md` shows display names, not keys)
   and have the user put them in the config's `aidp:` block before
   `/snowflake-catalog`.
