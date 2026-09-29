---
description: Register the target catalog on AIDP — EXTERNAL/SNOWFLAKE by default, a read-only pointer at the live source that copies nothing. Dry-run first; --execute only after showing the user exactly what will be registered.
---

# `/snowflake-catalog`

Thin wrapper over the registration phase of
[`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Let the CLI find the migration config and **repeat the `config:` line it
   prints**, plus the destination it resolved. Ask for any coordinate the file
   does not carry (DataLake OCID, workspace, cluster id, catalog name) **in
   this turn**. Ask permission before reading the config yourself: it holds
   live credentials. Never ask for a secret in the conversation.
2. Dry-run first: `snowmig.py catalog --out-dir ... --catalog <name>`. Show
   `CATALOG.md` — it lists the connection *field names*, never the values.
3. On the user's go-ahead, re-run with `--execute`. The destination must be
   confirmed in that same turn; a value sitting in the config is not an
   approval.
4. Report `action` and `verified` from `catalog_result.json` — never
   `executed`. `create_requested` means the create was accepted but the
   catalog never became visible: say it is pending, not done.
5. A **Standard** catalog is created here too, but only when the user
   explicitly asked for one, and only as the CONTAINER (runbook S4):
   `snowmig.py catalog --catalog <name> --catalog-type standard --execute`.
   `catalog_result.json` then carries `container_only: true` — pass that on,
   so nobody reads a created container as created structure. Its schemas and
   tables come later, at S10, from
   `snowmig.py run --job snowmig_01_structure` (one workflow per schema on
   AIDP compute), never through this command and never through the
   control-plane API, which can return `202 Accepted` and create nothing.
