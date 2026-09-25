---
description: Generate the shallow-clone migration notebook and place it in the AIDP workspace, ready for you to execute.
---

# `/snowflake-notebook`

Thin wrapper over [`snowflake-clone-notebook`](../skills/snowflake-clone-notebook/SKILL.md).

1. Require `ddl_plan.json`; run `/snowflake-plan` then the ddl stage first.
2. Generate locally and summarise what it will create.
3. Confirm the four AIDP coordinates in this turn — from the config's `aidp:`
   block, printed back to the user, plus whatever it does not carry — then
   upload.
4. **Ask before executing.** Report progress from the notebook's own per-object output.
