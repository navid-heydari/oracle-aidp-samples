---
description: Check Snowflake and AIDP connectivity and permissions before migrating. Read on the source, read/write on the destination.
---

# `/snowflake-smoke`

Thin wrapper over [`snowflake-smoke-test`](../skills/snowflake-smoke-test/SKILL.md).

1. Run against Snowflake first. Pass `--database` if the account has many.
2. If the user has AIDP coordinates, ask for all four and check the destination too.
3. Only pass `--write-probe` after telling them it creates a schema it cannot delete.
4. Present `SMOKE_TEST.md`. If the destination was skipped, say only the source was verified.
