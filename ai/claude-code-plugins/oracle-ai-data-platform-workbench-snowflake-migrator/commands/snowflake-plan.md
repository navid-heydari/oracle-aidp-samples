---
description: Build and present a high-level Snowflake to AIDP migration plan - dependency waves plus a proposed medallion layout - for approval.
---

# `/snowflake-plan`

Thin wrapper over [`snowflake-migration-plan`](../skills/snowflake-migration-plan/SKILL.md).

1. Require `inventory.json`; run `/snowflake-assess` first if absent.
2. Run `deps`, then `plan`. State the lineage source and whether it is partial.
3. Walk through the waves and the medallion assignment, calling out every fallback assignment.
4. Get explicit agreement before any clone.
