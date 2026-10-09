---
argument-hint: "[snapshot-or-export] [out-dir]"
description: Guided OCI Data Integration -> AIDP migration - extract (or use a snapshot/export), analyze, migrate, fill LLM work orders, verify; stops before anything is written to AIDP.
---

# `/ocidi-run`

Follow [`ocidi-migrator-overview`](../skills/ocidi-migrator-overview/SKILL.md)'s
fixed order, one step at a time, reporting after each:

1. **Input.** Ask whether there is a live OCI-DI workspace (→
   [`ocidi-extract`](../skills/ocidi-extract/SKILL.md)), an existing snapshot
   or export directory, or none (→ demo with
   `${CLAUDE_PLUGIN_ROOT}/tests/fixtures/snapshot_sales`).
2. **Analyze** ([`ocidi-analyze`](../skills/ocidi-analyze/SKILL.md)) and read
   the HIGH / MEDIUM objects and the source prerequisites back to the user.
3. **Migrate** ([`ocidi-migrate`](../skills/ocidi-migrate/SKILL.md)) into an
   output directory the user names (default `out/`). Summarise `REVIEW.md`.
4. **Fallback** ([`ocidi-fallback`](../skills/ocidi-fallback/SKILL.md)) if
   there are work orders: one `ocidi-fallback-author` agent per order, then
   `fallback apply`. List every recorded assumption.
5. **Verify** ([`ocidi-verify`](../skills/ocidi-verify/SKILL.md)).
6. **Stop.** Show the `publish` dry run and ask whether to provision and
   publish (→ `/ocidi-deploy`). Nothing is written to AIDP by this command.
