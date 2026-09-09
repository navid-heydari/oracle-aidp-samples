---
name: snowflake-migration-plan
description: Build a high-level Snowflake to AIDP migration plan and present it for approval. Derives a dependency graph from ACCOUNT_USAGE.OBJECT_DEPENDENCIES or, where that privilege is unavailable, by parsing view DDL; orders objects into topological waves so dependencies land first; proposes a medallion bronze/silver/gold assignment; and reports cycles, blocked objects and unsupported features. Use after an estate assessment, or when the user asks in what order to migrate, what depends on what, or wants to see the migration plan.
---

# Stage 2 — dependencies and plan

Two commands. The first needs Snowflake; the second is offline.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deps \
  --account <...> --user <...> --auth <...> [--key-path ...] --out-dir ./snowmig_out

python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py plan \
  --out-dir ./snowmig_out [--namespace-strategy layer-catalog] [--layer-map map.json]
```

## Lineage provenance matters — state it

`deps` prints its source. Report which one:

- `account_usage` — authoritative lineage across all object types
- `parsed_ddl` — the `ACCOUNT_USAGE` grant was unavailable, so edges come from
  parsing view DDL. **View→object edges only.** Tell the user the graph is
  partial and why; do not present it as complete lineage.

## Namespace strategy

Default `layer-catalog` → `bronze.<schema>.<table>`. It drops the source database,
so two databases sharing `schema.table` collide — the run halts with exit 3 if
that happens. Offer `preserve-source` (`<db>.<schema>.<table>`, never collides) or
`layer-flattened` (`bronze.<db>_<schema>.<table>`) when it does.

## Present for approval, do not just run

Walk the user through `MIGRATION_PLAN.md`:

1. the waves, and why the order is what it is
2. **`fallback_assignments`** — objects no naming rule matched, defaulted to
   BRONZE. These are the ones most likely wrong. Ask before accepting them; a
   `--layer-map` JSON of `{"SOURCE": "SILVER"}` overrides.
3. cycles, blocked objects, unsupported features
4. that `clone_targets` is tables only

Get explicit agreement on the layer assignment before stage 3.
