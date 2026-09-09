---
name: snowflake-assess-estate
description: Read-only investigation of a Snowflake environment. Lists every table and view with exact row counts, compressed byte sizes, full column types including numeric precision and scale, and the identifier case form of each object, then halts if two objects differ only by case. Use when the user asks what is in a Snowflake account, wants an inventory or estate assessment, asks how big the tables are, or before planning any migration.
---

# Stage 1 — assess the estate

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py assess \
  --account <org>-<account> --user <user> --auth <method> [--key-path ...] \
  [--warehouse <wh>] [--database DB]... --out-dir ./snowmig_out
```

Omit `--database` to scan every non-system database. Repeat it to scope.
Ask the user which databases to scan if the account is large — `count(*)` per
object is exact but costs warehouse time.

## Reading the result

`INVENTORY.md` is for the user; `inventory.json` feeds stage 2. Present:

- object counts by type, and total rows and bytes
- the largest objects
- anything with `compatibility_status: blocked` and why
- views, noting they are inventoried but **not translated** in MVP-1

## Three things to say out loud

1. **Row counts are exact**, from in-session `count(*)` — not `SHOW TABLES`
   estimates. Sizes are Snowflake's compressed bytes, which are not the size the
   data will occupy as Delta.
2. **Exit code 3 means HALT** on an identifier-case collision. Show the colliding
   names and stop. Snowflake treats `ORDERS` and `"orders"` as different objects;
   Spark folds to lower and would merge them, losing data with no error.
3. **`extraction_notes` is not decoration.** If it is non-empty, some scope could
   not be read, and absence from the inventory is not evidence the object does not
   exist. Say which scopes failed.
