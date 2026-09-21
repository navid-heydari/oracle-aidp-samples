---
name: snowflake-assess-estate
description: "Read-only PREVIEW of a Snowflake environment from the operator's machine - inventory of tables and views with row counts, byte sizes and column types, plus the census of objects that are not tables or views, table-maintenance state and security posture. Use ONLY when the user wants to look at an account without migrating it - answering what is in there, how big the tables are, what the security posture looks like. This is NOT the discovery step of a migration: a migration discovers inside AIDP as a workflow (runbook S6), because a laptop-side read leaves no log and no evidence on the platform. If the user asked to migrate, route to snowflake-migrator-overview and follow S1 through S12."
---

# Preview the estate — from the operator's machine

> **This is not the migration's discovery step.** A migration discovers the
> estate **inside AIDP, as a workflow** (`snowmig.py run --job
> snowmig_00_discover`, runbook S6), reading through the AIDP connector: two
> `INFORMATION_SCHEMA` queries for the whole database, a manifest backed up
> in the workspace, and a job run somebody can audit afterwards.
>
> What follows reads Snowflake from the laptop. It is the right tool for
> *"what is in this account?"* and the wrong one for *"migrate this
> account"* — it leaves no workflow, no log and no evidence inside AIDP, and
> it does not scale the way the in-AIDP path does.
>
> If the user asked to migrate, stop here and follow
> `snowflake-migrator-overview` from S1.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig assess \
  [--database DB]... \
  [--row-counts metadata|exact|none] \
  [--semi-structured block|string] [--geospatial block|string] 
```

Every Snowflake coordinate comes from the migration config (`snowmig-config.yaml`, discovered automatically and printed as `config: <path>`). Pass `--account/--user/--auth/...` only to override a field for one run.

Omit `--database` to scan every non-system database. Repeat it to scope.

## Row counts — pick deliberately

| Mode | What it does | Cost |
|---|---|---|
| `metadata` (default) | Reads Snowflake's maintained row count from `SHOW`. Views get no count. | Free |
| `exact` | `COUNT(*)` per object, views included | **Executes every view.** Warehouse time, per object |
| `none` | No counts at all | Free |

**Do not reach for `exact` by reflex.** On a table the maintained count already
agrees with `COUNT(*)` for settled data, so `exact` buys nothing. On a view
there is no stored count, so counting means *running the view* — on a wide join
that is minutes of warehouse time each, spent during what the user asked to be
an assessment. Offer `exact` when the user needs a verified number, and say
what it will cost first.

## Semi-structured columns

`VARIANT`, `OBJECT` and `ARRAY` **block their table by default**, because a
typed struct/map/array target is a design decision to make with the customer,
not one to guess. That is the right default and the wrong dead end: if the
estate is full of them, offer `--semi-structured string` to carry the JSON as
text, and be explicit that this defers the problem rather than solving it —
nothing on the target can address a field inside the string. `GEOGRAPHY` and
`GEOMETRY` have their own switch, `--geospatial`, because they are a separate
decision.

## Reading the result

`INVENTORY.md` is for the user; `inventory.json` feeds stage 2. Present:

- object counts by type, and total rows and bytes
- the largest objects
- anything with `compatibility_status: blocked` and why
- views, noting their SQL is captured verbatim and translated only as far as
  the dialect rules go — see `snowflake-migration-plan`

## Four things to say out loud

1. **Say where a row count came from.** `row_count_source` is
   `show_metadata`, `count_query`, `not_counted` or `error`. A metadata count is
   Snowflake's own and is normally right, but it can lag very recent DML and is
   not maintained for external tables — so do not call it verified. Never
   present a blank count without its reason.
2. **Sizes are Snowflake's compressed bytes**, which are not the size the data
   will occupy as Delta.
3. **Exit code 3 means HALT** on an identifier-case collision. Show the
   colliding names and stop. Snowflake treats `ORDERS` and `"orders"` as
   different objects; Spark folds to lower and would merge them, losing data
   with no error.
4. **`extraction_notes` is not decoration.** If it is non-empty, some scope
   could not be read, and absence from the inventory is not evidence the object
   does not exist. Say which scopes failed. Row-count failures land here too.

## Three companion stages — run them, do not skip them

```bash
# what is NOT a table or a view. Runs inside `assess` by default -> CENSUS.md
# (pass --no-census to skip, and the coverage claim then says so)

${CLAUDE_PLUGIN_ROOT}/bin/snowmig maintenance \
  --account <...> --user <...> --auth <...> [--key-path ...] [--history-days 30]

${CLAUDE_PLUGIN_ROOT}/bin/snowmig security \
  --account <...> --user <...> --auth <...> [--key-path ...]
```

### The census — say what was *not* examined

`assess` used to look at tables and views only, so "N of N objects can move"
was true of what had been examined and overstated the estate. `CENSUS.md` now
counts procedures, UDFs, tasks, streams, materialized and dynamic tables,
stages, pipes, sequences and file formats. **None of them migrate**, and no
equivalent is generated.

Two things to carry to the user:

- **A task that populates a migrated table means that table stops being
  populated after cutover.** The clone succeeds and then goes stale. This is
  the single most damaging thing in the census.
- Procedures and UDFs come with a **language** and an effort band. JavaScript
  is the hardest — there is no JavaScript runtime on AIDP, so the logic has to
  be understood and rewritten, not translated.

### Security — the only stage with an exposure consequence

A masked column arrives **unmasked**; a row-access policy simply is not there;
a secure view loses SECURE. The clone does not fail — it **succeeds without
the protection**, so anyone who can read the target sees what Snowflake was
hiding.

If `exposure_count` is `null`, `ACCOUNT_USAGE` could not be read: the answer is
**unknown, not zero**. Say that plainly and ask for the grant. Never let a
denied probe read as a clean bill of health.
