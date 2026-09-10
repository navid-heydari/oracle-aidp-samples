# Privacy

**Plugin:** `oracle-ai-data-platform-workbench-snowflake-migrator`

## Summary

This plugin **collects, stores and transmits no data to its authors or to
Oracle**. There is no telemetry, no analytics, no usage reporting and no
licence check. It is self-contained: the full engine ships under `engine/` as
plain Python and runs locally, against **your** Snowflake account and **your**
Oracle AI Data Platform (AIDP) tenancy.

## What runs

| Component | What it is |
|---|---|
| `engine/` | Python modules invoked as `python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage>` |
| `skills/`, `commands/` | Markdown instructions for Claude Code. No code, no network access |
| `references/` | Markdown documentation |

Runtime dependencies are `snowflake-connector-python` and `cryptography`
(`engine/requirements.txt`). `pytest` is used for development only.

## Where it connects

Two destinations, both yours:

1. **Your Snowflake account** — over the official
   `snowflake-connector-python`. `engine/snowflake_source/conn.py` is the only
   module that opens a socket.
2. **Your OCI tenancy / AIDP instance** — never directly. The plugin shells out
   to the `aidp` CLI, or to the `oci` CLI, using the OCI configuration and keys
   **already on your machine**. It does not read, copy or transmit your OCI
   credentials.

Nothing else. No third-party service, no update check, no package download at
runtime.

## Credentials

- Snowflake secrets are read **from files you name by path** (`--key-path`,
  `--pat-path`, `--password-path`). They are never accepted as inline
  arguments, so they do not reach your shell history or a process listing.
- No credential is logged, printed, or written into any artifact.
- AIDP target coordinates (datalake OCID, workspace, cluster, catalog) are
  **not persisted by the plugin**. They are supplied per invocation and are
  held only for the life of the process. `engine/target/coords.py` contains no
  filesystem or environment access at all, and a test enforces that.

## What it does to your Snowflake account

**Reads only.** The transport refuses any statement that is not a read, so
nothing is written to or dropped from the source whatever your credential
permits (`engine/snowflake_source/conn.py`). It issues `SHOW`, `DESCRIBE`,
`SELECT` against `INFORMATION_SCHEMA`, `GET_DDL()`, and — only with
`--row-counts exact` — `SELECT COUNT(*)`, which returns an aggregate and no row
content.

**Your table data is never read.** No stage selects rows from a user table.

## What it writes to disk

Artifacts go to the `--out-dir` you choose, locally. They contain **metadata
about your estate, not its contents**:

- database, schema, table and view names; column names, types, precision and
  nullability; row counts; warehouse names and sizes
- **view SQL, verbatim** — a view definition can embed literal values, so
  treat these artifacts as sensitive as your schema
- generated Spark SQL DDL and an `.ipynb` notebook

No table rows appear in any artifact.

## What it does to your AIDP tenancy

Dry-run by default: `deploy` prints the statements and executes nothing unless
you pass `--execute` **and** supply the target coordinates. When it does
execute, it creates schemas, empty tables and views, and optionally uploads a
notebook. It never drops or alters an existing object — the DDL is
`CREATE ... IF NOT EXISTS`, and an object that already exists with a different
structure is reported and left untouched.

The one exception is `smoke --write-probe`, which creates a schema named
`snowmig_permission_probe` to prove write access and then drops that one
schema again. It is opt-in, never uses `CASCADE`, and skips the drop if the
schema was already there.

## Two things worth knowing

- **SQL is passed to the `aidp`/`oci` CLI as a command-line argument**, so
  while a statement runs it is visible to other users on the same machine via
  `ps`. That is DDL and object names, never row data. On a shared host, keep
  that in mind.
- **This is a Claude Code plugin.** The skills instruct Claude, and the reports
  it generates are read back into the conversation so Claude can summarise
  them. Your conversation — including estate metadata and view SQL that Claude
  reads — is handled under the terms of whichever Claude product you are using.
  That is a property of using an AI assistant, not something this plugin adds,
  but it is the honest answer to "where does this information go".

## Removal

Delete the plugin directory. It leaves nothing behind outside the `--out-dir`
you chose.
