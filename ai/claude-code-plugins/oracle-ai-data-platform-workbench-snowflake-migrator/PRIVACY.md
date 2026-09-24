# Privacy

**Plugin:** `oracle-ai-data-platform-workbench-snowflake-migrator`

## Summary

This plugin **collects, stores and transmits no data to its authors**. There
is no telemetry, no analytics, no usage reporting and no licence check. It is
self-contained: the full engine ships under `engine/` as plain Python and
runs locally, against **your** Snowflake account and **your** Oracle AI Data
Platform (AIDP) tenancy — and, past `provision`, as notebooks on **your**
AIDP compute.

Two things do leave your machine, and only when you pass `--execute`: the
**Snowflake credential**, which travels to your AIDP tenancy so the data
plane can reach Snowflake from there, and the **migration plan**. This
document says exactly how. Table rows never pass through the operator's
machine.

## What runs

| Component | What it is |
|---|---|
| `engine/` | Python modules invoked as `python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage>` — the control plane, on your machine |
| `data-migration-scripts/` | Generated `.ipynb` notebooks that `provision` uploads and that run **inside AIDP**, on your cluster — the data plane |
| `skills/`, `commands/` | Markdown instructions for Claude Code. No code, no network access |
| `references/` | Markdown documentation |

Runtime dependencies are `snowflake-connector-python`, `cryptography` and
`pyyaml` (`engine/requirements.txt`). `pytest` is used for development only.
The notebooks need nothing installed on the cluster: the AIDP Snowflake
connector is built in.

## Where it connects

Two destinations, both yours:

1. **Your Snowflake account** — from your machine over the official
   `snowflake-connector-python` (`engine/snowflake_source/conn.py` is the
   only module that opens a socket), and from your AIDP cluster over the
   AIDP Snowflake connector when the data-plane notebooks run.
2. **Your OCI tenancy / AIDP instance** — never directly. The plugin shells
   out to the `aidp` CLI, or to the `oci` CLI (`oci raw-request` against the
   documented AIDP REST API), using the OCI configuration and keys **already
   on your machine**. Both speak HTTPS (TLS) to the OCI endpoint. It does not
   read, copy or transmit your OCI credentials.

Nothing else. No third-party service, no update check, no package download at
runtime.

## Credentials

- **The configuration is one file, and it holds the Snowflake credential in
  plain text.** `snowmig-config.yaml` carries the connection (`password:` or
  `private_key: |` inline) beside the AIDP destination. `key_path:` /
  `password_path:` remain as the way to keep the secret out of that file.
  Secrets are never taken as command-line flags, so they do not reach your
  shell history. The file is gitignored; `init-config` creates it readable
  by you alone where the OS has mode bits (Windows files inherit your
  profile's ACL instead).
- **An inline secret is spooled to a temp file for the life of one
  connection** (`tempfile.mkstemp(prefix="snowmig_secret_")` in
  `engine/snowmig.py`), restricted to the current user where the OS supports
  it, and removed in a `finally`.
- **Nothing local renders a secret.** `preflight` and every report pass the
  config through a redactor: an inline secret reads as *inline*, a `*_path`
  as the path. A config the YAML parser rejects is refused by position
  only, never quoted back.
- **Two paths transmit the Snowflake credential to your AIDP tenancy, both
  only with `--execute`:**
  1. `provision --execute --source-config <file>` reads that file and
     uploads a derived copy — the `snowflake:` block only, as JSON — to the
     workspace folder `backup-snowflake-migration/plan/<config stem>.json`
     (`source_config_payload` in `engine/target/provisioning.py`), so the
     data-plane notebooks can read it from the `/Workspace` mount. The
     `aidp:` block is not copied, and a config whose secret is a `*_path`
     field (`key_path`, `key_passphrase_path`, `password_path`, `pat_path`)
     is refused before anything is uploaded — the path names a file on your
     machine, which the cluster cannot see. It is uploaded only when you
     pass the flag; the diagnosis notebook `provision` uploads beside the
     scripts prints the config's shape (key names as set/unset) and the
     derived Snowflake host, never a secret. Who can read that folder is
     governed by AIDP workspace access, which the plugin does not set —
     treat the copy as readable by whoever can open the workspace.
  2. `catalog --execute` registers the EXTERNAL catalog with the credential
     in the request's `connectionDetails` (`SNOWFLAKE_PASSWORD` or
     `SNOWFLAKE_PRIVATE_KEY_CONTENT`, `engine/target/snowflake_catalog_connection.py`);
     `--test-connection` sends the same body to `testConnection`. Both bodies
     are spooled to a temp file the CLI reads by path — never an argv
     element — and travel over `aidp catalog create` / `oci raw-request`
     under TLS. AIDP then stores the credential as the catalog's connection.
- What follows from that, plainly: use a **dedicated, read-only Snowflake
  service user** for the migration; **rotate** its password or key when the
  migration is done; and delete
  `backup-snowflake-migration/plan/<config stem>.json` (for the default
  name, `plan/snowmig-config.json`) from the workspace once the data plane
  no longer needs it.
- **AIDP target coordinates** (datalake OCID, workspace, cluster, catalog)
  live in the `aidp:` block of the same config and are recorded in
  `PREFLIGHT.md` and the `*_result.json` artifacts, so a run is auditable.
  `engine/target/coords.py` itself contains no filesystem or environment
  access at all — it can only be handed a destination — and a test enforces
  that. A coordinate read from the file is printed before anything acts on it.

## What it does to your Snowflake account

**The control plane reads only.** The transport refuses any statement that is
not a read, so nothing is written to or dropped from the source whatever your
credential permits (`engine/snowflake_source/conn.py`). It issues `SHOW`,
`DESCRIBE`, `SELECT` against `INFORMATION_SCHEMA`, `GET_DDL()`, and — only
with `--row-counts exact` — `SELECT COUNT(*)`, which returns an aggregate and
no row content. No stage running on your machine selects rows from a user
table.

**The data plane reads every row of every in-scope table.**
`02_copy_schema`, running as an AIDP job on your cluster, copies a schema with
`INSERT INTO <target> SELECT * FROM <source>` and, with
`--verify counts+sums`, aggregates the rows again. Row data flows
Snowflake → your AIDP cluster → your AIDP catalog storage, never through the
operator's machine and never to the plugin's authors. The connector is
read-only against Snowflake, so the source is unchanged. Two consequences
worth stating: a **masked column arrives unmasked** and a row-access policy is
simply absent on the copy (see `SECURITY.md` from the `security` stage), and
each table is copied at its own moment, so a live source yields a target
that is consistent per table but not across tables.

## What it writes to disk

Artifacts go to `--out-dir`; when you do not pass one, the default is
`<plugin root>/migration-artifacts/`, inside the plugin directory and
gitignored. They contain **metadata about your estate, not its contents**:

- database, schema, table and view names; column names, types, precision and
  nullability; row counts; warehouse names and sizes
- **view SQL, verbatim** — a view definition can embed literal values, so
  treat these artifacts as sensitive as your schema
- generated Spark SQL DDL and `.ipynb` notebooks
- the AIDP coordinates the run used (`PREFLIGHT.md`, `*_result.json`)

Also on your machine: `./snowmig-config.yaml`, created by `init-config` and
holding the credential, and the short-lived temp files named above. No table
rows appear in any local artifact. The reports the data plane writes
(`copy_report_<schema>.json`, `MIGRATION_REPORT.md`) land in the workspace
folder `backup-snowflake-migration/reports/` and hold counts and sums only.

## What it does to your AIDP tenancy

Dry-run by default for every stage that has one: nothing reaches AIDP without
`--execute` **and** the target coordinates. The writers are:

| Stage | With | What it creates or changes |
|---|---|---|
| `deploy --execute` | coordinates | schemas, empty tables and views in a Standard catalog, `CREATE ... IF NOT EXISTS`; never drops or alters |
| `provision --execute` | coordinates | the workspace, the `migration-assets` cluster (and, with `--warehouse-clusters`, one per Snowflake warehouse), the folder `backup-snowflake-migration/`, the notebooks, the plan files — and, with `--source-config`, a derived `plan/<stem>.json` holding the `snowflake:` block |
| `catalog --execute` | coordinates | one EXTERNAL catalog carrying the Snowflake credential (or an INTERNAL container on request) |
| `run` | a provisioned job | **starts a job**; there is no dry-run flag, because the dry run happened at `provision`. The job then copies data on your cluster |
| `smoke --write-probe --execute` | coordinates and `--execute`; `--write-probe` alone is a dry run | a schema `snowmig_permission_probe_<8 hex chars>` to prove write access, then drops that one schema; never `CASCADE`, and a fresh suffix per run because a failed create poisons the name |

`notebook --upload` is not a writer: without `--execute` it is a dry run, with
it the upload is refused (GAPS.md 13); the generated notebook stays in
`--out-dir`.

An object that already exists with a different structure is reported and
left untouched.

## Two things worth knowing

- **SQL is passed to the `aidp`/`oci` CLI as a command-line argument**, so
  while a statement runs it is visible to other users on the same machine via
  `ps`. That is DDL and object names, never row data. The two request bodies
  that carry the credential (catalog registration and `testConnection`) are
  the exception: they travel by temp file, not argv. On a shared host, keep
  both in mind.
- **This is a Claude Code plugin.** The skills instruct Claude, and the reports
  it generates are read back into the conversation so Claude can summarise
  them. Your conversation — including estate metadata and view SQL that Claude
  reads — is handled under the terms of whichever Claude product you are using.
  That is a property of using an AI assistant, not something this plugin adds,
  but it is the honest answer to "where does this information go". The
  plugin never asks you to paste a secret into that conversation; if one
  lands there anyway, rotate it.

## Removal

Delete the plugin directory, the `migration-artifacts/` folder inside it (or
your `--out-dir`) and `./snowmig-config.yaml`. That removes everything on your
machine.

It does **not** touch the AIDP side. What a run created there stays until you
delete it: the workspace folder `backup-snowflake-migration/` — including the
derived `plan/<stem>.json` copy, which holds the credential — the EXTERNAL
catalog, which stores the credential as its connection, the clusters and the
jobs. Remove the two credential-bearing objects first, then rotate the
Snowflake credential they held.
