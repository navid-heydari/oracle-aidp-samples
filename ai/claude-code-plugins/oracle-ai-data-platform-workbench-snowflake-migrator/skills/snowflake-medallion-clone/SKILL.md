---
name: snowflake-medallion-clone
description: Create the medallion architecture on Oracle AI Data Platform - registering an EXTERNAL catalog of source type SNOWFLAKE by default, and generating Spark SQL for schemas, tables and views from an approved Snowflake plan only when the user has explicitly asked for a Standard catalog. Structure only; copies no data. Use when the user asks to create the medallion structure, soft clone, shallow clone, create the target catalog, or deploy the target schema.
---

# Stage 3 — target catalog and medallion structure

**The default target is an EXTERNAL catalog of source type SNOWFLAKE.** It is a
registered, read-only pointer at the live Snowflake source: it copies no bytes,
creates no tables and has nothing to keep in sync. That is Phase A, and for most
runs it is the whole of stage 3.

A **Standard catalog is managed storage** — real tables this migrator has to
create and someone has to keep current. Create one **only when the user has
explicitly asked for a Standard catalog**, and then via Phase C, not the
control-plane API. Never create an Internal or Standard catalog on your own
initiative, and never offer one as the obvious default.

## Phase A — register the EXTERNAL catalog (the default path)

**Ask the user for these now. They are stored nowhere and there is no default:**

- DataLake OCID
- workspace
- cluster id
- **target catalog name** — one catalog per Snowflake database; see the scoping rule
- path to the **connection config** (YAML or JSON)

The Snowflake account, warehouse, database, user and credential come from a
config **file**, never from inline arguments — see
`snowflake-catalog-connection.example.yaml`. The credential itself is a *path*
inside that config, so the config carries no secret.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py catalog --out-dir ./snowmig_out \
  --catalog <cat> --connection-config ./snowflake-catalog-connection.yaml \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>
```

Without `--execute` this is a dry run: it validates the config, reports which
connection fields were built, and creates nothing. Show `CATALOG.md`.

Then validate the connection with `aidp catalog test-connection` before
claiming the catalog is usable — a registered catalog that cannot reach
Snowflake reads as created and returns nothing.

## Phase B — generate the DDL (offline, safe; needed only for Phase C)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py ddl --out-dir ./snowmig_out
```

Show `DDL_PLAN.md`: the SQL, the rule behind each transformation, dropped
properties, and everything blocked. Statements come out in wave order, so a view
always follows the tables it reads. Nothing has touched AIDP.

## Phase C — Standard catalog only, and only when explicitly requested

An EXTERNAL catalog needs no tables. A Standard catalog does, and **those tables
are created on AIDP compute, not through the control-plane API**: a Spark run on
the cluster prints per-object progress and a real Spark error, where a series of
catalog-CRUD HTTP calls returns 202 Accepted and then fails silently.

So for a Standard catalog, hand over the script and let it run on compute — use
the **`snowflake-clone-notebook`** skill, which writes the table-creation script
to `/Workspace/Shared/` and runs it on the cluster. The user creates the Standard
catalog itself; `snowmig.py catalog --catalog-type standard` refuses and says so.

`snowmig.py deploy` is the older control-plane path. Prefer Phase C; reach for
`deploy` only when the user asks for it specifically.

## Rules

1. **One catalog per run.** Bronze mirrors the source, so a multi-database estate
   spans catalogs. A run covers only `--catalog` and reports the rest as out of
   scope. Another catalog is another explicit confirmation.
2. **EXTERNAL by default; Standard on explicit request only.** Say plainly what
   an EXTERNAL catalog is — a live read-only view of Snowflake, not a copy — so
   nobody expects migrated tables from it. If the user wants tables on AIDP,
   that is the explicit Standard-catalog request, and it goes through Phase C.
3. **Never `--execute` on an earlier approval.** Ask in the turn you run it.
4. **ADB/ADW/ALH EXTERNAL catalogs cannot hold managed Delta.** If a Standard
   clone is aimed at one, explain rather than trying.
Rules 5–7 govern the Phase C clone; there is nothing to verify per object when
a catalog is merely registered.

5. **Report `verified`, never `executed`.** A batch can report success while
   statements inside it failed, so every object is probed individually. The honest
   number is `verified/total`. `verified` means the object exists **with the
   planned column list**, checked by `DESCRIBE` — not merely that something of
   that name is there.
5b. **Two outcomes are not successes, and must be read out.** *Structure
   differs* means the name already belonged to an object with different
   columns; the DDL is `CREATE IF NOT EXISTS`, so it was left exactly as found
   and has **not** been cloned. Resolve the collision before re-running — do
   not describe it as migrated. *Structure not verified* means it exists but
   its columns could not be compared, so no clone claim has been earned.
5c. **Creation is asynchronous — a 202 is not a create.** Schema and table
   creates can appear seconds later or never. If `verified/total` is less than
   the statement count when the command returns, that batch is still settling,
   not failed and not done — say "pending, N of M verified so far" and check
   again rather than reporting the run as complete either way. Never say
   "migration complete" or "clone succeeded" before every object in this
   `--catalog` shows `verified`.
6. **Say the objects are empty.** This is a structural clone: schemas, tables and
   views with no rows. Data movement is a later phase.
7. **No `CREATE OR REPLACE`, no `DROP`.** Existing objects are left alone; a 409
   means "already exists", not a failure.
8. **Silver/Gold jobs are not created here.** The plan defines them, disabled and
   never triggered. Creating them on AIDP is a separate step the user asks for.
9. **When the stage returns, report the outcome and the next stage in the
   same turn** — the registered catalog, or the verified count for a Phase C
   clone — then run `summary` (stage 4) and name it, per the router's
   rule on reporting stage progress unprompted. Do not stop on the raw command
   output and wait for the user to ask what happened or what's next.

## Backend

The engine uses the `aidp` CLI when installed and falls back to `oci
raw-request`. It prints which one it chose. If neither CLI is present it fails
loudly rather than guessing a transport. `oci ai-data-platform` covers only the
control plane, which is why the data plane goes through `raw-request`.
