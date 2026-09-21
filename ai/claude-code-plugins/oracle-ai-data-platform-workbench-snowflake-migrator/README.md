# Oracle AI Data Platform — Snowflake Migrator (Claude Code plugin)

> ⚠️ **Before sharing or publishing this plugin, work through
> [CLEANUP-BEFORE-PUBLISH.md](CLEANUP-BEFORE-PUBLISH.md).** It carries
> customer-confidential context and a developer's test-account details that must
> come out first.

Investigate a Snowflake estate and migrate it onto Oracle AI Data Platform
(AIDP): inventory → what can and cannot move → medallion layout → the target
catalog, registered EXTERNAL against the live Snowflake source → the AIDP
migration environment (workspace, `migration_assets` cluster, jobs) → **data,
schema by schema**, via the parametrised PySpark **notebooks** in
[data-migration-scripts/](data-migration-scripts/) that run INSIDE AIDP and
verify every copy (row counts, exact decimal sums).

**The control plane copies no data itself.** Rows move only when the operator
runs the in-AIDP job `snowmig_02_copy_schema`, one schema per run. The data
plane is the scripts on AIDP compute, and the jobs that run them carry **no
schedule** — running one is always the operator's call. Stored procedures,
tasks, streams and pipes are inventoried with effort bands, never
auto-translated.

Two modes: **dev** (`snowmig.py demo` — the whole pipeline against a built-in
emulation, zero credentials, every artifact narrated in `DEMO.md`) and
**prod** (the pipeline below, with `--execute` gates).

**→ Start here: [How to run a migration](#how-to-run-a-migration-from-zero).**
The design record behind it — the full Snowflake→AIDP mapping and what is
deterministic versus AI — is
[MIGRATION-ARCHITECTURE.md](MIGRATION-ARCHITECTURE.md).

---

## How to run a migration, from zero

The migration is the overview skill's **twelve steps, S1–S12, in that
order** (`skills/snowflake-migrator-overview/SKILL.md` is the authority on
the sequence); the sections below are its runnable form and name the step
each command serves. Everything reads from one config file, and nothing
writes to AIDP without `--execute`.

> **On the paths below.** They are written `engine/snowmig.py`, which is what
> you type from a checkout of this repo. If the plugin is **installed** rather
> than cloned, the same file is at `${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py` —
> use that, from whatever directory you are in. You never need to `cd` into the
> plugin, and `--out-dir` and the config live wherever *you* are working.

### 0. Prerequisites

| | |
|---|---|
| Snowflake | a **read-only** role and a service user. Password or key-pair — **both go in the one config file**, the PEM pasted inline under `private_key: |`. Nothing else to create |
| AIDP | the `oci` CLI configured, plus the `aidp` CLI (`pip install aidp-python-client aidp-cli`) for workspace files. You need the **aiDataPlatform OCID** |
| Local | `pip install -r engine/requirements.txt` |

Never needed: any write grant on Snowflake. The transport refuses non-read
verbs regardless of what the credential permits.

### 1. One config file — and where it lives

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig init-config
```

That writes `./snowmig-config.yaml` from `snowmig-config.example.yaml` with
mode `0600` (on POSIX; Windows has no mode bits, so the file inherits your
profile's ACL), and refuses to overwrite one that already exists (it would be
holding credentials). Copying the template by hand works just as well —
`chmod 600` it yourself if you do. Fill it in:
**the Snowflake connection and the AIDP destination both live in that one
file**, so nothing has to be repeated on the command line.

```yaml
snowflake:
  account: MYORG-MYACCOUNT
  host: MYORG-MYACCOUNT.snowflakecomputing.com   # the Account/Server URL
  user: MIGRATION_READER
  warehouse: MIGRATE_WH
  database: SALES_DB
  role: MIGRATION_READER_ROLE
  schema: PUBLIC
  auth: password
  password: the-password            # or: auth: keypair + `private_key: |` inline

aidp:
  datalake_ocid: ocid1.aidataplatform.oc1.<region>.<unique-id>
  # catalog: my_target_catalog        # the INTERNAL target (create it yourself)
  # external_catalog: my_snowflake_source
```

**Where the CLI looks for it**, in order — every stage prints which file it
used:

1. `--config <path>`, when given;
2. `./snowmig-config.yaml` in the working directory;
3. the same name beside the plugin.

Any field can still be overridden per run with a flag (`--role`,
`--warehouse`, `--catalog`, `--datalake-ocid`), without editing the file.

#### What is *not* in that file

**AIDP authentication.** The plugin drives the `oci` and `aidp` CLIs with
your normal OCI setup (`~/.oci/config`, from `oci setup config`), and this
is exactly how: `oci raw-request` runs with that file's `DEFAULT` profile
(or whatever `OCI_CLI_PROFILE` selects in your shell; the config's
`aidp.oci_profile` key is accepted but not yet passed through), and every
`aidp` invocation is given `--auth api_key` plus `--region` derived from the
DataLake OCID, because the `aidp` CLI's own default is a session token. So
the API key the plugin uses is the one in that profile. The config only
names *which* AIDP resources to use — never a credential for them.

#### The rules that go with an inline secret

The file holds live credentials in plain text, which is the trade made for
having one file. So:

- it is **gitignored only inside this plugin's own folder** (the rule lives
  in the plugin's `.gitignore`), and the file belongs in *your* working
  directory — so add `snowmig-config.yaml` (and `.yml`, `.json`) to your own
  `.gitignore` before filling it in; `git add .` from another repo stages
  it otherwise. It must stay out of commits, tickets and chat;
- **secrets are never echoed** — `preflight` and every report render the
  config through a redactor, so a password cannot reach a log or a summary;
- **an agent asks before reading it**, and never asks you to paste a secret
  into the conversation. If one ends up there, rotate it;
- a destination that came from the file is **announced** before anything acts
  on it, and writing still needs `--execute`. The resolver itself does no I/O
  at all, so a stale config cannot silently redirect a write.

#### You do not need to be inside this repo

The plugin can be installed rather than cloned, and the working directory can
be anywhere. The engine is always at
`${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py` — or run it through
`${CLAUDE_PLUGIN_ROOT}/bin/snowmig`, which creates the environment on first
use — and the in-AIDP notebooks at
`${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`. The config, by contrast,
belongs in **your** working directory: an installed plugin's own directory may
be read-only.

### 2. Confirm the config out loud, and test both ends

```bash
bin/snowmig preflight --test-source
```

No `--config` needed — the CLI finds the file and prints which one. Add
`--config <path>` only to point at a different one.

`PREFLIGHT_CONFIG.md` echoes every field with what it is *for*, with every
secret masked — an inline one reads as *inline*, a `*_path` shows the path,
neither shows a value — then reports the checks. Read it with whoever owns the account before going further — a wrong
host or role here surfaces hours later as something that looks like a tool
failure.

- `--test-source` opts into the Snowflake connection; it is opt-in because it
  resumes the warehouse.
- The AIDP end is checked automatically when the config carries both
  `datalake_ocid` and `catalog`: it reports whether that catalog exists and
  whether it is `INTERNAL` or `EXTERNAL`.
- **A *skipped* check is not a pass.** If one end was never configured, the
  report says so rather than implying it passed.

### 3. Assess the estate (read-only, from the laptop — optional preview)

This is the pre-sales and estimation pass, and it is **optional for a
migration**: it reads Snowflake from your machine and leaves no workflow, no
log and no evidence inside AIDP, so the migration's own discovery is the
`snowmig_00_discover` job at step 9 (runbook S6). Run it to answer *"what is
in this account?"* before anyone commits to anything.

Every stage that reads Snowflake takes the same config file, so the
coordinates are written once:

```bash
bin/snowmig assess     
bin/snowmig deps       
bin/snowmig maintenance
bin/snowmig security   
bin/snowmig compute     --credit-price <USD>
```

No coordinates on the command line: they come from the config the CLI just
announced. Any one of them can still be overridden per run (`--role`,
`--warehouse`, `--database`).

Read `INVENTORY.md`, `CENSUS.md` (everything that is **not** a table or view,
and cannot migrate), `SECURITY.md` (what arrives unprotected) and
`MAINTENANCE.md`. If `assess` blocks tables on `VARIANT`/`GEOGRAPHY`, that is
the type mapper refusing to guess: re-run with `--semi-structured string` (or
`--geospatial string`) only as a **deliberate** decision, and say so.

### 4. Plan, generate DDL, get sign-off

```bash
bin/snowmig plan [--restrictions ./restrictions.json]
bin/snowmig ddl 
bin/snowmig summary
```

**`PLANNED_OBJECTS.md` is the approval artifact — stop here for sign-off.**
`ddl_plan.json` is the authority from this point on: the in-AIDP scripts create
only what it contains, and report anything else as `not_in_plan`. Use
`--restrictions` to scope a first wave (a canary of a few tables is a good
first live write).

### 5. Prove both ends reach each other

```bash
bin/snowmig smoke \
  [--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>]
```

Anything already in the config's `aidp:` block can be left off; the CLI prints
the destination it resolved. The values themselves come from the console, or
from `oci`/`aidp` list calls. **Read that printed line before you approve an
`--execute` later** — it is the last chance to notice a config written for a
different environment.

### 6. Provision the migration environment inside AIDP (S1, S2, S5)

**The environment comes first**, because every later AIDP write — including
registering a catalog — is addressed by four coordinates (DataLake OCID,
workspace, cluster, catalog) and `resolve_target()` requires all four. A run
that registers the source catalog before it has a workspace and a cluster
stops on `AIDP target coordinates not supplied`.

```bash
bin/snowmig provision \
  --workspace-name "<the Snowflake account or project name>" \
  --warehouse-clusters \
  --source-mode connector --source-config ./snowmig-config.yaml \
  --external-catalog <name> --target-catalog <internal catalog> \
  --datalake-ocid <ocid>                                             # dry run
bin/snowmig provision ... --execute
```

Creates the workspace (name translated to a charset the API cannot reject),
the `migration_assets` cluster, **one cluster per Snowflake warehouse with the
same name** on the AIDP default config, the workspace folder
`backup-snowflake-migration/` holding the scripts and the plan, and four
**unscheduled** jobs. `--external-catalog` and `--target-catalog` are names
being pre-declared for the job parameters, not catalogs that must already
exist. Read `PROVISION.md`: pending is pending, never rounded up.

**Hand-off.** `PROVISION.md` and the CLI output print the **workspace key**
and the **cluster key**. Paste them into `aidp.workspace` and
`aidp.cluster_id` of `snowmig-config.yaml` (the commented lines in the
template) before the next step — `provision` does not write them back, and
the next step cannot run without them.

`--source-config` is the one file being placed on the workspace mount so the
in-AIDP scripts can reach Snowflake themselves. **It carries the credential**,
which is why it is uploaded only when you pass it explicitly. The scripts read
YAML or JSON; hand them JSON if the cluster image has no PyYAML.

### 7. Register the source as an EXTERNAL catalog, then create the INTERNAL target (S3, S4)

```bash
bin/snowmig catalog --catalog <name> \
  --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>          # dry run
# then, after reading CATALOG.md:
bin/snowmig catalog ... --execute --test-connection
```

`--datalake-ocid`, `--workspace` and `--cluster-id` are **required for
`--execute`** unless the config's `aidp:` block carries them (step 6's
hand-off); the dry run tolerates their absence. The EXTERNAL catalog is a
read-only pointer at the live source; it copies nothing. The Snowflake
credential it registers is read from the same config file. `--test-connection`
only runs with `--execute`, because the API resolves RBAC on an existing
catalog.

Then the target **INTERNAL** catalog, which IS created by this plugin, as a
container and nothing more:

```bash
bin/snowmig catalog --catalog <internal catalog> --catalog-type standard --execute
```

(runbook S4, live-verified). The container is not the structure — its
schemas and tables are created later, on AIDP compute, by the structure
workflow at S10, because a control-plane table create can return
`202 Accepted` and create nothing.

### 8. Diagnose from inside AIDP, once

Open `backup-snowflake-migration/scripts/diagnose_environment.ipynb` on the
cluster and run it. It answers, with a verdict per check: is the workspace
mounted, can the cluster reach Snowflake, do the credentials work through the
connector, and did the external catalog's crawler actually populate anything.

### 9. Run the jobs inside AIDP

Run them with `bin/snowmig run --job <name>` (or from the console). Two of
the four are **part of the migration**; the other two are **run later, on
the customer's decision**.

Part of the migration (S6, S10):

| Job | What it does | Report |
|---|---|---|
| `snowmig_00_discover` | the whole estate in two `INFORMATION_SCHEMA` queries; back up the manifest (S6) | `discovery_manifest.json`, `DISCOVERY.md` |
| `snowmig_01_structure` | empty Delta tables from the **approved** `ddl_plan`, one workflow per schema (S10) | `structure_report_<schema>.json` |

At S12 the migration is **done**: the structure, the scripts, the plans and
the backups exist. **The data migration is not run.** Moving rows is a later
decision the customer makes, with the scripts already sitting there:

| Job | What it does | Report |
|---|---|---|
| `snowmig_02_copy_schema` | copies ONE schema and **verifies** it (row counts; `--verify counts+sums` adds exact decimal sums) | `copy_report_<schema>.json` |
| `snowmig_03_reconcile` | plan versus what the catalog actually holds | **`MIGRATION_REPORT.md`** |

Edit the `PARAMS` cell at the top of a job's notebook to change its
arguments (`schema`, `mode`, `verify`); they are there to be edited. There is
no driver wrapper — the job runs the stage notebook itself. Every stage is
resumable: a re-run skips what its report already records as done.

**`MIGRATION_REPORT.md` is the deliverable.** A table reads `NOT_MIGRATED`
when it was never attempted — expected while the migration is still running —
and only `MISSING_DESPITE_REPORT`, `STRUCTURE_ONLY_COPY_FAILED` or
`TARGET_UNREADABLE` mean something is wrong.

### Before a production cutover

Each table is copied at its own moment, so a live source yields a target that
is consistent per table but **not across tables**. Freeze writers, copy from a
point-in-time Snowflake `CLONE`, or plan an incremental re-sync. And remember
what does **not** travel: tasks, streams, pipes, procedures, UDFs and every
masking or row-access policy. `CENSUS.md` and `SECURITY.md` list them.

---

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | **EXTERNAL catalog, source type SNOWFLAKE** (default) — a read-only pointer at the live source. A **Standard catalog** only when you explicitly ask for one |
| Schema | Schema |
| Table | Table (managed Delta, empty) — Standard catalogs only |
| View | View — when its SQL is portable — Standard catalogs only |
| Warehouse | Spark compute cluster (see the compute proposal) |

Bronze mirrors the source 1:1, so target names equal source names. Silver and
Gold are requirement-driven: the plan emits **disabled job stubs** that the
migrator never triggers, because their content is a requirement to define with
the customer rather than logic to invent.

## Safety posture

| | |
|---|---|
| Against Snowflake | **Read-only, always.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL` |
| Against AIDP | **Dry-run by default.** Writing needs `--execute` plus all four target coordinates in the same command |
| Target coordinates | **Never used without being shown.** They come from a flag or from the one migration config; a value read from the file is printed before anything acts on it, and a write still needs `--execute` and a confirmation in that turn. No environment default, no cache, and `target/coords.py` performs no I/O of its own |
| Unmapped types/features | **Blocked with a reason.** Never approximated, never silently defaulted. Semi-structured and geospatial types have an explicit opt-in escape hatch — see below |
| "Verified" | **Means the planned columns are there**, checked by `DESCRIBE`. A name that already belonged to a different structure is reported as a mismatch and left untouched — never counted as cloned |
| Assessment cost | **Free by default.** Row counts come from Snowflake's maintained metadata; a `COUNT(*)` per object, which executes every view, is opt-in |
| Collisions | **Halt (exit 3).** Identifier-case and target-name collisions stop the run rather than picking a winner |

## Pipeline

```
Snowflake ──▶ inventory.json ──▶ dependencies.json ──▶ plan.json ──▶ ddl_plan.json ──▶ [AIDP]
              INVENTORY.md                           PLANNED_OBJECTS.md  DDL_PLAN.md   SOFT_CLONE_SUMMARY.md

Snowflake ──▶ warehouses.json ──▶ compute.json ──▶ COMPUTE_PROPOSAL.md
```

The two reports the plugin exists to produce:

- **`PLANNED_OBJECTS.md`** — what is planned to move, and what cannot with a
  brief reason per object
- **`SOFT_CLONE_SUMMARY.md`** — what the shallow clone actually created

Each stage reads the previous artifact and is independently re-runnable.

| Stage | Skill | Command |
|---|---|---|
| 0 · auth | `snowflake-migrator-bootstrap` | |
| 1 · investigate | `snowflake-assess-estate` | `/snowflake-assess` |
| 2 · plan | `snowflake-migration-plan` | `/snowflake-plan` |
| 3 · medallion + shallow clone | `snowflake-medallion-clone` | `/snowflake-soft-clone` |
| — · compute | `snowflake-compute-proposal` | `/snowflake-compute` |

`snowflake-migrator-overview` routes between them and carries the shared rules.

## Quick start

The full sequence is [How to run a migration, from
zero](#how-to-run-a-migration-from-zero). Two things worth knowing before the
first `assess`:

**Cost and refusal flags** (all default to the safe option):

| Flag | Default | Why you might change it |
|---|---|---|
| `--row-counts metadata\|exact\|none` | `metadata` | free, and exact for a settled table. `exact` runs `COUNT(*)` per object and **executes every view** |
| `--semi-structured block\|string` | `block` | `VARIANT`/`OBJECT`/`ARRAY` block their table until somebody decides; `string` carries the JSON as text, which **defers rather than solves** |
| `--geospatial block\|string` | `block` | same decision for `GEOGRAPHY`/`GEOMETRY` |
| `--timestamp-ntz preserve\|timestamp` | `preserve` | the catalog API cannot express `timestamp_ntz`; `timestamp` changes timezone semantics and records the caveat |

**Exit codes:** `0` ok · `1` error · `3` halt (an identifier-case or
target-name collision — shown, never resolved for you).

Dev mode needs none of this:

```bash
bin/snowmig demo --out-dir ./snowmig_demo
```

## Restrictions

Narrow the estate before planning with `--restrictions restrictions.json`. Every
exclusion appears in `PLANNED_OBJECTS.md` with the restriction that fired.

```json
{
  "exclude_databases": ["SNOWFLAKE_LEARNING_DB"],
  "exclude_schemas": ["STAGE"],
  "exclude_object_types": ["VIEW"],
  "exclude_name_patterns": ["^TMP_", "_BAK$"],
  "max_rows": 100000000
}
```

`include_*` variants act as allowlists. An unrecognised key is an **error**, not
an ignored line — a typo would otherwise apply nothing while appearing to work.

## Why a view might not migrate

Because Bronze mirrors the source, object references inside a view need no
rewriting; only dialect matters. 15 Snowflake-only constructs — `QUALIFY`,
`LATERAL FLATTEN`, `IFF`, `::`, `LISTAGG`, `DATEADD`, … — **block** the view with
the construct named, rather than being rewritten on a guess. Secure and
materialized views are blocked outright. See
[references/type-mapping.md](references/type-mapping.md).

## Tests

Offline, no credentials, no AIDP:

```bash
cd engine && python3 -m pytest tests -q
```

Live end-to-end against a real estate (read-only, does not deploy):

```bash
cd engine && SNOWMIG_LIVE=1 SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... \
  SNOWFLAKE_PRIVATE_KEY_PATH=... python3 -m pytest tests/test_live_smoke.py -q
```

The test corpus generator is `engine/snowflake_source/corpus/00_acme_setup.sql`
with `validate.py` asserting referential integrity, join fan-out and two exact
business invariants — row-count floors alone cannot catch a migration that runs
clean and returns the wrong numbers.

## Execution backend

AIDP writes go through the **`aidp` CLI** when installed, otherwise **`oci
raw-request`**. `oci ai-data-platform` covers only the control plane, which is why
the data plane uses `raw-request`. The engine prints which backend it chose and
fails loudly if neither CLI is present.

## Row counts, and what they cost

| Mode | Source | Cost |
|---|---|---|
| `metadata` (default) | Snowflake's maintained count, from `SHOW`. Views get none | Free |
| `exact` | `COUNT(*)` per object | **Executes every view.** Warehouse time per object |
| `none` | — | Free |

The metadata count agrees with `COUNT(*)` for a settled standard table — the
live corpus confirms it on all six — but it can lag very recent DML and is not
maintained for external tables, so the reports label it as metadata and never
call it verified. Every blank in a Rows column carries its reason: not counted,
not requested, or a named error.

## Semi-structured and geospatial types

`VARIANT`, `OBJECT` and `ARRAY` block their table by default, because the
target shape is a design decision rather than something to guess. Because that
would otherwise be a dead end on an estate full of JSON payloads,
`--semi-structured string` carries the value as text with a warning on every
affected column. `--geospatial string` does the same for `GEOGRAPHY` and
`GEOMETRY`. Two flags, not one: they are separate decisions.

Neither hatch solves the problem — both defer it. As text, nothing on the target
can address a field inside the value.

## What is not a table or a view

`assess` also censuses procedures, UDFs, tasks, streams, materialized and
dynamic tables, stages, pipes, sequences and file formats → `CENSUS.md`.
**None of them migrate**, and no equivalent is generated — a
plausible-but-wrong procedure translation is worse than an honest gap. The
scope statement travels into `PLANNED_OBJECTS.md` and `SUMMARY.md`, so the
migratable count is never mistaken for the size of the estate.

Procedures and UDFs are read from `INFORMATION_SCHEMA` rather than `SHOW`,
because `SHOW PROCEDURES` returns Snowflake's built-ins (33 on an empty
schema) and `INFORMATION_SCHEMA` does not. It also carries the handler
language, which drives the triage band — JavaScript is HIGH, since AIDP has no
JavaScript runtime.

The most damaging entry is a **task**: if it populates a table you are
migrating, that table stops being refreshed after cutover. The clone succeeds
and then goes stale.

## Security posture — the one with an exposure consequence

`snowmig security` reports masking, row-access, aggregation and projection
policy *attachments*, secure views, and who holds grants today →
`SECURITY.md`.

A masked column arrives **unmasked**. A row filter is simply absent. A secure
view loses `SECURE`. The clone does not fail — it **succeeds without the
protection**. AIDP has no masking API; the equivalent is a restricted view
plus ontology sensitivity granted per role, which is a design decision rather
than a translation, so this plugin reports and changes nothing.

If `ACCOUNT_USAGE` cannot be read, the exposure count is `null` and the report
says the question is **unanswered** — never "none found".

## Table maintenance — a difference worth reading before you migrate

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`**: it maintains layout and
reclaims storage in the background. AIDP has `OPTIMIZE`, `VACUUM`, `ZORDER BY`
and liquid clustering, and **runs none of them for you**. The capability
survives the migration; the *responsibility* moves.

`snowmig maintenance` (after `assess`) measures what the source actually does
— clustering keys, `automatic_clustering`, Search Optimization,
`change_tracking`, the retention cascade, and reclustering credits plus DML
churn from `ACCOUNT_USAGE` — and writes `MAINTENANCE.md`. An unreadable
`ACCOUNT_USAGE` reports **not measured**, never zero: those two lead to
opposite decisions. The retention *level* is inferred from effective values
rather than probed per table, so the stage costs a handful of queries;
`--probe-table-parameters` opts into the exact path.

Every data-movement option also states **who inherits `OPTIMIZE`/`VACUUM`** and
which traps apply to it, because that is decided by the architecture rather than
discovered afterwards.

This plugin reports the gap per object and applies nothing — no maintenance
DDL is generated, enforced by test. Source settings with a real equivalent
(`cluster_by`, `retention_time`, `change_tracking`) appear in the DDL plan under
*"Maintenance and layout — decisions, NOT applied"*, with the equivalent named,
and raise the object's risk to MEDIUM.

Two traps are worth knowing up front: on Delta **`VACUUM` is what bounds time
travel** (on Snowflake those are independent and automatic), and **`OPTIMIZE`
increases storage until `VACUUM` runs**. Full mapping and the planned work:
`references/maintenance-and-layout.md`, `ACTION-ITEMS.md`.

## Where this stands

One real migration has run end to end against a live AIDP DataLake: seven
objects created and read back, six verified exactly and one reporting derived
type drift. The Snowflake side and the target **catalog CRUD** transport are
live-verified; the SQL transport is dead (404). The notebook upload-and-run
path is now live-verified too: the four stage notebooks upload as `NOTEBOOK`
objects through the `aidp` CLI and the discovery job ran to SUCCESS on a
migration cluster, reading 1065 relations and 9935 columns from Snowflake in
two `INFORMATION_SCHEMA` queries.

Per-stage live status is kept in one place, `GAPS.md` → "What is actually
proven", and this is its sentence:

**What has run live:** the discovery job (`snowmig_00_discover`) ran to
SUCCESS on a migration cluster, reading 1065 relations and 9935 columns in
two `INFORMATION_SCHEMA` queries; the structure job (`snowmig_01_structure`)
ran on a cluster from the approved plan, a healthy 23-minute run left alone
by the cold-start guard (2026-09-19); the copy (`snowmig_02_copy_schema`)
and reconcile (`snowmig_03_reconcile`) jobs are **not yet confirmed by the
authors**.

Not yet proven: anything at full-estate scale (the largest run was one
schema), the EXTERNAL catalog crawler, the cluster-library item shape.
**Treat the first run on a new estate as a shake-out**: run
`diagnose_environment.ipynb`, then one small schema end to end, and read
`MIGRATION_REPORT.md` against the console before anything larger.

**`GAPS.md` is the ranked list of what is left**, including the two questions
that need Oracle rather than code. The Snowflake side is live-verified: 10
gated end-to-end tests run against a real account.

## Docs

- [docs/specs/2026-09-09-mvp1-design.md](docs/specs/2026-09-09-mvp1-design.md) — design
- [references/type-mapping.md](references/type-mapping.md) — the type table
- [ASSUMPTIONS.md](ASSUMPTIONS.md) — everything this rests on, and what breaks if each is wrong
- [references/data-movement-options.md](references/data-movement-options.md) — the ways bytes could move; one is implemented by the data plane (`snowmig_02_copy_schema`, in-AIDP INSERT-SELECT), the rest are options
- [CLEANUP-BEFORE-PUBLISH.md](CLEANUP-BEFORE-PUBLISH.md) — **do this before sharing**
