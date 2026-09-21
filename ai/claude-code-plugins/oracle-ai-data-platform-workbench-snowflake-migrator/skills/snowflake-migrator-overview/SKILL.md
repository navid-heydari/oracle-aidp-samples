---
name: snowflake-migrator-overview
description: Router, runbook and shared rules for migrating a Snowflake estate onto Oracle AI Data Platform (AIDP). Read this first whenever the user mentions migrating, moving, assessing, inventorying or cloning Snowflake databases, schemas, tables, views or warehouses onto AIDP, asks to "run the migration" or "start from zero", or asks what a Snowflake migration would involve. Carries the FIXED twelve-step order of operations and the rules that apply at every step; adds no API surface of its own.
---

# Snowflake → AIDP migrator — the runbook

A migration is **twelve steps, in this order, every time**. This is not a menu
of stages to pick from. If the user asks to migrate, you run S1 through S12 in
sequence. Steps are skipped only when the user explicitly says to skip one, and
you say out loud which step you skipped and what that costs.

Before S1, two things must exist: the one connection config (copied from
`snowmig-config.example.yaml`; `0600` on POSIX; gitignored only inside the
plugin folder, so the user adds it to their own repo's `.gitignore`) and the user's answer to
*which database*. `README.md` -> "How to run a migration, from zero" carries
the prerequisites and the flags; this file is the sequence and the rules.

**The whole data plane runs inside AIDP, on Spark, as workflows.** The
operator's machine registers coordinates, reads reports and drives the
conversation. It does not read the estate. A discovery that ran on the laptop
produced no workflow, no log and no evidence inside AIDP, and does not count.

## The rule that overrides convenience: CREATE, never reuse

**Never reuse a workspace, cluster, catalog, folder or job that already
exists.** Do not list existing ones and offer them. Do not "ensure" one. The
migration creates its own, named after this migration, so that everything it
touches can be identified, audited and torn down as a unit. The only question
you ask is *may I create it*, never *which of these should I use*.

If a name is already taken, that is a collision to report and resolve with the
user — a new name — not an invitation to adopt the existing object.

## The twelve steps

| # | Step | Creates | Gate |
|---|---|---|---|
| S1 | Create the **workspace**, named from the Snowflake project | 1 workspace | may I create |
| S2 | Create the **migration compute cluster** | 1 cluster | may I create |
| S3 | Register the source database as an **EXTERNAL catalog** | 1 catalog | user picks the database |
| S4 | Create the **INTERNAL target catalog** | 1 catalog | may I create |
| S5 | Create the migration folder and **upload the scripts** | folder + scripts | — |
| S6 | **Discovery, as a workflow inside AIDP**; back up the manifest | manifest + backup | — |
| S7 | **Generate the translation plan** by script; flag what needs review | plan JSON | — |
| S8 | **Resolve the flagged conflicts**, grouped | revised plan | grouped questions |
| S9 | **Present the summary** and take the go/no-go + scope reduction | reduced plan + backup | explicit OK |
| S10 | **Create the assets** by workflow, from the approved plan | schemas + tables | — |
| S11 | Generate the **per-schema data-migration scripts** + one workflow each | scripts + workflows | never run |
| S12 | Propose the **warehouse-equivalent clusters**, list, create on OK | clusters | explicit OK |

At S12 the migration is **done**: the assets exist, the scripts exist, the
plans and backups exist. **The data migration is not run.** Moving rows is a
later decision the customer makes, with the scripts already sitting there.

Budget the shake-out from what has actually run, not from what exists. The
per-stage register is `GAPS.md` → "What is actually proven"; its sentence:
**What has run live:** the discovery job (`snowmig_00_discover`) ran to
SUCCESS on a migration cluster, reading 1065 relations and 9935 columns in
two `INFORMATION_SCHEMA` queries; the structure job (`snowmig_01_structure`)
ran on a cluster from the approved plan, a healthy 23-minute run left alone
by the cold-start guard (2026-09-19); the copy (`snowmig_02_copy_schema`)
and reconcile (`snowmig_03_reconcile`) jobs are **not yet confirmed by the
authors**. Say so if the user asks whether the copy is proven.

---

### S1 — Create the workspace, named from the Snowflake project

**The environment comes first, because everything after it needs coordinates
that do not exist yet.** An AIDP write is addressed by four coordinates —
DataLake OCID, workspace, cluster and catalog — and `resolve_target()`
requires all four for *any* write, including a catalog registration. So a
migration that registers the source catalog before it has a workspace and a
cluster cannot run: it stops on `AIDP target coordinates not supplied`.

The name comes from the source, so the workspace is identifiable as this
migration's. It passes through the simplest-charset translation
(`[a-z0-9_]`, accents folded, separators to `_`); the rename is reported,
never silent.

Create it. Do not look for an existing one to use.

### S2 — Create the migration compute cluster

One cluster, default config, dedicated to this migration: discovery, asset
creation and later the data copy. Sizing is not guessed here — S12 handles
warehouse-equivalent sizing as a separate, explicit decision.

S1, S2 and S5 are all produced by one `provision` call; they are numbered
separately because each is a distinct object with its own *may I create* gate,
not because each needs its own command.

**A workspace reports `ACTIVE` some seconds after its POST returns.** Creating
the cluster immediately is a race, and losing it is a `409 Conflict — not in
an active state`. That leaves the workspace created and the cluster not: the
run is *partial*, not failed, and resuming it is `--reuse-existing` against
the workspace this migration just made. Re-adopting your own half-built
environment is not the reuse the rule below forbids — say which object you are
resuming and why it is yours.

### S3 — Register the source database as an EXTERNAL catalog

**One Snowflake database becomes one AIDP catalog. Always.** There is no
many-to-one and no partial registration: an EXTERNAL catalog registers the
whole database, and a plan-level restriction does not narrow it.

So if the account holds more than one database, **the user chooses which single
database this migration covers, now, before anything is created.** Run
`snowmig.py databases` and ask. Another database is another migration, run
again from S1.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig catalog \
  --catalog <source_db_lowercased> --config ./snowmig-config.yaml \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cluster>
```

Dry-run first, show `CATALOG.md`, then `--execute` with confirmation in that
turn. Then `--test-connection`, which POSTs the documented action and polls
it: a registered catalog that cannot reach Snowflake reads as created and
returns nothing. **`PENDING` is reported as pending, never as a pass** — and a
catalog showing zero schemas against a source that has many is the visible
shape of that failure, not a quiet success.

### S4 — Create the INTERNAL target catalog

This is the catalog the migrated schemas and tables land in. It is a
**container** — one control-plane object — and creating it is not the same as
creating its tables. The tables come later, at S10, on compute, because a
control-plane table create returns `202 Accepted` and can silently create
nothing.

**The type on the wire is `INTERNAL`.** The runbook and the CLI say
"standard", which is kept as an accepted alias and translated once, in
`normalize_catalog_type()`; AIDP itself rejects `catalogType=STANDARD` with
`400 InvalidParameter: Invalid CatalogType`. The two real types are `INTERNAL`
and `EXTERNAL`, verified by reading the catalogs of a live DataLake.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig catalog \
  --catalog <target> --catalog-type standard \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cluster>
```

The result carries `container_only: true`. Pass that on: the container
existing must never be reported as the structure existing.

To see what is actually on the DataLake, and with which types, ask the
server rather than assuming:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig catalogs \
  --datalake-ocid <ocid>
```

### S5 — Create the migration folder and upload the notebooks

`backup-snowflake-migration/` with `scripts/`, `plan/`, `reports/` and
`backup/`. The four data-plane **notebooks** from
`${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/` go into `scripts/`. Every
upload is read back before it is called done.

**Everything the data plane runs is an `.ipynb` notebook.** AIDP types a
workspace object by its extension — `.py` is stored as `FILE`, `.ipynb` as
`NOTEBOOK` — and a job task runs a NOTEBOOK. Uploading a `.py` with
`--type NOTEBOOK` does not make it one; the server stores it as a FILE.

Each stage notebook is **self-contained**: its parameters, the shared source
helpers and the stage logic are all in the one object, so the code a user
opens in the console is the code the job runs. There is no driver wrapper and
nothing is imported off the mount.

The notebooks are **generated** from `engine/dataplane/` and committed.
Regenerate them after changing a stage source:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig build-notebooks
```

Edit the source, never the generated notebook — a hand edit is overwritten on
the next build.

### S6 — Discovery, as a workflow inside AIDP

**This is the step that must not run on the operator's machine.** Discovery
is the `00_discover_snowflake` notebook, run as an AIDP **workflow** on the
migration cluster, reading Snowflake through the AIDP connector.

#### Discover through the WORKFLOW, not through the external catalog

There are two ways to learn what schemas and tables exist, and they are not
equivalent. **Use the workflow in `connector` mode. Do not enumerate the
estate through three-part names against the EXTERNAL catalog.** This is
settled; it is not a judgement call to re-make per migration.

| | workflow, `connector` mode | three-part names on the EXTERNAL catalog |
|---|---|---|
| Cost for a whole database | **two `INFORMATION_SCHEMA` queries** | `SHOW` + a `DESCRIBE` **per object** |
| Measured | 1065 relations and 9935 columns in one run | does not finish at estate scale |
| Depends on | the connector, which the same credentials already prove at smoke | the catalog crawl having completed first |
| Leaves behind | a job run, its task output, and a manifest | nothing on the platform |

The reason to care is **time**. The three-part-name route makes discovery
proportional to object count, so on a real estate it stops being slow and
starts being unusable — and it adds a dependency on crawl state that the
connector route simply does not have. An agent that reaches for it will
spend a long while finding that out.

So: `--source-mode connector` is the default and the answer. Reach for
`external-catalog` only if a user explicitly asks for it, and say plainly
what it costs before agreeing.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig run \
  --datalake-ocid <ocid> --workspace <ws> --job snowmig_00_discover
```

`run` starts the job, polls it to a terminal state, and writes `RUN_*.md` with
the task output as evidence. A poll budget that runs out is reported as
**STILL RUNNING** -- never rounded to success, never to failure.

#### The first run on a new workspace often is never picked up

**A cluster sometimes ignores a job run outright, and characteristically it
is the very first run on a freshly created workspace.** The run does not
fail. It reports `RUNNING` indefinitely, so nothing times out and nothing
alerts; an operator watching the status sees a job that appears to be
working, and waits.

The job-run status cannot tell this apart from real work. **One field can:**

| | wedged | healthy |
|---|---|---|
| job run `state.status` | `RUNNING` | `RUNNING` |
| **task run `startTime`** | **`null`** | a timestamp |

Measured live on 2026-09-19: the first run on a new workspace sat **9+
minutes** with its task unstarted; the identical job, cancelled and
resubmitted, succeeded in **90 seconds**.

`run` handles this itself. After `--cold-start-seconds` (default 60) with
the task still unstarted, it cancels the run and resubmits, up to
`--cold-start-restarts` times (default 1; `0` disables). The budget measures
**pick-up, not work** -- a task that has started is never cancelled however
long it then runs, because killing it would destroy real progress.

Two things to carry into the conversation when it fires:

- **The output belongs to the LAST run key, not the first.** The restart is
  written into `RUNS_*.md` as a table for exactly that reason. Someone
  comparing the report against the console must be able to see it.
- **A resubmit needs the slot free.** `maxConcurrentRuns: 1` accepts a second
  run while the first still holds it and then silently discards it, so the
  cancel is polled to a terminal state before the new run is submitted.
  Never fire a cancel and immediately resubmit by hand.

The manifest is written to `reports/` and **backed up into `backup/`**, dated,
as the reference input every later script reads. Never re-derive what the
manifest already holds.

### S7 — Generate the translation plan, by script

A script — not you — translates Snowflake types, names and structures into the
AIDP plan. It reads the manifest and emits one JSON with the **nested**
structure: schemas → tables → columns → types, plus the target name for each.

The script resolves everything with a direct correspondence, using the mapping
vocabularies it ships with. What it cannot map exactly it **flags for review**
rather than guessing. That flag is the deliverable of this step.

**You do not translate types by hand.** Your turn comes at S8, and only for
what the script flagged.

Download the manifest from `backup-snowflake-migration/reports/` and bridge
it into the shape the planning stages read:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig ingest \
  --manifest ./discovery_manifest.json --database-name <SOURCE_DB> \
  [--semi-structured string] [--timestamp-ntz timestamp]

${CLAUDE_PLUGIN_ROOT}/bin/snowmig plan \
  [--restrictions <file>]
${CLAUDE_PLUGIN_ROOT}/bin/snowmig ddl
```

`ingest` calls the **same type mapper** a live `assess` calls, so a column
planned from the manifest reaches the same verdict as one planned from a live
read. `--database-name` is required and never guessed: a manifest does not
record which database it describes, and a wrong name aims the plan at the
wrong catalog.

Two things `ingest` reports that you must pass on:

- **Views arrive without their SQL.** A manifest carries columns, not
  definitions, so a view cannot be dialect-translated from it and the planner
  refuses it. If views must migrate, plan them from a live `assess`.
- **Lineage was not extracted.** `ingest` writes `dependencies.json` empty
  with provenance `not_extracted`. That is correct — tables carry no
  inter-table dependency and manifest views are refused anyway — but it means
  "not looked at", never "looked at and found nothing". Run `deps` against a
  live session if view ordering matters.

### S8 — Resolve the flagged conflicts, grouped

Read every flagged item first. Then group them: on a real estate the same
decision repeats across hundreds of columns, and the flags fall into a handful
of families — semi-structured types that should become strings or maps,
timestamp variants, precision that does not fit, view SQL with no portable
rewrite.

**Ask about the families, not the instances.** One question that settles 400
`VARIANT` columns is right; 400 questions is a failure of this step. Say how
many objects each answer covers, so the user knows the weight of what they are
deciding.

Write the resolved decisions back into the plan JSON. The plan, not the chat,
is what S10 executes.

### S9 — Present the summary, take the go/no-go and the scope

Show tables **in the chat** — not a pointer to a file — covering: what was
discovered, what is planned, what problems were found, what has been created so
far. Then two questions, together:

1. **Go or no-go** on creating the assets.
2. **Full scope or reduced?** The engineer may not want the whole estate. If
   they reduce it, **back up the full plan JSON into AIDP first**, then write
   the reduced plan and run S10 from that. The full plan stays recoverable;
   the reduced one is what executes.

Reducing scope is an **input** change. Never edit a script to make it cover
less.

### S10 — Create the assets, by workflow

`01_create_structure.ipynb`, run as a workflow, reading the approved plan. It
creates the schemas and then the empty Delta tables. Per-schema, because a job
run costs five to six minutes of startup and per-table runs are the wrong
shape.

The plan it reads is `ddl_plan.json` **on the workspace**, so upload the
approved one to `backup-snowflake-migration/plan/` before running. The stage
runs in `ddl-plan` mode: those types are engine-translated. `manifest` mode
cannot be used with a connector-built manifest, which records SNOWFLAKE types
that Delta rejects verbatim.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig run \
  --datalake-ocid <ocid> --workspace <ws> --job snowmig_01_structure
```

**Stage parameters are NOT passed on this command line.** AIDP job parameters
reach a notebook as neither argv nor environment, so `--param` is refused
rather than accepted and dropped. Each stage notebook carries its own `PARAMS`
cell; `provision --execute --reuse-existing --refresh-notebooks` rewrites it
and re-uploads (without `--refresh-notebooks`, `--reuse-existing` keeps a
notebook already on the workspace, because its PARAMS cell may have been
edited in the console). To narrow what S10 creates, narrow the **plan** it
reads — that is the input — and never edit the stage logic to make it cover
less.

Monitor the runs and report progress. Report `verified`, never `executed` — a
batch can report success while statements inside it failed.

### S11 — Generate the data-migration scripts and register their workflows

**One script per SCHEMA, never per table. One workflow per script.** Register
them; **do not run them.**

Then summarise the data that would move: average table size, the largest and
the smallest, row and byte totals, and the count per schema. The engineer needs
the shape of the job before they decide to run it.

### S12 — Propose the warehouse-equivalent clusters

List the Snowflake warehouses with the cluster proposed for each, say plainly
that they will be created with **default configuration**, and create them only
on explicit confirmation.

---

## Where output goes: one directory, named for what it is

Every stage writes to **`${CLAUDE_PLUGIN_ROOT}/migration-artifacts/`**. One
directory, inside the plugin folder, and nothing else is created anywhere.

It persists between commands **on purpose**: the stages chain, and `plan`
reads the `inventory.json` that `assess` wrote. It is a hand-off, not scratch
— which is exactly why it cannot be a temp directory thrown away per call.

Three properties make it output rather than litter, and they are the point:

- **The name says what it holds.** An unexplained `snowmig_out/` of raw JSON
  appearing beside a plugin reads as a bug; `migration-artifacts/` does not.
- **It explains itself.** The directory carries a `README.md` describing
  what each file is, that everything is regenerable, and that it is safe to
  delete.
- **It is gitignored permanently**, in the plugin's `.gitignore` *and* by a
  `.gitignore` of its own, so it stays ignored even if copied elsewhere.
  These files name a real estate's databases, schemas, tables and columns —
  customer data, which must never reach a public samples repo.

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig assess          # writes there by default
${CLAUDE_PLUGIN_ROOT}/bin/snowmig clean           # removes it
```

`clean` deletes only that directory. It refuses to touch an `--out-dir` the
operator named — removing a path someone chose would be data loss wearing a
tidy-up costume.

**Nothing else is ever created.** No virtualenv in the user's home, no cache
beside the plugin, no scratch left behind. `bin/snowmig` runs on the current
interpreter when it already imports the dependencies, and otherwise builds a
venv in a temp directory that its `EXIT` trap removes — on success, on
failure and on interrupt. `bin/snowmig-test` follows the same rule. If a
stage needs working space mid-run, it uses a temp path and deletes it before
returning.

Use `--out-dir` only when the user wants artifacts kept somewhere they chose
— a migration whose record must outlive the plugin folder, for instance.

## Rules that apply at every step

0. **THE ENGINE IS THE METHOD. Never do by hand what a stage does.** Every
   decision — what can move, how a type maps, what a target is named, whether
   a create landed — belongs to `snowmig.py` and the scripts in
   `data-migration-scripts/`. They are deterministic, they refuse rather than
   guess, and each run leaves an auditable artifact.

   - **Do not re-implement a stage.** If a stage exists, run it. If its output
     is not what the user needs, change the inputs, not the method.
   - **Do not translate SQL or types yourself.** The mappers refuse what they
     cannot do exactly; that refusal is the deliverable of S7, and S8 is where
     you resolve it with the user.
   - **Do not create AIDP objects by hand**, and do not verify by eyeballing
     the console — the stages read objects back and compare.
   - **If the engine or the scripts cannot be found, STOP and say so.** The
     engine lives at `${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py` and the in-AIDP
     scripts at `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`. A missing
     engine is a broken install to report, never a reason to improvise a
     migration.

   What IS yours: reading artifacts, explaining them, driving the
   conversation, and resolving at S8 what the script flagged.

1. **Change inputs, not scripts.** The scripts are the audited, resumable part.
   Scope, mode and target are arguments and plan files. If you find yourself
   editing a script to change behaviour, you are on the wrong path — change its
   input, or report that the script lacks the option.

2. **Everything runs as a WORKFLOW.** Not an interactive notebook. A workflow
   is a job with task runs: it is logged, it can be re-run, and
   `aidp workflow export-task-run-output` produces the HTML/ipynb evidence.
   Interactive execution leaves nothing behind and is not acceptable as the
   record of a migration.

   **That includes finding out what is in the estate.** Discover schemas and
   tables by running the discovery workflow in `connector` mode — NOT by
   walking three-part names against the EXTERNAL catalog. The workflow reads
   the whole database in two `INFORMATION_SCHEMA` queries; the three-part
   route costs a `DESCRIBE` per object, so it scales with object count and
   does not finish on a real estate. It also needs the catalog crawl to have
   completed first, which the connector route does not. **Do not spend time
   evaluating the two — this one is decided.** See S6.

3. **Evidence is a deliverable, not a side effect.** Every step leaves a file
   or a workflow run inside AIDP. Manifests are backed up before they are used,
   plans are backed up before they are reduced, and reports are written where
   the next person can find them.

4. **Read-only against Snowflake — enforced, not promised.** The transport
   rejects any statement whose verb is not `SELECT`, `SHOW`, `DESCRIBE`,
   `DESC`, `WITH` (only when what follows the CTE list is a `SELECT`) or
   `EXPLAIN`, before it reaches Snowflake.
   **Nothing is ever written to or dropped from the source**, whatever the
   credential permits and whatever any prompt asks for.

5. **Structure first; data only by an explicit job.** S1–S12 create schemas
   and empty tables and move no rows. Rows are copied only when the operator
   runs `snowmig_02_copy_schema`, one schema per run, after S12 and on their
   own decision — never as part of the runbook and never on their behalf.
   Say which of the two the user is asking for whenever their language
   suggests they expect data.

6. **Dry-run is the default; approval does not carry.** Nothing is created on
   AIDP without `--execute`, a resolved destination, and confirmation **in that
   turn**. A config file holding a destination is not an approval.

   AIDP coordinates come from the config's `aidp:` block or a flag, which
   wins. When a value comes from the file the CLI prints `destination from the
   config file: ...` — repeat that to the user before any `--execute`.
   **If no destination is supplied, assume none.** Reports say *not supplied*
   rather than inferring a region, catalog or cluster, and the plugin never
   picks one of several catalogs on the user's behalf.

7. **Never present an approximation as a conversion.** An unmapped type is
   reported as flagged, with the reason, and resolved at S8 with the user. Do
   not substitute a "close enough" type silently.

8. **A halt is a halt.** Exit code 3 means an identifier-case or target-name
   collision. Show the collisions and stop; do not pick a winner.

9. **Never report success ahead of verification.** AIDP creates are
   asynchronous and settle late or fail silently. "Pending", "still settling"
   and "exit code nonzero" are not success — name which one it is. Never say
   a migration is complete before every object shows `verified`.

10. **After every step, say where the run stands — unprompted.** What this step
    actually produced, which step is next, and the command or confirmation that
    would run it. The user should never have to ask "where are we".

11. **Always present the data-movement architecture options.** S1–S12 create
    structure; they move no rows. *How* rows eventually move is a separate
    decision, and it belongs to the customer because it drives cost,
    wall-clock and whether a later migration can run unattended. Raise it at
    S11, when the per-schema copy scripts are handed over, and do not let the
    section pass unread.

    There are six. `A1` unload to object storage, `A2` federate through the
    EXTERNAL catalog, `A3` redirect ingestion, `A4` Iceberg interop, `A5`
    hybrid waves, and **`A6_CUSTOMER_DEFINED` — the open slot**. The customer
    may already run a pattern better than anything here, or may simply not
    have decided; both are valid answers and neither is forced into one of
    the others.

    If nothing has been chosen, say plainly that the architecture is
    **undecided**. Record a choice with `snowmig.py data-options --choose
    <id> --rationale "..."`; a rationale is mandatory. **None of the six is
    implemented** — recording a choice executes nothing, and
    `execute_transfer()` refuses by design.

    When the user describes their own design, record it verbatim with
    `--custom-name` and `--custom-description-file`. It is **never mapped**
    onto one of ours: a customer design filed under `A1` reads as an assessed
    variant of `A1`, and it is not. Never paraphrase their design into ours.

## Scale is the design constraint

An estate is thousands to hundreds of thousands of objects. Everything above is
shaped by that: two queries for discovery instead of one per object, a schema
as the unit of work instead of a table, resumable scripts that skip what a
report already records as done, and grouped decisions instead of per-column
questions. If you find yourself doing something once per table — asking,
translating, verifying, creating — stop: that is the shape that does not
finish.

## Engine

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig <stage> [...]
```

AIDP calls go through the `aidp` CLI when installed, otherwise
`oci raw-request`. The engine prints which backend it chose and fails loudly if
neither is present rather than guessing a transport.

The in-AIDP data plane is `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`:
`00_discover_snowflake.ipynb`, `01_create_structure.ipynb`,
`02_copy_schema.ipynb`, `03_reconcile.ipynb`.

**Everything that runs on AIDP is `.ipynb`; the `.py` are local only.** AIDP
types a workspace object by extension, so a `.py` is stored as a FILE and a
job task cannot run it. The `.py` under `${CLAUDE_PLUGIN_ROOT}/engine/dataplane/`
— the four stages over the shared `snowmig_source.py` — are the canonical
SOURCES: they stay on the operator's machine, they are what the tests import,
and `build-notebooks` compiles them into the committed notebooks above. Edit
the source, never the generated notebook.

## One thing to raise even when nobody asks

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`** — it maintains layout and
reclaims storage in the background, un-asked. AIDP has both, plus `ZORDER BY`
and liquid clustering, and **runs none of them for you**. Nothing is lost in
the migration; the *responsibility* moves, on day one, to whoever owns the
target. Raise it before S10, not after.
