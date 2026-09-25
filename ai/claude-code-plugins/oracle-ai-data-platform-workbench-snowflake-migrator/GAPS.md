# Gaps and next steps

**State:** v0.18.0 · 1005 offline tests · 14 live tests (skipped without
credentials) · dev mode (`demo`) runs the whole pipeline emulated · **a live
validation campaign on 2026-09-16 exercised the whole prod path**: 1002-object
assessment, EXTERNAL registration, canary structure clone, provisioning, and
in-AIDP discovery + structure creation as AIDP jobs.

This is the single list. `ACTION-ITEMS.md` holds the detail and the reasoning
behind each item; this file is the ranked view of what is left.
`ARCHITECTURE.md` holds the stage-level design the items below are measured
against.

**Reviewed 2026-09-16.** The 0.16.0 pivot's P0s (the EXTERNAL default not
wired through the deploy guard, the smoke probe, or the stage board), the P1
packaging/security items and every P2 documentation contradiction are now
closed — see *Recently closed*. What remains ranked below is the P3 list:
surfaces whose command shapes are inferred and unexecuted, and the analysis
items (blast radius, maintenance proposal, parity, cost).

---

## P0 — the twelve-step runbook is not fully wired

`skills/snowflake-migrator-overview/SKILL.md` now specifies the migration as a
fixed sequence, S1 to S12, with the data plane running inside AIDP as
workflows. Some of that sequence is wired and some of it is not. **An agent
following the runbook will reach S7 and find no bridge**, so these are ranked
first.

### Wired

| Step | How | State |
|---|---|---|
| S1 workspace | `snowmig.py provision` | creates; a taken name now HALTS |
| S2 migration cluster | `snowmig.py provision` | creates; a taken name now HALTS. A cluster POST fired before the workspace reports ACTIVE is a `409`; resume with `--reuse-existing` |
| S3 EXTERNAL catalog | `snowmig.py catalog` | live-verified. Needs `--workspace` and `--cluster-id`, which is why it follows S1/S2 |
| S4 INTERNAL target catalog | `snowmig.py catalog --catalog-type standard` | live-verified 2026-09-19; creates the container only. `STANDARD` is an alias — the wire value is `INTERNAL` |
| S5 folder + scripts | `snowmig.py provision` | live-verified |
| S6 discovery as a workflow | `snowmig.py run --job snowmig_00_discover` | **new stage**; `watch_job` was unreachable from the CLI before |
| S7 manifest -> plan | `snowmig.py ingest` | **new stage**; calls the same type mapper as `assess`, so verdicts agree |
| S10 structure as a workflow | `snowmig.py run --job snowmig_01_structure` | same stage, per schema |
| S12 warehouse-equivalent clusters | `provision --warehouse-clusters` | creates at default config |

### Not wired — ranked

1. **Views discovered in AIDP arrive without their SQL.** `00_discover`
   records a view's columns, not its definition, so `ingest` marks such a
   view untranslatable and the planner refuses it. Tables are unaffected.
   Closing this means having discovery also capture `GET_DDL` per view —
   cheap in absolute terms (views are a small fraction of an estate) but it
   does cost one query per view, giving up the two-queries-per-database
   property for that subset. Until then, an estate whose views must migrate
   has to be planned from a live `assess`.

2. **`deps` still needs a live Snowflake session.** `ingest` writes
   `dependencies.json` empty with provenance `not_extracted`, which is
   correct for a manifest — no view edges exist to lose, because manifest
   views are refused. But an estate that needs real view ordering cannot get
   it from the in-AIDP path.

3. **S7's plan is not the nested shape the runbook describes.** The runbook
   promises schemas -> tables -> columns -> types nested in one JSON with
   per-item review flags. `ddl_plan.json` is a flat statement list and
   `plan.json` splits `can_migrate` / `cannot_migrate`. The information is
   there; the shape and the explicit review flag are not.

4. **S8 has no grouping.** The runbook forbids asking about conflicts one at a
   time and requires them grouped into families with a count each. Nothing
   computes those families — an agent would have to group by hand, which is
   the failure mode the rule exists to prevent.

5. **S9's scope reduction is manual and unbacked.** Reducing scope today means
   hand-writing a `--restrictions` file; nothing backs the full plan up into
   AIDP first, and nothing writes the reduced plan as a distinct, named
   artifact. The runbook requires both.

6. **S11 creates one copy job, not one per schema.** `provision` wires four
   jobs, one of which is `snowmig_02_copy_schema` taking `--schema` as a
   parameter. The runbook asks for **one script and one workflow per schema**
   so each has its own run history and evidence. Parameterising one job is
   not the same deliverable.

7. **The estate statistics S11 promises are not computed.** Average table
   size, largest, smallest, totals per schema — `inventory.json` carries the
   inputs, no stage rolls them up.

### Behaviour changed in this pass

- **Never reuse.** `provision(reuse_existing=False)` is the default: a
  workspace, cluster or job whose name is taken is reported `name_taken` and
  the run stops, rather than being adopted. `--reuse-existing` opts back in.
  Previously all four were silently reused.
- **A managed target catalog may be created.** The CLI refused it outright;
  it now creates the container and says that only the container was made. The
  refusal that remains is the correct one: its *tables* are not created
  through the control-plane CRUD API.
- **`STANDARD` was never a real catalog type.** `CATALOG_TYPES` claimed
  `("EXTERNAL", "STANDARD")`, but AIDP answers `400 InvalidParameter: Invalid
  CatalogType: STANDARD`. Reading the catalogs of a live DataLake shows only
  `INTERNAL` and `EXTERNAL`. The constant is corrected and `STANDARD` is kept
  as an alias, normalised once so it can never reach the wire.
- **The environment is created before the catalogs.** `resolve_target()`
  requires all four coordinates for any AIDP write, so registering the source
  catalog first — as the runbook used to say — stopped on `AIDP target
  coordinates not supplied`. The order is now workspace, cluster, EXTERNAL,
  INTERNAL.
- **`snowmig.py run`.** `target/jobs.py` had a live-verified `watch_job` —
  run, poll to terminal, fetch task output — reachable from no CLI stage. It
  is now the `run` stage, and it reports a spent poll budget as STILL
  RUNNING rather than rounding it to a verdict.

---

## What is actually proven

Worth stating first, because "verified" now means something specific here.
Updated after the 2026-09-16 live validation campaign: a trial Snowflake
account (1002 objects, 11 schemas) and a shared AIDP DataLake. Coordinates
live in the gitignored config files, never here.

| Surface | Status |
|---|---|
| Snowflake extraction (inventory, census, lineage, maintenance, security) | **Live-verified**, now at 1002 objects / 11 schemas in one batched pass |
| Read-only enforcement on the source | Enforced at the transport, tested |
| Plan, waves, restrictions, DDL generation | Unit-tested; generated SQL parsed by a real Spark parser; 1002-statement plan built live |
| Target **catalog CRUD** transport | **Live-verified** — canary 5/5 tables created in an INTERNAL catalog and read back structure-equal |
| The deploy **EXTERNAL guard** | **Live-verified** — refused before the first create, clean message, exit 1 |
| Case folding, async polling, key resolution | **Live-verified** (each was a real failure first) |
| `asyncOperations` waiter (`aidp-async-operation-key`) | **Live-verified** — SUCCEEDED/FAILED with error fields |
| Target **SQL** transport | **Dead** — no SQL REST endpoint exists (doc + live 404 agree) |
| Smoke test, destination half | **Live-verified** — read + write probe with cleanup, catalog-type aware |
| **EXTERNAL catalog registration** | **Executed and created.** The API itself enumerated the real contract (`connectionDetails.connectionProperties`, `SNOWFLAKE_*` keys, auth enum `Basic\|KeyPair`) — see B2a. The connection **values** remain unproven: the crawler/testConnection fail with "Login has timed out", and every pre-existing external catalog in that DataLake is Oracle-network ATP/ADW, so crawler egress to the public internet is the suspect, not the body |
| Provisioning (workspace/cluster reuse, folder tree, uploads, jobs) | **Live-verified end to end, exit 0** — via the `workspace-object` surface; the Jupyter contents API on that build 200s on PUT and then 404/500s on read-back, and cannot create directories |
| Jobs | **Live-verified**: creation, run and output fetch for NOTEBOOK_TASK (driver notebooks). PYTHON_TASK is accepted at creation and fails every run resolving the file; job `parameters` reach the notebook neither as argv nor env — hence the generated drivers with inline args. `/Workspace` mount on cluster FS probed and confirmed |
| Data movement scripts | **Live-verified**: discovery (11 schemas / 1000 tables / 9935 columns in two `INFORMATION_SCHEMA` queries) and structure creation from the approved `ddl_plan` both ran SUCCESS inside AIDP. Copy + reconcile were exercised on one schema |
| **AIDP Snowflake connector** as the source | **Live-verified** — read a table and ran pushdown from the cluster with no extra library. Now the DEFAULT source mode |
| EXTERNAL catalog **crawler** | **Fails on the validated deployment** — `CONNECTOR_0067, Login has timed out`, with credentials the connector accepts. Not an FQDN form (both host forms resolve identically and both fail). Suspect: the crawler's network path, which is not the cluster's. Raised as B16 |

---

## P3 — carried forward, re-prioritised

### 12b. `backup/` is created and never written to — S6's backup is not real

Found on a live run, 2026-09-19. `BACKUP_FOLDER` is defined in
`target/provisioning.py:63`, created with the other folders at line 406, and
asserted by `test_provisioning.py:519` — **but nothing in the engine ever
writes a file into it.** The only assertion anywhere is that the folder
exists.

The runbook promises more than that. S6 says the manifest is "written to
`reports/` and **backed up into `backup/`**, dated, as the reference input
every later script reads", and S9 says the full plan is backed up into AIDP
before a reduced plan replaces it. Neither backup happens. On the 2026-09-19
run, discovery wrote `reports/discovery_manifest.json` and
`reports/DISCOVERY.md`, and `backup/` listed empty.

The consequence is not data loss today — `reports/` still holds the manifest —
but the runbook's evidence guarantee is weaker than it reads. In particular
S9's "the full plan stays recoverable" is currently untrue: a reduced plan
overwrites with no copy kept. **Rule 3 says evidence is a deliverable, so
this is a correctness gap in the audit trail, not a nice-to-have.**

Fix: have `00_discover` write `backup/discovery_manifest_<UTC date>.json`
alongside the report, and have the plan-reduction path at S9 copy the full
plan into `backup/` before writing the reduced one. Until then, either back
the manifest up by hand or accept that `backup/` is decorative.

### 12c. Discovery's summary line counts tables but not views

Cosmetic, same run. `00_discover` printed `11 schema(s), 1000 table(s)` for a
manifest that actually holds **1000 tables and 2 views**. The manifest itself
is correct and complete; only the stdout roll-up undercounts, because it sums
the `tables` bucket and ignores `views`.

It cost real time on the 2026-09-19 run: the line was reconciled against
Snowflake's 1065 relations and read as 65 objects lost, when the true
difference was the 63 `INFORMATION_SCHEMA` views that are correctly excluded.
**A summary that disagrees with its own artifact will be believed over the
artifact.** Print both counts.

### 13. `executor.py`'s notebook upload/run shapes are now known to be wrong

Settled by the live campaign, and **not yet removed**: `upload_notebook`,
`run_notebook` and `run_status` in `executor.py` are legacy guesses that the
validated surfaces contradict. Upload is `workspace-object` (relative paths,
via the `aidp` CLI) — the Jupyter contents API 200s and then cannot read the
file back. Execution is a **Job with a NOTEBOOK_TASK plus a jobRun**;
`notebookRuns` does not exist. Both working shapes are implemented in
`provision_api.py` / `jobs.py`.

**Fix:** point `snowmig.py notebook --upload` at the working surface and
delete the three dead operations. Until then, `notebook --upload` is the one
command in the plugin whose transport is known-bad.

### 13a. The external catalog's crawler cannot reach Snowflake *(B16)*

The one finding from the live campaign that is **not** fixed and is not ours
to fix. The catalog registers and is ACTIVE; `actions/refresh` and
`actions/testConnection` both fail `CONNECTOR_0067 — Login has timed out`,
while the AIDP Snowflake connector reads the same account from the cluster
with the same credentials. Ruled out: credentials (the connector uses them),
cluster egress (probed, :443 open), and the FQDN form (org-account and
account-locator hosts resolve to the same backend and fail identically).
Remaining hypothesis: the crawler runs outside the cluster's network path, and
this is the first non-Oracle-network external catalog on that DataLake.

**Consequence, already absorbed:** `--source-mode connector` is the default
for every in-AIDP script, so the migration does not wait on this. What is
still blocked is the *browsability* an external catalog provides.

**To do:** raise with the AIDP team; the Slack thread about mandating
`testConnection` before `createCatalog` is the same class of problem
(a catalog accepts metadata it cannot actually use, and the failure surfaces
much later from inside the crawler).

### 13b. Still inferred after the campaign *(B12)*

Only one shape remains unproven in `provision_api.py`: the cluster-library
item's artifact field (`package` / `path` / `coordinates`). Nothing in the
validated path installs a library, so it was never exercised. Everything else
in that module — workspaces, clusters, folders, uploads, jobs, job runs, task
runs, output fetch, asyncOperations, testConnection — is live-verified.

### 14. ~~The EXTERNAL `connectionDetails` shape is a guess~~ ✅ SETTLED *(B2a)*

The API enumerated its own contract when handed a wrong key, and a catalog now
exists that was built from it: the map nests under
`connectionDetails.connectionProperties`, the keys are `SNOWFLAKE_*`, and the
auth enum is `Basic | KeyPair`. `catalog --test-connection` is now a wired
command — and it needs an EXISTING catalog key, because it resolves RBAC
`DESCCATALOG` (B17).

### 14a. Structure creation is the only scale-tested write path

`--mode ddl-plan` applies the engine's already-translated types with no source
read, which is why it is the default: `--mode ctas` costs one Snowflake round
trip per table, and creating 100 tables that way was still running after
minutes. Neither mode has run against a full estate. Job STARTUP is ~5–6
minutes per run (measured), so the operating unit is a schema, never a table.

### 15. The region map covers 12 short codes

`target/coords.py` fails loud on an unmapped code, which is the right
behaviour — but `ord`, `sjc`, `arn`, `zrh`, `mel`, `qro` and the rest are
simply unreachable, and `_endpoint` hardcodes `oraclecloud.com`, so no OC2/OC3
realm can work.

### 16. Blast radius — MEDIUM *(C3)*

The census says *what exists*; it does not say *what stops working*. A task
that populates a migrated table means that table goes stale after cutover: the
clone succeeds and then quietly stops being correct. Join the census against
`OBJECT_DEPENDENCIES` and report, per migrating table, "populated by task X,
which does not migrate".

### 17. Maintenance proposal and job — MEDIUM *(M3, M4)*

M2 delivered the inputs. Still missing: a per-table `OPTIMIZE` cadence from
measured churn, `ZORDER` keys seeded from the source clustering key, a
`VACUUM` retention never shorter than the source's, and all of it emitted as a
**disabled** job.

### 18. Time-travel parity statement — LOW *(M5)*

Source recovery window (`DATA_RETENTION_TIME_IN_DAYS` + 7 days Fail-safe)
against the proposed Delta retention, with any reduction called out and
Fail-safe named as having no equivalent.

### 19. Cost comparison — LOW *(M6)*

Snowflake automatic-clustering credits versus the AIDP cost of the M3 cadence.
Needs a live AIDP run to calibrate.

---

## Recently closed

**2026-09-16 — the 0.16.0 P0/P1/P2 sweep.** All four P0s were one finding —
the EXTERNAL pivot changed the default without re-running the verification
discipline — and are now code:

- **`deploy --execute` refuses an EXTERNAL catalog.** `deploy_catalog` now
  resolves the target's `catalogType` before its first create and refuses
  EXTERNAL, an absent catalog, and an unreadable listing — "could not look"
  and "safe to write" are different claims. The resolved type is recorded in
  `deploy_result.json`. (Also fixed on the way: `catalog_deploy`'s
  `RefusedToExecute` was not in `main()`'s except tuple, so the refusal would
  have been a traceback, not a message.)
- **`smoke --write-probe` is catalog-type aware.** The probe is skipped with
  an explanatory note against an EXTERNAL catalog — read-only by design is not
  a failure — and `catalog_type` is reported either way (`"unknown"` when it
  could not be resolved, never silently assumed).
- **The stage board knows `catalog` exists.** It sits between `smoke` and
  `deploy`, reads `catalog_result.json`, flags `create_requested` as pending
  rather than success, and the board now claims **two** writers. Bonus:
  `data-options` moved to its real position (feeding `plan`) and is marked
  optional so `next_stage` never stalls on it.
- **`ensure_catalog` polls the read-back** — this had in fact already been
  fixed (`_poll_for_catalog`, bounded backoff, and `_find_catalog` raises on a
  failed listing); the entry here was stale. The remaining nuance is
  deliberate: a listing that fails *mid-poll* counts as "not visible yet",
  because it is not evidence either way.
- **The catalog credential left argv.** The `create_catalog` body now travels
  by a 0600 temp file (`file://`, removed after the call), and the printed
  command redacts `connectionDetails` **values** explicitly — field names
  stay, since they are what a human checks. The old 200-char truncation only
  hid the secret by luck.
- **`pyyaml` moved to `requirements.txt`** — it is a runtime dependency of the
  default path's connection config.
- **The ten tracked `.pyc` files are gone**, and the `.gitignore` re-include
  that pulled `engine/target/__pycache__` back in is explicitly counter-ruled.
- **ASSUMPTIONS.md tells the truth about AIDP** — the header now states the
  live-verified/never-executed split, B3/B4/B5 and "Outstanding items" item 1
  were rewritten, and the test that enforced the stale "Never contacted"
  literal now pins the split instead.
- **The router lists all 13 stages + `stages`**, `/snowflake-catalog` exists,
  and the `--write-probe` help says what the code does (creates one schema,
  removes it, names a failed cleanup).

Older closures:

- **The smoke test lied about the destination.** It called the SQL endpoint
  that returns 404 and reported FAIL against a destination that worked. The
  destination half now runs on the catalog API, with a uniquely-named write
  probe that confirms visibility and cleans up after itself. Live: PASS.
  *(Note item 3 above: the same class of bug has reappeared for EXTERNAL.)*
- **A poisoned name is now recognised and named** *(P1)*. On the first object
  that never appears, the plugin creates one throwaway object with a novel name
  in the same schema — once per schema — and distinguishes "your planned names
  are burned, retry into a fresh schema" from "the request itself is wrong".
  `--no-diagnose` turns it off, since the probe writes. Live: 5 of 5 correctly
  diagnosed against a deliberately poisoned schema.
- **Test-account identifiers genericised** in source and fixtures; real values
  now only in the gitignored `snowmig-config.yaml` -- the one config file.

---

## Parked — TBD, do not re-raise

These are recorded and deliberately **not** blocking. Nothing in the ranked
list above waits on them, and they should not be surfaced in reports or
conversation until the phase that needs them arrives.

| # | Parked item | Revisit when |
|---|---|---|
| **JDBC** | Whether AIDP reads Snowflake over JDBC, or the plugin uses JDBC instead of the Python connector | **The byte-movement phase.** It belongs in the generated notebook, where it is auditable by the user and reviewable afterwards — not in the structure clone |
| **P3** | Is a failed create *meant* to reserve the name permanently, and what clears it? | Oracle answers. Workaround in the meantime: retry into a fresh schema |
| **T5** | Is `timestamp_ntz` support planned on the catalog API? | Oracle answers. Workaround: `--timestamp-ntz timestamp`, which records the caveat |
| **—** | Is view column-type derivation documented? | Oracle answers. The plugin reports the drift per column regardless |

## Open questions that need the customer

| Question | Blocking |
|---|---|
| Silver/Gold job body shape | The medallion deliverable is one third stubs |


---

## Known limits, stated rather than hidden

- **No data is moved.** By design; `DATA_CLONE` and `DONE` are unreachable.
- **Scale is untested.** Validated on 7 objects. Pagination, per-schema column
  reads and metadata row counts are all in place, but nothing has run against
  a large estate.
- **`VARIANT`/`OBJECT`/`ARRAY` block their table** unless
  `--semi-structured string`, which defers rather than solves.
- **A view's column types are derived by the target**, not carried over.
  Verified: three aggregate columns changed type, two of them narrowing.
