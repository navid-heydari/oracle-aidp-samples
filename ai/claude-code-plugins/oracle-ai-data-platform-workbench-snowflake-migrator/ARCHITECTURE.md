# Stage-level design

How the Snowflake → AIDP migrator is put together, and how a run actually
executes. `GAPS.md` is the ranked list of where the code does not yet match
this design; every such divergence is cross-referenced below as **[GAP n]**.

---

## 1. The model

The plugin is **one CLI plus a filesystem**. Every stage is a subcommand of
`engine/snowmig.py`, and stages communicate only through JSON artifacts in
`--out-dir`. There is no daemon, no session, no shared in-memory state.

```
skill / slash command  →  snowmig.py <stage>  →  --out-dir/*.json + *.md
        (agent)              (engine)                 (the contract)
```

Three consequences, all deliberate:

- **A stage is resumable and re-runnable.** Its inputs are files on disk, so a
  failed run is resumed by re-running that stage, not the pipeline.
- **The agent never holds migration state.** It reads `STAGES.md`. A new
  conversation picks up an old `--out-dir` with no handover.
- **Every claim is auditable after the fact.** The artifact is the evidence,
  and it outlives the terminal it was produced in.

### Layering

| Layer | Module | Rule |
|---|---|---|
| Source | `snowflake_source/` | Read-only, enforced at the transport. Cannot emit a non-read verb |
| Translation | `snowflake_source/dialect/` | Pure. Refuses rather than guesses |
| Planning | `plan/` | Pure and offline. Touches no network |
| Rendering | `report/` | Pure. Reads artifacts, writes markdown |
| Target | `target/` | The only layer that can write to AIDP. All I/O injected as `call` / `run_sql` |

The injected-callable convention is what makes the whole target layer
unit-testable with no environment — 1032 offline tests, and every behaviour a
live AIDP taught us is regression-tested without one.

---

## 2. Stage inventory

Sixteen stages. `needs` is what must be reachable; `writes` means it can
change the destination.

| # | Stage | Needs | Reads | Produces | Writes to AIDP |
|---|---|---|---|---|---|
| 0 | `preflight` | a connection config *(+ either end, to test it)* | the config file | `preflight.json`, `PREFLIGHT_CONFIG.md` | no |
| 1 | `assess` | Snowflake | — | `inventory.json`, `INVENTORY.md`, `CENSUS.md` | no |
| 2 | `deps` | Snowflake | `inventory.json` | `dependencies.json` | no |
| 3 | `maintenance` | Snowflake | `inventory.json` | `maintenance.json`, `MAINTENANCE.md` | no |
| 4 | `security` | Snowflake | `inventory.json` | `security.json`, `SECURITY.md` | no |
| 5 | `compute` | Snowflake | — | `warehouses.json`, `compute.json`, `COMPUTE_PROPOSAL.md` | no |
| 6 | `data-options` | offline | — | `data_options.json`, `DATA_MOVEMENT_OPTIONS.md` | no |
| 7 | `plan` | offline | `inventory.json`, `dependencies.json`, *(`data_options.json`)* | `plan.json`, `PLANNED_OBJECTS.md` | no |
| 8 | `ddl` | offline | `inventory.json`, `plan.json` | `ddl_plan.json`, `DDL_PLAN.md` | no |
| 9 | `smoke` | Snowflake + AIDP | — | `smoke.json`, `SMOKE_TEST.md` | **only with `--write-probe --execute`** |
| 10 | `catalog` | AIDP | — | `catalog_result.json`, `CATALOG.md` | **yes, with `--execute`** |
| 11 | `deploy` | AIDP | `ddl_plan.json`, `plan.json`, `inventory.json` | `PREFLIGHT.md`, `deploy_result.json`, `SOFT_CLONE_SUMMARY.md` | **yes, with `--execute`** |
| 12 | `notebook` | offline | `ddl_plan.json`, `plan.json`, `inventory.json` | `*.ipynb`, `NOTEBOOK.md` | no — `--upload` is a dry run, refused with `--execute` **[GAP 13]** |
| 13 | `summary` | offline | `plan.json`, `inventory.json`, *(`deploy_result.json`)* | `SUMMARY.md` | no |
| 14 | `provision` | AIDP | the scripts + whatever plan artifacts exist | `provision_result.json`, `PROVISION.md`, and the AIDP-side folder, drivers and jobs | **yes, with `--execute`** |
| — | `stages` | offline | everything present | `STAGES.md` | no |
| — | `demo` | offline | — | every artifact above, emulated, + `DEMO.md` | no |

Past `provision`, the work moves INSIDE AIDP: four jobs run the scripts in
`data-migration-scripts/` — self-contained `.ipynb`, generated from
`engine/dataplane/` (discover → structure → copy, schema by schema →
reconcile), and their reports land in the workspace, not in `--out-dir`.

`stages` is not a pipeline step; it is the read-out of one.

**Four stages write — `provision`, `catalog` and `deploy`, each a dry run
without `--execute`, and `run`, which has no dry run and starts an in-AIDP
job when invoked — plus, narrowly and opt-in, `smoke --write-probe
--execute`.** The stage board
says exactly that, lists `provision` and `catalog` in their dependency
positions, and reads their artifacts (a `create_requested` that never became
visible is flagged as pending, not success).

---

## 3. Execution graph

```
                        ┌──────────┐
                        │  assess  │  (Snowflake)
                        └────┬─────┘
             ┌───────────────┼───────────────┬──────────────┐
             ▼               ▼               ▼              ▼
         ┌───────┐    ┌─────────────┐  ┌──────────┐   ┌──────────┐
         │ deps  │    │ maintenance │  │ security │   │ compute  │
         └───┬───┘    └─────────────┘  └──────────┘   └──────────┘
             │              (advisory — inform the report, gate nothing)
             │
             │        ┌──────────────┐
             │        │ data-options │  (offline; optional, records a choice)
             │        └──────┬───────┘
             ▼               ▼
          ┌────────────────────┐
          │        plan        │  ← restrictions.json, --bronze-* naming
          └─────────┬──────────┘
                    ▼
             ┌────────────┐
             │    ddl     │
             └─────┬──────┘
                   │
                   ▼
            ┌────────────┐
            │   smoke    │   both ends reachable? permissions? (gate)
            └─────┬──────┘
                  │
        ┌─────────┴──────────────────────────────┐
        ▼  DEFAULT                               ▼  ON EXPLICIT REQUEST ONLY
  ┌──────────────┐                        ┌──────────────┐
  │   catalog    │  EXTERNAL/SNOWFLAKE    │   catalog    │  STANDARD → CONTAINER only
  │  --execute   │  read-only pointer     │  (standard)  │
  └──────┬───────┘  copies nothing        └──────┬───────┘
         │                                       ▼
         │                                ┌──────────────┐
         │                                │   notebook   │  local .ipynb only
         │                                └──────┬───────┘
         │                                       ▼
         │                                run --job snowmig_01_structure
         │                                (Spark reports real errors;
         │                                 the CRUD API returns 202 and
         │                                 can silently create nothing)
         │                                       │
         └───────────────┬───────────────────────┘
                         ▼
                   ┌───────────┐
                   │  summary  │
                   └───────────┘
```

`deploy` is the legacy third branch: control-plane CRUD straight into a
Standard catalog. It is live-proven (7 objects) but is no longer the
recommended path for Standard catalogs, because a 202 Accepted can silently
create nothing and Spark cannot. It resolves the target's `catalogType`
before its first create and **refuses an EXTERNAL target** — and equally an
absent catalog or an unreadable listing, because "could not look" is not
"safe to write".

### Why the fork exists

| | EXTERNAL (default) | STANDARD (on request) |
|---|---|---|
| What it is | A registered, read-only pointer at the live Snowflake source | Managed Delta tables in AIDP storage |
| Copies data | No — nothing to keep in sync | No (structure only), but the storage is real |
| Blast radius | Registration only | Creates objects the customer now owns |
| Created by | `snowmig.py catalog --execute` | A script run on AIDP compute |
| Failure surface | One catalog | Every schema, table and view |

Default to the smaller blast radius. A Standard catalog is never offered,
never assumed, and only ever built when the user has asked for one in words.

---

## 4. The invariants

Six rules the design holds everywhere. Each was a real failure first.

**I1 — The source is read-only, at the transport.** Not by grant, not by
convention. `snowflake_source/conn.py` refuses a non-read verb, so no skill,
prompt or bug can write to the customer's Snowflake.

**I2 — Read back and compare; a 2xx is not the claim.** AIDP creates are
asynchronous and return **202 Accepted with an empty body and no work-request
id**, so there is no waiter and a failure reports nothing. Six tables once
returned 202 and not one existed. Verification is therefore: poll with a
bounded backoff, resolve the key from the server, `GET` the object, compare
the field list. "The planned columns are there" is the claim. `ensure_catalog`
polls the same way; a listing that fails mid-poll counts as "not visible yet",
because it is not evidence either way.

**I3a — ask the source that can answer, not the one that is convenient.**
Two sources answer "what is attached to this object": the account-wide
`ACCOUNT_USAGE` views, one statement for the estate but up to ~2 hours stale
and gated behind `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`; and the
`INFORMATION_SCHEMA` table functions, one round trip per object, current, and
needing no extra grant. A hedge about staleness is not a substitute for the
read that is not stale. Both are read and UNIONed, each attachment says which
source saw it, and a row only the stale one has is marked for confirmation
rather than believed or dropped. Where the per-object read cannot run --
denied, or an estate over the budget for one round trip per object -- the
older verdict and its hedge stand, and the report says which case produced
the number.

**I3 — "Could not look" never renders as zero.** An unreadable `ACCOUNT_USAGE`
reports `measured: false` with null counts, because *0 reclustering credits*
and *we could not check* lead to opposite decisions. Same for unreadable
scopes in the census and unresolvable policy references. The rule has a
second half: a verdict may not name a kind that was never enumerated. A
sentence like "no aggregation policy is attached" is only true if
`SHOW AGGREGATION POLICIES` was issued and answered; the security report
builds its clean sentence from the kinds that actually answered and names
the rest as not enumerated. And there is a third state between counted
and unreadable: *not distinguishable*, when rows were read and counted
under their parent kind but the column that tells a UDTF from a UDF, or
an external stage from an internal one, could not be read. That is
reported as such, never folded into either neighbour.

**I4 — Refuse rather than guess.** An unmappable type, an unknown OCI region,
a `LISTAGG … WITHIN GROUP`, a `::` cast over an expression: all raise. A
guessed value produces an object that silently differs from the plan, and the
structure probe then reports a mismatch it cannot explain.

**I5 — Nothing is chosen on the user's behalf.** Target coordinates may come
from the one migration config (`snowmig-config.yaml`), but never *silently*:
the CLI prints the file it read and the destination it took from it before
anything acts on them, and a write still needs `--execute`. `target/coords.py`
performs no I/O at all — it cannot discover a destination, only be handed one —
so a stale config can misdirect a run only in plain sight. Nothing is read from
the environment or a cache. The architecture options are presented in full with
`A6_CUSTOMER_DEFINED` as an open slot, and a customer design is recorded
verbatim, never mapped onto one of ours. One catalog per run: a multi-database
estate needs one approval each.

**I6 — Say where the run stands, unprompted.** After every stage the agent
reports the stage's real result, the board's `next_stage`, and the concrete
command that runs it. Pending is pending — never rounded up to success.

---

## 5. Gates

A gate is a point where the run stops and does not proceed on its own.

| Gate | Where | Condition |
|---|---|---|
| **Target collision** | `plan` | Two source objects fold to one target name (`ORDERS` / `"orders"`). Exits `HALT`. AIDP lower-cases identifiers, so the two would silently merge |
| **Unmappable type** | `ddl` | `VARIANT`/`OBJECT`/`ARRAY`/`GEOGRAPHY` block their table unless the operator opts into `string`, which defers rather than solves |
| **`timestamp_ntz`** | `ddl` | The catalog API silently rejects it. Blocked by default; `--timestamp-ntz timestamp` accepts the timezone-semantics change and records the caveat on the field |
| **Connectivity** | `smoke` | Both ends reachable with the permissions the next stage needs. The write probe is skipped, with a note, against an EXTERNAL catalog — read-only by design is not a FAIL |
| **`--execute`** | `catalog`, `deploy`, `provision`, `smoke --write-probe`, `notebook --upload` | Dry run otherwise. Nothing reaches AIDP without it (and `notebook --upload --execute` is then refused — **[GAP 13]**) |
| **EXTERNAL target** | `deploy` | The target's `catalogType` is resolved before the first create; EXTERNAL, absent, or unreadable → **refused** |
| **Managed catalog** | `catalog` | `--catalog-type standard` creates the CONTAINER only (as `INTERNAL`; `STANDARD` is an alias the API rejects) and returns `container_only`. Its **tables** are still refused here, with a pointer to the structure workflow (`run --job snowmig_01_structure`, S10) |
| **Explicit request** | skill layer | A Standard catalog requires the user to have asked, in words |

### Failure semantics

- **A failed async create poisons the object name in that schema, permanently.**
  Every later create for that name returns 202 and is silently dropped, and
  `DELETE` does not recover it. The plugin detects the signature — a novel name
  in the same schema succeeds — and says which situation the user is in. The
  only known recovery is a different schema.
- **A 409 "ongoing operation" is retried** with a bounded backoff, and the
  retry is recorded: a run that needed three attempts is worth knowing about.
- **An existing schema is never re-created.** POSTing one that is already there
  re-triggers async work, and tables created during that window are accepted
  and then dropped.
- **`oci raw-request` exits 0 on an HTTP error.** The status is in the response
  body, so exit code proves nothing; any non-2xx raises.

---

## 6. A run, end to end

The default (EXTERNAL) path, as the agent drives it.

```bash
E=${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py
OUT=./snowmig_out

# --- Investigate (read-only, Snowflake) ---
python3 $E assess      --out-dir $OUT --account ... --database MYDB
python3 $E deps        --out-dir $OUT --account ...
python3 $E maintenance --out-dir $OUT --account ...
python3 $E security    --out-dir $OUT --account ...
python3 $E compute     --out-dir $OUT --account ... --credit-price 3.00

# --- Decide (offline) ---
python3 $E data-options --out-dir $OUT              # present, never choose
python3 $E plan         --out-dir $OUT --restrictions ./restrictions.json
python3 $E ddl          --out-dir $OUT
#   → PLANNED_OBJECTS.md is the approval artifact. Stop here for sign-off.

# --- Prove the destination ---
python3 $E smoke --out-dir $OUT --account ... --datalake-ocid ... \
                 --workspace ... --cluster-id ... --catalog MYCAT

# --- Register (the only stage that writes, on this path) ---
python3 $E catalog --out-dir $OUT --catalog MYCAT \
                   --config ./snowmig-config.yaml \
                   --datalake-ocid ... --workspace ... --cluster-id ...
#   dry run first — prints the fields, never the secrets
python3 $E catalog ... --execute --test-connection
#   --test-connection only runs with --execute (RBAC resolves on an existing catalog)

python3 $E summary --out-dir $OUT
python3 $E stages  --out-dir $OUT        # where does this run stand?
```

At every step the agent states the stage's actual result, then `next_stage`
from the board, then the command — in the same turn, without being asked.

### Where the agent sits

Skills are the interface; the engine holds the decisions. A skill may not
re-implement engine logic, and the engine may not read anything the user did
not pass.

| Skill | Drives |
|---|---|
| `snowflake-migrator-overview` | Router + the shared rules above |
| `snowflake-migrator-bootstrap` | Dependencies and Snowflake auth |
| `snowflake-assess-estate` | 1–5 |
| `snowflake-migration-plan` | 6–8, and the approval conversation |
| `snowflake-smoke-test` | 9 |
| `snowflake-medallion-clone` | 10, and 11 for a requested Standard catalog |
| `snowflake-clone-notebook` | 12 |
| `snowflake-compute-proposal` | 5 |
| `snowflake-stage-board` | 13 / `stages`, proactively |

---

## 7. Where the code diverges from this design

Recorded here so the design is not read as a description of what ships. Full
detail and ranking in `GAPS.md`.

The 2026-09-16 sweep closed the wiring gaps (deploy's EXTERNAL guard, the
catalog-type-aware smoke probe, the stage board's `catalog` row and its
writer claim, the argv credential), and the live campaign that followed
settled the EXTERNAL registration contract and wired `test-connection` as a
command. What remains divergent:

| Design element | Divergence |
|---|---|
| `notebook --upload` | Still points at the Jupyter contents API and `notebookRuns`. Both are now **known wrong**: files go through `workspace-object`, and execution is a Job with a NOTEBOOK_TASK. The working shapes live in `provision_api.py` / `jobs.py`; this command has not been moved onto them **[GAP 13]** |
| EXTERNAL catalog browsability | The catalog registers, but its **crawler** cannot reach Snowflake on the validated deployment, so the estate is not browsable there. The data plane does not depend on it (connector mode) **[GAP 13a]** |
| Cluster libraries | The library-item artifact field is the last inferred shape; nothing in the validated path installs one **[GAP 13b]** |
