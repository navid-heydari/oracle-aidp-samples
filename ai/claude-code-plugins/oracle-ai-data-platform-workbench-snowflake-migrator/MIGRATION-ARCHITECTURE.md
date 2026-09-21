# Architecture: Snowflake → Oracle AI Data Platform migration

A deterministic, auditable migration of a Snowflake estate — structure first,
then data, schema by schema — into AIDP managed Delta, with **the whole data
plane running inside AIDP on Spark**: the operator's machine plans and
verifies, and no row of customer data ever passes through it.

This document is the migration design record: what maps to what, how a run
proceeds, what is automated, and what deliberately is not.

**To actually run one, follow `README.md` → "How to run a migration, from
zero".** That is the order of operations, with the commands and the one config
file they all read. This document explains *why* that order is what it is.
`ARCHITECTURE.md` is the companion engineering record of the CLI engine
itself; `GAPS.md` ranks what remains unproven.

---

## 1. The problem

A Snowflake estate is thousands to hundreds of thousands of objects. Migrating
it by conversation — an AI reading one table at a time — is slow, expensive
and unauditable. The straightforward alternatives all cost something:

| Approach | Cost |
|---|---|
| Hand-written one-off scripts | no inventory, no verification, no report — nobody can say what moved |
| ETL tooling per table | per-table configuration is the thing that does not scale |
| "AI does everything" | minutes per object, non-deterministic, and the audit trail is a chat log |

The answer here: **deterministic scripts discover, plan, create and copy;
verification is code; the AI plans, explains, and handles only what cannot be
translated mechanically** — and says so instead of guessing.

## 2. Solution in one diagram

```
 OPERATOR (Claude Code plugin — control plane)          ORACLE AIDP
 ┌───────────────────────────────┐        ┌──────────────────────────────────┐
 │ snowmig.py                    │        │  workspace  <ws-name, translated> │
 │  assess/deps/security/…  ─────┼─READ──►│  ┌────────────────────────────┐  │
 │  plan + ddl   (offline)       │  only  │  │ cluster: migration_assets  │  │
 │  provision  ──────────────────┼──REST─►│  │  Spark 3.5 · Delta 3.2     │  │
 │  catalog (register EXTERNAL) ─┼──REST─►│  └─────────────┬──────────────┘  │
 │  deploy / notebook (structure)│        │                │ runs            │
 │  reads reports, reconciles ◄──┼────────│  backup-snowflake-migration/    │
 └──────────────┬────────────────┘        │   ├─ scripts/ 00…03 (jobs)      │
                │                         │   ├─ plan/    plan.json, DDL    │
        Snowflake account                 │   └─ reports/ manifest, copies, │
 ┌──────────────┴───────────────┐         │               MIGRATION_REPORT  │
 │ databases · schemas · tables │◄──READ──│  EXTERNAL catalog (read-only    │
 │ views · warehouses · tasks…  │  only   │  3-part names: cat.schema.tbl)  │
 └──────────────────────────────┘         │  INTERNAL catalog(s) = target   │
                                          └──────────────────────────────────┘
```

Two read paths, one write surface. The operator's engine reads Snowflake
directly (fast, batched, read-only **enforced at the transport**) for the
inventory and plan; the data itself flows inside AIDP, through the **AIDP
Snowflake connector** on the cluster into managed Delta, where Spark reports
real errors and nothing transits the operator's machine.

Registering the account as an EXTERNAL catalog is still part of the flow — it
is how the estate becomes browsable in AIDP — but the data plane does not
depend on it. On the validated deployment the catalog registered fine and its
**crawler** could not reach Snowflake (`CONNECTOR_0067 — Login has timed
out`) while the connector, with the same credentials, read tables from the
cluster. So `--source-mode connector` is the default and
`external-catalog` is the option, not the other way round.

## 3. What becomes what

| Snowflake | AIDP | How | Fidelity notes |
|---|---|---|---|
| Account (the estate) | **Workspace** | `provision` — name translated to `[a-z0-9_]` so it cannot be rejected | rename reported, never silent |
| Database | **Catalog (INTERNAL)** | plan mirrors 1:1; created via structure clone | AIDP lower-cases identifiers; case collisions HALT the plan |
| Schema | Schema | structure clone | |
| Table | **Managed Delta table** | from the approved `ddl_plan` (engine-translated types, default), or CTAS `WHERE 1=0` through the source | `NUMBER(p,s)`→`DECIMAL(p,s)` exact; `VARIANT/GEOGRAPHY` **blocked** unless the operator opts into string; `TIMESTAMP_NTZ` needs an explicit downgrade decision |
| View | View | 6 exact dialect rewrites (`IFF`, `::`, `DATEADD`, `LISTAGG`…); 12 constructs (`QUALIFY`, `LATERAL FLATTEN`, …) **refused and named** for a human | target re-derives column types — drift is reported, narrowing flagged |
| Warehouse | **Compute cluster** | `provision` creates `migration_assets`; per-warehouse clusters proposed by `compute` with sizing left as a decision | same-name clusters, default config, per request |
| Table data | Delta rows | `02_copy_schema.ipynb` per schema: INSERT-SELECT through the connector (or the external catalog), verified by counts (+ exact decimal sums) | per-table snapshots — see §6 consistency |
| Task / Stream / Pipe / Dynamic table | **AIDP Job** (to be rewritten) | census inventories them with effort bands; **not auto-translated** | the blast-radius risk: a task that fed a migrated table stops feeding it after cutover |
| Procedure / UDF | Job or Spark UDF (rewrite) | census + language verdict (SQL/JS/Python/Java/Scala) | code is rewritten by humans/AI with review, never mechanically |
| Masking / row-access policy | — no equivalent API | security stage reports every exposure | data arrives **unprotected**; restricted views + classification is a design task |
| Secure view | — | blocked, named | guarantees do not survive |
| Time Travel + Fail-safe | Delta retention (+ nothing) | maintenance stage states the parity gap | Fail-safe has no equivalent |
| Stage / File format | Object-storage location / reader options | census pointer | |

## 4. Dev mode and prod mode

**Dev mode** (`snowmig.py demo`, `/snowflake-demo`) runs the entire pipeline
against a built-in emulated estate and an emulated AIDP — production code,
fake transports. It exists so anyone can see, in one minute and with zero
credentials, every artifact a real run produces and every refusal the design
makes (a blocked `VARIANT`, a `QUALIFY` view, a poisoned name, a deploy
refused against the read-only external catalog). Everything it writes is
marked emulated.

**Prod mode** is the same pipeline with credentials and `--execute` gates:

See `README.md` for the runnable form of this table, and
`skills/snowflake-migrator-overview/SKILL.md` for the twelve-step order
(S1–S12) it follows; the `S` labels below are that runbook's.

| # | Step | Command | Writes |
|---|---|---|---|
| 0 | **Confirm the connection config with the user, field by field**, and test both ends | `preflight` | no |
| 1 | Preview the estate from the laptop (objects, census, lineage, security, maintenance, warehouses) — optional; the migration's own discovery is step 6 | `assess` `deps` `security` `maintenance` `compute` | no |
| 2 | Plan + generate DDL, get sign-off (S7–S9) | `plan` `ddl` | no |
| 3 | Prove both ends | `smoke` | opt-in probe |
| 4 | Provision the AIDP environment: workspace (named after the source account), `migration_assets` cluster, **one cluster per Snowflake warehouse**, `backup-snowflake-migration/` (scripts + plan), 4 unscheduled jobs (S1, S2, S5). Copy `workspace.key` and `cluster.key` from `provision_result.json` into the config's `aidp:` block — `PROVISION.md` shows display names, not keys | `provision --execute` | yes |
| 5 | Register Snowflake as an EXTERNAL catalog (S3), then create the INTERNAL target catalog as a container (S4) | `catalog --execute`, then `catalog --catalog-type standard --execute` | yes |
| 5b | Confirm the environment from inside AIDP | `diagnose_environment.ipynb` | no |
| 6 | Discover the estate as a workflow inside AIDP; back up the manifest (S6) | `run --job snowmig_00_discover` | yes (workspace files) |
| 7 | Create the structure in the INTERNAL catalog on AIDP compute, from the approved plan, one workflow per schema (S10) | `run --job snowmig_01_structure` | yes |
| 8 | Later, on the customer's decision: **copy, schema by schema**, then reconcile | `snowmig_02_copy_schema`, `snowmig_03_reconcile` | yes (target only) |
| 9 | Read `MIGRATION_REPORT.md`: per table — structure, copy, live existence, verdict, why | `03_reconcile` | no |

`deploy` — control-plane CRUD straight into a Standard catalog — is not in
this table: it is live-proven but a `202 Accepted` can create nothing, so it
is reached for only when the user asks for it by name (see `ARCHITECTURE.md`).

Every write stage is a dry run until `--execute`; every create is read back
before it is called done; a 2xx is never the claim.

## 5. What is deterministic and what is AI

| Work | Who |
|---|---|
| Discovery, inventory, census, lineage, sizing inputs | scripts (batched SQL, paginated) |
| Type mapping, DDL, the 6 exact SQL rewrites | scripts — refuse rather than guess |
| Environment provisioning, uploads, job wiring | scripts, with per-step read-back |
| Data copy + verification (counts, exact decimal sums) | scripts, resumable per schema |
| Plan-vs-reality reconciliation | script, consulting the live catalog |
| Explaining reports, driving the conversation, sign-offs | AI (the plugin skills) |
| Rewriting the refused SQL (`QUALIFY`…), procedures, tasks, security design | AI/human, with the refusal artifact as the worklist |

## 6. Honest limits

- **Cutover consistency.** Each table is copied at its own instant; a live
  source yields a cross-table-inconsistent target. Freeze writers, copy from
  a point-in-time Snowflake `CLONE`, or plan an incremental re-sync. The
  copy report records timestamps so drift is attributable.
- **Pipelines do not migrate.** Tasks, streams, pipes, dynamic tables,
  procedures and UDFs are inventoried and effort-banded, not translated.
  On a real estate this is the largest human workstream.
- **Security does not travel.** Masking and row-access policies have no AIDP
  API; every exposure is listed and must be redesigned before real use.
- **Scale is designed for, not yet proven.** Pagination, per-schema batching
  and resumability are in place; the largest verified run is 7 objects.
- **Live-verified vs still unproven.** Live-verified (2026-09-16): the
  Snowflake extraction at 1002 objects; the structure-clone transport and its
  failure modes (async 202s, name poisoning, case folding, derived view
  types); the EXTERNAL registration contract; provisioning end to end
  (workspace, cluster, folders, uploads, jobs); the connector read and
  pushdown from the cluster. For the data plane the register is `GAPS.md` →
  "What is actually proven"; its sentence: **What has run live:** the
  discovery job (`snowmig_00_discover`) ran to SUCCESS on a migration
  cluster, reading 1065 relations and 9935 columns in two
  `INFORMATION_SCHEMA` queries; the structure job (`snowmig_01_structure`)
  ran on a cluster from the approved plan, a healthy 23-minute run left alone
  by the cold-start guard (2026-09-19); the copy (`snowmig_02_copy_schema`)
  and reconcile (`snowmig_03_reconcile`) jobs are **not yet confirmed by the
  authors**. Still unproven: anything at real scale (the largest run was one
  schema), the external catalog's crawl, and the cluster-library item shape.
- **Job startup dominates small work** — ~5–6 minutes per run, measured. The
  operating unit is a schema; per-table runs are the wrong shape.
- **A failed create can permanently burn its name** in that schema (observed
  live; not a plugin bug). The deploy diagnoses it; the recovery is a fresh
  schema — which is why nothing runs at scale before a canary schema passes.
