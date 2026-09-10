# Snowflake → AIDP Migrator — two-person delivery timetable

**Version 2** — re-cut 2026-09-08 after reading
`Rappi_Oracle_AIDP_Engagement_Enriched_v8.pptx`. v1 was anchored to a generic 13-week
plugin build. It is now anchored to a **named engagement already past executive demo**.

**Team:** 2 engineers. **Window:** W0 Sep 07 → W12 Dec 04 2026.
**Two hard dates, not one:**

| | Date | Deliverable |
|---|---|---|
| **📦 D1** | **Fri Oct 02** (end W3) | **Rappi Migration Assessment** — the customer-facing product. 3-week phase from the engagement deck |
| **📦 D2** | Fri Dec 04 (end W12) | MVP-1 migrator tooling per design §16 |

**Read first:** `RAPPI-CONTEXT.md` (customer facts, the 12 design-doc assumptions the
deck changes, open questions). Then design source of truth
`~/Workspace/oracle/snowflake_migrator/README.md`. Per-file disposition:
`PORTING-STATUS.md`.

---

## 0. What changed from v1, and why

v1 scheduled the estate assessment at W5 as a by-product of the tooling. **The deck
makes it the deliverable, on a 3-week clock.** Five re-orderings follow:

| # | Change | Driver |
|---|---|---|
| 1 | **Assessment moves W5 → W1–W3** and becomes D1. Tooling is built *in service of* it | Deck: "Migration Assessment (3 Weeks)" is the current phase |
| 2 | **Warehouse→compute sizing + credit model moves W12 → W2.** Was Low priority | 82 warehouses, 491 max clusters, **$5 MM/yr**. It is the business case |
| 3 | **`ACCOUNT_USAGE` access moves from a Medium risk to the W0 blocking gate** | **200,000 tables.** Per-object assessment is economically impossible; usage ranking is the only viable entry |
| 4 | **New workstream: Fivetran→AIDP ingestion.** Absent from v1 entirely | Heavy usage, "hard to displace", and **AIDP is not a supported Fivetran destination** |
| 5 | **Serving tier (ADW/ALH/ExaCS) moves from "out of scope" to in-deliverable** | All three reference architectures depend on it |

Also downgraded: **the unload path is no longer the biggest unknown.** The deck supplies
the strategy (same-region S3 unload → OCI/AWS Interconnect; historic via object storage,
current 1–2 weeks direct). What remains is a **region contradiction** — see R2.

---

## 1. The split, and why it does not overlap

Same principle as v1, re-drawn around the new workstreams. The two halves meet at
**three** frozen contracts now, not two.

| | **Engineer A — Target & Platform** | **Engineer B — Source & Translation** |
|---|---|---|
| Owns | Everything downstream of the landing zone: **AIDP, OCI, ADW/ExaCS, Fivetran, Airflow** | Everything upstream: **Snowflake, SQL, dialect, lineage** |
| Mental model | "Make the destination reliable, sized, and provable" | "Understand the estate and translate it" |
| D1 contribution | Target architecture recommendation · warehouse→compute sizing + credit model · ingestion path · transfer design | Inventory at 200k scale · usage profile · dependency waves · unsupported catalogue |
| Never touches | `snowflake_source/`, `references/` | `aidp_migration_core/`, `eval/`, `serving/`, `ingestion/` |

**The three seams (frozen W0, changed only by joint PR):**

1. **`InventoryRecord`** — B produces, A consumes. `contracts/inventory_record.schema.json`
2. **`DdlPlan`** — B emits target SQL + rule-audit trail, A executes and verifies. `contracts/ddl_plan.schema.json`
3. **`WarehouseProfile`** *(new)* — B extracts warehouse + query + credit history, A turns it into AIDP cluster shapes and cost. `contracts/warehouse_profile.schema.json`

**Sanctioned pairings — exactly two, both in the assessment sprint:**
- **W1 unload spike** (spans both halves; neither can retire it alone)
- **W3 assessment assembly** (D1 is one document with both names on it)

---

## 2. Write ownership — the hard anti-overlap rule

One owner per path. A PR touching a path you do not own is rejected on sight.

| Path | Owner |
|---|---|
| `contracts/` | **joint** — frozen after W0 |
| `aidp_migration_core/` · `eval/` · `engine/scripts/` (during extraction) | **A** |
| `ingestion/` (Fivetran, Kafka/OCI Streaming) · `serving/` (ADW, ExaCS, redaction views) · `sizing/` (warehouse→cluster, credits) | **A** |
| `commands/` · `.claude-plugin/` · `agents/migration-reviewer.md` | **A** |
| `snowflake_source/` · `references/` · `agents/snowflake-object-analyzer.md` | **B** |
| `assessment/` (the D1 report and its generators) | **joint** — one section per owner, never the same section |
| Skills `-migrator-overview`, `-bootstrap`, `-migrate-catalog`, `-migrate-data`, `-reconcile`, `-migration-status`, `-resume-migration`, `-acceptance-contract` | **A** |
| Skills `-assess-estate`, `-compat-report`, `-build-dag`, `-migrate-views`, `-assess-data` | **B** |
| Skills `-bucket-mapping`, `-fixup-cell`, `-migrate-job` | **A** — re-scope or delete, decided W4 |

---

## 3. Timetable

**⛔ Gate** = downstream work blocked until cleared. **📦** = shippable.

### Assessment sprint — W0 → W3, ending in D1

| Week | Dates | A — Target & Platform | B — Source & Translation | Gate / deliverable |
|---|---|---|---|---|
| **W0** | Sep 07–11 | Confirm PoC Execution is closed. Probe the target AIDP for the GA `20260430` surface. Stand up the 18 `aidp_migrator_codex/` tests as the regression net; pick the fork base per script (15 of 39 byte-identical, 24 differ). | **Get read-only Snowflake creds + the `ACCOUNT_USAGE` grant.** Ask the 6 open questions in `RAPPI-CONTEXT.md` §7 — especially **which region Rappi is actually in** (deck says Ashburn in the diagram, Oregon in a note). | **⛔ Gate 0:** three contracts merged and frozen · `ACCOUNT_USAGE` granted · region answered · PoC status confirmed. **Without the grant, D1 is not deliverable at 200k tables — escalate the same day.** |
| **W1** | Sep 14–18 | **Paired, OCI half:** land the spike's Parquet as managed Delta; exact-decimal `SUM()` compare. Then: OCI/AWS Interconnect feasibility for the *actual* region. | **Paired, Snowflake half:** one `NUMBER(38,2)` + one `NUMBER(38,9)` table → `COPY INTO @stage` Parquet → S3 same-region → transfer. Then: `extract/catalog.py` against 200k tables — incremental, resumable, `Retry-After` backoff. | **⛔ Gate 1:** retires **Q1** (does `NUMBER(p,s)` survive?) and validates the deck's transfer strategy end-to-end at one-table scale. One-page written finding. |
| **W2** | Sep 21–25 | `sizing/` — 82 warehouses → AIDP cluster shapes; 491-max-cluster concurrency ceiling; **credit → OCI cost model against the $5 MM baseline.** Consumes `WarehouseProfile`. Draft the **target-architecture recommendation** across the deck's 3 options. | `extract/usage.py` — `QUERY_HISTORY` + `WAREHOUSE_METERING_HISTORY` → `usage_profile.json`. **Rank all 200k tables by query frequency × credits.** `extract/lineage.py` — view→view, `TASK … AFTER`, `CALL`, dbt `ref()`. | Assessment has a spine: what matters, what it costs, what depends on what. |
| **W3** | Sep 28–Oct 02 | `ingestion/` — evaluate both Fivetran paths (Oracle destination Beta → ADW/ALH; Kafka → OCI Streaming, **verify SASL_SSL**) with a recommendation. Assessment sections: architecture, ingestion, sizing, cutover, operational readiness. | `dialect/identifiers.py` — case-folding policy + **collision detector that halts, never guesses** — run across all 200k. `report/unsupported.py` — Streams, Pipes, `CLONE`, Time Travel, masking/row-access, tags, Shares (both directions), `HASH()` keys, Snowpark, HEX, Hightouch. Assessment sections: inventory, waves, unsupported, risk. | **📦 D1 — Rappi Migration Assessment.** Parallel ingestion strategy · migration sequencing · operational readiness · cutover plan. **⛔ Gate 2:** customer picks a target architecture (option 1, 2 or 3). Everything past here is shaped by that answer. |

### Build phase — W4 → W12, ending in D2

| Week | Dates | A — Target & Platform | B — Source & Translation | Gate / deliverable |
|---|---|---|---|---|
| **W4** | Oct 05–09 | Begin `aidp_migration_core/` extraction: `executor`, `cluster_session`, `cluster_lifecycle`, `throttle/`. Run the 18 tests before *and* after each step. Decide the 3 questionable copied skills. | `dialect/types.py` — `NUMBER(p,s)`→`DECIMAL(p,s)` from `INFORMATION_SCHEMA`, **never inferred**; `VARIANT`/`OBJECT`/`ARRAY`; `TIMESTAMP_NTZ/_LTZ/_TZ`; `GEOGRAPHY` → no target. | Type map reviewed against the Gate-1 finding, not against docs. |
| **W5** | Oct 12–16 | Port MAXWELL `catalog.ts::publishTable` → `core/publish.py`: `assertWritableCatalog` → `inferFields` → register → poll async op → INSERT → `confirmPublish`. I/O injected so it unit-tests with no cluster. | `dialect/ddl_rules.py` on the rewriter's `RuleApplication`/`RewriteResult`/`UnsupportedDDL` audit architecture. Replace the property list (`CLUSTER BY`, `DATA_RETENTION_TIME_IN_DAYS`, `CHANGE_TRACKING`, tags, policies). | Durable publish lands a table visible **cross-session** — the defect the upstream sibling migrator has. |
| **W6** | Oct 19–23 | Extract the **write-redirect sandbox** from `job_migrate.py:1682-2500` into `core/sandbox/`. Extract `orchestrate/registry.py` (resume) + `report.py`. | `dialect/expressions.py` — `QUALIFY` → subquery + `WHERE`; `DATEADD`/`DATEDIFF` as a **signature table with unit normalisation**, not name substitution; `IFF`, `DECODE`, `TRY_CAST`, `LISTAGG`, `GENERATOR`, `SEQ4`, `::`, `LATERAL FLATTEN`, `MERGE` differences. | Sandbox provably rewrites only the **exec** copy. |
| **W7** | Oct 26–30 | `eval/` — port MAXWELL `agent-eval`: ground-truth differential diff, pure predicates, **fixture replay for $0 CI on every PR**, no silent passes. | **`snowflake-assess-estate` + `snowflake-compat-report` skills** — productise what W1–W3 did by hand, so the assessment is repeatable for the next customer. | The assessment becomes a reusable asset, not a one-off document. |
| **W8** | Nov 02–06 | `orchestrate/verify.py` — checks 1, 2, 3a and **3b (Spark driver-log scrape — do not drop this)**. Then wire check 4 to the parity diff: same query on Snowflake and AIDP, **per-column tolerance, exact-decimal for money**. | `translate/view.py` — **the hardest component, zero prior art.** Parse early, deploy last, topologically. Preserve original SQL alongside generated SQL; per-transformation conversion report. | Parity diff replaces the LLM judge. Migration has a ground truth by definition. |
| **W9** | Nov 09–13 | Batched DDL replay (one WebSocket, chunk 25 — AIDP discards per-statement DDL on session close) **plus per-statement existence probe**. `snowflake-migrate-catalog` rewrite. `serving/` — redaction views per the Porto pattern. | Finish `translate/view.py` + **`snowflake-migrate-views`** skill. Unsafe conversions become a manual task naming the exact unsupported syntax and its source location. | **⛔ Gate 3:** views deploy in topological order, or the view-heavy risk is escalated with a number attached. |
| **W10** | Nov 16–20 | Productionise the Gate-1 spike at 3 PB: staging, parallelism, cross-process token bucket, 429/circuit-breaker hardening, resume. **Detect append semantics and refuse to blind-replay.** | `snowflake-assess-data` — row counts, size, clustering, staging estimate, waves. **Label every value exact vs estimated.** Split historic vs current-1–2-weeks per the deck's strategy. | Data path runs unattended on a long-lived API-key profile (session tokens expire hourly). |
| **W11** | Nov 23–27 | `snowflake-migrate-data` behind a feature gate + approval workflow: dry-run for every operation, audit trail of planned *and* executed actions, secret redaction everywhere. Airflow/Astronomer DAG emission as the orchestration target. | `translate/snowpark.py` — `Session.table`→`spark.table`, `functions as F` alignment. The one mostly-mechanical rewriter. `references/` rewrite; delete the two upstream docs that are **factually wrong about their own code**. | End-to-end run on the CECL corpus (~40M rows): assess → plan → approve → land → reconcile. |
| **W12** | Nov 30–Dec 04 | Plugin polish: `-bootstrap`, `-overview` router, `-migration-status`, `-resume-migration`. CI green in fixture mode. | Semantic invariant checks per `validate.py` — row-count floors **plus business invariants**, PASS/FAIL, exit 0/1. Nightly live eval, budget-capped. | **📦 D2 — MVP-1** against §16. Phase-E scope written from D1 evidence. |

### Cadence

| Ritual | When | Purpose |
|---|---|---|
| Contract check | Mon, 15 min | Has anything forced a `contracts/` change? If so, joint PR before any other work |
| Gate review | at each ⛔ | One-page written finding, both sign. A verbal "it works" is not a retired gate |
| Ownership audit | Fri, 10 min | Scan the week's diffs for boundary crossings; revert and reassign the same day |
| Customer-question sweep | Fri | Any of `RAPPI-CONTEXT.md` §7 still open? Unanswered questions become assumptions, and assumptions get written into D1 |

---

## 4. Risk register — exactly one owner each

Marked **[NEW]** or **[CHANGED]** where the deck moved it from v1.

| # | Risk | If it fires | Owner | Retire by | Mitigation |
|---|---|---|---|---|---|
| R1 | **`NUMBER(p,s)` loses precision in transit.** Failed silently in an adjacent system: Arrow-IPC skipped, fallback through JSON, JSON has no Decimal, money became a string | Silently wrong money — not an error. Redesigns the unload phase | joint | ⛔ Gate 1, W1 | Parquet only, never JSON. p/s from `INFORMATION_SCHEMA`, never inferred. Exact-decimal reconciliation |
| R2 | **[CHANGED] Region contradiction.** Slide 10's diagram says AWS Ashburn → OCI Ashburn; a note on the same slide says *"They are in Oregon."* The whole "same-region unload, no egress cost" premise and the Ashburn interconnect depend on which is true | Egress cost and transfer wall-clock for **3 PB** both change materially. Cost model in D1 would be wrong | **A** | ⛔ Gate 0, W0 | Ask in W0. Do not model cost until answered |
| R3 | **[NEW] `ACCOUNT_USAGE` grant refused.** Was Medium in v1 | At **200,000 tables** the estate cannot be assessed at all. **D1 becomes undeliverable** | **B** | ⛔ Gate 0, W0 | Ask day 1. `INFORMATION_SCHEMA` retention is 7–14 days so there is no late substitute. Fallback is size + dependency depth only — say so explicitly in D1 |
| R4 | **[NEW] Fivetran cannot reach AIDP.** No AIDP destination, no generic protocol destination, S3 support is AWS-only | Bronze has no supply line. Everything downstream is theoretical | **A** | W3 | Evaluate both paths in W3. Oracle destination is **Beta** — confirm volume and object-type support at Rappi's ingest rate |
| R5 | **[NEW] Target architecture unchosen.** Three options, PB scale, hundreds of concurrent interactive users; internal review says a single ADW/ALH is wrong | The migrator's output shape is undefined. Build-phase work risks targeting the wrong tier | **A** | ⛔ Gate 2, W3 | Recommendation in D1. Request the ADW/ALH PM review the internal note calls for |
| R6 | **Views are the whole gold layer.** Migrator rejects 100% of views (`R15_VIEW_DEFERRED`); estates are view-heavy and medallion migration is **incomplete** | The hardest component with zero prior art becomes the critical path | **B** | W9 | W2–W3 measures the actual view share — 6 weeks before the build. Budget as a first-class component, not a DDL rule |
| R7 | **[CHANGED] 200k tables breaks per-object economics.** Was implicit | Any design that touches each object with an LLM cannot finish or cannot be paid for | **B** | W2 | Usage-ranked sampling as the primary entry. Deterministic rules first; the model sees only the residue |
| R8 | **[CHANGED] Warehouse sizing is the business case.** Was a Low W12 advisory | Without a credible credit→OCI cost model against the **$5 MM/yr** baseline, D1 has no commercial argument | **A** | W2 | 82 warehouses and the 491-cluster ceiling map to AIDP shapes in W2, not W12 |
| R9 | **[NEW] Orchestrator is Airflow (Astronomer), not AIDP workflows** | `clone_workflow.py` reuse is wrong; task/DAG translation targets the wrong system | **A** | W11 | Emit Airflow DAGs. Confirm the Astronomer deployment model early |
| R10 | **Identifier case collision.** Unquoted folds UPPER, quoted preserves and is case-sensitive; Spark folds lower. At 200k tables there will be real hits | Two source objects silently merge into one target. Data loss, no error | **B** | W3 | Capture `identifier_case_form` at extraction. **Detector halts rather than guesses** |
| R11 | **Silent DDL loss.** AIDP discards per-statement DDL on session close, and a batch reports success while individual statements failed | "Migrated" schemas that are not there | **A** | W9 | One WebSocket context (chunk 25) **and** per-statement existence probe |
| R12 | **`aidp_migration_core` extraction breaks working code.** The source-neutral and source-specific parts interleave inside a single large file | Regressions in the one part already debugged against a live cluster | **A** | W6 | Run the 18 fork-base tests before *and* after each step. That is what they are for |
| R13 | **No offline test story inherited** — this fork ships 0 test suites | Every change needs a live cluster; iteration collapses | **A** | W7 | Fixture replay in CI, same corpus and assertions as live, only the injected provider differs |
| R14 | **Unsupported features silently approximated.** In an adjacent system an unknown dialect string fell through to DuckDB *and reported DuckDB* | Wrong results presented as correct. Worst failure class here | **B** | continuous | Unmapped type/property/construct → **error**, never a default. `blocked` or `requires_manual_design`, resolution named |
| R15 | **GA `20260430` surface absent in the target deployment** | Durable publish unavailable; back to racing the metastore on implicit CTAS | **A** | W0 | Probe live. Fallback: explicit registration + `INSERT OVERWRITE` + in-session `count(*)` as the authoritative confirm |
| R16 | **[NEW] Outbound consumers break on cutover.** Hightouch reverse-ETL, Power BI, Snowflake Shares (Bronze **and** Gold), HEX notebooks all read Snowflake today | Migration succeeds and the business still stops | **B** inventories, **A** designs reconnection | W3 inventory, W9 design | Treat every outbound consumer as a first-class inventory object with its own cutover step |
| R17 | **[CHANGED] Masking has a pattern, not a product.** Porto uses a **redacted catalog of Spark views**; there is no AIDP masking REST API | Hand-built redaction across a 200k-table estate does not scale without generation | **A** | W9 | Generate redaction views from source policy metadata B extracts. Never assume a policy transferred |
| R18 | **Append-mode replay duplicates rows.** The resume registry does not idempotency-check appends | Silent duplication on resume after interruption | **A** | W10 | Detect append semantics and refuse to blind-replay |
| R19 | **[NEW] QA and DEV environments are in scope**, not just PROD | Scope is larger than the inventory implies; dev/prod parity is a deliverable, not an afterthought | **B** | W3 | Inventory all three environments separately in D1 |
| R20 | **Two-person bus factor.** Each half has one person who understands it | One absence stalls a half indefinitely | joint | continuous | Gate findings are written, not verbal. The Friday audit doubles as a 10-minute cross-brief |
| R21 | **Duplicate effort with the upstream sibling migrator.** The `aidp_migration_core` extraction benefits both | Fork drift; the same work done twice in two repos | **A** | W4 | Find the owner. If actively maintained, propose the extraction upstream instead of forking |

---

## 5. Gaps, mapped to owner and week

Per-file detail in `PORTING-STATUS.md`. This is the schedule view.

| Gap | Severity | Owner | Week |
|---|---|---|---|
| **[NEW] No Fivetran→AIDP path exists** | **Critical** | A | W3 |
| **[NEW] Target architecture unchosen (3 options)** | **Critical** | A | W3 |
| **Nothing in either codebase moves a single row** — at 3 PB | **Critical** | joint → A | W1 spike, W10 at scale |
| **[NEW] Region unconfirmed — Ashburn vs Oregon** | **Critical** | A | W0 |
| **[CHANGED] No usage/credit profiler** — the only viable entry at 200k tables | **Critical** | B | W2 |
| 100% of views rejected (`R15_VIEW_DEFERRED`) | **Critical** | B | W8–W9 |
| **[CHANGED] No warehouse→cluster sizing or credit model** vs a $5 MM baseline | **High** | A | W2 |
| No durable table registration — implicit CTAS races the metastore | High | A | W5 |
| 0 test suites ship in this fork | High | A | W7 |
| Verify check 4 is an LLM judge, not a parity diff | High | A | W8 |
| Only 11 of the documented 18 DDL rules exist | High | B | W5 |
| **[NEW] Outbound consumers uninventoried** (Hightouch, Power BI, Shares, HEX) | High | B / A | W3 / W9 |
| **[NEW] Orchestration targets AIDP workflows, not Airflow** | Medium | A | W11 |
| **[CHANGED] Masking → redaction-view generation** | Medium | A | W9 |
| No identifier case-folding policy | Medium (correctness) | B | W3 |
| 20 of 21 `aidp_compat/` modules are dead weight (their source-specific shims have no Snowflake analogue) | Medium | B | W6 |
| 4 spec'd skills missing: `assess-estate`, `compat-report`, `migrate-views`, `reconcile` | Medium | B (3) / A (1) | W7, W7, W9, W9 |
| Ingest phase never existed upstream — Phase 0 not implemented | Medium | B | W1 |
| **[NEW] QA/DEV environments unscoped** | Medium | B | W3 |
| Reference docs contradict their own code | Low | B | W11 |
| Stored procs, Tasks, Streams, Pipes, Dynamic Tables, grants | **Phase E — out of window** | B scopes | W12 scoping only |

---

## 6. Anti-duplication protocol

1. **One owner per path.** §2 is the contract. Cross-boundary PRs are rejected, not reviewed.
2. **Contracts before code.** `contracts/` freezes W0; after that, joint PR only, and neither person codes against an unmerged contract change.
3. **B never edits the fork tree.** A owns `engine/` through the extraction; B builds `snowflake_source/` greenfield against frozen interfaces.
4. **Two sanctioned pairings, both in the assessment sprint** (W1 spike, W3 assembly). A third means a contract is wrong — fix the contract instead.
5. **D1 has one section per owner.** Never the same section. The assembly pairing is for stitching, not co-writing.
6. **Gate findings are written.** One page, both sign.
7. **Friday ownership audit.** Revert and reassign the same day, while it is cheap.

## 7. Explicitly out of this window

Per design §17: production cutover · bidirectional sync · CDC / ongoing incremental
replication · Snowflake role and grant replication · automatic migration of stored
procs, UDFs, Tasks, Streams, Snowpipe, Dynamic Tables · automatic policy translation ·
full data migration without a user-approved wave.

**Honest scope note.** The upstream sibling migrator is ~32k lines of Python for a migration
that is *mostly metadata* between two Spark dialects. Rappi is 200k tables, 3 PB, 82
warehouses, an incomplete medallion refactor, and an ingestion tool that cannot reach
the destination. **D1 is achievable in 3 weeks; D2 is achievable by Dec 04. A migrated
Rappi is not, and nothing in this plan should be read as promising one.** Phase E
(stored procs, Snowpark at scale, task DAGs) is scoped from D1 evidence, not now.
