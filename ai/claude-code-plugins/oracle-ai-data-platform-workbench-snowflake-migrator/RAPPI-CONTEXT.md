# Customer context — Rappi (Oracle LAD, Data & AI)

Source: `~/Workspace/oracle/snowflake_migrator/Rappi_Oracle_AIDP_Engagement_Enriched_v8.pptx`
(17 slides, 8 with speaker notes). Read 2026-09-08.

**Why this file exists.** The design doc (`snowflake_migrator/README.md`) was written
as a *generic* Snowflake→AIDP migrator spec. The deck reveals it is a **named
engagement, already past executive demo, at a scale that reorders the entire plan.**
Several design-doc assumptions are confirmed, several are contradicted, and one of the
four "blocking unknowns" is largely answered. Read this before the design doc's §13.

---

## 1. Estate scale — the numbers that reset priorities

| Metric | Value | Consequence |
|---|---|---|
| Snowflake spend | **~$5 MM/year** | The business case. Warehouse→compute cost modelling is not a footnote, it is the pitch |
| Tables | **~200,000** | Per-object LLM assessment is economically impossible. Usage-ranked sampling is **mandatory**, not a differentiator |
| Data volume | **~3 PB** total | Transfer wall-clock is the schedule driver, not code |
| Warehouses | **82** (XS 54, S 12, M 6, L 2, XL 4, 2XL 2, 3XL 1, 4XL 1) — 63 active | A concrete sizing deliverable, not an advisory surface |
| Max clusters (worst case horizontal scaling) | **491** | Concurrency ceiling to reproduce on AIDP |
| Data lake users | **200–1,000** | Internal review says: do **not** put this on a single ADW/ALH |
| Data sources | 7 | See §4 AS-IS |
| Architecture | Medallion — **migration to it not yet complete** | Dependency graph will be messy; Gold not cleanly separable |
| Use cases | ML, complex analytics, batch | |

## 2. Engagement clock — the plan must anchor to this

| Phase | Deck status |
|---|---|
| Preparation (May/Jun 2026) — Snowflake trial env, Orders dataset, AIDP+ADW pipeline, Deep Insights Agent demo | **[DONE]** |
| Executive Demo (3rd week Jun 2026) — architecture vision, ingestion pipeline, multi-source agents, notebook assistants | **[DONE]** |
| **PoC Execution (3–4 weeks)** — Sales Orders PoC, **no disruptive migration**, validate **interoperability**, benchmark UX | not marked done |
| **Migration Assessment (3 weeks)** — parallel ingestion strategy, migration sequencing, operational readiness, cutover planning | not marked done |

**Two consequences.**

1. The near-term customer deliverable is a **3-week Migration Assessment**, not a
   13-week plugin. The plugin is the means; the assessment is the product.
2. The PoC is explicitly **interoperability / federation, not migration**. That
   validates the design doc's own alternative reading (§13 assumption 2: *"if the answer
   is 'just federate and don't migrate', most of this is moot"*). Near term, the read /
   federation path matters more than the write path.

> ⚠️ **Confirm with the account team whether PoC Execution is closed.** Only the first
> two phases carry `[DONE]`. The whole schedule in `PLAN-2PERSON-TIMETABLE.md` assumes
> the assessment is the current phase.

## 3. Design-doc assumptions this deck changes

| Design doc said | Deck says | Adjustment |
|---|---|---|
| **Q2 — the Snowflake→OCI transfer path is the biggest unknown**, possibly needing an S3→OCI hop that "will not scale" | Slide 10 gives the strategy: **unload to S3 in the same region** (no Snowflake egress cost) → **OCI/AWS Interconnect** → OCI. Split **historic** data (object storage) from **current** 1–2 weeks (direct from Snowflake) | Downgrade from unknown to **known approach with a live region contradiction** — see §5 R2′ |
| Serving into **ADB/ADW is out of scope** for MVP-1, "a separate product surface" | **All three** reference architectures put ADW/ALH — or a dedicated ExaCS — as the serving tier | The §3.1 *technical* constraint still holds (AIDP cannot create a managed table in an EXTERNAL catalog). But the **serving design is in the deliverable.** Only Bronze/Silver/Gold *land* in AIDP internal catalogs; ADW reads them back as external catalogs / Delta Share / Cloud Link / Delta Uniform |
| Orchestration target is AIDP jobs/workflows; reuse `clone_workflow.py` | **Airflow (Astronomer)** is the orchestrator in every reference architecture | Task/DAG translation targets **Airflow DAGs**. `clone_workflow.py` reuse is wrong for Rappi |
| Masking / row-access policies have **no target** | Porto reference deployment: *"users will access data on a **redacted catalog** which will use **Spark views** to obfuscate sensitive data"* | A known pattern exists. Automation work, not a dead end |
| Usage-driven prioritisation is a **differentiator / nice-to-have** (Medium) | 200,000 tables | **Critical.** Without `ACCOUNT_USAGE` the estate cannot be assessed at all |
| Warehouse→cluster mapping is a **Low** priority advisory surface | 82 warehouses, 491 max clusters, $5 MM/yr | **High**, and early. It is the business case |
| Snowflake Data Sharing → "partially covered by another plugin" | **Shares appear in Bronze *and* Gold** — inbound and outbound, in production | In scope, both directions |
| Snowpark rewriting is *"plausible"* | Snowpark is in the AS-IS stream-processing layer | Confirmed in scope |
| Nothing about **Fivetran** | *"Heavy usage — hard to displace; better to integrate than compete."* **AIDP is not a supported Fivetran destination**, there are no generic protocol destinations, and its S3 support is AWS-only | **An entire missing workstream, upstream of Bronze** — see §6 |

## 4. AS-IS architecture (slide 3 diagram)

```
Source Layer          Rappi: Doc db · Sheets · Amazon RDS · MySQL
                      Consumer external: Amazon S3 · Shares · FTPS
Extract Tools         Fivetran + ~5 others (Airflow, custom, …)
Landing Layer         Amazon S3 · FTPS · Shares · db/schema/tables
Snowflake (HG51401)   Medallion: Bronze → Silver → Gold  (Shares in Bronze AND Gold)
                      Processing Capacity Layer (Warehouses)
                      Data Strategy QA & DEV            ← non-prod envs in scope
                      Data Security (Domain, Roles & Permissions, Data masking)
Consumer/Stream       Snowpark · Kafka · Airflow · HEX    ("Streaming and AI/ML Strategy")
Data Catalog          Confluence + other tools
Delivery Layer        Power BI · BI tooling · Hightouch (reverse ETL)
Cross-cutting         Data Governance · Data Observability
```

**Newly confirmed in scope, absent from the design doc:** Snowpark · **HEX** notebooks ·
**Hightouch** reverse-ETL (outbound dependency — breaks if Gold moves) · Power BI
reconnection · Snowflake Shares both directions · **QA and DEV environments**, not just
PROD · Confluence as the catalog of record · the domain/role/permission model.

## 5. Target architecture — three options, customer has not chosen

Internal review (slide 13) with Lucas (FDE), Johannes and Caio (LAD SEs), drawing on
**Pagbank, Porto and Entel**:

> Rappi is PB-scale with potentially hundreds of concurrent interactive users. **Do not
> use a single ADW/ALH instance for the complete use case.** Break the data into
> multiple ADBs, or add a dedicated ExaCS. Each team gets its own "sandbox ADB" and a
> corporate process copies the Gold/Silver reports they need. **Bronze must not be
> allowed** into sandboxes. Recommend an ADW/ALH PM review once an option is agreed.

| Option | Shape | Key trade-off |
|---|---|---|
| **1 — Multiple ADW/ALH** | AIDP Bronze (standard catalog) + Silver/Gold as external catalogs across per-LoB ADWs; Delta Share + Cloud Link | No data duplication, but governance needs automation, lineage gets complex, BI users may span connections, **no masking for dev consumers today** |
| **2 — Multiple ADW/ALH + copy data** (the **Porto** deployment, slide 15) | Per-layer AIDP catalogs with PRD/DEV/REP + redaction, an **elastic pool** of external Autonomous catalogs, ORDS for Data-as-a-Service, Entra ID access control | Best LoB performance and simple lineage, but duplicates data and needs above-average CI/CD to promote LoB work back into AIDP |
| **3 — Single dedicated ExaCS + Bronze in AIDP** | ExaCS holds Silver+Gold with stored procs and country-based views; AIDP becomes largely a landing zone | Easiest to communicate, best performance, per-LoB VM clusters — but **costs start higher**, and many ADW UI features are unavailable. Choose only if bronze use is limited to bronze→silver jobs |

**This choice determines the migrator's output shape**, so it gates design of anything
past extraction.

## 6. Fivetran — the missing workstream

Fivetran feeds Bronze and is *"hard to displace."* It has **no AIDP destination**, no
generic protocol destination, and its S3 destination is AWS-only. Candidate paths:

| Path | Mechanism | Risk |
|---|---|---|
| **Oracle destination (Beta)** → ADW/ALH, then unload ADW→AIDP to cut cost (ADB as a Fivetran landing zone only) | Client-wallet TLS. Most direct — swap the Snowflake destination for the Oracle one | **Beta.** Confirm supported volume and object types at Rappi's ingest rate |
| **Kafka → OCI Streaming → AIDP Spark consumer** | Fivetran Kafka destination → OCI Streaming → Spark | More moving parts; **we own the sink**. SASL_SSL compatibility unverified |

## 7. Open questions to the customer — these gate sizing

From the deck's own notes, unresolved:

1. What is **peak concurrent usage**?
2. Are different domains sharing **tables**, or only **schemas**? Who owns the sharing?
3. Does each domain/business area have **its own schemas and ETL jobs**?
4. Does the **same ETL run across different domains**, or always within one?
5. **Which cloud region is Rappi actually in?** The slide-10 diagram says AWS
   **Ashburn (us-east-1)** → OCI Ashburn; a note on the same slide says
   *"They are in **Oregon**."* These are incompatible.
6. Is there **only one Bronze-layer database** in Snowflake? (deck TO-DO)

Domain policy already known: one domain accesses one virtual warehouse; domains belong
to a single business vertical; warehouses are generally not shared across verticals.
Finance is the exception today — one warehouse shared by four Finance domains.
