# Takeaways — AIDP data lineage

Point-in-time findings from a live probe of `navid-dev-sandbox` (us-ashburn-1), **2026-08-16**.
Re-verify per tenancy. Companion artifacts: [`README.md`](README.md),
[`test_aidp_lineage_api.py`](test_aidp_lineage_api.py), [`Verify_Data_Lineage.ipynb`](Verify_Data_Lineage.ipynb).

---

## 1. Lineage IS released — on a surface you won't find by guessing

Service **`SemanticCatalog`**, two operations, both **(Preview)** in the official CLI reference:

```
POST /20260430/aiDataPlatforms/{dataLakeOcid}/actions/fetchLineage   → EntityLineage{nodes[],links[]}
POST /20260430/aiDataPlatforms/{dataLakeOcid}/actions/exportLineage  → text/csv
```

- **Host:** `https://datalake.{region}.oci.oraclecloud.com` (endpoint prefix `datahub-dp`)
- **Body:** `anchorNode` (required), `direction` `UPSTREAM|DOWNSTREAM|BOTH`, `level` **`ENTITY|COLUMN`**,
  `maxDepth`, `nodeFilters`, `pathFilters`, `shouldIncludeEdges`
- **Nodes:** `id / qualifiedName / displayName / parentId / type / depth / properties`
- **Links:** `fromNodeId / toNodeId / type / providerType / properties`

Two things to carry into a customer conversation: **column-level lineage is in the released contract**,
and the API is **read-only** — there is no write/ingest operation, so a client cannot push its own
lineage graph into AIDP. Population is the platform's job.

## 2. The whole SDK changed generation

| | Old | Current |
|---|---|---|
| Host | `aidp.{region}` | **`datalake.{region}`** |
| Version | `/20240831` | **`/20260430`** |
| Resource | `dataLakes` | **`aiDataPlatforms`** |

This is **not** lineage-specific: all **18 service clients** in `oracle-samples/aidataplatform-sdk`
(Agent, Audit, Bundle, Catalog, Cluster, Credentials, DeltaShare, Git, MLOps, Notebook, Role, Schema,
SemanticCatalog, UserSetting, Volume, Workflow, Workspace, WorkspaceObject) are on `/20260430` at that
host. Any reference map, doc, or tool pinned to `/20240831` is a full generation stale for **everything**.

## 3. ⚠ Methodology — a 404 here can carry zero information

On the AIDP surface, a **nonexistent route** and a **real-but-unpermitted route** return byte-identical
replies: 111 bytes, `{"code":"NotAuthorizedOrNotFound","message":"Authorization failed or requested resource not found."}`.

The tempting-but-invalid inference — and one this repo has made before:

> "`/catalogs` returns 200, so auth works; therefore this 404 means the route doesn't exist."

That is wrong. Auth working on one route says nothing about **per-resource authorization** on another.
Applied to lineage, it produced a confidently-stated and false conclusion ("AIDP has no lineage API").

**Rules that follow:**

1. Before concluding a feature is absent, probe a **known-garbage control path**. If your "missing"
   endpoint is indistinguishable from `/zzzz_not_a_route`, you have learned nothing.
2. Confirm the **host and API version** are current before believing any absence claim.
3. **Prefer the SDK/CLI as the oracle.** The generated client is the authoritative declaration of what
   ships — grep it for the capability instead of guessing URLs. That is how lineage was actually found,
   after HTTP probing had produced the wrong answer.

> **Cross-reference — affects a sibling probe.**
> [`../transformation/capability-probes/README.md`](../transformation/capability-probes/README.md)
> concludes masking has no REST API because `maskingPolicies`, `dataClassifications`, `tags`, … all
> returned 404 "while `/roles` and `/catalogs` → 200 (feature-absence, not auth)" — the exact reasoning
> above. Re-checked 2026-08-16: on the new generation those paths still 404, **but so does a bogus
> control path**, so the 404s prove nothing either way. The conclusion nonetheless **holds** on better
> evidence: there are **no masking/classification models anywhere in the current SDK**. Worth updating
> that README to cite SDK absence rather than HTTP 404s.

## 4. Backed by Oracle Data Catalog, not a bespoke AIDP catalog

The seam is visible in table properties: `com.oracle.dcat.tracing.req.id`, `oci_dcat_metastore_*`,
`request-mode: DATAHUB_EXECUTE`. Together with the `datahub-dp` endpoint prefix and the fact that OCI
Data Catalog has its own identically-shaped `FetchEntityLineage`, AIDP's semantic catalog is a
**DCAT-backed surface**.

Architecturally sound (reuse the governance substrate) and it explains the `anchorNode` friction in §5:
node identity belongs to DCAT's graph, not to the `catalog.schema.table` namespace used for Spark.

**Positioning vs Databricks:** Unity Catalog offers table- *and* column-level lineage, **GA**,
auto-populated from job execution with a UI graph. AIDP's equivalent is **Preview** with population
unconfirmed. Know that gap before a customer asks; don't oversell it.

## 5. Open gap — the API answers, the graph does not

`fetchLineage` reaches its own parameter validation and rejects **every** `anchorNode` form tried:

| `anchorNode` | Result |
|---|---|
| `default.lin_demo.mart_customer_revenue` (table key, as `ListTables` returns) | `400 Invalid anchorNode` |
| `mart_customer_revenue` (bare name) | `400 Invalid anchorNode` |
| `hive.lin_demo.mart_customer_revenue` (catalogGuid-qualified — the `default` catalog's `catalogGuid` is literally `hive`) | `400 Invalid anchorNode` |
| `default.cat/lin_demo.db/mart_customer_revenue` (storage path) | `400 Invalid anchorNode` |
| `TABLE:default.lin_demo.mart_customer_revenue` (type-prefixed) | `400 Invalid anchorNode` |
| the DataLake OCID | `400 Invalid anchorNode` |
| *omitted* | `400 anchorNode must not be null` ← **different error** |

That last row matters: the server distinguishes *missing* from *unresolvable*, so it is performing a
real lookup and finding nothing. `POST .../tables/{key}/actions/refresh` returns `202` and changes
nothing.

Two explanations, not yet separated:

1. **the graph isn't populated** for this DataLake — plausibly lineage registers a process node only for
   **Job/Workflow** executions, not interactive notebook cells; or
2. **`anchorNode` expects an internal DCAT node id** whose format is undocumented — the CLI reference
   lists the field with an **empty description**.

→ **The one question for Oracle.** `test_B0` prints the full candidate matrix on every run, so the
evidence is ready to hand over and any change is immediately visible.

## 6. Verify the platform's claim against an independent oracle

The API is the platform's *claim*. Trustworthy lineage means two independent derivations agreeing. The
notebook builds the second from Spark's **Catalyst analyzed plan** — the one signal that cannot disagree
with what actually ran:

- **Table level:** `df._jdf.queryExecution().analyzed().collectLeaves()` →
  `leaf.catalogTable().get().qualifiedName()`
- **Column level:** top node's `projectList()` (Project) / `aggregateExpressions()` (Aggregate), then
  each expression's `references()` → `exprId` mapped back to leaf `output()`

**Gotcha that costs an hour:** resolution *must* go through `references()`. Aliases and aggregates mint
**fresh** `exprId`s — `SUM(o.amount) AS revenue` is a new attribute — so output ids never equal leaf ids.
Comparing them directly returns *no* column lineage **silently**, with no error.

**`DESCRIBE HISTORY` is not lineage.** The most common substitute, and it doesn't work: a CTAS commit's
`operationParameters` holds only `partitionBy` / `properties` / `isManaged` — never its source tables.
Temporal provenance, not a graph.

## 7. Test design — the controls are the evidence

A suite where everything passes proves less than it appears. Two of the eight existence tests exist
purely to give the others meaning:

- **`A2`** — a *fake* sibling action really does 404, so `A1`'s 400 is genuine discrimination and not
  just how this service talks.
- **`A6`** — `direction=SIDEWAYS` returns `Invalid LineageDirection: SIDEWAYS`. A stub that ignored the
  body and always complained about `anchorNode` would pass every other test and **fail this one**. This
  is what proves a real implementation sits behind the route.

For known gaps, `xfail(strict=False)` beats deleting or skipping: blocked tests stay executable and flip
to **XPASS** the moment lineage is populated. The suite becomes a **tripwire that reports when the gap
closes**, rather than a comment nobody re-reads.

---

## Scoreboard

| Claim | Status |
|---|---|
| Lineage API exists and is deployed | ✅ **proven** — 13 passing tests, incl. 2 controls |
| Column-level lineage in the contract | ✅ **proven** — `level=COLUMN` accepted |
| Lineage graph populated for our tables | ❌ **not proven** — no `anchorNode` resolves (4 xfail) |
| Plan-derived lineage is accurate | ✅ **proven** — 19/19, with negative controls |
| Masking has no REST API | ✅ holds, but **re-argue from SDK absence**, not 404s |

**Environment as tested:** Spark 3.5.0 · Delta 3.2.0-oci-1.0.0 · `spark.sql.sources.default=delta` ·
catalog impl `hive` · us-ashburn-1 · aidp SDK `oracle-samples/aidataplatform-sdk`.
