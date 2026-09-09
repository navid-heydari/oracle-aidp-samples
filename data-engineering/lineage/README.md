# Data Lineage in AIDP — API conformance tests + an independent oracle

Two complementary artifacts:

| File | What it does |
|---|---|
| [`test_aidp_lineage_api.py`](./test_aidp_lineage_api.py) | Proves **AIDP's own lineage API is released and live**, from real responses. 13 passed / 4 xfailed. |
| [`Verify_Data_Lineage.ipynb`](./Verify_Data_Lineage.ipynb) | Derives lineage independently from Spark's analyzed plan and verifies it against a known DAG. 19/19 checks. |
| [`TAKEAWAYS.md`](./TAKEAWAYS.md) | **Start here.** Findings, the scoreboard of proven vs unproven, and the methodology lesson on verifying an existence claim. |

## Status: the lineage API is released

AIDP ships lineage under the **`SemanticCatalog`** service:

| Operation | Call |
|---|---|
| Fetch entity lineage | `POST /20260430/aiDataPlatforms/{aiDataPlatformId}/actions/fetchLineage` |
| Export lineage (CSV) | `POST /20260430/aiDataPlatforms/{aiDataPlatformId}/actions/exportLineage` |

- **Host:** `https://datalake.{region}.oci.oraclecloud.com` (service endpoint prefix `datahub-dp`)
- **API version:** `20260430`
- **Request:** `anchorNode` (required), `direction` (`UPSTREAM|DOWNSTREAM|BOTH`), `level`
  (`ENTITY|COLUMN`), `maxDepth`, `nodeFilters`, `pathFilters`, `shouldIncludeEdges`
- **Response:** `EntityLineage { nodes[], links[] }`, where nodes carry
  `id / qualifiedName / displayName / parentId / type / depth / properties` and links carry
  `fromNodeId / toNodeId / type / providerType / properties`
- **Maturity:** both operations are marked **(Preview)** in the official CLI reference
- **Backing store:** Oracle Data Catalog — table properties carry `com.oracle.dcat.*` keys and
  `request-mode: DATAHUB_EXECUTE`

Column-level lineage (`level: COLUMN`) is part of the released contract, not just table-level.

### If you probe and see 404, you are on the wrong generation

The lineage API is **not** on the older surface. `aidp.{region}.oci.oraclecloud.com/20240831/dataLakes/{ocid}`
serves `/catalogs` and `/schemas` fine but has no lineage route — the whole SDK moved to
`datalake.{region}` + `/20260430`. Probing the old host yields `404 NotAuthorizedOrNotFound`, which is
easy to misread as "AIDP has no lineage API".

Worse, that 404 body is **byte-identical** (111 bytes, same `code`/`message`) to the one returned for a
route that never existed — so on that surface a 404 cannot distinguish *absent route* from *denied
permission*. `test_A2` and `test_A7` encode both halves of this trap so nobody re-derives the wrong
conclusion.

### Known gap: the graph is not populated here

`fetchLineage` reaches its own parameter validation and rejects every `anchorNode` we can construct:

```
default.lin_demo.mart_customer_revenue            -> 400 Invalid anchorNode
mart_customer_revenue                             -> 400 Invalid anchorNode
hive.lin_demo.mart_customer_revenue               -> 400 Invalid anchorNode
default.cat/lin_demo.db/mart_customer_revenue     -> 400 Invalid anchorNode
TABLE:default.lin_demo.mart_customer_revenue      -> 400 Invalid anchorNode
<the DataLake OCID>                               -> 400 Invalid anchorNode
```

Omitting the field instead returns `anchorNode must not be null`, so the server distinguishes *missing*
from *unresolvable* — it is doing a real lookup and finding nothing. `POST .../tables/{key}/actions/refresh`
returns `202` but does not change the outcome.

Two candidate explanations, not yet separated:

1. the lineage graph is not populated for this DataLake (no harvest configured, and interactive
   notebook writes may not register a process node — lineage may require Job/Workflow execution); or
2. `anchorNode` expects an internal Data Catalog node id whose format is undocumented — the CLI
   reference lists the field with an **empty description**.

`test_B0` prints the full candidate matrix on every run, so the moment any form resolves, you see it.

## Running the tests

```bash
pip install oci requests pytest
pytest test_aidp_lineage_api.py -v                 # everything
pytest test_aidp_lineage_api.py -v -m existence    # just the proof-of-release
pytest test_aidp_lineage_api.py -v -rX             # show why Part B is blocked
```

Override the target via env: `AIDP_PROFILE`, `AIDP_REGION`, `AIDP_DATALAKE`, `AIDP_SCHEMA`,
`AIDP_ANCHOR_TABLE`.

### Part A — API existence (green; this is the proof)

| Test | Proves |
|---|---|
| `A0_auth_works_on_dataplane_host` | authenticated on the new host, so later 4xx are unambiguous |
| `A1_fetchLineage_route_exists` | route deployed: `400 InvalidParameter`, not `404` |
| `A2_control_bogus_action_is_404` | **control** — a fake sibling action *does* 404, giving A1 meaning |
| `A3_exportLineage_route_exists` | the CSV export operation is deployed too |
| `A4_request_contract_is_enforced_server_side` | missing vs invalid `anchorNode` produce different errors |
| `A5_documented_enums_are_accepted` | `level=ENTITY/COLUMN`, `direction=UPSTREAM/DOWNSTREAM/BOTH` all parse |
| `A6_invalid_enum_is_rejected` | **control** — `direction=SIDEWAYS` → `Invalid LineageDirection: SIDEWAYS` |
| `A7_lineage_absent_from_legacy_api_generation` | documents the wrong-generation trap |

A5+A6 together are the strongest evidence: a stub that ignored the body and always complained about
`anchorNode` would pass A5 but **fail A6**. The server really parses `LineageDirection`, so a genuine
implementation is behind the route.

### Part B — graph population (xfail; the open gap)

`B1` entity graph · `B2` upstream contains `stg_orders` + `raw_customers` · `B3` column-level edges ·
`B4` CSV export. Marked `xfail(strict=False)` rather than skipped or deleted, so if lineage becomes
populated they flip to **XPASS** and the suite reports that the gap closed. `B2` asserts the same DAG
the notebook derives from Spark — so when it goes green, the platform's graph is confirmed against
independently derived ground truth.

## The notebook: independent ground truth

The API is the platform's *claim* about lineage. The notebook is the oracle you check it against,
deriving lineage from the Catalyst analyzed plan — the one signal that cannot disagree with what
actually ran.

```
raw_orders ──filter status='PAID'──> stg_orders ──┐
                                                 ├─join + GROUP BY──> mart_customer_revenue
raw_customers ───────────────────────────────────┘

unrelated_table                     (decoy — must produce no edges)
```

`mart_customer_revenue` reads **`stg_orders`**, not `raw_orders`; `raw_orders` is only *transitively*
upstream, and the notebook asserts the direct edge only.

Three layers must agree:

| Layer | Source of truth | Proves |
|---|---|---|
| 1. Plan-derived graph | Catalyst analyzed plan | which tables/columns fed each write |
| 2. Delta history | `_delta_log` commit log | the write happened, with row counts |
| 3. File provenance | `inputFiles()` + `DESCRIBE DETAIL` | bytes read live under the claimed table |

Plus **negative controls**, which are what make the result meaningful — an extractor that reported
*every* table would satisfy "no missing edges" while being useless: the decoy appears in no edge; no
direct `mart → raw_orders` edge; no unresolved leaves. Clean run: **19/19**.

`DESCRIBE HISTORY` deserves a specific warning: it is commonly mistaken for lineage, but a CTAS commit's
`operationParameters` holds only `partitionBy` / `properties` / `isManaged` — **no source tables**. It
gives temporal provenance, not a graph.

### How the extraction works

- **Table level** — `df._jdf.queryExecution().analyzed().collectLeaves()`, then
  `leaf.catalogTable().get().qualifiedName()` for a clean `catalog.schema.table`.
- **Column level** — the top plan node's `projectList()` (Project) or `aggregateExpressions()`
  (Aggregate); each output expression's `references()` are `AttributeReference`s carrying an `exprId`,
  mapped back to the leaf relations' output attributes.

  Resolution *must* go through `references()`. Aliases and aggregates mint **fresh** `exprId`s —
  `SUM(o.amount) AS revenue` is a new attribute — so output `exprId`s never equal leaf `exprId`s, and
  comparing them directly silently yields no column lineage.

### Scope

Capture covers writes routed through the notebook's `write_tracked()` helper, and the graph lives in
kernel memory for the session. Columns used only in `WHERE` / `JOIN ON` (e.g. `raw_orders.status`) are
real dependencies but not top-level outputs, so they do not appear in the column map.

`SemanticCatalog` exposes **no lineage write/ingest operation** — only `fetchLineage` and
`exportLineage` — so a client cannot push this graph into AIDP. Population is the platform's job. For
durable local capture, register a JVM `QueryExecutionListener` on `spark.listenerManager`, or install the
OpenLineage Spark listener
(`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`) and point
`spark.openlineage.transport.*` at a collector.

## Environment as tested

Spark 3.5.0 · Delta 3.2.0-oci-1.0.0 · `spark.sql.sources.default=delta` · catalog impl `hive` ·
region `us-ashburn-1` · aidp SDK `oracle-samples/aidataplatform-sdk` (all 18 service clients on
`/20260430`).
