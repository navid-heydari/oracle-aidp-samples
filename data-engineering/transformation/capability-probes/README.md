# AIDP capability probe — native masking & zero-copy clone

A conservative, **runnable** check of what Oracle AI Data Platform's Spark 3.5.0 / Delta Lake
engine supports **natively** for two capabilities customers migrating from **Snowflake** or
**Databricks** routinely ask about:

1. **Tag/policy-based column masking** — declarative masking bound to a column tag or policy
   (Snowflake *masking policies* + tag-based masking; Databricks Unity Catalog *column masks*).
2. **Zero-copy clone** — Snowflake `CREATE TABLE ... CLONE`; the lakehouse analogue is
   Delta `SHALLOW CLONE` (zero-copy) / `DEEP CLONE` (full copy).

The notebook is written as a *probe*: each SQL statement runs through a safe `run()` wrapper, so an
**unsupported** statement is recorded as `UNSUPPORTED/ERROR` rather than aborting the run. Read the
final summary cell for the verdict.

## Files

| File | What it is |
|---|---|
| [`native_masking_and_zero_copy_clone.ipynb`](native_masking_and_zero_copy_clone.ipynb) | The runnable probe (Part A control-plane note + Part B SQL tests T1–T5). |

## What each test asks

| Test | Capability | Snowflake / Databricks analogue | Expectation |
|---|---|---|---|
| **T1** | `ALTER COLUMN ... SET MASK` | Databricks UC column mask | Unsupported (managed feature) |
| **T2** | `CREATE MASKING POLICY`, column `SET TAGS`, `SET ROW FILTER` | Snowflake tag-based masking / row-access policy | Unsupported |
| **T3** | Restricted / redacting **VIEW** | Snowflake secure view | **Supported** — the recommended workaround |
| **T4** | `SHALLOW CLONE` (+ copy-on-write divergence) / `DEEP CLONE` | Snowflake zero-copy clone | Delta-format dependent — the probe tells you |
| **T5** | `CREATE TABLE ... LIKE`, empty CTAS | Snowflake `CREATE TABLE ... LIKE` / empty clone | Structure-only clone |
| **T6** | `SET TBLPROPERTIES` (table-level tags) | Snowflake object tag | Supported — descriptive, table-scoped |
| **T7** | column `COMMENT` as a tag | Snowflake column tag | Supported but limited (free-text) |
| **T8** | tag **registry table** (`INSERT` + `SELECT`) | Snowflake `TAG_REFERENCES` | Supported — the pattern that scales |
| **T9** | **bridge**: registry → generated masking view | Snowflake tag-based masking policy | The alternative solution (assembled, not declared) |

## Part C — tagging in SQL: attach vs. enforce

"Have a tag in SQL" is two separate things on AIDP, and only the first is free:

| | Meaning | AIDP in SQL |
|---|---|---|
| **Attach** a tag | store a key/value on a table/column | ✅ `TBLPROPERTIES` (table) / column comment / registry table (T6–T8) |
| **Enforce** a tag | tag auto-masks or restricts at query time | ❌ no policy engine — **you build the bridge (T9)** |

Snowflake couples the two (tag → masking policy → automatic redaction). AIDP decouples them: attach in SQL
today, but assemble enforcement yourself. **T9 is the bridge** — it reads the tag registry and *generates* the
redacting view, so tagging a column `pii` and regenerating makes the mask follow. Grant on the generated view,
not the base table. This reproduces tag-based-masking behaviour on AIDP without a policy engine.

> AIDP-native alternative: the **Ontologies** feature tags terms (`av:isSensitive` / `av:requiresRole`) but is
> UI-driven with no SQL/REST surface, and classifies without enforcing. For anything scriptable, use the
> registry-table bridge.

## How to run

1. Open the notebook in an AIDP workspace and attach it to a running cluster.
2. Adjust `CATALOG` / `DB` in the first code cell if your catalog isn't `default`.
   The notebook creates a scratch schema `mask_clone_probe` and **drops it** at the end.
3. Run all cells; read the **CAPABILITY SUMMARY** cell.

For the **control-plane** portion (Part A), run the `oci raw-request` loop from a workstation with a
valid api-key profile — it needs no cluster.

## Findings (recorded)

- **Tag/policy-based masking has no control-plane REST API** on the tested tenancy
  (`navid-dev-sandbox`, us-ashburn-1): `maskingPolicies`, `columnMaskingPolicies`,
  `dataClassifications`, `tags`, `classifications`, `maskingRules`, `policies`, `dataProtection`,
  `sensitiveDataTypes` all → **404**, while `/roles` and `/catalogs` → **200** (feature-absence, not auth).
- **Column-level masking today** = restricted/redacting **views** + granting on the view (T3).
- **Sensitivity tagging** exists as *governance metadata* via **Ontologies** (`av:isSensitive` /
  `av:requiresRole`, UI-driven) — it classifies, it does **not** enforce masking at query time.

> **Zero-copy clone — operational gotcha vs Snowflake.** A Delta `SHALLOW CLONE` references the
> *source table's* data files. Running `VACUUM` on the source (or rewriting/optimizing it) can delete
> those files and break the clone. Snowflake manages this storage lifecycle for you; Delta does not —
> enforce a retention contract, or use `DEEP CLONE` (full copy) where isolation matters.

> Run dates and exact statuses should be re-verified per tenancy — capability probes are point-in-time.
