---
name: snowflake-migration-plan
description: Build a high-level Snowflake to AIDP migration plan and present it for approval. States which objects can migrate and which cannot with a brief reason for each, applies user-supplied restrictions such as excluded databases or size caps, derives dependency order so views follow their base tables, and lists the Silver and Gold job stubs. Use after an estate assessment, or when the user asks what can be migrated, in what order, why something is excluded, or wants to see the migration plan.
---

# Stage 2 — dependencies and plan

Two commands. The first needs Snowflake; the second is offline.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deps \
  --account <...> --user <...> --auth <...> [--key-path ...] --out-dir ./snowmig_out

python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py plan --out-dir ./snowmig_out \
  [--restrictions restrictions.json] [--bronze-catalog-prefix bronze]
```

`PLANNED_OBJECTS.md` is the report to walk the user through. It is the answer to
"what are the objects planned to move".

## The bronze mapping is structural, not a choice

```
Snowflake database  ->  AIDP Standard Catalog
Snowflake schema    ->  AIDP schema
Snowflake table     ->  AIDP table
Snowflake view      ->  AIDP view
```

Bronze mirrors the source 1:1, so target names equal source names and nothing is
flattened. `--bronze-catalog-prefix` is the only variation: it puts everything in
one catalog and folds the database into the schema name, for deployments that
want a single bronze catalog.

## Can and cannot, with reasons

Every inventoried object lands in exactly one of `can_migrate` or
`cannot_migrate`. Read the reasons out — they are the point of the report.
Categories:

| Category | Means |
|---|---|
| `restriction` | The user's own restriction excluded it. Name which one |
| `unmapped_type` | A column type has no Delta equivalent, e.g. `VARIANT`, `GEOGRAPHY` |
| `snowflake_only_sql` | A view uses `QUALIFY`, `LATERAL FLATTEN`, `IFF`, `::`, … |
| `unsupported_object` | Secure view, materialized view |
| `no_definition` / `unparseable_sql` | The view SQL could not be read or parsed |

## Restrictions — ask for them, do not invent them

`--restrictions` takes a JSON file. Ask the user what they want to exclude before
running; do not guess a scope.

```json
{
  "exclude_databases": ["SNOWFLAKE_LEARNING_DB"],
  "exclude_schemas": ["STAGE"],
  "exclude_object_types": ["VIEW"],
  "exclude_name_patterns": ["^TMP_", "_BAK$"],
  "exclude_objects": ["DB.SCHEMA.SCRATCH"],
  "max_rows": 100000000,
  "max_bytes": 1099511627776
}
```

`include_*` variants act as allowlists. An unrecognised key is an error, not an
ignored line — a typo would otherwise apply nothing while appearing to work.

## Lineage provenance matters — state it

`deps` prints its source:

- `account_usage` — authoritative lineage across all object types
- `parsed_ddl` — the `ACCOUNT_USAGE` grant was unavailable, so edges come from
  parsing view DDL. **View→object edges only.** Say the graph is partial; do not
  present it as complete lineage.

## Present, do not just run

1. The can/cannot split and every reason.
2. **Catalogs the user must create or confirm as INTERNAL** before stage 3.
3. The waves — views follow their base tables.
4. Cycles, if any: they need a human decision, not a broken edge.
5. The Silver/Gold job stubs: created, disabled, never triggered.
6. **The data-movement architecture options — always.**

Exit 3 means a target-name collision — show it and stop.

## The architecture options are not optional reading

`PLANNED_OBJECTS.md` ends with all five. Do not skip past them because this MVP
moves no data: the user needs to know which architecture they are heading toward
*before* structure lands, because it decides whether the destination is an
INTERNAL catalog they will fill, an EXTERNAL catalog they will read through, or
both.

| | Moves bytes | Handles |
|---|---|---|
| `A1` unload → object storage → managed Delta | yes | historic bulk, cutover |
| `A2` federate through an EXTERNAL catalog | no | read without copy |
| `A3` redirect ingestion (Fivetran / Kafka) | yes | ongoing incremental, cutover |
| `A4` Iceberg interop — share storage | no | read without copy, ongoing |
| `A5` hybrid waves | yes | all four |
| `A6` **customer-defined — or not decided yet** | *unknown* | *unknown until described* |

State whether one has been chosen. If not, say the architecture is **undecided**
and put all six in front of the user.

**`A6` is a real answer, not a fallback.** The customer may already have a
pattern their platform team runs, and it may be better than anything here. They
may also simply not have decided. Either way, record `A6` with their reasoning
rather than pressing them toward `A1`–`A5`. Nothing in the assessment, the plan
or the shallow clone depends on the answer.

When they do describe a design — including one not listed here — record it
verbatim:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py data-options --out-dir ./snowmig_out \
  --choose A6_CUSTOMER_DEFINED --chosen-by <name> --rationale "<why>" \
  --custom-name "<their name for it>" --custom-description-file <file>
```

It is recorded as-is and **never mapped** to one of ours. Say plainly that this
plugin has not assessed it, so none of the trade-offs or unknowns listed against
`A1`–`A5` transfer to it.

**If the user has no preference**, the honest recommendation is `A2` first — it
moves nothing, needs the least building, and lets results be validated against
the source before anything is copied — with `A1` for whatever usage data later
shows is worth making resident. Say clearly that this is a recommendation, not a
decision: it belongs to the customer because it drives cost, wall-clock and
whether a later migration can run unattended.

Record a choice with:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py data-options --out-dir ./snowmig_out \
  --choose A2_FEDERATE_EXTERNAL_CATALOG --chosen-by <name> --rationale "<why>"
```

A rationale is mandatory. Recording executes nothing — **none of the six is
implemented**, and `execute_transfer()` refuses by design.

## Raise the maintenance question — it does not raise itself

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`**. It maintains layout via
Automatic Clustering and reclaims storage in the background, un-asked. AIDP has
`OPTIMIZE`, `VACUUM`, `ZORDER BY` and liquid clustering, and **runs none of
them**. So a customer who asks for "the same maintenance on AIDP" is not asking
for a missing feature — they are inheriting a responsibility.

If the DDL plan has a *"Maintenance and layout — decisions, NOT applied"*
section, read it out. Say three things:

1. **Every listed setting has an AIDP equivalent, and none of them was
   applied.** A clustering key that does not arrive is a performance regression
   on the largest tables in the estate, and it is silent.
2. **On Delta, `VACUUM` is what bounds time travel.** On Snowflake, retention
   and storage reclamation are independent and automatic. A customer used to
   reclaiming storage freely will delete their own recovery window. Snowflake's
   7-day Fail-safe has **no** equivalent at all.
3. **`OPTIMIZE` without `VACUUM` increases storage.** It leaves the old files
   behind until retention expires, so half the job is a cost regression.

Do not propose a cadence or a retention. Both need the customer's recovery
requirements and query patterns. `references/maintenance-and-layout.md` has the
full mapping; `ACTION-ITEMS.md` has the planned work.

## Finish with `summary` — it is not optional

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py summary --out-dir ./snowmig_out
```

`SUMMARY.md` is the per-object roll-up the user asked for: one row per table,
view and job with its row count, migration risk, migration status and a note.
Run it after `plan`, and again after `deploy` so the statuses reflect what
actually happened. It also carries the source→destination header, the
row-count provenance, and the data-movement architecture options.
