# Changelog

All notable changes to this plugin are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

This plugin began as a copy of the AIDP workbench plugin layout. Nothing of that
scaffold's behaviour survives — the engine, skills, commands and docs are all
specific to Snowflake — so the history below starts with this plugin's own
first release.

## [0.15.0] — 2026-09-10

### Fixed

- **The smoke test reported FAIL against a working destination.** Its
  destination half still posted to `.../sql/execute`, which returns 404, so
  anyone running the pipeline in order stopped there — *before* reaching the
  transport that works. A false FAIL costs as much as a false PASS. It now runs
  on the catalog API: `list_schemas` for read, and a write probe that creates a
  schema, confirms it is **visible** (creates are async and can fail silently)
  and deletes it again. Live: PASS, nothing left behind.
  - The probe name is unique per run. A fixed name would burn itself the first
    time it failed, and stay burned.
  - The report no longer names a cluster for the destination — the catalog API
    needs none, which is the point.

### Added

- **Poisoned-name diagnosis** (P1). When an object never appears, the plugin
  creates one throwaway object with a novel name in the same schema — once per
  schema, not per object — and reads the answer. A novel name that lands means
  the planned names are **burned**, and the report says so and says to retry
  into a fresh schema. A novel name that also fails means it is *not* a burned
  name, and points at the request or the permissions instead. `--no-diagnose`
  turns it off, since the probe writes.

## [0.14.0] — 2026-09-10

### Added

- **`GAPS.md`** — the single ranked list of what is left, what is proven, and
  which questions need Oracle or the customer rather than code.

### Changed

- `ASSUMPTIONS.md` reconciled with the live run. Five assumptions moved from
  *unverified* to **live-verified** (catalog `GET` field shapes, key
  qualification, case folding, async-create-with-no-waiter, poisoned names,
  derived view types), and two `DESCRIBE`/`SHOW` assumptions are marked
  superseded — structure is verified through the catalog API now, not SQL.
- Fixed a live test that still expected un-folded target names.

## [0.13.1] — 2026-09-10

### Added

- **`derived_type_drift`** — a view that was created with every planned column
  in order, but whose column *types* the target computed from the SQL rather
  than taking the declared ones. Reported separately from a mismatch, because
  the two mean opposite things: a table mismatch means the object is **not
  ours** and was left alone; drift means the object **is ours** and the engine
  disagreed about a type. The old wording ("left as it was found and has NOT
  been cloned") was simply wrong for the second case.
  - Narrowings are called out as an overflow risk.

Found live on `RAPPI_ORDER_360_VW`, created correctly with all 17 columns:

| Column | Snowflake declares | AIDP derived |
|---|---|---|
| `ITEM_COUNT` | `decimal(18,0)` | `bigint` |
| `TOTAL_ITEM_QUANTITY` | `decimal(22,0)` | **`decimal(20,0)`** |
| `ITEM_TOTAL_AMOUNT` | `decimal(30,2)` | **`decimal(28,2)`** |

All three are aggregates (`COUNT`, `SUM`). Snowflake reports a view's declared
output types; the target derives its own. The two narrowings are a real
fidelity risk worth checking before anything depends on those columns.

## [0.13.0] — 2026-09-10

**First objects actually created on AIDP.** Six tables landed as managed Delta
with the planned field types, from a real Snowflake estate.

### Fixed

- **Structure is now read with a GET, not from the list.** The list response
  proves existence and gives the server's real key case, but carries **no
  `tableFields` at all** — so six tables that had been created *correctly* were
  every one reported a MISMATCH against a fieldless list entry. Existence comes
  from the list; structure comes from a GET on the resolved key.
- **An existing schema is no longer re-created.** Resolve first, create only if
  absent, and wait for `ACTIVE`.

### Learned — a failed create POISONS the name

The most operationally dangerous finding so far, and **not** a plugin bug.
When an asynchronous create fails, that name becomes permanently unusable in
that schema: every later create returns **202 Accepted** and is silently
dropped. Isolated one variable at a time —

| Attempt | Result |
|---|---|
| `test_table` (after earlier failures) | 202, **never appears** |
| `fresh_probe_9`, **identical body** | 202, **appears** |
| `test_table` after an explicit `DELETE` | 202, **still never appears** |

`DELETE` returns 202 and does not recover the name. The only recovery found is
a **different schema**.

This matters well beyond a test: a customer whose first run fails for any
reason cannot fix the cause and re-run into the same schema. Every failed
table will keep returning 202 and keep not existing, with nothing in the API
saying why. Tracked as P1–P3 in `ACTION-ITEMS.md`; the practical guidance is
**if a run fails, retry into a new schema**.

## [0.12.0] — 2026-09-10

### Added

- **`snowmig stages`** and the `snowflake-stage-board` skill — the pipeline as
  a table: which stages run, what each needs, which have run, and what each
  one found, with `deploy` marked as the only stage that writes. Offline and
  read-only, so it is safe to run before anything else. A stage that **could
  not look is flagged, never shown as clean** — "0 exposures" and "policy
  attachments unreadable" are opposite findings.
  - `DONE` means the stage ran, not that the result was good: a `deploy` row
    can read `DONE ⚠️ verified 0/7`, and the skill says to report that plainly
    rather than as a completed migration.

### Changed

- **The `TIMESTAMP_NTZ` decision moved into the translator**, where type
  decisions belong. It was briefly handled in the catalog transport, which was
  the wrong place: the translator owns what a Snowflake type becomes and the
  transport should only refuse what it genuinely cannot express. The flag is
  now `assess --timestamp-ntz {preserve,timestamp}`, so the **plan shows the
  type that will really be created** instead of one the transport silently
  rejects later. The transport keeps its refusal as defence in depth, and its
  message now points at the upstream fix.

### Learned

- **The async catalog API offers no waiter.** A `POST` returns 202 Accepted
  with an empty body and **no `opc-work-request-id`** — only an
  `opc-request-id` — so there is no work request to poll and no CLI
  `--wait-for-state` to lean on. `oci raw-request` has no waiter either, and
  the `aidp` name on PyPI is an unrelated 0.0.1 placeholder, not Oracle's CLI.
  Poll-and-verify is therefore not a workaround; it is the only sound method
  available, which is worth knowing before anyone tries to replace it.

## [0.11.0] — 2026-09-10

**First live migration against a real AIDP DataLake.** Five undocumented
behaviours, every one of which broke the run. All handled and covered by tests.

### Added

- **`catalog_api` transport (now the default)** — creates schemas, tables and
  views through the catalog CRUD API instead of SQL. `POST
  /workspaces/<ws>/sql/execute` returns 404, so the SQL path could not create
  anything; and for a structure-only clone the catalog API is better anyway: it
  needs **no Spark cluster** (every cluster in the target environment was
  stopped) and has no session to lose DDL to. `--transport sql` remains.
- **`PREFLIGHT.md`** — written and echoed before the first write, once both
  ends are known: every source → destination mapping, what will be created,
  and what will *not* happen. A migration that starts without this is one the
  user did not approve.
- `--bronze-schema-style db` for a `<catalog>.<snowflake database>.<table>`
  layout, alongside the collision-safe default.
- `--timestamp-ntz {block,timestamp}` and `--probe-table-parameters`.

### Changed

- **Target names are now planned lower-cased**, because AIDP folds
  identifiers. The plan shows the name the destination will really use instead
  of one that silently differs. This also makes the case-collision detector
  load-bearing: Snowflake keeps `ORDERS` and `"orders"` apart, AIDP cannot, so
  two such tables halt the run rather than merging.

### Fixed

- **AIDP lower-cases identifiers.** A schema created as `TEST_DB_20260908_1529`
  is stored as `test_db_20260908_1529`, so every `schemaKey` and read-back key
  built from the requested case was wrong — all seven objects reported failed
  while the schema had in fact been created. Keys are now resolved from the
  server and compared case-insensitively.
- **Creation is asynchronous and can fail silently.** `POST /tables` returns
  **202 Accepted with an empty body**; the object appears seconds later, or
  never if the async work fails — and when it fails nothing reports it. Six
  tables once returned 202 and not one existed. The read-back now polls with a
  bounded backoff, and an object that never appears is a failure that says so.
  This is the strongest argument for read-back-and-compare: a plugin trusting
  the create's status code would have reported a clean seven-object migration
  into an empty schema.
- **`timestamp_ntz` is the only standard type the catalog API rejects**
  (`timestamp`, `date`, `boolean`, `binary`, `double`, `bigint`, `int`, `float`
  all work). It returns 202 and then fails silently, which is how it hid. Since
  Snowflake `TIMESTAMP_NTZ` maps to Spark `TIMESTAMP_NTZ` deliberately — bare
  `TIMESTAMP` is session-timezone-dependent — the downgrade is an explicit
  decision, not a default.
- **A list response is a collection; a create response is one object.**
  Collapsing both to `rows[0]` meant key resolution saw one schema instead of
  the list and found no match.
- `list_tables` sent a bare `schemaKey`, which returns 400 InvalidParameter.

## [0.10.1] — 2026-09-10

### Fixed

Both found on first contact with a real AIDP environment.

- **The smoke test reported `PASS` against an endpoint that does not exist.**
  `oci raw-request` exits **0** on an HTTP error and returns the error in the
  response *body* — `{"data": {"code": "NotAuthorizedOrNotFound"}, "status":
  "404 Not Found"}` — so the exit code proves nothing. `parse_cli_json` ended
  in `return [payload]`, which turned that error object into one row of
  "results", and the destination check counted it as "1 schema(s) visible".
  A 404 now raises `BackendError` with the status and body. This affected
  **every** AIDP call, not just the smoke test.
- **Nested `data.items` was not unwrapped**, so a collection response of three
  schemas was also reported as one row. Every AIDP collection response has
  that shape, so successful reads were being miscounted too.

### Verified live

- `https://aidp.<region>.oci.oraclecloud.com/20240831` is the correct base,
  and control-plane reads work: `GET /dataLakes/<ocid>/workspaces`,
  `/clusters`, `/catalogs` and `/schemas?catalogKey=<cat>` all return 200.
- **`POST /workspaces/<ws>/sql/execute` does not exist (404).** The invented
  SQL path cannot work, so `deploy --execute` cannot run through the
  `oci_raw` backend at all. See `ACTION-ITEMS.md` (T1).

## [0.10.0] — 2026-09-10

### Added

- **Estate census** — `assess` now inventories everything that is *not* a
  table or a view: stored procedures, UDFs, tasks, streams, materialized and
  dynamic tables, stages, pipes, sequences and file formats. Writes
  `CENSUS.md`, and the resulting scope statement is carried into
  `PLANNED_OBJECTS.md` and `SUMMARY.md`, so `"N of N objects can move"` is
  qualified by what was actually examined. `--no-census` skips it, and the
  coverage claim then says so.
  - Procedures and UDFs come from `INFORMATION_SCHEMA`, not `SHOW`:
    `SHOW PROCEDURES IN SCHEMA` returns Snowflake's own built-ins — 33 of them
    on a completely empty schema — and `INFORMATION_SCHEMA` does not. It also
    carries the handler language, which is what decides the effort.
  - Per-language triage bands, with JavaScript flagged HIGH: there is no
    JavaScript runtime on AIDP, so the logic has to be understood and
    rewritten, not translated.
  - **Nothing in the census is migratable**, and no equivalent is generated.
- **`snowmig security`** — masking, row-access, aggregation and projection
  policy *attachments* from `ACCOUNT_USAGE.POLICY_REFERENCES`, secure views,
  and a grant summary. Writes `SECURITY.md`.
  - This is the only report with a data-**exposure** consequence: a masked
    column arrives unmasked, a row filter is simply absent, and a secure view
    loses `SECURE`. The clone does not fail — it succeeds without the
    protection.
  - When `ACCOUNT_USAGE` cannot be read, `exposure_count` is `null` and the
    report says the question is **unanswered**. It must never read as "no
    policies found", which is the dangerous false negative.
  - Grants are reported and **never replayed**, enforced by test.

### Fixed

- **Two CLI stages were orphaned** — no skill or command invoked `summary` or
  `maintenance`, so nothing would ever run them. `summary` produces
  `SUMMARY.md`, one of the plugin's headline deliverables. Both are now owned
  by a skill, and a test fails if any stage becomes unreachable again.

## [0.9.0] — 2026-09-10

### Added

- **`snowmig maintenance`** (item M2) — Snowflake maintenance and layout state
  as first-class inventory: clustering keys and `automatic_clustering`, Search
  Optimization, `change_tracking`, the Time Travel retention cascade across
  account/database/schema with per-table effective values, and reclustering
  credits plus DML churn from `ACCOUNT_USAGE`. Emits `maintenance.json` and
  `MAINTENANCE.md`, with a per-table list of signals naming what each will
  require on AIDP. **Reports; proposes nothing** — enforced by a test that
  rejects statement-shaped output.
- Capabilities with **no AIDP equivalent** named explicitly (item M7):
  Fail-safe, Search Optimization Service, and
  `MAX_DATA_EXTENSION_TIME_IN_DAYS`.
- **Maintenance ownership is now part of the architecture choice.** Every
  data-movement option states who inherits `OPTIMIZE`/`VACUUM` and which of
  the three Delta traps apply to it — federating leaves the work with
  Snowflake, landing Delta tables transfers it on day one, hybrid means both
  regimes at once, and a customer-defined design makes no claim. The three
  traps travel with the options in every plan and summary.
- The inventory now captures every maintenance-relevant `SHOW TABLES` column,
  which was free and was the missing input to all of the above.

### Fixed

- `architecture_decision()` projects a fixed field set and dropped
  `maintenance_ownership` on the way through, so every rendered row read
  "*unknown*" while unit tests reading `OPTIONS` directly all passed. Now
  carried through, with a test that asserts against the **rendered table**.

## [0.8.0] — 2026-09-10

### Added

- `references/maintenance-and-layout.md` — how Snowflake and AIDP differ on
  `OPTIMIZE`/`VACUUM`-shaped maintenance. Snowflake exposes neither and
  maintains layout automatically; AIDP has both plus `ZORDER` and liquid
  clustering and runs none of them for you. The capability survives the
  migration, the responsibility moves.
- `ACTION-ITEMS.md` — eight tracked items (M1–M8) to close the maintenance gap.
- A *"Maintenance and layout — decisions, NOT applied"* section in the DDL
  plan, naming each source setting, its value and its AIDP equivalent.
- Live tests for `SHOW` pagination semantics and for the semi-structured
  escape hatch against real `VARIANT`/`OBJECT`/`ARRAY` columns. 12 live tests.

### Fixed

- **`cluster_by`, `retention_time`, `data_retention_time_in_days` and
  `change_tracking` were reported as "dropped, no Delta equivalent".** All four
  have AIDP equivalents — liquid clustering / `ZORDER`,
  `delta.deletedFileRetentionDuration` + `delta.logRetentionDuration`, and
  Change Data Feed. The claim was false in the expensive direction: it invited
  a customer to accept a silent performance regression on their largest tables.
  Now split into properties with genuinely nowhere to go and properties
  *deferred* with the equivalent named, which raise risk to MEDIUM.

### Verified

- Assumption **B9** (`SHOW … LIMIT n FROM`) live: name-ordered, exclusive
  resume, and a full paged walk equals the unpaged list.
- Issue **#7** live against real semi-structured columns in
  `SNOWFLAKE.ACCOUNT_USAGE`: 8 and 5 columns blocked by default with
  per-column reasons, carried as `STRING` with a warning on every one when
  `--semi-structured string` is passed.

## [0.7.1] — 2026-09-09

### Fixed

- **Spark string literals were escaped by doubling the quote**, which is
  correct in Snowflake and in standard SQL but is *not an escape in Spark* —
  Spark reads `'it''s'` as two adjacent literals and concatenates them. A
  column comment of `Customer's orders` therefore arrived as
  `Customers orders`. Spark escapes with a backslash. Fixed in the column
  `COMMENT`, in both `LIKE` existence probes, and in the generated notebook.

### Added

- `tests/test_generated_sql_parses.py` — parses the generated Spark DDL with a
  real Spark-dialect parser (`sqlglot`, dev-only). This is what found the
  escaping bug above: 675 hand-written assertions had accepted it, because the
  expectations and the implementation shared an author.

## [0.7.0] — 2026-09-09

**Correctness and cost pass.** Findings from a self-review, addressed in the
order they were prioritised.

### Added

- **`engine/snowflake_source/dialect/lexer.py`** — a string-aware SQL scanner.
  Literals, quoted identifiers and comments are opaque: their contents are
  never matched against, split on, or rewritten. Comment stripping, statement
  splitting, leading-verb detection, identifier quoting, LIKE escaping and
  code-restricted substitution all come from this one scan.
- Structure verification on deploy. `verified` now means *the object exists
  with the planned column list*, checked with `DESCRIBE`. New outcomes
  `mismatch` (exists with a different structure) and `unverified_structure`
  (exists, could not be compared) are reported separately and neither counts
  as verified.
- `--row-counts {metadata,exact,none}` on `assess`.
- `--semi-structured {block,string}` and `--geospatial {block,string}` — an
  escape hatch so one `VARIANT` column no longer blocks its whole table with no
  way to proceed.
- SHOW pagination, so an estate above 10,000 objects is no longer silently
  truncated.
- A test that compiles every generated notebook code cell.
- `StatementTooLarge`, refusing an argv-oversized batch with advice instead of
  a bare `E2BIG`.

### Changed

- **Row counts default to Snowflake's maintained count** (free) rather than a
  `COUNT(*)` per object. Views are not counted unless asked, because counting a
  view executes it. The metadata count is labelled as such and never called
  "exact".
- `INFORMATION_SCHEMA.COLUMNS` is read per schema rather than per database.
- An untranslated view is **HIGH** risk, not MEDIUM: it creates successfully
  and then returns wrong numbers.
- A structure mismatch reports `BLOCKED`, not `SHALLOW_CLONE`.
- The destination write probe now **cleans up after itself**. The no-DROP rule
  is a source guarantee and had been wrongly applied to AIDP as well.
- `TIME -> STRING` carries a warning; integer-to-`DECIMAL(38,0)` carries an
  informational note that does not inflate risk.

### Fixed

- Statement splitting on `;` inside a string literal, which fabricated a second
  statement out of `select 'a;drop table t'`.
- Comment stripping that ate the rest of a line on `'a -- b'`.
- Translator rules rewriting text **inside string literals**, which changed
  query results rather than SQL.
- The `T16_VARIANT_PATH` detector firing on any JSON-ish literal, blocking
  views with no VARIANT access at all.
- `LIKE '<name>'` existence probes treating `_` as a wildcard, and never
  comparing the returned name to the target.
- `f'"{name}"'` identifier building, which produced broken SQL for a name
  containing a double quote.
- View-body extraction failing on a quoted view name containing a space, and
  mistaking an `AS` inside a header comment or literal for the end of the
  header.
- `PRIVACY.md`, `NOTICE` and this changelog described a different plugin
  entirely.
- Three declared runtime dependencies that nothing imported.
- A quadratic scan when splitting statements by catalog scope.

## [0.6.0] — 2026-09-09

### Added

- `A6_CUSTOMER_DEFINED` data-movement option: the architecture may be the
  customer's own and need not be one of ours, and deferring the decision is
  recorded as a deliberate deferral rather than a gap.

## [0.5.0] — 2026-09-09

### Added

- The data-movement architecture options are **always** presented with every
  plan and summary, so the choice is never made by default.
- Source read-only enforced at the transport, with a default-deny verb
  allowlist.
- Dialect translation skeleton: rules that rewrite exactly, and rules declared
  as needing a decision, reported separately.
- `ASSUMPTIONS.md`, `references/data-movement-options.md`,
  `CLEANUP-BEFORE-PUBLISH.md`.

### Removed

- The forked WebSocket/Spark-session transport, unused after the CLI backends
  replaced it.

## [0.4.0] — 2026-09-09

### Added

- Per-object summary table: name, row count, risk, migration status.
- Smoke test across both ends.
- The shallow clone delivered as an executable AIDP notebook with per-object
  progress and verification.

## [0.3.0] — 2026-09-09

### Added

- Bronze mirrors the source 1:1 — database to Standard Catalog, schema to
  schema, table to table, view to view. Views in scope.
- Disabled, never-triggered Silver and Gold job stubs.
- `aidp` CLI and `oci raw-request` execution backends.
- Warehouse-to-cluster compute proposal.

## [0.2.0] — 2026-09-09

### Added

- `snowmig` CLI pipeline: assess, deps, plan, ddl, deploy.
- Estate inventory with column types, identifier case capture and a halting
  case-collision detector.
- Snowflake-to-Spark type mapping; an unmapped type is blocked, never
  approximated.
- Dual-source dependency extraction and topological waves, with cycles
  reported rather than broken.
- Delta DDL generation with a per-rule audit trail.
- Dry-run-default deployment.

## [0.1.0] — 2026-09-08

### Added

- Plugin skeleton, file-based Snowflake authentication (key-pair, programmatic
  access token, password, SSO), and inert target resolution that can neither
  discover nor persist AIDP coordinates.
