# Changelog

All notable changes to this plugin are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

This plugin began as a copy of the AIDP workbench plugin layout. Nothing of that
scaffold's behaviour survives — the engine, skills, commands and docs are all
specific to Snowflake — so the history below starts with this plugin's own
first release.

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
