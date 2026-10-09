# Changelog

All notable changes to this project are documented here. This project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — unreleased

Initial release: an OCI Data Integration → Oracle AI Data Platform migrator,
packaged as a Claude Code plugin.

### Added

- **Engine `ocidi2aidp`.** Verbs: `doctor`, `extract`, `analyze`, `migrate`,
  `fallback` (list / prompt / apply / run), `verify`, `provision`, `publish`,
  `status`.
- **Data flow compiler.** Covers sources (catalog, Object Storage files with
  their declared schema, JDBC through AIDP credentials), filter, expression
  (including bulk/macro), join, lookup (multi-match strategies, null fill),
  aggregate, distinct, sort, union/minus/intersect, split (FIRST/ALL),
  pivot, flatten, and targets (APPEND / OVERWRITE / MERGE / IGNORE, direct,
  name, position and pattern field maps, `loadOrder`). Incremental loads use
  Delta watermarks.
- **Expression translator.** Uses a curated function table (180+ functions)
  in which unknown names are never passed through. Covers
  `OPERATOR.ENTITY.ATTR` resolution, quoted run-time parameters, `SYS.*`
  parameters, UDF inlining and Oracle-type CASTs.
- **Pipelines → AIDP jobs.** `dependsOn`, `runIf` from trigger rules and
  merges, retries, nested pipelines inlined, Decision branches withheld as
  `manual`.
- **Schedules → Quartz + IANA zone, PAUSED.** Includes OCI-DI's Monday-first
  custom-cron weekdays. A schedule with no exact Quartz form is left off and
  reported, never approximated.
- **LLM fallback (work orders).** Static validator with per-kind
  capabilities, splicing, declines, the `ocidi-fallback-author` agent, and a
  headless Anthropic provider (`claude-opus-5-5`, server-side fallbacks).
- **Target DDL, watermark seed, reconciliation notebook, `sources.md`,
  `REVIEW.md`.**
- **`provision`.** Workspace `ocidi_migrated` and an INTERNAL catalog; the
  instance can be resolved by display name. **`publish`:** dry run by
  default, never overwrites, notebooks before jobs.
- **Fixture workspace** `tests/fixtures/snapshot_sales` (6 data flows, 11
  tasks, 2 pipelines, an application with 6 published tasks, 4 schedules)
  and its generator.
- **Tests.** Execution tests run the generated notebooks on local Spark 3.5 +
  Delta and assert on rows.

### Found by running it

- `DeltaTable.forName` rejects `catalog.schema.table`, so MERGE uses SQL
  `MERGE INTO` over a temp view.
- Text-file sources need the entity's declared schema, otherwise every CSV
  column is STRING.
- A write into a DDL-created table needs a cast to the table's types. A cast
  that would lose a value now stops the write rather than nulling it.
- `aidp workspace-object create` needs `--body ''` even for a folder (live,
  2026-10-09).
- AIDP rejects a job task without `cluster` (400, live 2026-10-09). Publish
  now uploads notebooks and defers jobs until `--cluster-key` is given. A
  later publish may use notebooks an earlier one uploaded.
- The fallback prompt told authors to return "exactly" the port's listed
  columns. For pass-through operators those are only the added fields; two
  authors flagged this in the first in-session run.
