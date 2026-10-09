# oracle-ai-data-platform-workbench-ocidi-migrator

Migrate **OCI Data Integration (OCI-DI)** workspaces to **Oracle AI Data
Platform (AIDP)** from Claude Code.

OCI-DI keeps no data of its own; it moves data between data assets. What
migrates is its logic:

| OCI-DI | → | AIDP |
|---|---|---|
| Data flow + Integration task, Data Loader task | → | PySpark notebook (one per task) |
| Pipeline (Pipeline task) | → | AIDP job: `NOTEBOOK_TASK`s with `dependsOn` / `runIf` / retries |
| Published task outside a pipeline | → | single-task AIDP job |
| Schedule + task schedule | → | job `schedule` (Quartz + IANA zone), **created PAUSED** |
| Target data entity | → | managed **Delta** table in catalog `ocidi_migrated` |
| Source data asset | → | AIDP external catalog / `oci://` path / AIDP credential (JDBC) |
| Incremental load, `SYS.LAST_LOAD_DATE` | → | `ocidi_control.watermarks` Delta table, seeded from the last DI run |
| User-defined functions | → | inlined at each call site |
| SQL task / REST task / OCI Data Flow task | → | notebook: LLM work order / compiled HTTP call / manual stub |

The default AIDP workspace is **`ocidi_migrated`**; `provision` creates it.

## How it works: a deterministic compiler, with an LLM for the long tail

```
extract ─▶ analyze ─▶ migrate ─▶ fallback ─▶ verify ─▶ provision ─▶ publish ─▶ reconcile
live DI    offline    offline    Claude,      offline    AIDP         AIDP        AIDP
read-only                        in-session              dry run      dry run     (user runs)
```

1. **`extract`** reads the OCI-DI REST API (`/20200430`) into a snapshot
   directory of raw JSON, with secrets redacted. It reads design-time objects
   *and* applications' published objects, schedules and the last successful
   run of each task. Raw JSON is kept because the typed OCI SDK drops fields
   of models it does not know.
2. **`migrate`** compiles each data flow graph in dependency order: one
   DataFrame per operator port, one notebook cell per operator. Expressions
   go through a curated function table
   ([references/expression-functions.md](references/expression-functions.md)).
   `OPERATOR.ENTITY.ATTR` references are resolved to columns, `l.`/`r.` inside
   joins. Parameters become run-time-quoted placeholders. `DECODE` becomes a
   null-safe CASE.
3. Anything without a safe deterministic translation becomes a **work
   order**, `fallback/<id>.json`, plus a notebook stub that raises
   `NotImplementedError`. The notebook stays complete and fails loudly; it
   never writes wrong rows. Examples: an unknown function, `ORA_HASH`, a
   custom-SQL source, an OCI Function operator, a SQL task.
4. **`fallback`**: Claude, in the session, answers each work order through
   the `ocidi-fallback-author` agent. The validator only accepts the contracted
   function, with allow-listed imports and no network, filesystem, `exec` or
   credentials unless the work-order kind grants it. Accepted code is spliced
   in under an `LLM-ASSISTED — REVIEW REQUIRED` banner. The author may decline;
   the object then becomes `manual`. `--provider anthropic` does the same
   headless for CI.
5. **`verify`** grades every artifact PASS / REVIEW / FAIL.
6. **`provision`** and **`publish`** are dry runs until `--apply`:
   - `publish` never overwrites.
   - It uploads notebooks before jobs.
   - It refuses any job whose notebooks this migration did not upload.
7. **`reconcile`** compares every legacy DI target with its new Delta table
   (row counts, numeric sums, one-sided keys).

## Quick start

From the plugin directory (or `${CLAUDE_PLUGIN_ROOT}` once installed):

```bash
# No OCI-DI needed: a complete example workspace ships as a fixture
python3 -m ocidi2aidp analyze tests/fixtures/snapshot_sales -o /tmp/analysis
python3 -m ocidi2aidp migrate tests/fixtures/snapshot_sales -o /tmp/out
python3 -m ocidi2aidp fallback list /tmp/out
python3 -m ocidi2aidp verify /tmp/out
python3 -m ocidi2aidp publish /tmp/out                     # dry run

# Against the real thing
python3 -m ocidi2aidp extract --workspace-id <dis workspace OCID> --di-region us-ashburn-1 -o snapshot/
python3 -m ocidi2aidp provision --instance-name <aidp-instance-name>            # dry run
python3 -m ocidi2aidp provision --instance-name <aidp-instance-name> --apply    # creates ocidi_migrated
python3 -m ocidi2aidp publish out/ --instance-name <aidp-instance-name> --cluster-key <key> --apply
```

In Claude Code, `/ocidi-run` walks through the offline steps and
`/ocidi-deploy` through the AIDP ones. `ocidi-migrator-overview` routes
everything else.

## Plugin contents

| Kind | Name | Does |
|---|---|---|
| skill | `ocidi-migrator-overview` | Router, fixed order, standing rules, limitations |
| skill | `ocidi-migrator-bootstrap` | Dependencies, `ocidi-config.yaml`, smoke tests |
| skill | `ocidi-extract` | Live OCI-DI → snapshot (read-only) |
| skill | `ocidi-analyze` | Inventory, coverage, effort bands |
| skill | `ocidi-migrate` | Compile notebooks, jobs, DDL, work orders |
| skill | `ocidi-fallback` | Fill work orders with Claude; validate; splice |
| skill | `ocidi-verify` | Static PASS/REVIEW/FAIL |
| skill | `ocidi-provision` | Workspace `ocidi_migrated` + catalog |
| skill | `ocidi-publish` | Upload notebooks, create PAUSED jobs |
| skill | `ocidi-reconcile` | Watermark seeding, reconciliation, cutover |
| agent | `ocidi-fallback-author` | Answers one work order |
| agent | `ocidi-migration-reviewer` | Reviews a notebook against its DI JSON |
| command | `/ocidi-run`, `/ocidi-deploy`, `/ocidi-status` | Guided flows (offline run; provision + publish; status) |

Engine: `ocidi2aidp/` (standard library only for the offline verbs; `oci`
for `extract` and lookup by instance name; the `aidp` CLI for
provision/publish).

## Configuration

`ocidi-config.yaml` (git-ignored), from
[examples/ocidi-config.example.yaml](examples/ocidi-config.example.yaml). The
AIDP instance can be given by display name (`aidp.instance_name`) or OCID. Map DI data assets to AIDP access paths under
`sources:` and DI schemas to AIDP schemas under `target.schema_map:`.
Identifiers can come from `OCIDI_*` environment variables instead of the file.

## Evidence: what has been checked, and how

| Check | Result |
|---|---|
| Unit + integration tests (`pytest`) | 117 tests. The 7 Spark tests skip when PySpark is not installed. |
| Generated notebooks executed on local Spark 3.5 + Delta 3.2 | Asserts result rows: MERGE idempotence, parameter override, join/lookup null-fill/split/aggregate, watermark incrementality, refusal of a lossy cast. |
| In-session LLM path (option C), fixture run | 3 work orders: 2 written and spliced (JDBC stored-procedure call; CRC32 bucket for ORA_HASH, executed on Spark), 1 correctly declined (OCI Function, code not available). |
| Live AIDP, a development instance (2026-10-09) | `provision --apply` created workspace `ocidi_migrated` (ACTIVE). `publish --apply` created folders and uploaded 11 notebooks. Re-publish skipped all 11 (never overwrites). Jobs are deferred: AIDP rejects a task without `cluster` (400), and the new workspace has no cluster. |
| Live OCI-DI | **Not yet.** No OCI-DI workspace was reachable. Shapes come from the OCI SDK 2.165.1 models; see [references/known-limitations.md](references/known-limitations.md). |

## Running the tests

```bash
pip install -e ".[dev]"            # offline suite
pip install -e ".[dev,spark]"      # + execution tests (Java 8/11/17)
pytest
python tests/fixtures/make_snapshot_sales.py   # regenerate the fixture
python -m ocidi2aidp.docs                      # regenerate expression-functions.md
```

## Documents

- [references/design.md](references/design.md): architecture, snapshot and output layouts.
- [references/expression-functions.md](references/expression-functions.md): every DI function and its translation.
- [references/known-limitations.md](references/known-limitations.md): what is unverified or unsupported, and why.
- [PRIVACY.md](PRIVACY.md): what leaves the machine, and where it goes.
