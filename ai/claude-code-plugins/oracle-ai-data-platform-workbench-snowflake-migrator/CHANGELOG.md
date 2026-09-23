# Changelog

All notable changes to this plugin are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

This plugin began as a copy of the AIDP workbench plugin layout. Nothing of that
scaffold's behaviour survives — the engine, skills, commands and docs are all
specific to Snowflake — so the history below starts with this plugin's own
first release.

## [Unreleased]

### Fixed — the CLI could not run on a Windows machine

Every `read_text()`/`write_text()`/`open()` in the engine, the data-plane
scripts and the tests used the platform default encoding. On Windows that is
cp1252, which cannot encode the arrows and dashes every report carries, so
`snowmig.py demo` — and every stage that renders a report — died with
`UnicodeEncodeError`, and 34 tests failed or errored while the same tree
passed under `PYTHONUTF8=1`.

- Every text-mode file operation and CLI-output decode now says
  `encoding="utf-8"`; `main()` reconfigures stdout/stderr the same way, so a
  piped report line cannot raise either.
- `tests/test_locale_independence.py` runs the demo in a subprocess under an
  ASCII locale on every platform and fails on the previous tree.

### Fixed — one skill loaded with empty metadata

`skills/snowflake-assess-estate/SKILL.md` had an unquoted description
containing `: `, which YAML rejects; Claude Code then loaded the skill with no
description to route on. The description is quoted, and the plugin-surface
test parses frontmatter with `yaml.safe_load` instead of splitting each line
on its first colon, which is how the file passed before.

### Fixed — housekeeping

- `.claude-plugin/marketplace.json` now carries the same version as
  `plugin.json` (0.25.0); it said 0.23.1.
- README no longer links `docs/plans/2026-09-09-snowflake-migrator-mvp1.md`,
  which is not in the repository.
- The `0600` promise for the config file is stated as POSIX-only, and the test
  pinning it skips on Windows instead of failing.
- `bin/snowmig` and `bin/snowmig-test` find the interpreter of a venv built on
  Windows (`Scripts/python.exe`) and accept `python` as a last-resort name.
- `--help` no longer opens with "Five subcommands" (there are 24); pyflakes
  is clean apart from one deliberate re-export.

### Fixed — credential handling

- The diagnose notebook redacted only top-level keys while the config nests
  everything under `snowflake:`, so the nested JSON the README recommends for
  a PyYAML-less cluster was printed whole — password and PEM — into cell
  output; as shipped it could not run anyway (`provision` never uploaded it).
  It is now generated from `engine/dataplane/diagnose_environment.py`,
  uploaded by `provision` beside the stage notebooks with no job, and echoes
  key names with `<set>`/`<unset>` only.
- Two secrets still reached the command line: `catalog --execute
  --test-connection` put the `testConnection` body (password or whole PEM)
  inline on the `oci` argv, and `--key-passphrase` took the passphrase inline
  on every Snowflake stage. The body is spooled to a temp file like the
  `create_catalog` body; the flag is removed in favour of `key_passphrase:`
  or `key_passphrase_path:` in the config and refused without echoing.
- `provision --execute --source-config` uploaded the whole migration config
  verbatim to `backup-snowflake-migration/plan/`, secret and unread `aidp:`
  block alike, and accepted a `key_path:` config whose laptop path the job
  then failed on with a raw `FileNotFoundError`. Only the `snowflake:` block
  travels now, as `<stem>.json`, announced on stderr and in `PROVISION.md`
  with the advice to remove it afterwards; a `*_path` secret is refused.
- The plugin `.gitignore` ignored `*.pem` and `*.key` but not `*.p8`, while
  the docs generate an unencrypted PKCS#8 key into the working directory, so
  `git add -A` would have staged a private key. It now covers `*.p8`,
  `*.pk8`, `rsa_key*`, `*_rsa_key*` and `sf_key*`, pinned by a test that runs
  `git check-ignore` for every name the docs create.
- `load_config()` left `yaml.safe_load` unguarded, so a password containing
  `{`, `[`, `*`, `: ` or a leading quote — unquoted, as the example shows it
  — escaped `preflight` as a raw traceback quoting the password line. It now
  raises `ConfigError` with the file and line, never the text; the
  cluster-side loader in `engine/dataplane/snowmig_source.py` still lacks the
  guard, so a malformed provisioned config can still quote its line in a log.
- `PRIVACY.md` still described the structure-only design: path-only secrets,
  no credential in any artifact, "table data is never read", only `deploy`
  writes. It now names the two paths that carry the credential to the tenancy
  (`provision --source-config`, `catalog --execute`), that the data plane
  reads every row, that `run` has no dry-run flag, and what removal leaves.

### Fixed — the read-only Snowflake transport

- `assert_read_only` judged a statement by its first keyword and `WITH` is
  allowlisted for CTE-SELECTs, so `WITH x AS (...) INSERT` (or `DELETE`,
  `UPDATE`, `MERGE`, `CREATE TABLE AS`) passed as a read — nothing in the
  engine sends a CTE, but the guard failed open. `lexer.cte_body_verb()`
  finds the keyword after the CTE list; `WITH` is allowed only before a
  `SELECT`, and the tests assert a refused CTE never reaches `cursor.execute`.
- `--auth pat` could never log in: the token went in `password` while
  snowflake-connector-python builds `AuthByPAT` from `token`, so the login
  body carried `TOKEN: null` and the hint blamed the password or key. It is
  passed as `token`; a contract test drives the connector's own `AuthByPAT`.
- The config's `host:` fed the EXTERNAL catalog body and the in-AIDP
  connector but never the laptop-side driver, so `preflight --test-source`
  passed a wrong host that failed minutes later inside AIDP.
  `build_connect_kwargs` takes `host` and `_snowflake_coords` hands it on.

### Fixed — view translation and DDL

- The `::` cast rule matched its literal with `'[^']*'`, so `'don\'t'::string`
  matched the tail `'t'::string`, spliced `CAST` into the literal and the
  re-lex raised `UnterminatedLiteral` out of the planner: `plan` and `ddl`
  exited 1 for the whole estate with no view named. The pattern follows the
  lexer's escape rules, and a rule that raises blocks only its own construct.
- T02 emitted `CAST(x AS <snowflake type>)` verbatim as an "exact rewrite":
  Spark `FLOAT` is single precision, bare `DECIMAL` is `DECIMAL(10,0)`, and
  `NUMBER`/`TEXT`/`TIME`/`VARIANT` are not Spark types, so views truncated
  silently or failed on first query. The type now goes through the same
  `map_type` table DDL uses; a blocked type refuses the statement.
- `DATEADD` was labelled exact and was not: the amount was interpolated
  unparenthesised (`a + b * 7`), a column amount on a time unit became
  `INTERVAL n_hours HOUR`, `date_add` returns `DATE` so a `TIMESTAMP` lost its
  time of day, and forms the regex missed shipped unreported as portable.
  Only literal or column amounts are rewritten, the rest refuse by name, the
  rule carries a DATE-only caveat, and a generic guard reports any rule whose
  construct survives its own pass.
- `DATEDIFF`, `TIMESTAMPDIFF`, `TIMESTAMPADD` and `TIMEADD` were documented as
  blocking but had no rule, so such views were planned migratable and stamped
  portable; `views.UNSUPPORTED_CONSTRUCTS` was dead. Declared rules
  `T19_DATEDIFF` and `T20_TIMESTAMPADD` refuse the family, and the table is
  derived from `translate.RULES` so the two cannot drift.
- Snowflake quoting shipped verbatim: Spark reads `"Order ID"` as a string
  literal, so a view returned the constant text on every row while the
  structure check still matched; `'O''Brien'` is two literals that Spark
  concatenates, so the predicate matched `OBrien`; `$$...$$` failed at parse
  time. `T07_QUOTED_IDENTIFIER` rewrites `"x"` to a backtick identifier,
  `T08_STRING_ESCAPE` rewrites `''` to `\'`, `T09_DOLLAR_QUOTED` refuses.
- R42 asserted "no Snowflake-only construct present" when only the rule
  table's regexes had been tried; `GREATEST`/`LEAST`, `SPLIT`, `ZEROIFNULL`
  and `TO_CHAR` formats ship verbatim. It now reads "no known Snowflake-only
  construct matched; functions not in the rule table are carried verbatim
  and may fail or differ at Spark parse time".
- `build_create_view` rewrote object references with an unanchored `re.sub`
  over the whole body: it mutated string literals, missed a reference whose
  parts `GET_DDL` had quoted, hit `DB.S.ORDERS` inside `DB.S.ORDERS_ARCHIVE`,
  and `DDL_PLAN.md` reported a rewrite that never happened. A lexer-aware
  `_rewrite_view_refs` matches whole three-part names outside literals and
  comments; R41 lists only real hits.
- `unsupported_target_types()` anchored on `` `(\w+)` TIMESTAMP_NTZ ``, which
  cannot match names the emitter itself backticks (spaces, hyphens, dots), so
  a plan whose NTZ columns were all so named passed the HALT gate and failed
  per table on the cluster. The gate reads each statement's `expected_columns`.
- `PLANNED_OBJECTS.md` lists cycle members as excluded from the ordering, but
  `build_ddl_payload` re-added every un-waved clone target — exactly those —
  so both got `CREATE VIEW` statements in arbitrary order and a failed create
  could burn the name. They are held back and listed under "Blocked — no DDL
  generated" naming the other members.
- Snowflake's default `TIMESTAMP` is `TIMESTAMP_NTZ`, the mapper preserves it
  and the AIDP metastore refuses it, so `ddl` halted (exit 3) on nearly every
  real estate and the only remedy was a paid re-read through `assess`. `ddl
  --timestamp-ntz {preserve,timestamp}` re-maps every NTZ column offline from
  `inventory.json`; `ddl_plan.json` records the mode and the columns.

### Fixed — metadata extraction and in-AIDP discovery

- `_show_all` resumed a SHOW walk with `LIMIT n FROM '<name>'` but
  LIKE-escaped the name (`ORDER\_ITEMS`); `FROM` takes a plain name, so past
  one 10k page whose boundary name had an underscore the page repeated, the
  cursor guard stopped the walk and the tail of the schema silently never
  entered the inventory. The cursor is embedded with only its quotes doubled.
- Lineage lost edges two ways: quoted mixed-case identifiers
  (`DB.S.SalesView` over `DB.S.Orders`) were compared upper-cased against the
  inventory's exact spelling and dropped, and a readable but empty
  `OBJECT_DEPENDENCIES` (it lags DDL by hours) was still labelled
  "authoritative lineage" — so views were emitted before their base tables.
  Both paths keep the inventory's spelling, views with no ACCOUNT_USAGE edge
  are parsed from their DDL, and a `warning` names any view still unordered.
- `_account_usage_summary` reported two ACCOUNT_USAGE reads through one
  `readable` flag: when clustering history succeeded and `TABLE_DML_HISTORY`
  then failed, every table got a measured churn of 0 rows. The status carries
  `clustering_readable` and `dml_readable`; churn becomes `measured: false`.
- `inventory_from_manifest` wrote the size as `BYTES` while every reader uses
  `bytes`, so on the in-AIDP path `max_bytes` excluded nothing while
  `plan.json` recorded the cap as applied and `INVENTORY.md` showed `-` for
  every size. The bridge writes `rows` and `bytes`.
- SHOW commands, the INFORMATION_SCHEMA views and
  `ACCOUNT_USAGE.POLICY_REFERENCES` return nothing, successfully, for what
  the role cannot see or the view has not caught up with, and every consumer
  read that as "none exist": a read-only role made the census say "only
  tables and views ... the whole estate", and the security verdict said
  "Nothing is protected today" while policy objects had just been listed.
  Census counts are worded as a lower bound for the recorded `role` with the
  grants a complete census needs listed, and defined policies with no visible
  attachment are UNCONFIRMED exposure with the lag named.
- Connector-mode discovery ran `INFORMATION_SCHEMA.TABLES`/`COLUMNS` with no
  predicate and applied `--schemas` after the fetch, so an estate over
  Snowflake's result cap died with an empty manifest and a hint blaming the
  credentials; a `--schemas` run then overwrote the loaded manifest with its
  filtered result (and `--force` never loaded it), so `DISCOVERY.md`'s own
  advice left one schema in `discovery_manifest.json`. `--schemas` is pushed
  down as `where TABLE_SCHEMA in (...)` and merges into the manifest;
  `--force` alone rediscovers the whole estate.

### Fixed — planning, restrictions and the smoke test

- `include_objects`/`exclude_objects` upper-cased both sides, so excluding
  `DB.S.t` to defer the quoted lower-case twin also dropped `DB.S.T`, and
  `PLANNED_OBJECTS.md` blamed the operator for both. A double-quoted part
  (`"DB"."S"."t"`) matches exactly, unquoted parts still fold, the reason says
  which happened, and the `TargetCollision` HALT ends with the remedy.
- `validate_restrictions()` checked only Python types, so `{"include_objects":
  []}` planned the whole estate under "Restrictions in force", blank patterns
  excluded everything, `max_rows: -1` excluded every counted table, and
  `max_rows`/`max_bytes` silently kept any object whose count or size was
  unknown — every view under the default `--row-counts metadata`. Degenerate
  values are rejected (`plan` exits 1, no `plan.json`), and a cap that cannot
  be evaluated excludes the object with that reason.
- `build_plan` never consulted dependency edges and handed only planned ids to
  `compute_waves`, which drops edges to unknown nodes, so a view over a
  VARIANT-blocked or restriction-excluded table sorted first in wave 1 and
  got a `CREATE VIEW` over a table that will never exist. Dependents of any
  non-migrating object cascade to `cannot_migrate` under
  `dependency_not_migrated`, naming the object and why.
- `run_smoke` computed `ok` over checks that ran; with no AIDP coordinates the
  destination was skipped with an empty check list, so CLI, `SMOKE_TEST.md`
  and the stage board all said PASS — on the default first run, since the
  example config ships the coordinates commented out. The result carries
  `verdict` (`PASS`, `PARTIAL`, `FAIL`), `smoke_verdict()` derives it for an
  older `smoke.json`, and all three surfaces say PARTIAL.
- The plan names targets `<source_db>.<schema>.<name>`, but the structure job
  never reads `target_fqn` and creates `<--target-catalog>.<schema>.<name>`,
  so the sign-off named catalogs the job does not create — by default the
  very name the runbook registers as the EXTERNAL pointer. Naming is
  unchanged; `plan.json` carries `target_catalog_note` explaining both, and
  `PLANNED_OBJECTS.md` renders it.
- `SHOW TABLES` flags dynamic, external, Iceberg, event and hybrid tables and
  the extractor kept the flags, but `build_plan` never read them: a dynamic
  table was "Can migrate" and "never migrates" in two reports at once, and a
  one-time copy would have stopped refreshing after cutover unannounced.
  Those kinds are `cannot_migrate` with a kind-specific reason;
  `TRANSIENT`/`TEMPORARY` stay migratable, listed in `table_kind_warnings`.

### Fixed — AIDP control-plane calls, jobs and provisioning

- `_find_schema`/`_resolve_object` swallowed every listing error as `None`, so
  a 403 on `list_schemas` re-POSTed an existing schema and a 403 on
  `list_tables_in` reported every created table as "never appeared" after
  33 s of polling each, leaving a probe behind. Schemas are listed before any
  write and a failure refuses; on read-back a permission error stops at once
  and the table is reported UNKNOWN, not absent.
- Every AIDP list read was a single GET with no `page=`, and the parser
  dropped the `headers` block carrying `opc-next-page`, so every existence
  decision read page one: a catalog past it was re-created, a table created
  past it "never appeared" and was called burned, and `in_flight_runs` missed
  the run it guards against. Both transports follow the token via
  `collect_pages`; the `aidp` CLI, which cannot request a second page,
  refuses a truncated listing instead of returning page one as the whole.
- `TERMINAL_STATES` omitted `INTERNAL_ERROR`, `SKIPPED`, `UPSTREAM_CANCELED`
  and `EXCLUDED`, so a run that died that way was polled for the whole budget,
  cancelled and resubmitted by the cold-start watchdog, reported STILL
  RUNNING and exited 0; `cancel_run` also swallowed its own failure, so the
  resubmit went into the occupied slot and was discarded. The four states are
  terminal (exit 1, no resubmit), an unknown status exits 1 as
  `unrecognised`, and a resubmit needs a confirmed cancel — otherwise the
  original run is kept and `run` exits 1 saying so.
- After `create_workspace`, `provision()` waited only for the name to be
  visible and POSTed the cluster at once; the 409 "ongoing operation" that
  `GAPS.md` records as expected escaped as `ProvisionTransportError`, so no
  `provision_result.json` or `PROVISION.md` was written and the next run
  halted on `name_taken` calling the workspace someone else's. The workspace
  is polled to `ACTIVE`, the POST retries 409 with bounded delays, and both
  artifacts are written on failure with exit 1 and a `--reuse-existing` hint.
- `POST /actions/testConnection` answers 202 with an empty body and the async
  key in a response header; the parser dropped headers and `cmd_catalog`
  looked in the body, so the poll was dead code, the outcome always `PENDING`
  and `CATALOG.md` silent. Results keep their headers under `_headers`,
  `connection_test_outcome` polls `GET /asyncOperations/{key}` to a verdict,
  and `CATALOG.md` gains a "Connection test" section; the exit code stays 0
  on `FAILED` — reported, not enforced.
- Every `provision --execute` regenerated all four stage notebooks and
  uploaded them with `--is-overwrite`, so `--reuse-existing` (the documented
  resume) silently reset console-edited PARAMS — `schema` to `None`, `verify`
  to `counts`, the reconcile `counts` to `False`. With `--reuse-existing` a
  notebook already on the workspace is kept and `PROVISION.md` says so;
  `--refresh-notebooks` asks for the overwrite explicitly.

### Fixed — the in-AIDP structure, copy and reconcile jobs

- Views in the plan and manifest were created by no job and appeared in no
  report: an estate with every view missing read "No table is in a problem
  state", and a view deployed via the catalog API was flagged "someone else's
  object". Neither job creates views and both say so: `01` records them as
  `not_created_by_this_path`, `03` emits a `VIEW_NOT_CREATED_BY_THIS_PATH`
  row each (not a problem, not pending) and names `deploy --execute`.
- `01_create_structure` recorded `created` right after `CREATE TABLE IF NOT
  EXISTS`, a no-op on a table already there, so a stale layout was certified
  as the plan's and the copy INSERTed positionally into it (same column
  count: wrong columns, matching counts). The stage DESCRIBEs every table and
  compares names and types in order — `created`, `already_existed` or
  `type_drift` (left as found, exit 1); the copy never takes a drifted table
  and reconcile reports `STRUCTURE_TYPE_DRIFT`, which outranks a verified copy.
- A structure run in which every table was `not_in_plan` created nothing and
  exited 0, the copy recorded every table `target_missing` and exited 0, and
  reconcile exited 0 — three SUCCESS jobs for a migration that did nothing.
  The structure job exits 1 saying the plan and the requested schemas do not
  overlap when nothing was created (a canary plan over one schema still
  passes), and the copy's scope log says how many were `not_in_plan`.
- A table whose copy ended `count_mismatch` was re-attempted on the next run;
  skip-existing saw rows, returned `skipped_nonempty`, overwrote the failure,
  and reconcile rendered `PRESENT_NOT_REVERIFIED` under "No table is in a
  problem state" — re-running the failed job erased the signal. Skip-existing
  now compares counts, a re-run that copied nothing keeps a prior failure,
  and `--force` with the default mode is refused, naming `--mode overwrite`
  or `--mode append`.
- `--verify counts+sums` took the DECIMAL column set and cast scale from the
  target's DESCRIBE: a `NUMBER(18,2)` landing in a `bigint` column was never
  summed (cents lost, counts equal, `verified`), and a `decimal(18,0)` target
  rounded both sides to scale 0 and passed. Before any row moves the copy
  DESCRIBEs source and target and records `type_drift` (not copied) when a
  source DECIMAL would lose type, integer digits or scale; sums use the
  source's columns at the source's scale on both sides.
- Only the INSERT was guarded, so one transient connector login timeout on a
  source read, `COUNT(*)`, DESCRIBE or SUM ended a 200-table schema with a
  traceback, the failing table unrecorded and the rest never attempted. Any
  exception is recorded as `failed` and the loop continues (the stage still
  exits 1); a failure after the INSERT landed records `insert_completed:
  true` and says to re-copy with `--mode overwrite`, not `append`.
- Reconcile trusted the wrong evidence three ways: it kept only the schema
  half of a report's `target`, so a copy report verified against a test
  catalog was applied to production and empty tables rendered
  `MIGRATED_VERIFIED`; a `CREATE` that raised reconciled as `NOT_MIGRATED`,
  the same as never attempted; and with `--counts` it fetched a live
  `COUNT(*)` but never compared it. A report for another catalog is ignored
  and recorded, a failed create is `STRUCTURE_FAILED`, and a live count that
  differs from the verified one is `COUNT_DRIFT` — all problem verdicts.
- The committed stage notebooks had drifted from their `engine/dataplane/`
  sources and were written with CRLF on Windows, so whole-file diffs hid the
  real changes. All five are regenerated LF-only, one test pins each notebook
  to its generator and another fails on any CRLF byte; the reconcile headline
  counts "object(s)" when views are tallied under an unreadable target.

### Fixed — CLI wiring and configuration

- Root-level `--out-dir X <stage>` (the placement the usage line advertises)
  was silently discarded: the same parent parser sat on the root and every
  subparser, and argparse re-applied the subparser's `None` default, so
  `snowmig --out-dir X clean` deleted the default artifact directory — the
  live run's evidence — with exit 0. The subparser copy uses
  `argparse.SUPPRESS`, so it can only add, never overwrite.
- Four operator inputs produced raw tracebacks or nameless messages:
  `--out-dir` naming an existing file, a restrictions file holding a JSON
  list, unparseable restrictions JSON, and a corrupt JSON artifact read by
  `_read()`. Each is one `error:` line naming the file, exit 1.
- `catalog --execute` with the name only in the config announced the
  destination, then passed `args.catalog` (`None`) into `ensure_catalog` and
  crashed with an `AttributeError`; the dry run wrote `"catalog": null`. The
  name is resolved once by catalog type (`aidp.external_catalog` for
  EXTERNAL, `aidp.catalog` for `--catalog-type standard`; `--catalog` wins),
  and a missing name is one `error:` line before any API call.
- `deploy`, `catalog` and `provision` wrote the same result artifact whether
  executed or rehearsed, so re-running one without `--execute` replaced the
  `dry_run: false` record — verified counts, burned names, catalog key — and
  `STAGES.md`/`SUMMARY.md` then said nothing had been created. A dry run that
  finds an executed record prints one `error:` line and exits 1 without
  writing.
- `smoke --write-probe` and `notebook --upload` wrote to AIDP on their own
  flag with no `--execute`, and `--upload` drove the Jupyter contents API that
  `GAPS.md` 13 records as known-bad (PUT returns 200, file unreadable),
  reported "Uploaded" with no read-back, and pointed at `aidp notebook run`,
  which does not exist. Both flags are dry runs without `--execute`; with it
  the probe runs as before and the upload is refused (exit 1) naming the
  verified path, `provision --execute` then `run --job snowmig_01_structure`.
- `AIDP_FIELDS` accepted `oci_profile`, `external_catalog`, `target_catalog`
  and `subnet_id` under `aidp:` and the CLI announced them, but nothing read
  them: every `oci raw-request` ran under `DEFAULT` and config-only catalogs
  never reached the jobs' PARAMS defaults. `aidp.oci_profile` reaches every
  `oci` argv as `--profile <p>` (the `aidp` argv is left alone); `provision`
  takes the two catalogs from the config; `subnet_id` is announced as unused.

### Fixed — reports, the stage board and the documentation

- `INVENTORY.md` hard-coded "Row counts are exact", labelled the column `Rows
  (exact)` and rendered every missing count as `ERROR` regardless of
  `row_count_mode`, so in the default metadata mode SHOW estimates read as
  verified and every view as an extraction failure. The header follows the
  mode, blank cells read `not counted`, and `ERROR` means `source=error`.
- `render_summary` scored risk from plan entries carrying only
  id/type/target/rows/columns, so the MEDIUM rules for deferred maintenance
  settings, dropped properties and column warnings never fired and the
  demo's clustered `ORDERS` table read LOW. Each `can_migrate` entry carries
  `warnings`, `deferred_properties` and `omitted_properties`, and
  `assess_risk` only raises, so a view with column warnings stays HIGH.
- The stage board broke its own "could not look is flagged" rule on four
  rows: `deps` flagged on a key `dependencies.json` never has, `compute`
  ignored blocked warehouses, `provision` ignored steps left `verified=None`,
  and `deploy` summed only failed+mismatched so unverified structure, type
  drift and transport errors were invisible. Each row now raises the marker
  for what it could not confirm; the deploy row is split by cause.
- The manifest, `NOTICE`, `GAPS.md`, `ASSUMPTIONS.md` D3, the data-movement
  reference, two skill descriptions and runtime text said the plugin copies
  no data or moves no bytes, while it provisions `snowmig_02_copy_schema`,
  which INSERT-SELECTs every row. Every surface now says the control plane
  copies no data and rows are copied only by the operator-run in-AIDP job;
  `SUMMARY.md` reads the deploy result only and points at reconcile.
- README ran `catalog --execute` before `provision` with `--workspace
  --cluster-id` bracketed as optional, made laptop `assess` a migration step
  and ran the copy jobs as part of the sequence; `MIGRATION-ARCHITECTURE.md`
  had a third order. All three runbooks share one order — provision, paste
  the keys, EXTERNAL registration and INTERNAL container, workflows — and the
  hand-off sends the operator to `provision_result.json` for the keys, since
  `PROVISION.md` shows display names, which used as keys create unrunnable
  objects.
- Five documents gave four answers to which data-plane stages have run live.
  `GAPS.md` "What is actually proven" is the one home (`00_discover` and
  `01_create_structure` live-verified; `02_copy_schema` and `03_reconcile`
  not yet confirmed), repeated verbatim in README, `ASSUMPTIONS.md`,
  `MIGRATION-ARCHITECTURE.md`, the data-plane README and the overview skill.
  This supersedes the 0.25.0 line below that called the copy and reconcile
  stages unexecuted: the data-plane README had described a five-table copy
  verified row for row, and which account is right is recorded as open.
- `/snowflake-catalog` said a Standard catalog "is refused here by design"
  and routed its tables to `snowmig.py notebook`; `/snowflake-soft-clone`
  routed them through the known-bad clone-notebook upload. Both route the way
  the runbook and CLI do: the container from `catalog --catalog-type standard
  --execute`, the tables from `run --job snowmig_01_structure`.
- Three doc claims about secrets and auth were false: the medallion-clone
  skill said the config "carries no secret" and had none of the redaction
  rules, so an agent had licence to show a file holding the password inline;
  README and two skills said the config "is gitignored" when the rule lives
  only inside the plugin folder, so `init-config` in another repository and
  `git add .` stages the password; the bootstrap skill said AIDP auth is "not
  configured in this plugin at all" while every `aidp` call gets `--auth
  api_key` and `oci raw-request` uses the shell's `~/.oci/config` profile.
  Each surface now states the inline default and the redaction rules, the
  scoped gitignore claim, and the auth mechanism.

### Changed — behaviour an operator will notice

- `smoke` reports `PARTIAL` and exits 1 when AIDP was not checked.
- `run` exits 1 on any non-success terminal state, on a state it cannot
  classify and on an unconfirmed cold-start cancel; a job still running when
  the poll budget ends keeps exit 0.
- `snowmig_01_structure` exits 1 when it created nothing and something was
  `not_in_plan`, and on any `type_drift`.
- `snowmig_02_copy_schema` records `count_mismatch` (exit 1) on a pre-loaded
  target whose count differs, refuses a DECIMAL that would narrow on the
  target, and refuses `--force` without `--mode overwrite` or `--mode append`.
- `snowmig_03_reconcile` exits 1 on `STRUCTURE_FAILED`, `STRUCTURE_TYPE_DRIFT`
  and (with `--counts`) `COUNT_DRIFT`; a report for another catalog is
  ignored.
- `smoke --write-probe` and `notebook --upload` need `--execute`; without it
  they are dry runs. `notebook --upload --execute` is refused and points at
  `provision --execute` and `run --job snowmig_01_structure`.
- `--key-passphrase` is removed in favour of `key_passphrase:` or
  `key_passphrase_path:` in the config.
- `provision --source-config` uploads only the `snowflake:` block, as
  `<stem>.json`, and refuses a block naming a `*_path` secret.
- `provision --execute --reuse-existing` keeps stage notebooks already on the
  workspace unless `--refresh-notebooks`; `provision` also uploads
  `diagnose_environment.ipynb`.
- A dry run of `deploy`, `catalog` or `provision` refuses to overwrite an
  executed result; rehearse into another `--out-dir`.
- `max_rows`/`max_bytes` exclude objects whose count or size is unknown —
  under the default `--row-counts metadata` that is every view, so pair
  `max_rows` with `exclude_object_types: ["VIEW"]` or use `--row-counts exact`.
- Empty `include_*`/`exclude_*` lists, blank or non-string entries, boolean
  or negative caps and object types outside `TABLE`/`VIEW` are errors; a
  double-quoted part in `include_objects`/`exclude_objects` matches exactly.
- Dynamic, external, Iceberg, event and hybrid tables are planned as
  `cannot_migrate`, as is a view over a base that does not migrate.
- Plans built from estates with quoted mixed-case views or an empty
  `OBJECT_DEPENDENCIES` must be regenerated: views move to later waves.
- `00_discover_snowflake --schemas` merges into the existing manifest;
  `--force` alone rediscovers the whole estate.
- `WITH` is allowed on the read-only transport only before a `SELECT`.
- `--auth pat` sends the token in the connector's `token` field.
- `DATEADD` forms outside the exact set, `$$...$$` strings and the
  `DATEDIFF`/`TIMESTAMPDIFF`/`TIMESTAMPADD`/`TIMEADD` family are refused;
  view casts emit Spark types, quoted identifiers become backticks and `''`
  becomes `\'`.
- `ddl --timestamp-ntz timestamp` re-maps `TIMESTAMP_NTZ` offline; the
  default stays `preserve`.
- `aidp.oci_profile` reaches every `oci` call as `--profile`; `provision`
  takes `external_catalog`/`target_catalog` from the config when the flags
  are absent.
- `--out-dir` before the subcommand is honoured; a value after it still wins.
- AIDP list reads follow `opc-next-page`; on the `aidp` CLI backend a
  truncated listing raises instead of passing as the whole.
- `catalog --execute --test-connection` reports a real verdict in
  `CATALOG.md`; the exit code stays 0 on `FAILED`.

### Fixed — after the merge of the fixes above

- The `::` cast rule matched its operand against the raw view text, so an
  operand could end inside a `$$...$$` string or a line comment and the
  rewrite was spliced into it (`select $$a$$::string` became `select
  $$CAST(a$$ AS STRING)`, recorded as an exact rewrite). The operand must now
  be wholly code or exactly one whole string or identifier segment, and `$`
  cannot precede it, so dollar-quoted operands and `$1` positional references
  are refused rather than mangled.
- The VARIANT-path detector wanted an identifier after the colon, so
  `v:"Field Name"` slipped past it, the quoted-identifier rule turned the
  field into a backtick and the view was stamped an exact rewrite with a
  colon path still in the body. The detector is an identifier followed by one
  colon that is not `::`; the view is refused, named.
- The copy's manifest fallback re-admitted every table the structure report
  recorded as `type_drift` (the `created` filter only ran when at least one
  table had been created), so an all-drift schema was copied positionally
  into the drifted layout. Drifted tables are excluded, the scope line says
  how many, and a schema with nothing left refuses with exit 1.
- The diagnose notebook's default config path named a `.yaml` under `plan/`
  that nothing creates; `provision` uploads the `snowflake:` block as
  `plan/<stem>.json`. Default, header and the comment in the shared source
  say so; the notebooks that inline them are regenerated.
- Two calls past the cluster still escaped `provision()`: the job listing and
  the cluster listing inside the warehouse loop. An expired session token on
  either lost `provision_result.json` and `PROVISION.md` with the workspace
  and cluster already created. Both are recorded steps; the job listing halts
  with the resume hint, the warehouse listing moves on to the next warehouse.
- The STANDARD-catalog note in `catalog_result.json` still routed the operator
  to `snowmig.py notebook`, whose upload is refused, and called the INTERNAL
  create runbook S3 where the overview says S4. It names `run --job
  snowmig_01_structure` and GAPS 13, and the labels agree.
- `STAGES.md` said every stage but three is read-only "except the narrow
  opt-ins `smoke --write-probe` and `notebook --upload`"; the probe writes
  only with `--execute` and `--upload` never writes. The preamble, the board
  module docstring and the smoke stage's default note name the `--execute`
  gate; the demo's `NOTEBOOK.md` routes to `provision` and `run`.
- The board's `deps` row labelled every non-`account_usage` graph "partial
  graph (view DDL only)" although the extractor now also writes
  `account_usage+parsed_ddl` and `account_usage_empty`; it surfaces the
  producer's own warning for those, keeps the unresolved-reference count on
  every partial branch, and flags a provenance it does not recognise.
- The board's `security` row, and the console line, read "0 policy
  exposure(s)" with no hedge when policy objects existed but
  `POLICY_REFERENCES` showed no attachment, or when the policy listing was
  denied. Both now say UNCONFIRMED, as `SECURITY.md` already did.
- `PLAN.md` stated "views follow their base tables" unconditionally. A view
  with no edge from either lineage source sorts by size and can land ahead of
  its base; `plan.json` carries `dependency_warning`, `dependency_edge_count`
  and `views_without_dependency_edge`, and `PLANNED_OBJECTS.md` names the
  views ordered by size only.
- TRANSIENT/TEMPORARY tables were recorded as planned "as permanent Delta
  tables" and rendered nowhere; `SUMMARY.md` scored them LOW. Each plan entry
  carries `kind_warning`, the risk becomes MEDIUM with the sentence, and
  `PLANNED_OBJECTS.md` lists them under "Planned, but not as what they were".
- "Target structure to exist first" told the reader in three lines to create
  catalog `d`, that `d` is the EXTERNAL pointer and not the target, and that
  the clone creates `d.public`, a schema the structure job never creates. The
  section is split per path: the source schemas the S10 job creates under
  `--target-catalog`, and the plan's names for the older `deploy`/`notebook`
  path only.
- Documentation that had fallen behind the merged behaviour was brought back
  in line: `PRIVACY.md` (the derived `plan/<stem>.json` copy, the `--execute`
  gate on the write probe, `notebook --upload` no longer a writer), README
  (`aidp.oci_profile` reaches every `oci` call; the view-translation
  paragraph counts the implemented and refused rules from `translate.RULES`),
  `ARCHITECTURE.md`, `MIGRATION-ARCHITECTURE.md` (rule counts, S10 route),
  `ASSUMPTIONS.md` B2, the smoke, migration-plan, bootstrap, overview and
  stage-board skills, the `/snowflake-notebook` command, and the data-plane
  README's config path. `references/env-coords.template.md`, which described
  environment variables nothing reads, is removed.

### Fixed — the DDL applied is the DDL the operator approved

- `DDL_PLAN.md` showed `NOT NULL` and column comments; neither execution
  path applied them, because the column list handed to the catalog API and
  to the in-AIDP structure notebook carried name and type alone, and both
  verifications compared that same reduced shape, so a table that differed
  from the approved SQL read back as verified. The column spec is now
  rendered from one place for the SQL, the API body and the notebook, both
  verifications compare nullability and comments, and a property that could
  not be read back is reported UNCHECKED rather than counted as applied.
- The table and view `COMMENT` travel as the catalog's description (it was
  always sent empty); a description the target drops is reported.
- The catalog API body has no nullability field. That gap is stated per
  object in `DDL_PLAN.md` next to the rules (`R21`) and in the deploy result
  and report, and the generated SQL keeps the `NOT NULL`.
- Column `DEFAULT`, `IDENTITY_START` and `IDENTITY_INCREMENT` are read from
  `INFORMATION_SCHEMA.COLUMNS` and recorded as warnings (`R22`, `R23`),
  raising the object to MEDIUM. They are not emitted: nothing offline
  confirms the target accepts either, and a DDL the target rejects is worse
  than a stated gap. After cutover an insert Snowflake would have populated
  arrives NULL or fails; that is now a decision, not a discovery.
- Rule `R20` claimed PK/FK/UNIQUE constraints were "captured in the
  inventory" while no extractor captured them. A new extractor reads
  `SHOW PRIMARY KEYS`, `SHOW UNIQUE KEYS` and `SHOW IMPORTED KEYS` per
  database, and the rule names the constraints and their columns. Snowflake
  has no `CHECK` constraint, so the one class Delta would enforce has nothing
  to carry over, and the rule says so.

### Added — the census sees the whole estate

- Thirteen more artifact kinds are enumerated. Per database: alerts,
  secrets, network rules, Streamlit apps, notebooks and container services.
  Once per account, through a new `scope: account` on the census entry:
  shares, roles, network policies, applications and compute pools. None of
  them migrate; before this they were missing from the questions rather than
  from the report, so `CENSUS.md` read as complete when it was not. An
  outbound share is the sharpest case, a live contract whose consumer finds
  out at cutover.
- A UDTF and an external function were both counted as scalar UDFs; each now
  has its own kind and verdict. An external stage, already in object storage
  so AIDP can be pointed at it, no longer gets the same verdict as an
  internal one, which has to be unloaded first.
- Cost: six more `SHOW` statements per database and five per run. Where a
  wider column select is refused, the census re-asks for the columns it has
  always read and reports the distinction as *not distinguishable*, never as
  none. Every new read is a `SHOW` or a `SELECT`; the transport is unchanged.

### Fixed — the in-AIDP planning path dropped the column facts

- Live 2026-09-23, the same estate planned both ways and compared column by
  column: **71 columns, identical types, identical verdicts — and six facts
  present from a live `assess` and absent from a manifest.** Three column
  `DEFAULT`s, an `IDENTITY` start and increment, and a column `COMMENT`.
- Those are exactly what `R22`, `R23` and the column-comment fidelity work
  report on. So an estate planned through runbook S6/S7 — the path that
  exists *because* the estate is too large for the laptop path — got a DDL
  plan with no warning that its defaults and identity columns stop working
  at cutover, while the same estate planned from a laptop warned about both.
  Neither plan said it differed from the other.
- Discovery now selects `COLUMN_DEFAULT`, `IDENTITY_START`,
  `IDENTITY_INCREMENT` and `COMMENT`; the bridge carries them; and the two
  paths were re-compared live afterwards: **0 differences over 71 columns.**
- A manifest written by an older discovery carries none of them, and that is
  recorded as UNKNOWN for every object rather than rendered as "this column
  has no default" — the same false negative the census rule exists to
  prevent, and the quiet kind, because the plan simply omits the warning.

### Fixed — "as visible to role X" was not true when secondary roles were on

- Live 2026-09-23. A session connected as `role=SNOWMIG_LIMITED`, a role
  holding `USAGE` on exactly one database, read the whole account:

      current_role()            SNOWMIG_LIMITED
      current_secondary_roles() {"roles":"ORGADMIN,ACCOUNTADMIN","value":"ALL"}

  Snowflake activates every role granted to the user by default. The same
  `assess` against the same two databases counted **24 objects** with
  secondary roles on and **10** with them off — same role name, same
  command, 2.4x the estate.
- Every count in `CENSUS.md`, `SECURITY.md` and the `INVENTORY.md` header is
  attributed to `CURRENT_ROLE()`, so the attribution named an authority the
  numbers were not produced under — and it erred in the unsafe direction,
  making a restricted role look sufficient when the run had leaned on
  `ACCOUNTADMIN`. `CURRENT_SECONDARY_ROLES()` is read now and every sentence
  that names a role names them too.
- **`--only-primary-role`** drops them for the session, so a run sees exactly
  what `--role` can see. That is what a migration rehearsal needs: proving a
  least-privilege role is sufficient BEFORE cutover, rather than finding out
  at cutover that every read had been served by something else. Asked for,
  never assumed, and said on the console when it happens.

### Fixed — an inventory headline that disagreed with the inventory

- Same run: `Databases in scope: SNOWMIG_COVERAGE, SNOWMIG_COV_B` above
  `**16 objects**`, where `SNOWMIG_COV_B` had been refused outright and
  contributed nothing. The refusal was disclosed thirty-five lines lower
  under *Extraction notes*, which is not where a reader who has taken the
  headline goes.
- A database in scope that could not be read is marked `(**NOT READ**)` in
  the scope line, and a note under the count says the count does not cover
  it and that this is a privilege result rather than an empty database. An
  object-level extraction note is not mistaken for a refused database.

### Fixed — the four items raised on the review PR

- **Required before merge: census readability was global, not per database.**
  A kind that answered in one database and was denied in another reported as
  *not visible to this role* with a null count, while the rows counted in the
  database that answered sat in the same report's `by_kind` and object table.
  The report contradicted itself and the number it hid was real. Three states
  now, not two: all answered, none did, or some did -- the last keeps its
  count, names the databases that were denied, and renders as **partial**.
- **The in-AIDP transport refuses a write too.** `conn.py` was hardened
  against CTE-prefixed writes while `dataplane/snowmig_source.py`'s
  `pushdown()` had no verb enforcement at all, and that is the transport the
  migration notebooks run on the cluster against the customer's live
  Snowflake. It runs standalone and cannot import the engine's lexer, so the
  guard is deliberately stricter rather than a second implementation of the
  same analysis: one statement, one leading read verb, and a CTE refused
  rather than followed to its body. It runs before the mode check, so the
  refusal cannot depend on configuration being right.
- **`include_name_patterns` / `exclude_name_patterns` fold case**, like every
  sibling restriction. Snowflake upper-cases every unquoted identifier, so a
  hand-written `^tmp_` matched nothing in a real estate -- the one
  restriction that could look right and silently do nothing.
- **An inline `token:` is resolved**, like an inline `password` or
  `private_key`. The config already listed `token` as a secret field, so one
  was accepted, validated and redacted, then ignored at connect time -- which
  reads to the operator as "the PAT is wrong".

### Fixed — the view reference the target could not resolve

- **Root cause of every failed view create on the live run.** Snowflake's
  `GET_DDL` writes a view body that references its own schema unqualified --
  `select ... from ORDERS`. The reference rewriter only ever matched
  three-part names, so `ORDERS` travelled verbatim into the target, where it
  means nothing. The AIDP catalog API answered `500 InternalError` with no
  detail, four times.
- Proven directly against the DataLake: a view whose body says `from orders`
  returns 500; the identical view with
  `from <catalog>.<schema>.orders` is created ACTIVE. So the transport was
  never the problem and neither was the SQL -- only the reference.
- A view's unqualified reference resolves in the VIEW'S OWN schema in
  Snowflake, so the target name is known rather than guessed. One- and
  two-part references to objects in that schema are now rewritten to the
  planned target (`R41`). Only for the view's own database and schema: a bare
  `ORDERS` in a view in SALES cannot mean `MARKETING.ORDERS`, and guessing
  across schemas is how a view silently reads the wrong table.
- A bare name is rewritten only where nothing but a table can appear --
  directly after `FROM` or `JOIN` -- so `select ORDERS from ...` stays the
  column it is. String literals and comments are masked as before.
- A reference that is still unqualified afterwards belongs to an object
  outside this migration, so there is no target name for it. It is named in
  a warning and in a new rule `R42_VIEW_REFS_UNRESOLVED`, because on the
  catalog-API transport it is an undetailed 500 later.

### Fixed — a diagnosis that generalised from the wrong kind of object

- The probe that tells a burned name from a bad request always created a
  TABLE. Live 2026-09-22, four `create view` calls failed on a catalog where
  every view create returned 500; the table probe landed, and its success was
  read as *"the schema and your request are both fine"* about the views. A
  table landing says nothing about whether a view can be created there.
- The probe is now of the same kind as the object that failed, the verdict is
  cached per schema **and** kind, and the probe record says which kind it
  was. `delete_view` exists so a view probe cleans up after itself.

### Added — a 4xx is taken at its word, a 5xx is investigated

- A client error carries the reason and the fix (`Invalid name: ... Only
  lower-case characters, numbers and underscores are allowed`), so it is
  reported as-is and costs no probe.
- A server error carries nothing, and the operator's next move turns on
  something it does not say: was THIS object rejected, or can this catalog
  not perform this operation at all? One probe per schema and kind answers
  it, and the report says which: *"this catalog CAN create a view, so the
  error is about this object"*, or *"this catalog cannot create a view
  through the catalog API at all -- nothing here will succeed on a retry or
  in a fresh schema; create the structure on AIDP compute instead"*.

### Fixed — a create the target refused, reported as a burned name

- Live 2026-09-22: AIDP answered one create with 400 `Invalid name` and four
  `create view` calls with 500 `InternalError`. Every one of those errors was
  recorded, and every one of those objects was then reported as *"the create
  returned 202 Accepted ... A NOVEL name in this schema was created
  successfully, so the schema and your request are both fine and this NAME IS
  BURNED ... Retry into a FRESH SCHEMA."*
- Three things wrong at once. The create did not return 202, it raised. The
  request was not fine, and the target had said so in the response for that
  object. And the advice sends the operator to build a new schema, where the
  400 and the 500 both happen again.
- A create the target refused now reports the target's own answer, is not
  called a burned name, does not enter `poisoned_names`, and runs no
  diagnosis probe -- the probe exists to tell a burned name from a bad
  request, and that question is already answered, so running it is a write
  to the customer's catalog for nothing.
- The burned-name inference is unchanged where it belongs: a create the
  target ACCEPTED, reported nothing about, and never materialised.

### Fixed — a target name the destination can never accept

- Live 2026-09-22: Snowflake's `"Mixed Case Table"` folded to the planned
  target `mixed case table`, DDL was generated for it, and the create was
  attempted. AIDP answered 400 `InvalidParameter: Invalid name: mixed case
  table. Only lower-case characters, numbers and underscores are allowed.`,
  and a view from `"V Quoted"` got `Should start with a letter, no spaces or
  special characters except for underscore`. The name is then burned for the
  rest of the run, which is the worst place to learn it.
- The planner now refuses such an object where every other impossible object
  is refused, with the destination's rule, the part that breaks it, and why
  the source was allowed to have it. **No name is rewritten**: `orders 2024`
  quietly becoming `orders_2024` is a different table to everything that
  reads it, so the choice stays with whoever owns the estate.
- This is a naming rule, not a case rule. Folding was already handled, and
  the identifier-case collision HALT is unchanged.

### Fixed — a NOT NULL gap reported against an object that was never created

- Live 2026-09-22: `v_customer_totals` failed to create and the run still
  reported its two `NOT NULL` columns as "created NULLABLE here". The gap is
  recorded before the create deliberately -- it is a property of the
  catalog-API transport rather than something discovered afterwards -- but it
  may not survive into the artifact for an object that does not exist. It is
  dropped for anything that ended up in `failed`.

### Fixed — an `--execute` that matched nothing and exited 0

- Live 2026-09-22: `deploy --execute --catalog snowmig_coverage_internal`,
  against a plan whose 11 objects target catalog `snowmig_coverage`, created
  nothing, recorded `executed: 0, errors: []`, printed the dry-run pre-flight
  and returned exit code 0. Every statement had been filtered out by the
  catalog-scope check, which is correct on its own and is how a multi-catalog
  plan is deployed one catalog at a time; what was missing is that *all* of
  them being filtered out means this run can only do nothing. A caller
  reading the exit code would have recorded a successful structural clone of
  11 objects that do not exist.
- The result now carries `matched_nothing`, the console prints
  `NOTHING MATCHED` naming both catalogs, `SOFT_CLONE_SUMMARY.md` leads with
  it instead of the ordinary "Not deployed in this run" note, and the stage
  exits 1. An empty plan and a partial match are each still exactly what they
  were: only *every* statement being out of scope is this case.

### Fixed — an attachment the account-wide view had not caught up to

- Live run, 2026-09-22: a masking policy, a row-access policy and two tags
  were attached to a seeded table and `SECURITY.md` reported *defined, but
  not seen attached*. `SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES` and
  `TAG_REFERENCES` lag up to ~2 hours and were still empty. The hedge was
  right, and the answer had been readable the whole time:
  `<db>.INFORMATION_SCHEMA.POLICY_REFERENCES` and
  `TAG_REFERENCES_ALL_COLUMNS` answer per object with no lag, and need no
  `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`. Both are now read, one round
  trip per in-scope object, and they are what the verdict is built from.
- The two sources are UNIONed, never substituted: the lag runs both ways, so
  the account view also keeps showing an attachment that was just removed.
  Every attachment says which source saw it, and one only the stale view has
  is kept and marked *confirm* rather than believed or dropped.
- A defined policy attached to nothing is now a read result instead of
  UNCONFIRMED, once every in-scope object has answered. Where the per-object
  read is denied, or the estate is over the 500-object budget for a read that
  costs a round trip each, the previous lagging verdict and its hedge stand
  unchanged and the report says which case it is.
- An object the per-object read could not reach is named in the statement, on
  the console and in `SECURITY.md`. Nothing above it is a verdict about that
  object (I3).
- A table-level tag comes back once per column from
  `TAG_REFERENCES_ALL_COLUMNS`. Four columns are one finding, on the table and
  not on a column.
- The policy read and the tag read fail independently, because a role can
  hold one and not the other. Counting them together let an all-denied tag
  read ride on a successful policy read and report itself as measured.
- The emulated estate answers both per-object functions for the object asked
  about, so `demo` teaches the same lesson; answering them account-wide
  inflated the demo's own exposure count sevenfold.

### Fixed — a clean security verdict on a question never asked

- `SECURITY.md` could state that no masking, row-access, aggregation or
  projection policy was attached while `SHOW AGGREGATION POLICIES` and
  `SHOW PROJECTION POLICIES` were never issued, and the two-hour
  `POLICY_REFERENCES` staleness tripwire summed only masking and row-access.
  All four kinds are enumerated, all four feed the tripwire, and the sentence
  is built from the kinds that actually answered; a refused `SHOW` reads
  "not visible to this role" and is named, never covered by a clean verdict.
- Tag attachments are read from `ACCOUNT_USAGE.TAG_REFERENCES`, with the same
  unreadable handling and latency caveat as policy references. A tag count
  with no attachment list was a number with no verdict.
- Grants are read for 22 object classes instead of three, so grants on a
  schema, database, warehouse, stage, procedure or function are visible. The
  report names the classes it asked for. Nothing is replayed on the target.
- The stage board flags a denied policy `SHOW` and unreadable tag attachments
  instead of reading clean. `ARCHITECTURE.md` I3 now states both halves of
  the rule and its third state.
- The demo estate answers every new read, with one alert, one outbound share
  and one tag attachment, so `demo` teaches the lessons the census can now
  teach.

## [0.25.0] — 2026-09-19

### Fixed — the data plane could not read the one config file it is given

`provision --source-config` uploads the migration config **verbatim**, and that
file nests the connection under `snowflake:`. The stage loader read the top
level for `account`, found only the two envelope keys, and every stage failed
on the cluster with *source config is missing required field(s): account, auth,
database, user, warehouse* — which reads like a dead credential rather than a
config one level too deep. The design is one file for both ends; the loader now
honours it.

- `load_source_config()` unwraps a `snowflake:` block when present, and still
  accepts a flat mapping (JSON remains the documented fallback for a cluster
  image without PyYAML).
- Regression test asserts the nested shape loads and that `aidp:` does not leak
  into the connection options.

### Fixed — the dry run advertised uploads that never happen

`provision` without `--execute` previewed the `.py` sources under
`engine/dataplane/` as the objects it would upload. Execute never uploads them:
it compiles a generated `.ipynb` per stage, because AIDP types a workspace
object by extension and a `.py` lands as a `FILE` that no job task can run. A
dry run may mislead about many things; what it will create is not one of them.

### Fixed — the runbook wrote backups into a folder nothing created

S6 backs the discovery manifest up before any later stage reads it, and S9
backs the full plan up before scope is reduced. Provisioning created
`scripts/`, `plan/` and `reports/` only, so the first backup had nowhere to
land. `BACKUP_FOLDER` is now created and read back with the others.

### Added — `ddl` halts on a type the TARGET refuses

The Hive metastore behind an AIDP catalog rejects `timestamp_ntz` at `CREATE
TABLE` — `InvalidObjectException: Invalid column type: timestamp_ntz` — even
though Delta and Spark 3.4+ support it and the statement runs on compute, not
through the catalog API. "Delta supports it" is therefore not sufficient; the
metastore is a second, stricter gate (B19).

Found the expensive way: five to six minutes of cluster startup, then a Java
traceback partway down a thousand-line log. `ddl` now checks the **emitted**
SQL against `TARGET_REJECTED_COLUMN_TYPES` — types live-verified as refused,
each paired with the remedy that cleared that refusal — and HALTs
with exit 3, naming the offending columns. `DDL_PLAN.md` leads with the halt
rather than footnoting it. The check is offline and runs in under a second.

Anchored on the backtick-quoted column declaration, so the mapper's own
`TIMESTAMP_NTZ -> TIMESTAMP` rule note is not a false positive. The remedy
states plainly that the downgrade changes timezone semantics — Spark
`TIMESTAMP` is an instant read through the session timezone, `TIMESTAMP_NTZ`
is wall-clock with none — so it stays a decision, not an inheritance.

### Fixed — a second run was started while one was in flight

A job with `maxConcurrentRuns: 1` ACCEPTS a second run and then discards it:
created, ended the same millisecond, no task output. Meanwhile the console
streams the OLD run's log, so a freshly deployed fix looks like it never
took — which is exactly how a corrected notebook appeared not to apply.
`watch_job` now refuses to start a run while one is in flight, names the run
holding the slot and how to cancel it. It is a guard, not a gate: a transport
that cannot list runs never blocks a migration. Adds the `list_job_runs`
operation, including the live quirk that `sortBy` is mandatory
(`400 WORKFLOW_0007 ... Invalid SortBy: null`) and that `startTime` is
rejected as a sort key while `timeCreated` works.

### Fixed — S10 shipped a default pair that could not work

`01_create_structure` shipped `source-mode: connector` beside `mode: manifest`
in its PARAMS cell. Those two cannot be combined: a manifest built in
connector mode records SNOWFLAKE types, and Delta rejects them verbatim. A
first, unmodified S10 run therefore refused **every** table with *"this
manifest carries SNOWFLAKE types ... use --mode ddl-plan"*. The default is now
`ddl-plan`, which matches the stage's own argparse default and runbook S10
("reading the approved plan"). The stage refusing rather than creating
mistyped tables is the behaviour working; the default that guaranteed the
refusal is the bug.

### Fixed — `run --param` was accepted and silently discarded

AIDP job parameters reach a notebook as neither argv nor environment. `snowmig
run --param schema=SALES` was still handed to the run API, which dropped it,
and the stage executed whatever its PARAMS cell already held — so a scope flag
read as applied while doing nothing. It is now refused before the transport is
even built, naming the two places a stage parameter is actually read
(`provision --execute --reuse-existing`, or the notebook's PARAMS cell). The
runbook's S10 example, which used the flag, was corrected with it.

### Added — preflight catches the empty session schema

`schema:` is not a discovery filter; it is the real schema the AIDP connector
needs to open a pushdown session, and the connector resolves it by **fetching
its relations**. A schema that exists but holds nothing comes back as
`DATA_ACCESS_LAYER_0031 - Schema: X not found`, which reads as missing rather
than empty — minutes into a job run. `PUBLIC` is the usual default and the
usual casualty, so it is no longer the value in the example config.

Preflight now fails on it in about a second and **names schemas from the
account in front of it** that would work. The suggestion is derived, never
shipped: a generic migrator cannot know which schema is populated in someone
else's estate.

### Changed — claims that this session disproved

- The target INTERNAL catalog **is** created by this plugin, as a container.
  The README said it was not, the runbook and CLI said it was, and a surface
  test pinned the wrong one. The invariant that actually matters — the
  container is not the structure, because a control-plane table create can
  return `202 Accepted` and create nothing — is what the test now pins.
- The notebook upload-and-run path is **live-verified**: four `.ipynb`
  uploaded as `NOTEBOOK` objects and `snowmig_00_discover` ran to SUCCESS,
  reading 1065 relations and 9935 columns in two `INFORMATION_SCHEMA`
  queries. `01_create_structure` has now executed on a cluster too — its
  first run is what surfaced the `manifest`/`ddl-plan` default above. The
  copy and reconcile stages remain unexecuted.
- The EXTERNAL catalog's connection **values** are proven — the same
  credentials read the estate from the cluster. The catalog **crawler** is
  what remains unproven (`testConnection` returns `PENDING`).
- Deleting a workspace object needs `oci raw-request` with the path
  percent-encoded as a single segment; `aidp workspace-object delete` leaves
  the slashes unencoded and returns `NotAuthorizedOrNotFound`.
- Stale `.py` references to AIDP-side artifacts corrected to `.ipynb` across
  the runbook and architecture docs: everything that runs on AIDP is a
  notebook, and the `.py` are local canonical sources only.
- Broken links to the removed customer-engagement docs dropped from the
  README and the MVP-1 design spec.

## [0.24.0] — 2026-09-19

### Changed — one artifact directory, and it explains itself

The plugin used to leave three things behind: an unexplained `snowmig_out/`
beside the plugin, a virtualenv in the operator's home, and whatever scratch
a stage felt like writing. Two of those are gone outright. The third was a
presentation problem, not a design one: the stages chain — `plan` reads the
`inventory.json` that `assess` wrote — so the directory has to persist, and
a bare folder of JSON with a cryptic name reads as a bug rather than as
output.

- **`snowmig_out/` → `migration-artifacts/`.** The name says what it holds.
- **It documents itself.** A generated `README.md` explains what each file
  is, that the `.json` is the stage hand-off and the `.md` the human
  deliverable, that everything is regenerable, and that it is safe to
  delete.
- **Gitignored permanently** — in the plugin `.gitignore` under a
  do-not-remove banner, *and* by a `.gitignore` of its own so it stays
  ignored if copied elsewhere. It names a real estate's databases, schemas,
  tables and columns; that must never reach a public samples repo. The old
  `snowmig_out/` rule is kept so an older working tree cannot commit one.
- **`snowmig clean`** removes it, and refuses to touch an `--out-dir` the
  operator named: deleting a chosen path would be data loss wearing a
  tidy-up costume.
- **`bin/snowmig` persists nothing.** It runs on the current interpreter when
  that already imports the dependencies — creating nothing at all — and
  otherwise builds a venv in a temp directory removed by an `EXIT` trap, on
  success, failure and interrupt. The engine runs as a child rather than via
  `exec`, because `exec` would discard the trap and strand the directory.
- **`bin/snowmig-test`** runs the suite under the same rule.
- Every skill, command and README example now invokes
  `${CLAUDE_PLUGIN_ROOT}/bin/snowmig` and passes no `--out-dir`.


Four changes found by running the runbook end to end against a live account.
Three of them are things the documentation claimed already worked.

### Fixed — `STANDARD` was never a real catalog type

`CATALOG_TYPES` was `("EXTERNAL", "STANDARD")`. AIDP answers
`400 InvalidParameter: Invalid CatalogType: STANDARD`. Reading the
`catalogType` of every catalog on a live DataLake shows exactly two values:
**`INTERNAL`** and **`EXTERNAL`**.

- `CATALOG_TYPES` corrected, and `normalize_catalog_type()` added so the
  runbook's word `STANDARD` keeps working as an alias and is translated
  once — it can no longer reach the wire.
- `catalog_provision.ensure_catalog` refused STANDARD outright while
  `GAPS.md` claimed the refusal had been lifted and the CLI advertised the
  option. It is now a gate, not a wall: it creates the CONTAINER and returns
  `container_only: true`. The refusal that matters — tables never through the
  control-plane CRUD API — still stands.
- Three report defects: the dry-run renderer hardcoded "EXTERNAL" regardless
  of the requested type (so a `--catalog-type standard` dry run claimed it
  would send `SNOWFLAKE_PASSWORD`); the "read-only pointer" paragraph printed
  for managed catalogs; `(source type SNOWFLAKE)` printed beside a catalog
  that has no source.

### Changed — the environment is created BEFORE the catalogs

`resolve_target()` requires all four coordinates for any AIDP write, so
registering the source catalog first — as the runbook said — could not run:
it stopped on `AIDP target coordinates not supplied: cluster_id, workspace`.

The order is now **S1 workspace → S2 cluster → S3 EXTERNAL source → S4
INTERNAL target**. S5–S12 keep their numbers. `resolve_target()` grew an
opt-in `require=` so a read that genuinely needs fewer coordinates — listing
catalogs needs no catalog — does not have to invent one.

S2 also documents the race that costs a `409`: a workspace reports `ACTIVE`
seconds after its POST returns, and a cluster created inside that window is
rejected. Resuming with `--reuse-existing` against the workspace this
migration just created is not the reuse the rule forbids.

### Changed — the data plane is notebooks, and only notebooks

AIDP types a workspace object by its **extension**: a `.py` uploaded with
`--type NOTEBOOK` is stored as a `FILE`, and only `.ipynb` becomes a
`NOTEBOOK`. A job task is a `NOTEBOOK_TASK`, so a `.py` on the workspace can
never be a job target.

- The four stages ship as `.ipynb` under `data-migration-scripts/`. The
  canonical Python moved to `engine/dataplane/`.
- **The driver wrapper is gone.** Each stage is one self-contained notebook —
  PARAMS cell, inlined helpers, stage logic — and the job points straight at
  it, so the code a user opens in the console is the code that runs. Nothing
  is imported off the `/Workspace` mount.
- `snowmig build-notebooks` regenerates them from the sources. Generated so
  the shared helpers cannot drift across copies; committed so what ships is
  reviewable. Two guards refuse to emit a broken notebook: one if the shared
  import is not stripped, one if the `__main__` guard survives — that
  `SystemExit` is what once made a *successful* discovery report as FAILED.

### Added — the lookups that used to be improvised

Driving a migration by hand meant hand-writing SQL and REST calls that left
no artifact. Each is now a stage:

- `snowmig databases` — the source databases a role can see, flagging the
  system DB, shares and personal DBs as not migratable. This is the S3 input:
  one database becomes one catalog, so the user picks one.
- `snowmig catalogs` — every catalog on the DataLake with the type the
  **server** reports. This is the stage that showed `STANDARD` to be fiction.

### Added — `bin/snowmig`, so there is no interpreter path to remember

Every stage needs a venv, and leaving that to the operator produced a
hand-typed path that differed per machine and turned doc examples into
fiction. `bin/snowmig` bootstraps on first use and dispatches.

The venv stays **outside** the plugin folder (`~/.local/share/snowmig/venv`,
override with `SNOWMIG_VENV`) because the installer copies the plugin
directory on every version bump. The launcher decides whether it is usable by
importing the dependencies rather than by the directory existing, so a venv
left half-built by an interrupted install is rebuilt instead of failing later.

### Documented — discover through the WORKFLOW, not three-part names

Discovery in `connector` mode costs two `INFORMATION_SCHEMA` queries for the
whole database (measured: 1065 relations, 9935 columns in one run). Walking
three-part names against the EXTERNAL catalog costs a `DESCRIBE` per object,
so it scales with object count and does not finish at estate scale, and it
needs the catalog crawl to have completed first.

This is now stated as decided rather than as a default — in rule 2, in S6,
in the data-plane README and in the discovery notebook itself — so an agent
does not spend time rediscovering it.

### Fixed — `pytest` from the plugin root

The plugin sits in a shared samples monorepo, and collection from the repo
root fails on other samples' `test_*.py` scripts (one calls `sys.exit(1)` at
import, taking pytest down with an `INTERNALERROR`). A plugin-root
`pytest.ini` scopes collection to `engine/tests`. Scoped, not deleted: those
samples belong to other people.

## [0.23.0] — 2026-09-18

### Fixed — a public sample carried a customer's name

51 occurrences across 17 files. A reusable sample names no customer.

- Every fixture, doc and corpus reference genericised to `Acme`; the corpus
  file is now `00_acme_setup.sql` generating `ACME_ORDER_360_VW`.
- The two customer-confidential engagement records (customer context, delivery
  timetable) moved out of the repo entirely. `.gitignore` blocks
  `*-CONTEXT.md` and `*-TIMETABLE.md` so they cannot return by accident.
- `CLEANUP-BEFORE-PUBLISH.md` item 1 is now DONE in the working tree. It
  remains OUTSTANDING in git history on the remote, which a working-tree
  cleanup cannot close — the checklist says so, and says it without naming
  the customer.

## [0.22.0] — 2026-09-18

### Fixed — the plugin wrote outside its own folder

Every skill documented `--out-dir ./snowmig_out`, which resolves against
**the user's** working directory. Run from inside any repo -- the normal
case -- the plugin dropped artifacts into somebody else's project, and
`snowmig_out/` carries a real estate's schema, table and column names. 19
occurrences across 10 skills now use `${CLAUDE_PLUGIN_ROOT}/snowmig_out`, so
output lands in the plugin's own folder and nowhere else. This repo gives
each sample one folder and everything related to it lives there.

- `snowmig_out/` and `snowmig_demo/` added to the plugin `.gitignore`: written
  inside the plugin folder, never committed.
- `snowflake-migrator-bootstrap` said to create a venv and never said where.
  It now says **outside the repo** (`~/.venvs/...`), never beside the plugin:
  the installer copies the plugin folder on every version bump, so a venv or a
  credential living there is duplicated per version.
- `snowflake-migrator-overview` carries the rule: never create anything
  outside the plugin folder -- no output directory, no virtualenv, no scratch
  file.

## [0.21.0] — 2026-09-18

The migration is now a **fixed twelve-step sequence**, not a menu of stages,
and the data plane is stated as running inside AIDP as workflows.

### Changed — behaviour

- **Nothing is reused.** `provision` created its environment by "look first,
  create if absent", silently adopting a workspace, cluster or job that
  already carried the name. It now **stops** and reports `name_taken`: a
  migration creates its own environment so its blast radius is knowable and
  it can be torn down as a unit. `--reuse-existing` opts back in and is never
  what gets offered first.
- **A STANDARD catalog can be created.** The CLI refused `--catalog-type
  standard` outright. It now creates the catalog **container** — one
  control-plane object — and says so. The refusal that remains is the correct
  one: its *tables* are not created through the catalog CRUD API, because a
  table create there can return `202 Accepted` and create nothing.

### Added

- **`snowmig.py ingest`** — the bridge the runbook needed: the in-AIDP
  `discovery_manifest.json` (S6) into the `inventory.json` the planning
  stages read (S7). It calls the **same** `map_type` as a live `assess`, on
  the same four raw INFORMATION_SCHEMA fields, so a column planned from a
  manifest and the same column planned live reach identical verdicts — two
  mappers that agree today diverge after the first change to either.
  `00_discover_snowflake.py` now carries those raw fields through instead of
  discarding them behind a formatted type string. Views arrive without SQL
  and are marked untranslatable rather than passed off as fine, and
  `dependencies.json` is written empty with provenance `not_extracted`.
- **`snowmig.py run`** — run an AIDP job as a workflow, poll it to a terminal
  state, and write `RUN_*.md` with the task output as evidence.
  `target/jobs.py` already carried a live-verified `watch_job`; no CLI stage
  reached it, so the in-AIDP data plane could be provisioned but not driven.
  A spent poll budget is reported as **STILL RUNNING**, never rounded to a
  verdict.

### Changed — skills

- `snowflake-migrator-overview` rewritten as the twelve-step runbook: S1
  EXTERNAL catalog, S2 workspace, S3 STANDARD catalog, S4 cluster, S5 scripts,
  S6 discovery as a workflow, S7 script-generated plan, S8 grouped conflict
  review, S9 summary + scope gate, S10 asset creation, S11 per-schema copy
  scripts, S12 warehouse-equivalent clusters. Adds the never-reuse rule, the
  workflow-not-notebook rule, "change inputs, not scripts", and scale as the
  design constraint.
- `snowflake-assess-estate` reframed as a laptop-side **preview**, explicitly
  **not** the migration's discovery step — a migration discovers inside AIDP
  at S6, because a laptop read leaves no log or evidence on the platform.
- `snowflake-medallion-clone` now covers S1 and S3 as two distinct steps, and
  states that an EXTERNAL catalog registers the **whole database** so a
  plan-level restriction does not narrow it.
- `snowflake-provision-environment` carries the never-reuse rule and the
  workflow framing.

### Known gaps

`GAPS.md` P0 ranks what the runbook specifies and the code does not yet wire —
chiefly that S7's planner reads the operator-side `inventory.json` and has no
bridge from the in-AIDP `discovery_manifest.json` that S6 produces.

## [0.19.0] — 2026-09-18

The usability release: **one config file for both ends**, and a run that works
from anywhere on the machine. No new stages; what changed is how a migration is
set up, and how much of it a person has to remember. 1055 offline tests
(was 1032).

### One file, both ends

- **`snowmig-config.yaml` is now the single place coordinates live** — the
  Snowflake connection under `snowflake:` and the AIDP destination under
  `aidp:`. `engine/migration_config.py` parses it and does nothing else: no
  network, no environment, no cache. Every stage resolves flags first, then the
  file, so a flag still wins for a one-off override.
- **The secret goes in that file, inline.** `password:` or `private_key:`
  alongside `key_path:` for a PEM. This is a deliberate trade — an engineer
  with full access to both environments was being made to juggle three files —
  so the protections that remain are explicit: the file is gitignored, created
  `0600`, and `redact()` is the only path by which this plugin renders a
  config. Setting both an inline secret and its `*_path` is refused rather
  than ranked; the wrong guess is an auth failure nobody can explain.
- **`init-config`** writes the template where the operator is working and
  refuses to overwrite an existing config, because the file it would clobber is
  the one holding live credentials.
- **Discovery, announced.** With no `--config`, the CLI looks at
  `./snowmig-config.yaml` then beside the plugin, and prints `config: <path>`
  plus `destination from the config file: …` before acting. The old separate
  `snowflake-catalog-connection.yaml` is gone.
- **`preflight`** reads the whole config back field by field with what each one
  is *for*, secrets masked, then tests whichever ends were configured. A
  skipped end is reported as skipped — never as a pass.

### Behaviour corrected

- **Invariant I5 restated to match the code.** It used to claim target
  coordinates were "never stored, no config file". They may now come from the
  config, so the real guarantee is stated instead: never *silently* — the
  destination is printed before anything acts on it, `target/coords.py` still
  performs no I/O of its own, and writing still needs `--execute` plus a
  confirmation in that turn. `ASSUMPTIONS.md` B1, the README invariant table
  and overview rules 3–4 were saying the superseded thing.
- **A failed source connection is explained, not tracebacked.** The driver
  reports a mistyped `account` as `404 Not Found: post
  <account>.snowflakecomputing.com/session/v1/login-request` — the single most
  common first-run mistake, arriving as a Python traceback that sends people
  hunting for a bug in this tool. Driver errors are now re-raised naming the
  likely field and where to fix it, with the credential never in the message.
- **A new config is created `0600`**, opened closed and then filled, since a
  `chmod` after the write leaves a window where a plain-text password is
  world-readable.

### The `aidp` CLI backend was guessed, and it was the default

Found while proving the one-file flow end to end on a machine with both CLIs
installed. **Live-verified 2026-09-18** against the real DataLake:

- `aidp catalog list` takes **`--instance-id`**, not `--datalake-id`, and
  **has no `--output` flag at all** — this module used both, at thirteen call
  sites. It also needs `--auth api_key` (the CLI default is
  `security_token`) and an explicit `--region`, which are now appended once in
  `build_command` rather than at every call site.
- The CLI prefixes its JSON with a literal **`Response:`** line, so every
  successful call parsed as *"backend output is not JSON"*.
- **`detect_backend` preferred `aidp_cli`.** So on any machine with `aidp`
  installed — the documented setup — every AIDP-side check took the path
  built from those guesses. It now prefers `oci_raw`, the documented REST
  surface the whole control plane was actually verified against; the CLI
  remains the fallback and is still the verified transport for workspace files
  in `provision_api.py`, which had the flags right all along.

Both backends now return `destination catalog | PASS | gold is EXTERNAL`
against the live DataLake.

### One file, actually one file

The 0.19.0 work above left three places still behaving as though a second
credential file existed. All three were on the main path, and the first two
were fatal:

- **`catalog` ignored a discovered config.** It built the connection only
  `if args.config`, so registering the EXTERNAL catalog "from the config file"
  silently did nothing unless you repeated `--config`; without the flag it
  crashed on `args.connection_config`, an attribute that stopped existing when
  the flag was renamed. It now discovers the config like every other stage.
- **The catalog builder REFUSED a config carrying `schema:`.** That made one
  file for both ends impossible: the source side needs a real schema to scope
  the connector's pushdown session, and the live catalog contract has no schema
  property at all. It is now accepted and left out of the request body — which
  is validated key by key, so nothing may be smuggled in — and the omission is
  printed (`an EXTERNAL catalog registers the whole database`).
- **`preflight` called an inline secret "an unrecognised key" and said inline
  was "not accepted".** It is the documented default. An inline credential now
  reports as *present, inline*, is excluded from the unknown-key warning, and
  is still never rendered. The "no credential" warning now means exactly that:
  neither inline nor a path.
- **The second config file is gone, template and all.** A tracked
  `local-test-account.example.yaml` still shipped inside the plugin, and the
  live tests plus the corpus validator read `local-test-account.yaml` for one
  value (the database name). They now read the same `snowmig-config.yaml`
  every stage reads. The old names stay in `.gitignore` purely so a stale copy
  on someone's disk cannot be committed; nothing reads them.
- Template, README and the bootstrap skill now lead with the PEM pasted inline
  under `private_key: |`. `key_path:`/`password_path:` still work and are
  mentioned as the alternative they are, rather than the first suggestion.
- A `.claude-plugin/marketplace.json` was added, matching the sibling plugins'
  convention, so the migrator can be installed (`claude plugin marketplace add
  <path>`) instead of only read from a checkout. Without it, a fresh
  conversation could not see the skills at all — and an agent that cannot find
  the engine is an agent that improvises, which is the whole thing Rule 0
  forbids.

### Never do by hand what the engine does

- **Rule 0 in the overview skill:** *the engine is the method*. Do not
  re-implement a stage, do not translate SQL or types yourself, do not create
  AIDP objects by hand. **If the engine or the scripts cannot be found, STOP
  and say so** — a missing tool is never a licence to improvise, because a
  hand-made migration is exactly the unauditable thing this plugin replaced.
- **Bootstrap no longer assumes a repo checkout.** Every path is built from
  `${CLAUDE_PLUGIN_ROOT}`, the user is never asked to `cd`, and the config
  belongs in the working directory precisely because an installed plugin's own
  directory may be read-only. The four stage skills now show the
  config-driven invocation, with the flags as the override they are.

## [0.18.0] — 2026-09-16

The live-validation release. A real Snowflake trial (1002 objects) and a real
AIDP DataLake were driven end to end, and the API taught us several contracts
one error at a time. 1032 offline tests (was 974).

### Live-verified, and corrected where it had been guessed

- **The EXTERNAL/SNOWFLAKE catalog contract, enumerated by the API itself.**
  The map nests under `connectionDetails.connectionProperties`; the keys are
  `SNOWFLAKE_HOST/PORT/USERNAME/PASSWORD/DATABASE_NAME/WAREHOUSE/ROLE/`
  `AUTHENTICATION_METHOD/PRIVATE_KEY_FILE/PRIVATE_KEY_CONTENT/`
  `PRIVATE_KEY_PASSPHRASE` plus `WORKSPACE_KEY/WORKSPACE_NAME`; the auth enum
  is `Basic | KeyPair`; there is no PAT and no schema property. A catalog was
  registered from it and read back. `catalog --test-connection` is now a wired
  command — and it needs an EXISTING catalog key (RBAC `DESCCATALOG`).
- **Jobs.** A task needs `runIf` and its own `cluster: {clusterKey}`; a
  NOTEBOOK_TASK needs `notebookPath` + `source: WORKSPACE` and RUNS.
  PYTHON_TASK is accepted at creation and fails every run resolving the file,
  and job `parameters` reach a notebook neither as argv nor as environment —
  so each job now runs a **generated driver notebook** with its arguments
  inline, which also turns the script's `sys.exit` code into the job verdict
  (a notebook reports `SystemExit` as an error, so a fully successful
  discovery had been coming back FAILED).
- **Workspace files** go through `workspace-object` (relative paths, via the
  `aidp` CLI), not the Jupyter contents API — which 200s on PUT and then
  404/500s on read-back, and cannot create directories.
- **`/Workspace`** is the mount of the workspace tree on cluster filesystems.

### Added

- **`snowmig.py preflight`** — reads the connection config back to the user
  field by field, with the credential **paths** (never contents), then tests
  both ends. A skipped end reads as skipped, never as a pass. Wired into the
  bootstrap skill, which now walks the table with the user before anything
  runs, and onto the stage board as an optional first stage.
- **`--source-mode connector`, now the default for the in-AIDP scripts.** The
  AIDP Snowflake connector reads the source from the cluster (verified: a
  table read and a pushdown, no extra library), so the data path does not
  depend on the external catalog's crawler — which on this deployment fails
  `CONNECTOR_0067, Login has timed out` with the very credentials the
  connector accepts (GAPS 13a; not an FQDN form — both host forms resolve to
  the same backend and both fail).
- **Discovery that scales**: two `INFORMATION_SCHEMA` pushdown queries cover
  the whole database — live, 1065 relations and 9935 columns — instead of a
  `DESCRIBE` per object.
- **`01_create_structure.py --mode ddl-plan`** (new default): applies the
  engine's already-translated types from the approved `ddl_plan.json`, with no
  source read, and reports a table absent from the plan as `not_in_plan`
  rather than creating something nobody approved. `ctas` costs one Snowflake
  round trip per table; `manifest` now refuses a connector-mode manifest
  instead of feeding Snowflake types to Delta.
- **`data-migration-scripts/diagnose_environment.ipynb`** — the diagnosis
  notebook the campaign turned out to need: mount, egress, config echo,
  connector, and whether the external catalog is actually populated.
- **`engine/target/jobs.py`** — local instrumentation for in-AIDP work: run,
  poll to a terminal state, and fetch a task run's output (two calls, and
  `sortBy` is required). A budget that runs out reports "still running",
  never a verdict.
- `snowmig_source.py`, shared by the in-AIDP scripts, carrying the verified
  connector option names and the session-schema rule.

### Fixed

- **Resumability was keyed by source schema alone**, so pointing a re-run at a
  different target schema skipped every create as "already created" — found
  live, against an empty target. Both the structure and copy reports now
  refuse a prior record from a different target (and keep it aside).
- Script refusals print on **stdout as well as stderr**: a notebook task
  captures stdout only, so an exit-1 with a stderr-only message produced a
  job failure with no explanation anywhere.
- `Decimal` from Snowflake broke the manifest write **after** a successful
  1065-relation read; integral values now keep their exactness as ints.
- `executor.py`'s `create_catalog` nesting, and `main()` now also catches the
  executor's `BackendError`, so a live 400 prints a message rather than a
  traceback.

### Fixed after a review of this release

Found by an adversarial review of the change, most of it in the new in-AIDP
scripts — which until now had **no test coverage at all**, and now have 21
tests:

- **`fail()` recursed into itself** in all four scripts: every refusal path
  would have produced a RecursionError instead of one message and exit 1.
  Introduced by the very refactor that made refusals visible on stdout.
- **A table with no target crashed the whole run.** Live, the copy died on
  the sixth table of a schema because the approved plan covered five and the
  manifest listed a thousand. A missing target is now recorded as
  `target_missing` and skipped, and the copy's default scope is what the
  structure step actually created for THIS target — not the whole manifest.
- **A cluster that never becomes visible now halts**, like the workspace
  already did; it used to fall through and bake the display name into four
  job bodies as a `clusterKey`, producing "created" jobs bound to nothing.
- **The driver-notebook upload is read back** like every other upload, per
  the module's own "a 2xx is not the claim" rule.
- **A failed library install no longer loses the run's record**, and a
  transport failure raises a named error the CLI reports with the partial
  result intact rather than a traceback.
- `preflight` reads the documented `key_passphrase_path` instead of ignoring
  it; `--test-connection` uses the catalog KEY and is skipped (with a reason)
  on a dry run, since the API resolves RBAC on an existing catalog.
- Table names are escaped in the batched count query, so one apostrophe
  cannot break a 50-table chunk.
- `CATALOG.md`'s dry run lists the connection property NAMES, which the
  command already promised and the renderer did not do.
- **`03_reconcile` no longer exits non-zero on "not migrated yet".** A
  migration runs schema by schema, so most of the estate is un-attempted for
  most of the project; failing on that is how a real signal gets ignored.
  Only `MISSING_DESPITE_REPORT`, `STRUCTURE_ONLY_COPY_FAILED` and
  `TARGET_UNREADABLE` are problems, and the report leads with that count.

### Added, after the campaign

- **`README.md` now carries the runbook**: "How to run a migration, from
  zero" — prerequisites, the one config file, and every stage in order
  through the four in-AIDP jobs, with the two things it must not let slide
  (the target INTERNAL catalog is not created by this plugin, and a cutover
  needs frozen writers or a point-in-time clone). Pinned by tests, pointed at
  from the router skill and from `MIGRATION-ARCHITECTURE.md`. The stale
  "Quick start" that stopped at `deploy` is gone.
- **One AIDP compute cluster per Snowflake warehouse**, same name, AIDP
  default config (`provision --warehouse-clusters`, names read from
  `warehouses.json` so they come from the account rather than from memory).
  The warehouse SIZE is reported and deliberately not translated — a
  Snowflake size is not a Spark shape. A failure on one cluster is recorded
  and the rest continue; the migration's own jobs are never rebound.
- The cluster body is now the live-verified one, learned from three separate
  400s: `driverConfig` **and** `workerConfig` (shape + min/max workers), an
  AIDP compute shape (`amd.generic` — an OCI VM shape is rejected), and
  `clusterRuntimeConfig.sparkVersion`.

### Live-verified in this release

Workspace creation (named after the source account, with the name
translation reported) and cluster creation — both had previously only ever
reused existing objects. A full `provision --execute` into a fresh workspace
completed 22 steps with **0 failures**: workspace, migration cluster, three
warehouse clusters, 9 uploads, 4 driver notebooks, 4 jobs.

### Known-bad, newly

- `snowmig.py notebook --upload` still points at the Jupyter contents API and
  `notebookRuns`; both are now known to be the wrong surfaces (GAPS 13).

## [0.17.0] — 2026-09-16

The automation release: a dev mode that emulates the whole pipeline, the
provisioning of the AIDP migration environment, and the schema-by-schema data
plane that runs inside AIDP. 974 offline tests (was 932).

### Added

- **Dev mode** — `snowmig.py demo` / `/snowflake-demo`
  (`snowflake-migrator-demo` skill): the entire pipeline against a built-in
  emulated Snowflake estate (`engine/emulation/`) and an emulated AIDP that
  reproduces the live-learned behaviours (identifier folding, fieldless
  lists, 202-and-vanish creates, poisoned names, derived view types,
  read-only EXTERNAL). Production code over fake transports; writes every
  real artifact plus a narrated `DEMO.md`, all unmistakably marked emulated.
- **`snowmig.py provision`** / `/snowflake-provision`
  (`snowflake-provision-environment` skill): ensures the workspace — its name
  translated by the new `target/naming.py` to the simplest safe charset
  (lowercase ASCII, starts with a letter, collision-safe truncation, every
  change reported) so a name the API might reject never reaches it — the
  `migration_assets` cluster, cluster libraries from `requirements-aidp.txt`
  (PyPI/Maven via the documented libraries PATCH, restart included), the
  workspace folder `backup-snowflake-migration/` carrying the data-migration
  scripts and the plan artifacts, and four parametrised jobs. Look-first,
  poll-the-read-back, per-step `{action, verified}`; dry run by default. The
  stage board gains `provision` (optional position) and now claims three
  writers.
- **`data-migration-scripts/`** — four self-contained, parametrisable PySpark
  scripts that run INSIDE AIDP against the external catalog, schema by
  schema: `00_discover` (resumable `discovery_manifest.json`),
  `01_create_structure` (CTAS `WHERE 1=0` or manifest types; never drops),
  `02_copy_schema` (INSERT-SELECT with row-count and exact-decimal-sum
  verification; skip-existing/append/overwrite; resumable),
  `03_reconcile` (plan vs the live catalog → `MIGRATION_REPORT.md`). Plus
  `requirements-aidp.txt` for the fallback connector paths — empty on the
  default path, because the external catalog needs no extra library.
- **`target/provision_api.py`** — pure builders on the DOCUMENTED
  `/20260430/aiDataPlatforms` REST contract (workspaces, clusters, libraries,
  contents upload, jobs/jobRuns, asyncOperations, testConnection). oci_raw
  only; the `aidp` CLI flags are deliberately unmapped until live validation.
- **`MIGRATION-ARCHITECTURE.md`** — the migration design record: the
  Snowflake→AIDP mapping table, dev vs prod mode, the 8-step prod flow, what
  is deterministic vs AI, and the honest limits.

### Changed

- The `ddl` stage's logic moved to `target.ddl.build_ddl_payload`, shared by
  the CLI and the demo so they cannot drift.
- Official-doc findings folded into the register (ASSUMPTIONS B11–B13, GAPS
  13/13a): the documented API family is `/20260430/aiDataPlatforms` with an
  `aidp-async-operation-key` waiter; `catalogType` is `EXTERNAL|INTERNAL`;
  there is **no SQL REST endpoint** and **no `notebookRuns`** (programmatic
  execution is a Job with a NOTEBOOK_TASK); the `aidp` CLI installs via
  `pip install aidp-python-client aidp-cli`. The live-verified legacy
  `dataLakes` transport stays for the structure clone until a live run proves
  the documented family.

## [0.16.1] — 2026-09-16

The 0.16.0 pivot made EXTERNAL the default without re-running the plugin's own
verification discipline across the surfaces that assumed a Standard target.
This release wires the default path through and stops the docs contradicting
the code. 932 offline tests (was 910).

### Fixed

- **`deploy --execute` now refuses an EXTERNAL catalog.** The target's
  `catalogType` is resolved before the first create; EXTERNAL, an absent
  catalog, and an unreadable listing are all refused — managed Delta cannot
  live in a read-only pointer, and the creates would 202-and-vanish. The
  resolved type is recorded in `deploy_result.json`. Also fixed:
  `catalog_deploy.RefusedToExecute` was missing from `main()`'s except tuple,
  so the refusal would have surfaced as a traceback.
- **`smoke --write-probe` is catalog-type aware.** Against an EXTERNAL catalog
  the probe is skipped with a note — read-only by design is not a FAIL — and
  `catalog_type` is reported either way (`"unknown"` when unresolvable). This
  was the same failure class the smoke test was rebuilt to prevent: a FAIL
  against a destination that works, on the default path.
- **The stage board knows `catalog`.** It sits between `smoke` and `deploy`,
  reads `catalog_result.json` (pending registrations are flagged, not rounded
  up), and the header claims two writers, not one. `data-options` moved to its
  real position feeding `plan`, marked optional so `next_stage` never stalls
  on it.
- **The catalog credential no longer travels in argv.** The `create_catalog`
  body is spooled to a 0600 temp file (`file://`, removed after the call), and
  the printed command redacts `connectionDetails` values explicitly — the old
  200-char truncation only hid the secret by coincidence of field order.
- **`pyyaml` is a runtime dependency** (`requirements.txt`); a clean install
  no longer ImportErrors on the documented EXTERNAL-catalog happy path.
- **Ten tracked `.pyc` files removed**, including one for a module that no
  longer exists, and the `.gitignore` re-include that resurrected
  `engine/target/__pycache__` is counter-ruled.
- **Docs stopped contradicting the code**: ASSUMPTIONS.md states the
  live-verified/never-executed split (and the test that enforced the stale
  "Never contacted" literal now pins the split); the router lists all 13
  stages; `--write-probe` help describes the cleanup the code performs.

### Added

- `/snowflake-catalog` — a thin command for the default registration stage.
- Tests: the EXTERNAL/absent/unreadable deploy refusals, the catalog-aware
  smoke probe, the board's `catalog` row and optional-stage skip, and the
  secret-redaction/spool-file lifecycle.

## [0.16.0] — 2026-09-11

### Changed

- **The target catalog is now EXTERNAL with source type SNOWFLAKE by default.**
  A new `snowmig.py catalog` stage registers it: a read-only pointer at the live
  Snowflake source that creates no tables and copies no bytes, so there is
  nothing to keep in sync. Previously the plugin assumed a pre-existing Standard
  (INTERNAL) catalog and cloned structure into it, which meant a migration
  produced managed storage nobody had asked for.
- **Internal and Standard catalogs are no longer created unless the user
  explicitly asks for one.** `snowmig.py catalog --catalog-type standard` is
  refused and says where the Standard path is; the router, the medallion-clone
  skill and `/snowflake-soft-clone` all state EXTERNAL as the default and
  require an explicit request before naming a Standard catalog.
- **A requested Standard catalog gets its tables from a script run inside AIDP
  compute.** The `notebook` stage already writes that script to the workspace
  `Shared/` directory, where it can be re-run and debugged independently; it is
  now the documented path for Standard catalogs, following the established
  schemas → tables → views pattern with per-object progress and individual
  verification. Spark reports a real error on the cluster, where the
  control-plane CRUD API returns 202 Accepted and can silently create nothing.

### Added

- `snowflake-catalog-connection.example.yaml` and a YAML/JSON connection-config
  loader. The Snowflake account, warehouse, database, user and credential for an
  EXTERNAL catalog are read from a config **file** — never inline arguments or
  the environment — and every credential inside it is a path read at call time,
  so the config itself carries no secret.
- `create_catalog` and `list_catalogs` on both execution backends.

### Known limitation

- The `connectionDetails` field names for an EXTERNAL/SNOWFLAKE catalog are
  inferred from the AIDP Snowflake connector's options, not a verified REST
  contract (see assumption B2a). Validate with `aidp catalog test-connection`
  before relying on a registered catalog.

## [0.15.2] — 2026-09-10

### Fixed

- **Skill instructions now require verification before reporting success.**
  AIDP creates are asynchronous and can settle late or fail silently; the
  router's shared rules and the `deploy` phase of `snowflake-medallion-clone`
  now say explicitly that a pending or unsettled result ("N of M verified so
  far", a schema still `CREATING`, exit code nonzero) must be reported as
  pending, never rounded up to success.
- **The plugin now reports migration status proactively, without waiting for
  the next prompt.** After any stage finishes during a migration session, the
  router's shared rules require stating what just happened, what the stage
  board's `next_stage` is, and the concrete command that runs it — in the same
  turn, unprompted. `snowflake-stage-board` is now explicitly in scope for
  this proactive use, not only for on-request status checks.

## [0.15.1] — 2026-09-10

### Fixed

- The diagnosis probe is now **named in the result whether or not its delete
  took**. Deletes are asynchronous like creates, so cleanup is best-effort —
  and an object the plugin created must never be silently abandoned in a
  customer's catalog. Observed live: a probe survived its own delete call.

### Verified live

Both of this release's features, against a deliberately poisoned schema:

- **5 of 5 failed objects correctly diagnosed as burned names**, with the
  right remedy — *"A NOVEL name in lake.snowmig_testdb was created
  successfully, so the schema and your request are both fine and this NAME IS
  BURNED … Retry into a FRESH SCHEMA."*
- The smoke test **PASSES** on the catalog API, write probe included, with
  nothing left behind.

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

Found live on `ACME_ORDER_360_VW`, created correctly with all 17 columns:

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

- **AIDP lower-cases identifiers.** A schema created as `SNOWMIG_TESTDB`
  is stored as `snowmig_testdb`, so every `schemaKey` and read-back key
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
