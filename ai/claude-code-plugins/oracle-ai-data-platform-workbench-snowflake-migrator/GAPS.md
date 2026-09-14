# Gaps and next steps

**State:** v0.16.0 · 910 offline tests · 14 live tests (skipped without
credentials) · one real structure migration completed end to end against a
live AIDP DataLake.

This is the single list. `ACTION-ITEMS.md` holds the detail and the reasoning
behind each item; this file is the ranked view of what is left.
`ARCHITECTURE.md` holds the stage-level design the items below are measured
against.

**Reviewed 2026-09-11.** The 0.16.0 EXTERNAL-catalog pivot is the dominant
source of new gaps. It changed the default target without re-running the
plugin's own verification discipline across the stage board, the smoke probe,
the deploy guard and the dependency list — so the P0 items below are all one
finding wearing four hats.

---

## What is actually proven

Worth stating first, because "verified" now means something specific here.

| Surface | Status |
|---|---|
| Snowflake extraction (inventory, census, lineage, maintenance, security) | **Live-verified** against a real account |
| Read-only enforcement on the source | Enforced at the transport, tested |
| Plan, waves, restrictions, DDL generation | Unit-tested; generated SQL parsed by a real Spark parser |
| Target **catalog CRUD** transport | **Live-verified** — 7 objects created and read back |
| Case folding, async polling, key resolution | **Live-verified** (each was a real failure first) |
| Target **SQL** transport | **Dead** — `POST …/sql/execute` returns 404 |
| Smoke test, destination half | **Live-verified** — read + write probe, against a **Standard** catalog |
| **EXTERNAL catalog registration** (the 0.16.0 default) | **Never executed.** Body shape inferred, not verified — see B2a |
| Notebook upload / run | **Never executed** |
| Data movement | **Not implemented, by design** |

---

## P0 — the 0.16.0 default path is not wired through

### 1. `ensure_catalog` never polls the read-back

`target/catalog_provision.py` calls `_find_catalog` once, immediately after
the create. Simulated against a normal asynchronous create (catalog visible on
the third list), it returns `action: "create_requested", verified: False`.

So **every successful registration reports pending.** `catalog_deploy.py`
already learned this — assumption D11, creates are async and settle seconds
later — and polls with a bounded backoff. The new module regresses it. With
router rule 9 ("never report success ahead of verification"), the default path
can never self-verify, and the honest "this is pending" message in
`render_catalog` degrades into noise the user has to check past by hand.

**Fix:** reuse the bounded backoff from `catalog_deploy.py`.

Second-order: `_find_catalog` swallows every exception and returns `None`, so a
failed `list_catalogs` is indistinguishable from "the catalog is absent" — and
the pre-create call then decides to create.

### 2. Nothing stops `deploy --execute` targeting an EXTERNAL catalog

No `catalogType` check exists anywhere in `target/catalog_deploy.py`. The old
assumption B2 said *"an EXTERNAL catalog cannot hold managed Delta and is
refused"* — the refusal was never code. It did not matter while the default
was a pre-existing Standard catalog. It matters now: EXTERNAL is the default,
and `plan/data_movement.py` states in its own option text that EXTERNAL
catalogs are read-only in AIDP.

**Fix:** resolve the target catalog's `catalogType` before the first create and
refuse in `deploy_catalog`, in the same shape as
`catalog_provision.RefusedToExecute`.

### 3. `smoke --write-probe` fails against the new default

`plan/smoke.py` creates a schema in `target.catalog` to prove write access.
That cannot succeed in a read-only EXTERNAL catalog.

This reproduces exactly the failure the smoke-test item (now closed, below)
was opened for: **a FAIL reported against a destination that works.** Anyone
running the pipeline in order on the default path hits it and stops.

**Fix:** make the probe catalog-type aware — read-only checks for EXTERNAL,
the write probe only for Standard.

### 4. The stage board does not know `catalog` exists

`report/stages.py` lists twelve stages; `catalog` is not one of them, and
`catalog_result.json` is not read. The rendered header still asserts *"Every
stage is read-only except `deploy`"*, which is false now that
`catalog --execute` writes.

This is load-bearing, not cosmetic: router rule 10 requires reporting
`next_stage` from this board after every stage, so **the new default stage is
unreachable by the documented flow.**

**Fix:** add `catalog` to `STAGES` in its dependency position, mark it
`writes: True`, and restate the one-writer claim as two.

---

## P1 — security and packaging

### 5. Credentials are passed in argv

`executor.py` puts the whole `create_catalog` body — including `privateKey`,
`password` or `token` — into `--request-body`, so the secret is visible in
`ps` to any other user on the host.

`runner.make_call` prints the command before running it. Measured: the secret
does **not** appear in the printed line, but only because the flat 200-char
per-argument truncation happens to cut just before it (271-byte body with
minimum-length inputs, password at ~254). That is incidental, not designed —
any reordering of `connectionDetails`, or a shorter envelope, and it prints.

The dry-run artifact is handled correctly: `cmd_catalog` records only
`connection_fields` (the key names), never values.

**Fix:** redact `connectionDetails` explicitly before printing, and pass the
body by file rather than argv — `upload_notebook` already uses the `file://`
form, so the pattern exists.

### 6. `pyyaml` is a runtime dependency of the default path, listed as dev-only

`target/snowflake_catalog_connection.py` imports `yaml` to read the connection
config. YAML is what `snowflake-catalog-connection.example.yaml` and the
`snowflake-medallion-clone` skill both document. But `pyyaml` appears only in
`requirements-dev.txt`, and `snowflake-migrator-bootstrap` installs
`requirements.txt`.

A clean install therefore hits the ImportError on the documented happy path.
It degrades with a clear message, which is why it is P1 and not P0.

**Fix:** move `pyyaml` to `requirements.txt`.

### 7. Ten `.pyc` files are tracked in git

Including `engine/target/__pycache__/aidp_runner.cpython-311.pyc`, for a module
that no longer exists. They ship inside the published plugin, and one shows as
modified in the working tree. `.gitignore` cannot help — they were committed
before the rule, so it does not apply to them.

**Fix:** `git rm --cached` the ten, and confirm the `!engine/target/**`
re-include does not pull `__pycache__` back.

### 8. All of 0.16.0 is uncommitted

`plugin.json` says `0.16.0` and `CHANGELOG.md` carries a dated release entry,
but `catalog_provision.py`, `snowflake_catalog_connection.py`, both of their
test files and `snowflake-catalog-connection.example.yaml` are untracked.

---

## P2 — the documentation contradicts the code

### 9. ASSUMPTIONS.md still says AIDP was never contacted — and a test pins it

The header table reads **"AIDP | None. Never contacted."** That is flatly
contradicted by this file, by `CHANGELOG.md`, and by the live-verified rows in
the table above.

Worse: `tests/test_plugin_surface.py::test_assumptions_register_states_that_aidp_was_never_contacted`
asserts `"Never contacted" in text`. **Correcting the document breaks the test
suite.** The test was right when written and is now enforcing a false claim.

B3, B4, B5 and "Outstanding items that block real use" item 1 are stale for
the same reason.

**Fix:** rewrite the header table to state what *was* contacted and what was
not, and rewrite the test to assert the live/unverified split rather than the
literal phrase.

### 10. Router lists 6 of 13 stages

`skills/snowflake-migrator-overview/SKILL.md` ends with
*"Stages: `assess` · `deps` · `plan` · `ddl` · `deploy` · `compute`."* Missing:
`catalog`, `maintenance`, `security`, `smoke`, `notebook`, `summary`,
`data-options`, `stages`.

### 11. No `/snowflake-catalog` command

Six less-central stages have a slash command; the new default stage does not.

### 12. `--write-probe` help says the opposite of what the code does

`plan/smoke.py` creates the probe schema, checks visibility, and then calls
`delete_schema`. The CLI help still reads *"it is NOT dropped afterwards"*,
and `test_smoke_skill_warns_the_write_probe_leaves_a_schema` is still named
for the old behaviour. The skill text itself was corrected; the help string
and the test name were not.

Separately, that visibility check is a single `list_schemas` immediately after
the create, with no backoff — the same unbounded-async assumption as item 1,
and the reason it is grouped with it rather than treated as cosmetic.

---

## P3 — carried forward, re-prioritised

### 13. Notebook upload and run — never executed

Unchanged as a gap, but **its priority went up in 0.16.0**: it is no longer a
convenience, it is the only sanctioned path for Standard-catalog tables. The
`upload_notebook` and `run_notebook` command shapes are invented, exactly as
the SQL path was — and that one turned out to be a 404. Assume wrong until
proven.

### 14. The EXTERNAL `connectionDetails` shape is a guess *(B2a)*

Field names are inferred from the AIDP Snowflake Spark connector's options,
not a verified REST contract. `aidp catalog test-connection` is recommended in
prose but wired into nothing.

**Fix:** add a `catalog --test-connection` step so validation is a command,
not a suggestion.

### 15. The region map covers 12 short codes

`target/coords.py` fails loud on an unmapped code, which is the right
behaviour — but `ord`, `sjc`, `arn`, `zrh`, `mel`, `qro` and the rest are
simply unreachable, and `_endpoint` hardcodes `oraclecloud.com`, so no OC2/OC3
realm can work.

### 16. Blast radius — MEDIUM *(C3)*

The census says *what exists*; it does not say *what stops working*. A task
that populates a migrated table means that table goes stale after cutover: the
clone succeeds and then quietly stops being correct. Join the census against
`OBJECT_DEPENDENCIES` and report, per migrating table, "populated by task X,
which does not migrate".

### 17. Maintenance proposal and job — MEDIUM *(M3, M4)*

M2 delivered the inputs. Still missing: a per-table `OPTIMIZE` cadence from
measured churn, `ZORDER` keys seeded from the source clustering key, a
`VACUUM` retention never shorter than the source's, and all of it emitted as a
**disabled** job.

### 18. Time-travel parity statement — LOW *(M5)*

Source recovery window (`DATA_RETENTION_TIME_IN_DAYS` + 7 days Fail-safe)
against the proposed Delta retention, with any reduction called out and
Fail-safe named as having no equivalent.

### 19. Cost comparison — LOW *(M6)*

Snowflake automatic-clustering credits versus the AIDP cost of the M3 cadence.
Needs a live AIDP run to calibrate.

---

## Recently closed

- **The smoke test lied about the destination.** It called the SQL endpoint
  that returns 404 and reported FAIL against a destination that worked. The
  destination half now runs on the catalog API, with a uniquely-named write
  probe that confirms visibility and cleans up after itself. Live: PASS.
  *(Note item 3 above: the same class of bug has reappeared for EXTERNAL.)*
- **A poisoned name is now recognised and named** *(P1)*. On the first object
  that never appears, the plugin creates one throwaway object with a novel name
  in the same schema — once per schema — and distinguishes "your planned names
  are burned, retry into a fresh schema" from "the request itself is wrong".
  `--no-diagnose` turns it off, since the probe writes. Live: 5 of 5 correctly
  diagnosed against a deliberately poisoned schema.
- **Test-account identifiers genericised** in source and fixtures; real values
  now only in the gitignored `local-test-account.yaml`.

---

## Parked — TBD, do not re-raise

These are recorded and deliberately **not** blocking. Nothing in the ranked
list above waits on them, and they should not be surfaced in reports or
conversation until the phase that needs them arrives.

| # | Parked item | Revisit when |
|---|---|---|
| **JDBC** | Whether AIDP reads Snowflake over JDBC, or the plugin uses JDBC instead of the Python connector | **The byte-movement phase.** It belongs in the generated notebook, where it is auditable by the user and reviewable afterwards — not in the structure clone |
| **P3** | Is a failed create *meant* to reserve the name permanently, and what clears it? | Oracle answers. Workaround in the meantime: retry into a fresh schema |
| **T5** | Is `timestamp_ntz` support planned on the catalog API? | Oracle answers. Workaround: `--timestamp-ntz timestamp`, which records the caveat |
| **—** | Is view column-type derivation documented? | Oracle answers. The plugin reports the drift per column regardless |

## Open questions that need the customer

| Question | Blocking |
|---|---|
| Silver/Gold job body shape | The medallion deliverable is one third stubs |
| `RAPPI-CONTEXT.md` is on the remote — rewrite history, delete the branch, or accept? | Publishing |
| Rappi's region: Ashburn, Oregon or Ohio? | Any cost or transfer estimate |

## Publish blockers

- `RAPPI-CONTEXT.md` (customer-confidential, already pushed) — still tracked
- `PLAN-2PERSON-TIMETABLE.md` and `docs/specs/` ship inside the plugin — still tracked
- Ten tracked `.pyc` files (item 7)
- ~~Test-account identifiers in eight fixtures (`npxbexe`, `DU58131`, `NHEYDARI`)~~ ✅ DONE — genericized in source/fixtures; real values now only in gitignored `local-test-account.yaml`

---

## Known limits, stated rather than hidden

- **No data is moved.** By design; `DATA_CLONE` and `DONE` are unreachable.
- **Scale is untested.** Validated on 7 objects. Pagination, per-schema column
  reads and metadata row counts are all in place, but nothing has run against
  a large estate.
- **`VARIANT`/`OBJECT`/`ARRAY` block their table** unless
  `--semi-structured string`, which defers rather than solves.
- **A view's column types are derived by the target**, not carried over.
  Verified: three aggregate columns changed type, two of them narrowing.
