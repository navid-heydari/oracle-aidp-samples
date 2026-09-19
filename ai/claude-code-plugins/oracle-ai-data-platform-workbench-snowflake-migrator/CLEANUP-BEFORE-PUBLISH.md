# ⚠️ Cleanup required before this plugin is shared or published

A checklist, not a suggestion. Items 1 and 2 are confidentiality issues.

## 1. Remove the customer-confidential engagement docs

**Status: DONE in the working tree, OUTSTANDING in git history.** It is committed and **already pushed** to
`origin/features/snowflake_migrator_wip` (commit `d6d48fd "skeleton init"`).

It contains a named customer's annual Snowflake spend, estate scale, warehouse
inventory, internal-review notes naming three colleagues, and lessons cited from
three other named customers. None of it is needed to use the plugin.

```bash
# DONE in the working tree: the engagement docs (the customer-context file and
# the delivery timetable) were moved to a private directory outside this repo
# and dropped from the index. `.gitignore` now blocks `*-CONTEXT.md` so they
# cannot come back by accident.

# STILL OUTSTANDING: they remain in git HISTORY on the remote branch. Removing
# them properly needs a history rewrite and a force-push, or deleting the
# remote branch:
#     git push origin --delete <branch>
# Decide which, then do it deliberately. This is the one item a working-tree
# cleanup cannot close.
```

Then remove the two references to it:

- `README.md` — the last bullet under "Docs"
- `docs/specs/2026-09-09-mvp1-design.md` §1

## 2. Check for any other customer specifics

```bash
grep -rniE '<customer>|\$5 ?MM|200,?000 tables' . --exclude-dir=.git
```

Expect hits in `PLAN-2PERSON-TIMETABLE.md` and `PORTING-STATUS.md` too. Decide
per file whether it is genericisable or should move out with the context doc.

**DONE:** the corpus is `00_acme_setup.sql` generating `ACME_ORDER_360_VW`;
no source file, fixture or doc names a customer.

## 3. Remove the developer-specific test account

**Status: DONE.** No source file, fixture, or doc names a real account or
database anymore. Live-only defaults (`test_live_smoke.py`, `validate.py`)
fall back to the placeholder `SNOWMIG_TESTDB` and otherwise read the database
name from **`snowmig-config.yaml`** -- the one migration config, gitignored and
never committed. (There used to be a second file for this,
`local-test-account.yaml`, with its own tracked template; both are gone. One
config file is the design.) Set `SNOWMIG_LIVE_DB` / `CORPUS_DB` instead if you
would rather not keep a config at all.

## 4. Confirm no credential material

```bash
grep -rniE 'BEGIN (RSA )?PRIVATE KEY|sf_key|\.p8|password *=' . --exclude-dir=.git
```

The private key lives at `~/.sf_key.p8`, outside the repo — verify it stayed there.

## 5. State the unverified surface in the README

**Superseded — do NOT re-add the old wording.** The README used to say *no
AIDP environment has ever been contacted*; that has been false since
2026-09-16 and is now emphatically false. Live-verified against a real
DataLake: workspace and cluster creation, `workspace-object` upload of
`NOTEBOOK` objects, job creation, a job run reaching SUCCESS, EXTERNAL/SNOWFLAKE
and INTERNAL catalog creation read back, and the AIDP Snowflake connector
reading 1065 relations from a cluster.

What to confirm instead is that the README states the **remaining** unproven
surface accurately, rather than overclaiming in either direction: the catalog
CRAWLER (B16 — `testConnection` returns `PENDING`), cluster libraries (B12),
`01_create_structure` and everything downstream, and anything at real scale.
See `ASSUMPTIONS.md` §B.
