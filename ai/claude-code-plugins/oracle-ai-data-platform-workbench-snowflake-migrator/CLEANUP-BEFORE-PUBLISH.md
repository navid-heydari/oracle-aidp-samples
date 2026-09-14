# ⚠️ Cleanup required before this plugin is shared or published

A checklist, not a suggestion. Items 1 and 2 are confidentiality issues.

## 1. Remove `CUSTOMER-CONTEXT.md` — customer-confidential

**Status: OUTSTANDING.** It is committed and **already pushed** to
`origin/features/snowflake_migrator_wip` (commit `d6d48fd "skeleton init"`).

It contains a named customer's annual Snowflake spend, estate scale, warehouse
inventory, internal-review notes naming three colleagues, and lessons cited from
three other named customers. None of it is needed to use the plugin.

```bash
# 1. move it somewhere private
mv CUSTOMER-CONTEXT.md ~/Workspace/oracle/snowflake_migrator/

# 2. drop it from the working tree and index
git rm --cached CUSTOMER-CONTEXT.md && git commit -m "chore: remove customer context from the plugin"

# 3. it is STILL IN HISTORY on the remote. Removing it properly needs a
#    history rewrite and a force-push, or deleting the remote branch:
#      git push origin --delete features/snowflake_migrator_wip
#    Decide which, then do it deliberately.
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

The corpus files are named `00_acme_setup.sql` and generate a `ACME_ORDER_360_VW`
view — rename if the customer name should not appear at all.

## 3. Remove the developer-specific test account

**Status: DONE.** No source file, fixture, or doc names a real account or
database anymore. Live-only defaults (`test_live_smoke.py`, `validate.py`)
now fall back to the placeholder `SNOWMIG_TESTDB` and read a real database
name from `local-test-account.yaml` at the plugin root -- gitignored, never
committed, copied from the tracked `local-test-account.example.yaml`. Set
`SNOWMIG_LIVE_DB` / `CORPUS_DB` instead if you'd rather not keep the file.

## 4. Confirm no credential material

```bash
grep -rniE 'BEGIN (RSA )?PRIVATE KEY|sf_key|\.p8|password *=' . --exclude-dir=.git
```

The private key lives at `~/.sf_key.p8`, outside the repo — verify it stayed there.

## 5. State the unverified surface in the README

Already noted, but confirm it still says: **no AIDP environment has ever been
contacted**, so the `deploy --execute` path, the `aidp`/`oci` command shapes, and
the existence-probe mechanism are all unproven. See `ASSUMPTIONS.md` §B.
