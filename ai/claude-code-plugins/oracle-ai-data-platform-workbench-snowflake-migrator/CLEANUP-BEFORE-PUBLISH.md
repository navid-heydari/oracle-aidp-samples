# ⚠️ Cleanup required before this plugin is shared or published

A checklist, not a suggestion. Items 1 and 2 are confidentiality issues.

## 1. Remove `RAPPI-CONTEXT.md` — customer-confidential

**Status: OUTSTANDING.** It is committed and **already pushed** to
`origin/features/snowflake_migrator_wip` (commit `d6d48fd "skeleton init"`).

It contains a named customer's annual Snowflake spend, estate scale, warehouse
inventory, internal-review notes naming three colleagues, and lessons cited from
three other named customers. None of it is needed to use the plugin.

```bash
# 1. move it somewhere private
mv RAPPI-CONTEXT.md ~/Workspace/oracle/snowflake_migrator/

# 2. drop it from the working tree and index
git rm --cached RAPPI-CONTEXT.md && git commit -m "chore: remove customer context from the plugin"

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
grep -rniE 'rappi|pagbank|porto|entel|\$5 ?MM|200,?000 tables' . --exclude-dir=.git
```

Expect hits in `PLAN-2PERSON-TIMETABLE.md` and `PORTING-STATUS.md` too. Decide
per file whether it is genericisable or should move out with the context doc.

The corpus files are named `00_rappi_setup.sql` and generate a `RAPPI_ORDER_360_VW`
view — rename if the customer name should not appear at all.

## 3. Remove the developer-specific test account

`engine/tests/test_live_smoke.py` defaults `SNOWMIG_LIVE_DB` to
`TEST_DB_20260908_1529`, and the memory notes and skill examples reference
account `npxbexe-op03637`. Replace with placeholders.

## 4. Confirm no credential material

```bash
grep -rniE 'BEGIN (RSA )?PRIVATE KEY|sf_key|\.p8|password *=' . --exclude-dir=.git
```

The private key lives at `~/.sf_key.p8`, outside the repo — verify it stayed there.

## 5. State the unverified surface in the README

Already noted, but confirm it still says: **no AIDP environment has ever been
contacted**, so the `deploy --execute` path, the `aidp`/`oci` command shapes, and
the existence-probe mechanism are all unproven. See `ASSUMPTIONS.md` §B.
