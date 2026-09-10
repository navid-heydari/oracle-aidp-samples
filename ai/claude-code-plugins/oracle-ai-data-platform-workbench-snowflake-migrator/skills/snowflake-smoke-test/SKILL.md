---
name: snowflake-smoke-test
description: Check connectivity and permissions on both ends before any migration work. Verifies Snowflake is reachable and readable (list databases, read INFORMATION_SCHEMA) and, when AIDP coordinates are supplied, that the target catalog is readable and optionally writable. Use before the first assessment, when credentials change, or whenever a stage fails with an authorization error and you need to know which end is at fault.
---

# Smoke test — both ends

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py smoke --out-dir ./snowmig_out \
  --account <...> --user <...> --auth <...> [--key-path ...] \
  [--database <db>] \
  [--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>] \
  [--write-probe]
```

Exit 0 = every check passed. Exit 1 = at least one failed. `SMOKE_TEST.md` shows
which, so a failure points at one end rather than "it doesn't work".

## What it checks

| End | Check | Needs |
|---|---|---|
| Snowflake | identity: user, role, account, region | connection |
| Snowflake | list databases | `USAGE` on at least one database |
| Snowflake | read `INFORMATION_SCHEMA` | read on a real database — the probe is **qualified**, because a fresh session has no current database and an unqualified reference fails with `090105` even for `ACCOUNTADMIN` |
| AIDP | read the target catalog (`SHOW SCHEMAS`) | the four coordinates |
| AIDP | **write** — creates a probe schema | `--write-probe` |

## The write probe writes — say so before you pass the flag

Proving write means actually writing. `--write-probe` creates a schema named
`snowmig_permission_probe` in the target catalog, confirms it is visible, then
**drops that one schema again**. Never `CASCADE`, and it skips the drop if the
schema was already there — a schema it did not create is not its to remove.

The no-`DROP` rule is a **source** guarantee: nothing is ever written to or
dropped from Snowflake. It does not extend to AIDP, which is where this plugin
legitimately creates objects.

It is still off by default, because it writes. Tell the user what it will create
before you pass the flag. If cleanup fails, the report names what was left under
"left behind" — pass that on.

## If the destination is skipped

Without all four AIDP coordinates the destination section reads *Skipped*. That
is not a pass. Say plainly that only the source was verified, and ask for the
coordinates if the user wants the destination checked too.
