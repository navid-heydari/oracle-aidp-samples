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

## The write probe leaves something behind — say so

Proving write means actually writing, and this plugin never issues `DROP`. So
`--write-probe` creates `snowmig_permission_probe` in the target catalog and
**does not remove it**. The report names it under "left behind".

It is off by default for that reason. Tell the user what it will create and that
they will need to delete it, before you pass the flag.

## If the destination is skipped

Without all four AIDP coordinates the destination section reads *Skipped*. That
is not a pass. Say plainly that only the source was verified, and ask for the
coordinates if the user wants the destination checked too.
