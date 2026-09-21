---
name: snowflake-migrator-bootstrap
description: First-run setup for the Snowflake to AIDP migrator. Finds or creates the one migration config file (snowmig-config.yaml), reads every field back with secrets masked, verifies Snowflake authentication by password, key-pair, programmatic access token or SSO, verifies the AIDP end, then smoke-tests the connection and reports the account, region, role and warehouse. Use the first time the migrator runs on a machine, or when any other stage fails with an authentication, connection or "no config found" error.
---

# Bootstrap

Getting from "nothing set up" to "both ends verified". Four steps, in order.

## 1. Dependencies — one command, not a procedure

The plugin ships its own launcher. It creates the environment on first use
and then gets out of the way:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig --bootstrap
```

After that, **every stage is run through the launcher** and there is no
interpreter path to remember:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig preflight --test-source
```

`bin/snowmig` bootstraps automatically on any stage, so `--bootstrap` is only
needed to rebuild a broken environment. `--python` prints the interpreter it
uses, for the cases that genuinely need one.

**The venv is deliberately NOT inside the plugin folder.** It lives under
`${XDG_DATA_HOME:-~/.local/share}/snowmig/venv`, overridable with
`SNOWMIG_VENV`. The installer copies the plugin directory on every version
bump, so a venv living there would be duplicated per version, and a
credential file beside it would be copied too.

A half-built venv from an interrupted install *looks* present, so the
launcher decides by importing the dependencies rather than by the directory
existing, and rebuilds when that import fails.

**Do not hand-roll a venv and do not pass `--break-system-packages`.** If the
launcher cannot find a Python 3.10+, it says so and stops — that is a machine
to fix, not a step to improvise around.

## 2. Find the plugin, and the config — do not assume either

**Do not assume the user is inside this repo, or that any folder is open.** The
plugin may be installed rather than cloned, and the working directory may be
anywhere at all.

- **The engine is always at `${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py`**, and the
  in-AIDP scripts at `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`. Build
  every path from `${CLAUDE_PLUGIN_ROOT}`, never from the user's current
  directory, and never ask them to `cd` anywhere.
- **If the engine or the scripts cannot be found, STOP and say so.** Never
  substitute your own SQL, your own API calls or your own translation for a
  stage that is missing. The engine is the method; there is no hand-made
  fallback.
- **The config is the OPERATOR's file, not the plugin's.** The CLI looks for it
  in this order — `--config <path>` → `./snowmig-config.yaml` in the working
  directory → the same name beside the plugin — and **every stage prints the
  file it used**. Repeat that line to the user; it is how they catch a run
  pointed at last month's environment.

If there is no config, create one where the user is working:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig init-config
```

That writes `./snowmig-config.yaml` from the template, mode `0600` on POSIX
(Windows has no mode bits), and refuses
to overwrite an existing one (that file holds live credentials). An installed
plugin's own directory may be read-only, which is exactly why the config
belongs in the working directory.

Then ask the user to fill it in and tell you when it is ready.

## 3. One file, both ends — what goes where

**Say this explicitly, because it is the question users actually ask.** There is
one file to fill in, and the secret goes *in that file*, not into the chat:

| What | Where | Note |
|---|---|---|
| Snowflake account/host, user, warehouse, database, role, schema | `snowmig-config.yaml`, under `snowflake:` | created from the template; gitignored, `0600` |
| The Snowflake **password or private key** | the same file — `password:` or `private_key: |` inline | inline is the default; `*_path` variants exist but are not what you propose first |
| Which AIDP resources to use (DataLake OCID, workspace, cluster, catalog) | the same file, under `aidp:` | any of them can also be passed as a flag, and a flag wins |
| AIDP **authentication** | `~/.oci/config` (`oci setup config`) | **not configured in this plugin at all** — it drives the `oci`/`aidp` CLIs with the user's normal OCI setup |

Rules that come with an inline secret, and they are not optional:

- **Ask the user before reading the config**, and say why you need it.
- **Never print, echo, quote or summarise a secret value** — not in chat, not in
  a report, not in a commit message. Render a config only through
  `migration_config.redact()`, which is what `preflight` uses.
- **Never ask the user to paste a password or a private key into the
  conversation.** They put it in the file, on their own machine. If one does end
  up in the chat or in a committed file, say so plainly and tell them to rotate
  it.
- The file is gitignored. Keep it out of tickets and commits too — an inline
  secret is a secret that leaks the moment the file travels.

Key-pair setup, if the user wants one instead of a password:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out ./sf_key.p8
openssl rsa -in ./sf_key.p8 -pubout | grep -v '^-----' | tr -d '\n'
# then in Snowsight:  ALTER USER <user> SET RSA_PUBLIC_KEY='<that string>';
```

Then set `auth: keypair` and paste the PEM **into the same config file**, under
`private_key: |`. A key pair is also what AIDP's own Snowflake connector uses,
so it is not throwaway setup. (`key_path:` also works if they would rather keep
the PEM on disk — but do not send them to a second file by default.) `pat`
works for the engine but **not** for the EXTERNAL catalog registration: that
live contract has no token property.

SSO (`authenticator: externalbrowser`) needs a SAML IdP on the account; a plain
Snowflake account fails it with `390190`.

## 4. Read the config back, then actually connect

This is the step that saves hours:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig preflight \ --test-source
```

No `--config` needed once the file is in place — the CLI discovers it and prints
which one. `PREFLIGHT_CONFIG.md` lists every field with what it is for, secrets
masked, then the results of the live checks:

- `--test-source` opts into the Snowflake connection. It is opt-in because it
  resumes the warehouse; without it the source is reported *skipped*.
- The **AIDP end is checked automatically** whenever the config carries both
  `aidp.datalake_ocid` and `aidp.catalog` — it lists the catalogs and reports
  whether that one exists and whether it is `INTERNAL` or `EXTERNAL`. Read-only,
  so it needs no flag. Without those two fields it is reported *skipped*.

Walk the table with the user and ask them to confirm, especially:

- **`host`** — the *Account/Server URL* from the Snowflake console
  (`Account → Account/Server URL`), e.g. `ORG-ACCOUNT.snowflakecomputing.com`.
  Derived from `account` when absent, which is right for the org-account form;
  set it explicitly for the account-locator form or PrivateLink.
- **`role`** — what the migration can see is *exactly* this role's grants. A
  missing `role` means the user's default role, which is rarely what they meant.
- **`warehouse`** — must be resumable; a suspended one auto-resumes on the first
  query, so the first call can be slow.
- **`schema`** — any real schema. It only scopes the connector's pushdown
  session inside AIDP, and that option rejects `INFORMATION_SCHEMA`.
- **`datalake_ocid`** — the destination. Say it out loud and have the user
  confirm it is the right environment.

**A skipped check is not a pass.** If only one end was configured, say which end
was never tested rather than reporting "preflight OK".

## 5. Smoke-test the source

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig assess \ --database <one small database>
```

Report the account, region, role and warehouse back to the user. Then hand off
to [`snowflake-migrator-overview`](../snowflake-migrator-overview/SKILL.md) for
the stage sequence.
