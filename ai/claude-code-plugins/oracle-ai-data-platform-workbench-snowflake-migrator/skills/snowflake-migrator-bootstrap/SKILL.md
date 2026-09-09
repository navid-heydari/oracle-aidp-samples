---
name: snowflake-migrator-bootstrap
description: First-run setup for the Snowflake to AIDP migrator. Verifies Python dependencies and Snowflake authentication by key-pair, programmatic access token, password, or SSO, then smoke-tests the connection and reports the account, region, role and warehouse. Use the first time the migrator runs on a machine, or when any other stage fails with an authentication or connection error.
---

# Bootstrap

## 1. Dependencies

```bash
python3 -m pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt
```

If the system Python is externally managed (PEP 668), create a venv and use its
interpreter for every later command. Do not pass `--break-system-packages`.

## 2. Ask which auth method the user has

**Never ask for a password or token in chat.** Have the user put the secret in a
file and give you the path.

| Method | What to ask for | Notes |
|---|---|---|
| Key-pair *(preferred)* | path to an unencrypted PKCS#8 key | Also what AIDP's native Snowflake connector uses, so it is not throwaway setup |
| PAT | path to a file holding the token | Scoped, expiring, revocable. Some accounts require a network policy first |
| Password | path to a file holding it | |
| SSO | nothing | Needs a SAML IdP on the account. A plain Snowflake account fails with `390190` |

Key-pair setup, if they need it:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out ~/.sf_key.p8
chmod 600 ~/.sf_key.p8
openssl rsa -in ~/.sf_key.p8 -pubout | grep -v '^-----' | tr -d '\n'
# then in Snowsight:  ALTER USER <user> SET RSA_PUBLIC_KEY='<that string>';
```

## 3. Smoke-test

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py assess \
  --account <org>-<account> --user <user> --auth keypair --key-path ~/.sf_key.p8 \
  --database <one small db> --out-dir ./snowmig_out
```

Report the account, region, role and warehouse back to the user. A suspended
warehouse auto-resumes on the first query — mention it if the first call is slow.

**Do not ask for AIDP coordinates here.** They belong to stage 3 only.
