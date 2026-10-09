---
name: ocidi-migrator-bootstrap
description: First-run setup for the OCI Data Integration -> AIDP migrator. Checks Python, the optional packages each verb needs (oci for extract, anthropic for headless fallback, pyspark for local tests), the aidp CLI for provision/publish, and writes ocidi-config.yaml with the AIDP instance, workspace (default ocidi_migrated) and target catalog. Use on first run, when the user asks to set up or configure the migrator, or when any ocidi-* step fails with an import, auth, "no instance configured" or "aidp not found" error.
---

# `ocidi-migrator-bootstrap`

## 1. Check what is installed

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp doctor
```

| Needed for | Install |
|---|---|
| `analyze`, `migrate`, `verify`, publish dry run | nothing (standard library; Python >= 3.9) |
| `extract`, `provision --instance-name` | `pip install oci requests` |
| `provision` (dry run and apply), `publish --apply` | the `aidp` CLI on PATH (`pip install aidp-cli`) |
| YAML config | `pip install pyyaml` (or use `ocidi-config.json`) |
| `fallback run --provider anthropic` | `pip install anthropic` |
| local execution tests | `pip install -e "${CLAUDE_PLUGIN_ROOT}[spark]"` + Java 8/11/17 |

## 2. Write the config

Copy `${CLAUDE_PLUGIN_ROOT}/examples/ocidi-config.example.yaml` to the
working directory as `ocidi-config.yaml` and fill it in **with the user's
real values** — never invent OCIDs or names. Ask for anything missing.

The fields that matter first:

- `aidp.instance_name` — the AIDP instance (DataLake) display name, as the
  user names it. Or `aidp.instance_id` with its OCID.
- `aidp.workspace_name` — default `ocidi_migrated`.
- `aidp.profile` / `aidp.auth` — OCI config profile; `api_key` (default) or
  `security_token`.
- `target.catalog` — default `ocidi_migrated`.
- `di.workspace_id`, `di.region` — only for `extract`.

Identifiers can also come from the environment (`OCIDI_AIDP_INSTANCE_ID`,
`OCIDI_AIDP_WORKSPACE_KEY`, `OCIDI_AIDP_CLUSTER_KEY`, `OCIDI_DI_WORKSPACE_ID`, …)
so they never have to be written to a file. `ocidi-config.yaml` is
git-ignored; keep it that way.

## 3. Smoke test the AIDP side (read-only)

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp provision
```

A dry run: it resolves the instance and lists whether workspace and catalog
exist. Nothing is created. If it fails with 401/403 under `api_key`, retry
with `--auth security_token --profile <session profile>` after
`oci session authenticate`.

## 4. Smoke test the engine (offline)

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp migrate \
  "${CLAUDE_PLUGIN_ROOT}/tests/fixtures/snapshot_sales" -o /tmp/ocidi-demo
```

Expect 9 notebooks, 7 jobs, 3 work orders, and a `REVIEW.md`.
