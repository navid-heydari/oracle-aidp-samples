---
name: ocidi-extract
description: Read a live OCI Data Integration workspace (projects, folders, data assets, connections, data flows, tasks, pipelines, user-defined functions, applications, published objects, schedules, task schedules, last successful task runs) over the OCI-DI REST API into a local snapshot directory, read-only, with secrets redacted. Use when the user wants to start a migration from their real OCI-DI workspace, "pull" or "export" it, or refresh a snapshot. Also covers using an OCI-DI export zip instead.
---

# `ocidi-extract` — live OCI-DI -> snapshot (read-only)

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp extract \
  --workspace-id <OCI-DI workspace OCID> --di-region <region> \
  [--di-profile <OCI profile>] -o snapshot/
```

- Needs `pip install oci requests` and an OCI profile with read access to
  the DI workspace (`inspect`/`read` on `dis-workspaces` and its objects).
- **Ask the user for the workspace OCID and region** — never guess. To find
  it: `oci data-integration workspace list --compartment-id <compartment>`.
- Only GETs. Nothing in OCI-DI changes.

## What it reads, and why both halves

Design-time objects (projects, folders, data flows, tasks, pipelines) **and**
runtime objects (applications → published objects, schedules, task
schedules, task runs). What runs in production is the *published* copy of a
task, which can differ from the design-time copy; `migrate` prefers it. The
last successful run of each published task is kept so watermarks can be
seeded at cutover.

Raw REST JSON is written verbatim (with `expandReferences=true`): the typed
OCI SDK drops fields of models it does not know (the Table Function
operator), and the compiler needs them.

## Secrets

Every value under a secret-named key (password, private key, wallet,
credential file, token, …) is replaced with `<redacted>` before it is
written. OCI Vault `secretId` OCIDs are kept — they are references, which
the migration maps to AIDP credential names. Still: treat `snapshot/` as
sensitive (it names hosts, schemas and tables) and do not commit it.

## Using an export instead

An OCI-DI export (`*.workspace.zip`, `*.project.zip`, … from Object Storage,
or the unpacked directory) can be passed straight to `analyze`/`migrate` in
place of a snapshot. Its layout is undocumented, so objects are classified
by `modelType` and published objects/schedules are usually absent — prefer
`extract` when the workspace is reachable.

## Status

The extractor is tested against a fake REST API built from the SDK model
shapes; it has **not yet run against a real workspace**. On the first real
run, check the counts it prints against the DI console, and report any
object type that comes back empty unexpectedly.
