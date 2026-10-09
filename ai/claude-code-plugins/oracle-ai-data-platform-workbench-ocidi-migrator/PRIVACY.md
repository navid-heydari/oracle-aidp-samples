# Privacy

## What this plugin sends off your machine

- **To your OCI tenancy (OCI-DI, read-only).** `extract` sends signed GET
  requests to the OCI Data Integration API in your region, under your own
  OCI credentials.
- **To your AIDP instance.** `provision --apply` and `publish --apply` send
  notebook content, job definitions, folder and workspace/catalog names
  through the `aidp` CLI, under your own OCI credentials. Dry runs send
  read-only list requests.
- **To Anthropic, only in two cases:**
  - **In a Claude Code session:** work-order prompts are read by Claude as
    part of the conversation (the `ocidi-fallback` skill). They contain the
    DI JSON of one operator or task, field and parameter names and defaults,
    and the reason it was not converted.
  - **`fallback run --provider anthropic`:** the same prompts are sent to the
    Anthropic API, using your Anthropic credentials. It only runs when you
    ask for it.

## What it does not send

- **Row data.** The compiler never reads data. The reconcile notebook runs
  on your AIDP cluster and prints only aggregates (counts, sums, key-difference
  counts).
- **Secret values.** `extract` replaces passwords, private keys, wallets,
  credential files and tokens with `<redacted>` before writing the snapshot.
  OCI Vault secret OCIDs are kept as references. Generated notebooks read
  credentials at run time with `aidputils.secrets.get(name=..., key=...)`
  and never embed them. The fallback validator rejects code that contains a
  credential literal or a JDBC URL.

## What is written locally

- `snapshot/`: raw DI object JSON (redacted). It names hosts, schemas,
  tables and columns, so treat it as confidential.
- `out/`: notebooks, jobs, DDL, work orders, `report.json`, `REVIEW.md`,
  `publish.json`.
- `ocidi-config.yaml` and `--save` files hold identifiers (OCIDs, workspace
  keys) and are git-ignored by this plugin.
