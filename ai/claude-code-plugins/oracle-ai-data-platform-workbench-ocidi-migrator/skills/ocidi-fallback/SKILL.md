---
name: ocidi-fallback
description: Fill the LLM work orders an ocidi-migrate run left behind (fallback/FB_*.json) - the constructs the deterministic compiler would not translate, such as unknown or Oracle-only functions (ORA_HASH), custom-SQL sources, Table Function and OCI Function operators, SQL tasks and authenticated REST tasks. Dispatches the ocidi-fallback-author agent once per work order, then validates and splices each answer into its notebook under an LLM-ASSISTED REVIEW REQUIRED banner. Use when migrate reports work orders, when a notebook is fallback_pending, or when the user asks to fix the REVIEW REQUIRED stubs.
---

# `ocidi-fallback` — the LLM path

This is the only place an LLM touches the migration, and it is fenced:

- The author writes **one function** per work order with a fixed signature,
  into `out/fallback/<id>.py`. It never edits a notebook.
- `fallback apply` **validates statically** (parses; exactly that function;
  imports from an allow-list; no network, filesystem, process, `exec`,
  credential literals or JDBC URLs unless the work-order kind grants it —
  only `sql_task` may use the JVM/credential store, only `rest_task` may use
  `urllib`) and only then splices it in, with a banner.
- An answer that just raises `NotImplementedError` is a legitimate
  **decline**: the object becomes `manual` with the author's reason.

## Steps

1. List the open work orders:
   ```bash
   PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp fallback list out/
   ```
2. For **each pending id**, get its prompt and hand it to the
   `ocidi-fallback-author` agent — one agent per work order; independent
   orders can run in parallel:
   ```bash
   PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp fallback prompt out/ --id <FB_id>
   ```
   Give the agent the full prompt text and the answer path printed at its
   end (`out/fallback/<FB_id>.py`). Do not write the answer yourself in this
   conversation; the agent works from the work order alone, which keeps the
   answer reproducible.
3. Validate and splice:
   ```bash
   PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp fallback apply out/ --author claude-session
   ```
   Each id comes back `applied`, `declined`, `rejected` (with the validator's
   reasons) or `pending`. For a `rejected` one, send the errors back to a
   fresh author agent once; if it is rejected again, leave it pending and
   say so.
4. Report to the user: how many applied / declined / still pending, and
   **every `# ASSUMPTION:` the authors wrote** — those are the things a
   human must confirm. Then run [`ocidi-verify`](../ocidi-verify/SKILL.md).

## Headless alternative (CI, no Claude Code session)

```bash
pip install anthropic   # credentials: ANTHROPIC_API_KEY or `ant auth login`
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp fallback run out/ --provider anthropic
```

Same prompts, same validator; calls `claude-opus-5-5`. Ask before using it —
it sends the work orders (DI JSON, field and parameter names; no row data)
to the Anthropic API. See `${CLAUDE_PLUGIN_ROOT}/PRIVACY.md`.

## Guardrails

- Never edit the stub cells by hand to make a notebook "pass"; fill the
  work order or leave it pending.
- Re-running `migrate` regenerates notebooks with stubs but keeps
  `fallback/*.py` answers; run `fallback apply` again afterwards.
- An `llm_assisted` notebook is not "verified". It needs a human read and
  a reconcile run before cutover.
