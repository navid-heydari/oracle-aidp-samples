---
argument-hint: "<out-dir>"
description: Summarise an OCI-DI migration output directory - object statuses, open LLM work orders, verify grades, and what was published.
---

# `/ocidi-status`

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp status <out>
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp fallback list <out>
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp verify <out>
```

Report: counts per status; each `manual` / `fallback_pending` object and
why; the verify totals; and, if `<out>/publish.json` exists, what was
uploaded, skipped and refused. Suggest the next step from the overview's
fixed order.
