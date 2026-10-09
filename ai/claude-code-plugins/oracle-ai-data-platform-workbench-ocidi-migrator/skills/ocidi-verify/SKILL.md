---
name: ocidi-verify
description: Static PASS / REVIEW / FAIL check of an ocidi-migrate output directory before anything is published - every notebook cell parses, no credential literals, parameters cell present, unfilled work orders counted; every job's tasks point at notebooks this migration produced, dependencies exist and are acyclic, runIf values are known, schedules are valid Quartz with an IANA timezone and PAUSED. Offline. Use after ocidi-migrate or ocidi-fallback, or when the user asks whether an OCI-DI migration output is ready to publish.
---

# `ocidi-verify`

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp verify out/
```

Exit code 1 if anything FAILs.

- **FAIL** — broken as written. Do not publish; fix the cause (usually by
  re-running `migrate`, or by re-filling a work order).
- **REVIEW** — well formed, but with open work orders, LLM-written code, or
  findings in the job's `.review.md`. Publishing is allowed; the user must
  know which notebooks will raise at their first unconverted step.
- **PASS** — nothing to flag statically.

Verify does not run Spark. Behavioural evidence comes from the local
execution tests (`pytest "${CLAUDE_PLUGIN_ROOT}/tests/test_execution.py"`, needs
the `[spark]` extra) and, on AIDP, from [`ocidi-reconcile`](../ocidi-reconcile/SKILL.md).
