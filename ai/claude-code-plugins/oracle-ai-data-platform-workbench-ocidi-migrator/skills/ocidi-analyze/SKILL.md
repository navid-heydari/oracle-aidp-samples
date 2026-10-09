---
name: ocidi-analyze
description: Inventory an OCI Data Integration snapshot or export and estimate the migration before running it - object counts, data flow operators, which tasks compile cleanly, which need LLM work orders or manual rebuilds, the sources AIDP must be able to read, the Delta tables that will be written, and a LOW/MEDIUM/HIGH effort band per object. Offline and free. Use when the user asks what a migration would involve, how hard it is, what is supported, or wants an assessment before converting anything.
---

# `ocidi-analyze` — what would this migration involve?

```bash
PYTHONPATH="${CLAUDE_PLUGIN_ROOT}" python3 -m ocidi2aidp analyze <snapshot-or-export> -o analysis/
```

Writes `analysis/analysis.md` and `analysis.json`. It runs the real compiler
into a scratch directory, so its verdicts are exactly what `migrate` will do.

## Read it back to the user in this order

1. **HIGH effort objects** — they contain `manual` items: no faithful AIDP
   form (pipeline Decision branches, OCI Data Flow tasks, Fusion BICC
   sources, MERGE targets with no key). Name each one and why.
2. **Work orders** (MEDIUM) — constructs the LLM fallback will be asked to
   write (unknown functions, `ORA_HASH`, custom-SQL sources, OCI Function
   operators, SQL tasks). Say how many, and that each is validated and
   marked for review.
3. **Sources AIDP must reach** — every DI data asset and the action needed
   (register an external catalog, bucket read access, an AIDP credential +
   JDBC driver). These are prerequisites for the jobs to run at all.
4. **Delta tables** the migration will create in the target catalog.

Do not present LOW as "done": LOW objects still carry `review` findings
(e.g. an OVERWRITE target fed by an incremental source).
