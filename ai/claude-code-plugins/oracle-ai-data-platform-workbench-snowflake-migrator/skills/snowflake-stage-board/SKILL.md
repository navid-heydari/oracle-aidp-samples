---
name: snowflake-stage-board
description: Show the Snowflake-to-AIDP migration as a stage table before running it - which stages are supposed to run, which have run, and what each one found, with the single writing stage marked. Use when the user asks what the plugin will do, what stage comes next, where a run got to, why something was skipped, or wants an overview before approving a migration. Read-only; touches no environment.
---

# Stage board — read the run before you execute it

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py stages --out-dir ./snowmig_out
```

Offline. It reads the artifacts already in `--out-dir` and reports the pipeline
against them. It makes no decisions, contacts nothing, and changes nothing —
so it is always safe to run, including before anything else has.

## Lead with this, not with a wall of stages

Present the table. It has four columns and they answer the four questions a
user actually has: **what runs**, **what it needs**, **whether it has run**,
and **what it found**.

Then say three things out loud:

1. **`deploy` is the only stage that writes anything**, and it is a dry run
   unless `--execute` is passed with all four AIDP coordinates. Everything else
   is read-only. If someone is nervous about running the pipeline, this is the
   sentence that answers them.
2. **Read out every ⚠️ row.** A flagged stage either found something or could
   not look, and those are not the same. `0 policy exposures` means the check
   ran and found none; *"policy attachments unreadable — exposure UNKNOWN, not
   zero"* means nobody knows yet. Never summarise the second as the first.
3. **Name the next stage** and what it is for. `next_stage` is the first one
   with no artifact.

## What to do with it

- **Before a migration** — run it, present it, and get agreement on the plan
  before `deploy --execute`. Together with `PREFLIGHT.md` (which shows every
  source → destination mapping) this is what the user is approving.
- **Mid-run or after a failure** — it is the fastest way to see where a run got
  to and which stage needs attention, without re-reading six artifacts.
- **When the user asks "what does this plugin do"** — this is a better answer
  than the README, because it is grounded in their actual run.

## Do not present it as progress

`DONE` means *the stage ran and wrote its artifact*, not that the result was
good. A `deploy` row can read `DONE ⚠️ verified 0/7` — the stage completed and
created nothing. Say so plainly rather than reporting a completed stage as a
completed migration.
