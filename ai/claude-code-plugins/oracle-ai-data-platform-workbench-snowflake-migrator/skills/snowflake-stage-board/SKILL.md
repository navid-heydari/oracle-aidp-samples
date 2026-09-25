---
name: snowflake-stage-board
description: Show the Snowflake-to-AIDP migration as a stage table before running it - which stages are supposed to run, which have run, and what each one found, with the single writing stage marked. Use when the user asks what the plugin will do, what stage comes next, where a run got to, why something was skipped, or wants an overview before approving a migration - AND proactively after any stage finishes during a migration session, to report status and the next stage without waiting to be asked. Read-only; touches no environment.
---

# Stage board — read the run before you execute it

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig stages
```

Offline. It reads the artifacts already in `--out-dir` and reports the pipeline
against them. It makes no decisions, contacts nothing, and changes nothing —
so it is always safe to run, including before anything else has.

## Lead with this, not with a wall of stages

Present the table. It has four columns and they answer the four questions a
user actually has: **what runs**, **what it needs**, **whether it has run**,
and **what it found**.

Then say three things out loud:

1. **Three stages write to AIDP — `provision`, `catalog` and `deploy`** — and each is a dry
   run unless `--execute` is passed with all four AIDP coordinates. Everything
   else is read-only, apart from two narrow opt-ins that say so themselves
   (`smoke --write-probe`, `notebook --upload`). If someone is nervous about
   running the pipeline, this is the sentence that answers them.
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

## At the end of a run: what was translated, and what it cost

Two roll-ups close a migration session. `summary` writes both into
`SUMMARY.md`; each also stands alone.

**Translation map** — `TRANSLATION_MAP.md`, written by `ddl` and `summary`.
Every Snowflake type seen and the Spark type it became, every dialect rule
and whether this estate made it apply, refuse or never occur, and every
source → target name. Present the refused rules and the unmapped types
first: those are what blocked objects.

**LLM token usage** — `TOKENS.md`, per stage and per phase:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig tokens
```

Every stage appends its start, end and the Claude Code session id to
`run_log.jsonl`; `tokens` reads that session's transcript (and its
subagents') from `~/.claude/projects/` and credits each stage with the
tokens spent after the previous stage ended. Local files only; nothing is
sent anywhere. Say what the numbers are: **the engine calls no model — these
are the tokens the agent spent driving it.** Tokens outside the run are
excluded and counted, not folded in; `--since <ISO time>` credits setup
work to the first stage. A stage run outside a Claude Code session is
reported as *not measured*, never as zero.

## Publish the finished report into the workspace

When the run is done, copy its record into AIDP so it outlives the operator's
machine:

```bash
${CLAUDE_PLUGIN_ROOT}/bin/snowmig publish            # dry run: lists the files
${CLAUDE_PLUGIN_ROOT}/bin/snowmig publish --execute  # uploads and reads each back
```

Inputs and outputs (plans, inventory, DDL, reports, the phase diagram, the run
log) go to `backup-snowflake-migration/reports/final-<UTC>/` in the migration
workspace, through the same workspace-object calls `provision` uses. The
connection config and anything named like a credential are never published.
Report `N/M read back`; a file that did not appear in the listing is not
published.
