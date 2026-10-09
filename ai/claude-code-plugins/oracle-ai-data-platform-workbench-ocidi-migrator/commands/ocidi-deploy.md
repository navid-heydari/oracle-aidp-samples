---
argument-hint: "<out-dir>"
description: Provision the AIDP workspace (default ocidi_migrated) and catalog, then publish a verified migration's notebooks and PAUSED jobs - dry run first, --apply only after the user confirms each list.
---

# `/ocidi-deploy`

1. Run `verify` on the output directory; stop on any FAIL.
2. [`ocidi-provision`](../skills/ocidi-provision/SKILL.md) **dry run**. Show
   what exists and what would be created (workspace `ocidi_migrated`,
   catalog `ocidi_migrated` unless configured otherwise). Ask before
   `--apply`.
3. [`ocidi-publish`](../skills/ocidi-publish/SKILL.md) **dry run**. Show the
   notebooks (with status) and jobs (with schedules, all PAUSED). Ask which
   cluster key to use: without one the notebooks upload but no job is created
   (AIDP requires a cluster on every task), and publishing again later with
   `--cluster-key` creates the jobs. Ask before `--apply`.
4. After apply, report `out/publish.json` and the user's next steps: run
   `00_setup`, seed watermarks, run each job once, reconcile, then switch
   schedules ([`ocidi-reconcile`](../skills/ocidi-reconcile/SKILL.md)).
