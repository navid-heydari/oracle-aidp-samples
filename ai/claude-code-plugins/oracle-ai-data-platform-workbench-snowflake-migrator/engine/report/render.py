"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

from plan.data_movement import architecture_decision
from plan.status import assess_risk, migration_status

__all__ = ["render_inventory", "render_ddl_plan", "render_planned_objects",
           "render_soft_clone_summary", "render_compute", "render_summary",
           "render_smoke", "render_data_options",
           "architecture_section"]


def _bytes(n) -> str:
    if not n:
        return "-"
    n = float(n)
    step = 1024.0
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < step:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= step
    return f"{n:.1f} EB"


def render_inventory(inv: dict) -> str:
    s = inv.get("session", {})
    out = ["# Snowflake estate inventory", "",
           f'Probed **{inv.get("probed_at")}** · account `{s.get("A")}` · '
           f'region `{s.get("R")}` · role `{s.get("ROLE")}`',
           f'Databases in scope: {", ".join(inv.get("databases_in_scope") or []) or "-"}',
           "",
           f'**{inv.get("object_count", 0)} objects** — '
           + " · ".join(f"{k} {v}" for k, v in (inv.get("counts_by_type") or {}).items()),
           ""]

    collisions = inv.get("identifier_case_collisions") or {}
    if collisions:
        out += ["## ⚠️ HALT — identifier-case collisions", "",
                "These differ only by case. Snowflake treats them as distinct objects; "
                "Spark folds to lower and would merge them. Resolve before migrating.",
                ""]
        out += [f"- `{k}` ← {', '.join('`' + x + '`' for x in v)}"
                for k, v in collisions.items()] + [""]

    out += ["## Objects", "",
            "Row counts are **exact** (in-session `count(*)`), not `SHOW` estimates. "
            "Sizes are Snowflake-reported compressed bytes.", "",
            "| Object | Type | Rows (exact) | Size | Cols | Case form | Compatibility |",
            "|---|---|---:|---:|---:|---|---|"]
    for r in inv.get("inventory", []):
        rows = r.get("row_count_exact")
        out.append(
            f'| `{r["source_identifier"]}` | {r["object_type"]} '
            f'| {rows if rows is not None else "ERROR"} '
            f'| {_bytes((r.get("source_metadata") or {}).get("bytes"))} '
            f'| {len(r.get("columns") or [])} | {r.get("identifier_case_form")} '
            f'| {r.get("compatibility_status")} |')

    notes = inv.get("extraction_notes") or []
    if notes:
        out += ["", "## Extraction notes", "",
                "Objects or scopes that could not be read. Absence below is not "
                "evidence the object does not exist.", ""]
        out += [f"- {n}" for n in notes]
    return "\n".join(out) + "\n"


def render_ddl_plan(ddl: dict) -> str:
    stmts = ddl.get("statements") or []
    out = ["# Target DDL plan", "",
           f"{len(stmts)} statement(s). Nothing has been executed.", ""]
    for st in stmts:
        out += [f'## `{st["source_identifier"]}` → `{st["target_fqn"]}`', "",
                "```sql", st["sql"], "```", ""]
        rules = st.get("rules_applied") or []
        if rules:
            out += ["Rules applied:", ""]
            out += [f'- `{r["rule_id"]}` — {r["detail"]}' for r in rules] + [""]
        if st.get("omitted_properties"):
            out += ["Properties dropped (no Delta equivalent): "
                    + ", ".join(f"`{p}`" for p in st["omitted_properties"]), ""]
        if st.get("warnings"):
            out += ["Warnings:", ""] + [f"- {w}" for w in st["warnings"]] + [""]
    if ddl.get("blocked"):
        out += ["## Blocked — no DDL generated", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in ddl["blocked"]]
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The two headline reports the plugin exists to produce:
#   1. what objects are planned to move (and what cannot, with reasons)
#   2. what was actually created by the soft clone
# ---------------------------------------------------------------------------

_CATEGORY_TITLES = {
    "restriction": "Excluded by a restriction you set",
    "unmapped_type": "Column types with no Delta equivalent",
    "snowflake_only_sql": "View SQL that is Snowflake-only",
    "unsupported_object": "Object kinds with no AIDP equivalent",
    "no_definition": "Definition could not be read",
    "unparseable_sql": "SQL could not be parsed",
}


def render_planned_objects(plan: dict) -> str:
    s = plan.get("summary", {})
    out = ["# Objects planned to move", "",
           f'Planned **{s.get("can_migrate", 0)}** of '
           f'{s.get("objects_inventoried", 0)} inventoried objects — '
           f'{s.get("tables", 0)} table(s), {s.get("views", 0)} view(s). '
           f'**{s.get("cannot_migrate", 0)}** cannot move.', "",
           f'Bronze mapping: {plan.get("bronze_mapping")}.', ""]

    if plan.get("restrictions_applied"):
        out += ["## Restrictions in force", "",
                "Applied at your request, before planning:", ""]
        out += [f"- `{k}`: {v}" for k, v in plan["restrictions_applied"].items()]
        out.append("")

    out += ["## Target structure to exist first", "",
            "Catalogs (create these, or confirm they exist and are INTERNAL): "
            + ", ".join(f'`{c}`' for c in plan.get("catalogs_to_create") or []),
            "",
            "Schemas the clone will create: "
            + ", ".join(f'`{a}.{b}`' for a, b in plan.get("schemas_to_create") or []),
            ""]

    out += ["## Can migrate", "",
            "| Object | Type | Target | Rows | Cols |", "|---|---|---|---:|---:|"]
    for c in plan.get("can_migrate") or []:
        out.append(f'| `{c["source_identifier"]}` | {c["object_type"]} '
                   f'| `{c["target"]}` | {c.get("rows") if c.get("rows") is not None else "-"} '
                   f'| {c.get("columns", "-")} |')
    out.append("")

    cannot = plan.get("cannot_migrate") or []
    if cannot:
        out += ["## Cannot migrate", ""]
        grouped = {}
        for c in cannot:
            grouped.setdefault(c["category"], []).append(c)
        for category, items in grouped.items():
            out += [f'### {_CATEGORY_TITLES.get(category, category)} '
                    f'(`{category}`) — {len(items)}', ""]
            out += [f'- `{i["source_identifier"]}` ({i.get("object_type")}) — '
                    f'{i["reason"]}' for i in items]
            out.append("")

    if plan.get("cycles"):
        out += ["## Dependency cycles", "",
                "Excluded from the ordering; they need a human decision rather than "
                "an arbitrary broken edge.", ""]
        out += [f'- {", ".join(f"`{n}`" for n in c)}' for c in plan["cycles"]] + [""]

    out += ["## Order of creation", "",
            "Dependencies land before their dependents, so views follow their base "
            "tables.", ""]
    for i, wave in enumerate(plan.get("waves") or [], 1):
        out.append(f'### Wave {i} — {len(wave)} object(s)')
        out += [f'- `{n}` → `{plan.get("target_names", {}).get(n, "?")}`'
                for n in wave]
        out.append("")

    jobs = plan.get("silver_gold_jobs") or []
    if jobs:
        out += ["## Silver and Gold jobs", "",
                "Created as placeholders and **never triggered**. Their content is a "
                "requirement to define with the customer, so no transformation logic "
                "is generated here.", "",
                "| Job | Layer | Reads from | Enabled | Body |", "|---|---|---|---|---|"]
        out += [f'| `{j["name"]}` | {j["layer"]} | `{j["reads_from"]}` '
                f'| {j["enabled"]} | {j["body_status"]} |' for j in jobs]
        out.append("")

    out += architecture_section(plan)

    if plan.get("dependency_source"):
        out += [f'---', "",
                f'Lineage source: **{plan["dependency_source"]}** — '
                f'{plan.get("dependency_coverage_note") or ""}']
    return "\n".join(out) + "\n"


def render_soft_clone_summary(plan: dict, res: dict) -> str:
    total = res.get("statement_count", 0)
    scope = res.get("catalog_in_scope")

    if res.get("dry_run"):
        out = ["# Soft clone — DRY RUN", "",
               f"{total} object(s) would be created; **nothing was created**.", "",
               "Re-run with `--execute` plus the AIDP target coordinates to apply."]
    else:
        out = ["# Soft clone summary", "",
               f'Catalog in scope: **{scope}**', "",
               f'Created and **verified {res.get("verified", 0)}/{total}** '
               f'(executed {res.get("executed", 0)}/{total}).', "",
               "Verification probes each object individually, because a batch can "
               "report success while statements inside it failed. `verified` is the "
               "honest number.", "",
               "**These objects are empty — the clone copies structure, no data.**",
               ""]

    by_type = {}
    for c in plan.get("can_migrate") or []:
        if not scope or c["target"].split(".", 1)[0].upper() == str(scope).upper():
            by_type[c["object_type"]] = by_type.get(c["object_type"], 0) + 1
    if by_type:
        out += ["## What the clone covers", ""]
        out += [f"- {v} {k.lower()}(s)" for k, v in sorted(by_type.items())]
        out.append("")

    if res.get("out_of_scope_count"):
        out += ["## Not deployed in this run", "",
                f'{res["out_of_scope_count"]} object(s) belong to other catalogs: '
                + ", ".join(f'`{c}`' for c in res.get("out_of_scope_catalogs") or []),
                "",
                "Each catalog is a separate, explicitly confirmed run.", ""]

    if res.get("blocked_count"):
        out += [f'{res["blocked_count"]} object(s) were blocked before deployment '
                "and never attempted. See the planned-objects report.", ""]

    if res.get("failed"):
        out += ["## Failed verification", ""]
        out += [f'- `{f["target_fqn"]}` — {f["reason"]}' for f in res["failed"]]
        out.append("")
    if res.get("chunk_errors"):
        out += ["## Batch errors", ""] + [f"- {e}" for e in res["chunk_errors"]] + [""]

    jobs = plan.get("silver_gold_jobs") or []
    if jobs and not res.get("dry_run"):
        out += [f"## Silver/Gold jobs", "",
                f"{len(jobs)} job(s) defined in the plan, disabled and never "
                "triggered. Creating them on AIDP is a separate step.", ""]
    return "\n".join(out).rstrip() + "\n"


def render_compute(sizing: dict) -> str:
    out = ["# Compute proposal — warehouses to AIDP clusters", "",
           f'{sizing.get("warehouse_count", 0)} warehouse(s) · '
           f'{sizing.get("total_source_nodes", 0)} Snowflake node-equivalents · '
           f'max {sizing.get("max_concurrent_clusters", 0)} concurrent cluster(s) '
           "to absorb at peak", ""]

    credits = sizing.get("observed_credits_total")
    if credits is not None:
        out += [f'Observed credit consumption: **{credits:,.1f}** '
                f'({sizing.get("credits_basis")})', ""]
    else:
        out += [f'Credit consumption: not observable '
                f'({sizing.get("credits_basis")})', ""]

    cost = sizing.get("cost_model")
    if cost:
        out += ["## Cost", "",
                f'At ${cost["credit_price_usd"]}/credit: '
                f'**${cost["snowflake_monthly_usd"]:,.0f}/month** '
                f'(${cost["snowflake_annual_usd"]:,.0f}/year) on Snowflake, from '
                f'{cost["observed_credits"]:,.1f} credits over '
                f'{cost["observed_days"]} days.', "",
                cost["aidp_comparison"], ""]
    else:
        out += ["## Cost", "", sizing.get("cost_note", ""), ""]

    out += ["## Proposed clusters", "",
            "| Warehouse | Snowflake size | Nodes | Workers | Autoscale max | OCPU/worker | Shape family |",
            "|---|---|---:|---:|---:|---:|---|"]
    for p in sizing.get("proposals") or []:
        out.append(f'| `{p["name"]}` | {p["source_size"]} | {p["source_nodes"]} '
                   f'| {p["worker_count"]} | {p["autoscale_max_workers"]} '
                   f'| {p["worker_ocpus"]} | {p["worker_shape_family"]} |')
    out += ["",
            "⚠️ **Shape families require confirmation.** Shape availability varies "
            "by tenancy and region, so no exact SKU is asserted. Confirm against the "
            "target tenancy before provisioning.", ""]

    if sizing.get("blocked"):
        out += ["## Warehouses with no proposal", ""]
        out += [f'- `{b["name"]}` — {b["reason"]}' for b in sizing["blocked"]]
    return "\n".join(out).rstrip() + "\n"


# ---------------------------------------------------------------------------
# The migration summary: one row per object -- tables, views and jobs alike --
# with row count, migration risk, and migration status. Plus a brief
# source -> destination header.
# ---------------------------------------------------------------------------

def render_summary(plan: dict, inventory: dict, deployed: dict | None,
                   target: dict | None) -> str:
    session = (inventory or {}).get("session", {})
    dbs = ", ".join((inventory or {}).get("databases_in_scope") or []) or "-"

    out = ["# Migration summary", "", "## Source → destination", "",
           "| | Source (Snowflake) | Destination (AIDP) |", "|---|---|---|"]
    unset = "*not supplied*"
    if target:
        dest_id = target.get("datalake_ocid", unset)
        dest_ws = target.get("workspace", unset)
        dest_cl = target.get("cluster_id", unset)
        dest_cat = target.get("catalog", unset)
        dest_region = "*derived from the OCID*"
    else:
        dest_id = dest_ws = dest_cl = dest_cat = dest_region = unset
    out += [f'| Account / DataLake | `{session.get("A", "-")}` | `{dest_id}` |',
            f'| Region | `{session.get("R", "-")}` | {dest_region} |',
            f'| Role / workspace | `{session.get("ROLE", "-")}` | `{dest_ws}` |',
            f'| Version / cluster | `{session.get("V", "-")}` | `{dest_cl}` |',
            f'| Scope | {dbs} | catalog `{dest_cat}` |', ""]
    if not target:
        out += ["**No destination was supplied, so none is assumed.** The target "
                "names below are derived from the SOURCE (bronze mirrors it 1:1); "
                "they do not imply that any AIDP catalog, workspace or cluster "
                "exists.", ""]
    out += [f'Mapping: {plan.get("bronze_mapping")}.', ""]

    rows: list[tuple[str, str, str, str, str, str]] = []

    for c in plan.get("can_migrate") or []:
        level, note = assess_risk(c)
        status = migration_status(c["source_identifier"], deployed=deployed)
        rows.append((c["source_identifier"], c["object_type"],
                     "-" if c.get("rows") is None else f'{c["rows"]:,}',
                     level, status, note))

    for c in plan.get("cannot_migrate") or []:
        level, note = assess_risk(c, blocked=True)
        rows.append((c["source_identifier"], c.get("object_type") or "-", "-",
                     level, "BLOCKED", note))

    for j in plan.get("silver_gold_jobs") or []:
        rows.append((j["name"], "JOB", "-", "LOW",
                     migration_status(j["name"], deployed=deployed),
                     f'{j["layer"]} job: {j["body_status"]} body, disabled and '
                     "never triggered. Content is a requirement to define."))

    out += ["## Objects", "",
            "| Object | Type | Rows | Risk | Migration status | Notes |",
            "|---|---|---:|---|---|---|"]
    out += [f'| `{n}` | {t} | {r} | {lv} | {st} | {note} |'
            for n, t, r, lv, st, note in sorted(rows, key=lambda x: (x[1], x[0]))]
    out.append("")

    status_counts: dict[str, int] = {}
    for _, _, _, _, st, _ in rows:
        status_counts[st] = status_counts.get(st, 0) + 1
    risk_counts: dict[str, int] = {}
    for _, _, _, lv, _, _ in rows:
        risk_counts[lv] = risk_counts.get(lv, 0) + 1

    out += ["## Roll-up", "",
            "By migration status: "
            + " · ".join(f"**{k}** {v}" for k, v in sorted(status_counts.items())),
            "",
            "By risk: "
            + " · ".join(f"**{k}** {v}" for k, v in sorted(risk_counts.items())),
            "",
            "Status vocabulary: `NOT_YET_DONE` → `IN_PROGRESS` → `SHALLOW_CLONE` → "
            "`DATA_CLONE` → `DONE`, or `BLOCKED`.", "",
            "**`DATA_CLONE` and `DONE` are unreachable in this version: the plugin "
            "copies structure only and moves no data.** Every object that reports "
            "`SHALLOW_CLONE` exists in AIDP with its columns and zero rows.", ""]

    if deployed and not deployed.get("dry_run"):
        out += [f'Deployed against catalog '
                f'`{deployed.get("catalog_in_scope")}`; '
                f'{len(deployed.get("verified_targets") or [])} verified, '
                f'{len(deployed.get("failed_targets") or [])} unverified.', ""]
    elif deployed:
        out += ["Last run was a **dry run** — nothing was created.", ""]
    else:
        out += ["No deployment has been attempted yet.", ""]

    out += architecture_section(plan)
    return "\n".join(out).rstrip() + "\n"


def render_smoke(result: dict) -> str:
    src, dest = result.get("source", {}), result.get("destination", {})
    out = ["# Smoke test — connectivity and permissions", "",
           f'Verdict: **{"PASS" if result.get("ok") else "FAIL"}**', "",
           "## Source (Snowflake)", ""]
    if not src.get("reachable"):
        out += [f'**Unreachable.** {src.get("error", "")}', ""]
    else:
        out += [f'Connected as `{src.get("user")}` / role `{src.get("role")}` on '
                f'account `{src.get("account")}` (`{src.get("region")}`)', "",
                "| Check | Result | Detail |", "|---|---|---|"]
        out += [f'| {c["name"]} | {"PASS" if c["ok"] else "FAIL"} | {c["detail"]} |'
                for c in src.get("checks") or []]
        out.append("")

    out += ["## Destination (AIDP)", ""]
    if dest.get("skipped"):
        out += [f'**Skipped.** {dest.get("reason")}', ""]
    else:
        out += [f'Catalog `{dest.get("catalog")}` on cluster '
                f'`{dest.get("cluster_id")}`', "",
                "| Check | Result | Detail |", "|---|---|---|"]
        out += [f'| {c["name"]} | {"PASS" if c["ok"] else "FAIL"} | {c["detail"]} |'
                for c in dest.get("checks") or []]
        out += ["",
                f'Write access: **{"verified" if dest.get("write_verified") else "not verified"}** '
                f'— {dest.get("write_note", "")}', ""]
        if dest.get("left_behind"):
            out += ["⚠️ Left behind by the write probe (this plugin never issues "
                    "`DROP`, so remove these yourself): "
                    + ", ".join(f'`{x}`' for x in dest["left_behind"]), ""]
    return "\n".join(out).rstrip() + "\n"


def render_data_options(options: list[dict]) -> str:
    out = ["# Data-movement options — for you to choose", "",
           "**This plugin moves no bytes, and nothing below is implemented.** "
           "These are the realistic ways data could move in a later phase, with "
           "the trade-offs and the open unknowns attached, so the choice is made "
           "deliberately rather than defaulting to whichever path got built "
           "first.", "",
           "| Option | Catalog | Moves bytes | Phase |", "|---|---|---|---|"]
    for o in options:
        out.append(f'| **{o["id"]}** — {o["name"]} | {o["catalog_type"]} '
                   f'| {"yes" if o["moves_bytes"] else "no"} '
                   f'| {", ".join(o["phase"])} |')
    out.append("")

    for o in options:
        out += [f'## {o["id"]} — {o["name"]}', "",
                f'**Path:** {o["etl"]}', "",
                "**For**", ""]
        out += [f"- {x}" for x in o["pros"]]
        out += ["", "**Against**", ""]
        out += [f"- {x}" for x in o["cons"]]
        out += ["", "**Still unknown**", ""]
        out += [f"- {x}" for x in o["unknowns"]]
        out += ["", f'Status: `{o["status"]}`', ""]

    out += ["---", "",
            "## What happens after you choose", "",
            "`record_choice()` captures the option and the reasoning. It executes "
            "nothing. Every unknown listed against the chosen option has to be "
            "retired by a hand-run spike on one representative table before any "
            "tooling is built — the outstanding one that invalidates the most is "
            "whether `NUMBER(p,s)` survives an unload round trip with exact "
            "precision, which has never been measured.", ""]
    return "\n".join(out).rstrip() + "\n"


def architecture_section(plan: dict) -> list[str]:
    """The data-movement architecture options. ALWAYS included, never optional.

    Rendered into every report that describes a migration, whether or not a
    choice has been made and whether or not a destination was supplied. A user
    who gave no instruction still has to be shown what the choices are; a user
    who chose still benefits from seeing what they chose against.
    """
    decision = architecture_decision(plan.get("architecture_choice"))
    out = ["## Data-movement architecture", "",
           "**This plugin moves no bytes.** None of the paths below is "
           "implemented; they are the ways data could move in a later phase.", "",
           decision["statement"], ""]

    if decision["decided"]:
        chosen = decision["chosen"]
        executed = ("yes" if chosen["executed"]
                    else "no — this plugin executes nothing")
        custom = chosen.get("custom_architecture")
        if custom:
            out += ["### The customer's own architecture", "",
                    f'**{custom["name"]}**', "", custom["description"], "",
                    f'- Recorded by: {chosen["chosen_by"]}',
                    f'- Because: {chosen["rationale"]}',
                    f'- Executed: **{executed}**', "",
                    "Recorded verbatim and **not mapped** to any option below. "
                    "This plugin has not assessed it, so none of the trade-offs, "
                    "costs or unknowns listed against `A1`–`A5` apply to it.", ""]
        else:
            out += [f'- Chosen: **{chosen["id"]}** — {chosen["name"]}',
                    f'- By: {chosen["chosen_by"]}',
                    f'- Because: {chosen["rationale"]}',
                    f'- Executed: **{executed}**', ""]
        if decision["unknowns_outstanding"]:
            out += ["Outstanding unknowns for that choice:", ""]
            out += [f"- {u}" for u in decision["unknowns_outstanding"]] + [""]

    out += ["| Option | Catalog | Moves bytes | Handles |",
            "|---|---|---|---|"]
    for o in decision["options"]:
        marker = " ✅" if (decision["decided"]
                          and o["id"] == decision["chosen"]["id"]) else ""
        moves = {True: "yes", False: "no", None: "*unknown*"}[o["moves_bytes"]]
        handles = ", ".join(o["handles"]) or "*unknown until described*"
        out.append(f'| **{o["id"]}**{marker} — {o["name"]} | {o["catalog_type"]} '
                   f'| {moves} | {handles} |')
    out += ["",
            "`A6_CUSTOMER_DEFINED` is the open slot: **the eventual design does "
            "not have to be one of the others**, and \"not decided yet\" is a "
            "valid answer that blocks nothing here.", "",
            "Full trade-offs, open unknowns and what each option would take to "
            "build: `references/data-movement-options.md`, or run "
            "`snowmig data-options`.", ""]
    return out
