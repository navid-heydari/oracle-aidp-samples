"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

__all__ = ["render_inventory", "render_ddl_plan", "render_planned_objects",
           "render_soft_clone_summary", "render_compute"]


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
