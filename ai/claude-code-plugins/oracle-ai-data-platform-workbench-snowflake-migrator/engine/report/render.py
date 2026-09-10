"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

from plan.data_movement import MAINTENANCE_TRAPS, architecture_decision
from plan.status import assess_risk, migration_status

__all__ = ["render_stages", "render_preflight", "render_census", "census_scope",
           "render_maintenance",
           "render_security",
           "render_inventory", "render_ddl_plan", "render_planned_objects",
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
            out += ["Properties dropped (no AIDP equivalent): "
                    + ", ".join(f"`{p}`" for p in st["omitted_properties"]), ""]
        if st.get("deferred_properties"):
            out += ["Maintenance/layout settings NOT applied: "
                    + ", ".join(f'`{d["property"]}`'
                                for d in st["deferred_properties"]), ""]
        if st.get("warnings"):
            out += ["Warnings:", ""] + [f"- {w}" for w in st["warnings"]] + [""]
    if ddl.get("blocked"):
        out += ["## Blocked — no DDL generated", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in ddl["blocked"]]

    deferred = [(s["source_identifier"], d) for s in stmts
                for d in (s.get("deferred_properties") or [])]
    if deferred:
        out += ["", "## Maintenance and layout — decisions, NOT applied", "",
                "These source settings have a real AIDP equivalent, and this "
                "version applies **none** of them. They are listed so the "
                "choice gets made deliberately rather than lost: a clustering "
                "key that quietly fails to arrive is a performance regression "
                "on the largest tables in the estate.", "",
                "The deeper difference is *who runs maintenance*. Snowflake "
                "maintains layout and reclaims storage in the background, "
                "un-asked. On AIDP the equivalents exist and are **explicit** "
                "— they have to be scheduled, and `VACUUM` is what bounds how "
                "far time travel can reach. See "
                "`references/maintenance-and-layout.md`.", "",
                "| Object | Source setting | Value | AIDP equivalent |",
                "|---|---|---|---|"]
        out += [f'| `{ident}` | `{d["property"]}` | `{d["value"]}` | '
                f'{d["aidp_equivalent"]} |' for ident, d in deferred]
        out.append("")
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
           f'Bronze mapping: {plan.get("bronze_mapping")}.', "",
           # Coverage caveat, never silent: the count above is of what was
           # EXAMINED, and absence of a census is exactly the bug being fixed.
           census_scope(plan), ""]

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
               "report success while statements inside it failed. `verified` "
               "means the object exists **with the planned column list** — the "
               "DDL is `CREATE IF NOT EXISTS`, so a name that already belonged "
               "to a different table is reported below, not counted here.", "",
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

    if res.get("mismatches"):
        out += ["## Structure differs — left as found, NOT cloned", "",
                "These names already existed in AIDP with a different structure. "
                "`CREATE IF NOT EXISTS` left them exactly as they were, so they "
                "have **not been cloned** and nothing of theirs was altered. "
                "Resolve the name collision before re-running.", ""]
        out += [f'- `{m["target_fqn"]}` — {m["reason"]}'
                for m in res["mismatches"]]
        out.append("")

    if res.get("derived_type_drift"):
        out += ["## Created, but the target derived different column types", "",
                "These views **were created** with every planned column, in "
                "order. The target computed some column types from the view "
                "SQL rather than taking the declared ones — Snowflake reports "
                "a view's *declared* output types, and the target derives its "
                "own. Aggregates are where this shows up.", "",
                "**A narrowing can overflow.** Check any column marked below "
                "before anything depends on it.", ""]
        for d in res["derived_type_drift"]:
            out.append(f'- `{d["target_fqn"]}` — {d["reason"]}')
        out.append("")

    if res.get("unverified_structure"):
        out += ["## Structure not verified", "",
                "These exist, but their columns could not be compared against "
                "the plan, so they are not counted as verified.", ""]
        out += [f'- `{u["target_fqn"]}` — {u["reason"]}'
                for u in res["unverified_structure"]]
        out.append("")

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

def _row_count_provenance(inventory: dict | None) -> list[str]:
    """Explain every blank in the Rows column.

    A `-` for a view that was not counted and a `-` for a job that has no rows
    are different facts and must not render identically. A count that FAILED is
    a third thing again, and its reason used to be discarded entirely.
    """
    records = (inventory or {}).get("inventory") or []
    if not records:
        return []
    mode = (inventory or {}).get("row_count_mode", "metadata")
    described = {
        "metadata": "Snowflake's maintained row count, read from `SHOW` at no "
                    "cost. It agrees with `COUNT(*)` for a settled standard "
                    "table, but it can lag very recent DML and is not "
                    "maintained for external tables, so it is not a verified "
                    "number.",
        "exact": "a `COUNT(*)` per object — verified, and it executes every "
                 "view to get there.",
        "none": "not collected; `--row-counts` was `none`.",
    }.get(mode, mode)

    out = ["## Row counts", "",
           f"Mode: **{mode}** — {described}", ""]

    not_counted = [r for r in records if r.get("row_count_source") == "not_counted"]
    errored = [r for r in records if r.get("row_count_source") == "error"]

    if not_counted:
        notes = {r.get("row_count_note") for r in not_counted if r.get("row_count_note")}
        out.append(f"**{len(not_counted)} object(s) show `-` because they were "
                   f"not counted**, not because they are empty:")
        out.append("")
        out += [f'- `{r["source_identifier"]}`' for r in not_counted[:20]]
        if len(not_counted) > 20:
            out.append(f"- …and {len(not_counted) - 20} more")
        out.append("")
        out += [f"Reason: {n}" for n in sorted(notes)]
        out.append("")

    if errored:
        out.append(f"**{len(errored)} row count(s) FAILED** — these blanks are "
                   f"an error, not a zero:")
        out.append("")
        out += [f'- `{r["source_identifier"]}` — {r.get("row_count_note", "no detail")}'
                for r in errored[:20]]
        out.append("")

    return out


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

    out += _row_count_provenance(inventory)

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

    out += ["| Option | Catalog | Moves bytes | Maintenance owner | Handles |",
            "|---|---|---|---|---|"]
    for o in decision["options"]:
        marker = " ✅" if (decision["decided"]
                          and o["id"] == decision["chosen"]["id"]) else ""
        moves = {True: "yes", False: "no", None: "*unknown*"}[o["moves_bytes"]]
        handles = ", ".join(o["handles"]) or "*unknown until described*"
        owner = (o.get("maintenance_ownership") or {}).get("owner")
        owns = {"customer": "**you**", "snowflake": "Snowflake",
                "shared": "whoever writes", "both": "**both**",
                None: "*unknown*"}.get(owner, str(owner))
        out.append(f'| **{o["id"]}**{marker} — {o["name"]} | {o["catalog_type"]} '
                   f'| {moves} | {owns} | {handles} |')
    out += ["",
            "**The maintenance column is a real operating cost, not a "
            "footnote.** Snowflake maintains layout and reclaims storage in "
            "the background; AIDP has `OPTIMIZE`, `VACUUM`, `ZORDER BY` and "
            "liquid clustering and runs none of them for you. So the choice "
            "below decides *who inherits that work* — federating leaves it "
            "with Snowflake, landing Delta tables transfers it to you on day "
            "one.", "",
            "### What each choice does to maintenance", ""]
    for o in decision["options"]:
        own = o.get("maintenance_ownership") or {}
        applies = own.get("traps_apply")
        if applies is None:
            which = "unknown until the design is described"
        elif not applies:
            which = "**none of the Delta traps apply**"
        else:
            which = f"all {len(applies)} Delta traps below apply"
        out.append(f'- **{o["id"]}** — {own.get("note", "")} ({which}.)')
    out += ["", "### The three traps", "",
            "Each is something a Snowflake customer has never had to think "
            "about, because Snowflake did it for them.", ""]
    for i, trap in enumerate(MAINTENANCE_TRAPS, 1):
        out.append(f'{i}. **{trap["trap"]}** {trap["consequence"]}')
    out += ["",
            "Measured state for this estate — clustering keys, reclustering "
            "credits, churn and retention overrides — is in `MAINTENANCE.md`. "
            "This plugin proposes no cadence and applies nothing.", "",
            "`A6_CUSTOMER_DEFINED` is the open slot: **the eventual design does "
            "not have to be one of the others**, and \"not decided yet\" is a "
            "valid answer that blocks nothing here.", "",
            "Full trade-offs, open unknowns and what each option would take to "
            "build: `references/data-movement-options.md`, or run "
            "`snowmig data-options`.", ""]
    return out


# ---------------------------------------------------------------------------
# Maintenance and layout (item M2).
#
# Snowflake maintains layout and reclaims storage in the background; AIDP has
# the equivalents and runs none of them. This report exists so that difference
# is a decision on the table rather than something discovered in month three.
# ---------------------------------------------------------------------------

def _fmt(value, suffix: str = "") -> str:
    if value is None:
        return "*not measured*"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    if isinstance(value, int):
        return f"{value:,}{suffix}"
    return f"{value}{suffix}"


def render_maintenance(maint: dict) -> str:
    tables = maint.get("tables") or []
    flagged = [t for t in tables if t.get("signals")]
    acct = maint.get("account_usage") or {}
    ret = (maint.get("retention") or {}).get("account") or {}

    out = ["# Maintenance and layout", "",
           "Snowflake exposes **no `OPTIMIZE` and no `VACUUM`** — it maintains "
           "layout through Automatic Clustering and reclaims storage in the "
           "background. AIDP has `OPTIMIZE`, `VACUUM`, `ZORDER BY` and liquid "
           "clustering, and **runs none of them for you**.",
           "",
           "Nothing is lost in the migration. The *responsibility* moves. This "
           "report records what the source does today; **it proposes no "
           "cadence and applies nothing** — that needs the customer's recovery "
           "requirements and query patterns.", "",
           f'Source Time Travel default: **{_fmt(ret.get("data_retention_time_in_days"))} '
           f'day(s)** (set at: {ret.get("set_at", "unknown")}); '
           f'max data extension: {_fmt(ret.get("max_data_extension_time_in_days"))} day(s).',
           ""]

    if not acct.get("readable", True):
        out += ["> **`ACCOUNT_USAGE` was not readable**, so reclustering credits "
                "and DML churn are **not measured** — that is not the same as "
                "zero, and the difference decides whether clustering matters "
                f'here. Reason: `{acct.get("note", "unknown")}`.', ""]

    if not flagged:
        out += ["## Nothing flagged", "",
                f"{len(tables)} table(s) examined; **no maintenance or layout "
                "signal found**. No clustering keys, no Search Optimization, no "
                "change tracking, no table-level retention overrides"
                + (" and no measured churn above the threshold."
                   if acct.get("readable", True)
                   else ", and churn could not be measured."), "",
                "That is a real finding, not an empty section: a lift-and-shift "
                "of this estate inherits no maintenance obligation beyond the "
                "AIDP defaults.", ""]
    else:
        out += [f"## {len(flagged)} of {len(tables)} table(s) need a "
                "maintenance decision — **none applied**", "",
                "| Table | Cluster key | Auto-cluster | Recluster credits | "
                "Rows rewritten | Retention (days) |",
                "|---|---|---|---:|---:|---|"]
        for t in flagged:
            rec, churn = t["reclustering"], t["dml_churn"]
            retention = _fmt(t.get("retention_days"))
            if t.get("retention_set_at") == "table":
                retention += f' *(table override; schema default ' \
                             f'{_fmt(t.get("retention_inherited_value"))})*'
            out.append(
                f'| `{t["source_identifier"]}` | `{t["cluster_by"] or "—"}` | '
                f'{"ON" if t["automatic_clustering"] else "off"} | '
                f'{_fmt(rec.get("credits")) if rec.get("measured") else "*not measured*"} | '
                f'{_fmt(churn.get("rows_rewritten")) if churn.get("measured") else "*not measured*"} | '
                f'{retention} |')
        out.append("")

        out += ["### What each signal will require on AIDP", ""]
        for t in flagged:
            out.append(f'**`{t["source_identifier"]}`**')
            out.append("")
            for s in t["signals"]:
                equivalent = s.get("aidp_equivalent") or "**no equivalent**"
                out.append(f'- {s["signal"]} — {s["detail"]}. '
                           f'AIDP: {equivalent}; requires {s["aidp_requires"]}.')
            out.append("")

    out += ["## Three things to settle before anyone commits", "",
            "1. **On Delta, `VACUUM` is what bounds time travel.** On Snowflake "
            "retention and storage reclamation are independent and automatic. A "
            "customer used to reclaiming storage freely will delete their own "
            "recovery window.",
            "2. **`OPTIMIZE` increases storage until `VACUUM` runs.** It leaves "
            "the old files behind until retention expires, so compaction without "
            "reclamation is a cost regression.",
            "3. **Nothing runs itself.** Every `OPTIMIZE`/`VACUUM` is a "
            "scheduled AIDP Job — new operational surface the customer did not "
            "have on Snowflake.", ""]

    gaps = maint.get("no_equivalent") or []
    if gaps:
        out += ["## Capabilities with no AIDP equivalent", "",
                "Named here because each is otherwise discovered at the worst "
                "possible moment.", ""]
        for g in gaps:
            out += [f'**{g["capability"]}** — {g["snowflake"]}',
                    "", f'No equivalent: {g["impact"]}', ""]

    if maint.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in maint["unreadable"]] + [""]

    probing = ("on" if maint.get("table_parameters_probed") else
               "off — inferred from effective values, to avoid one query per table")
    out += ["---", "",
            f'History window: {maint.get("history_days")} day(s). '
            f'Per-table parameter probing: {probing}.',
            "", "Planned work to close this gap: `ACTION-ITEMS.md` (M3–M8)."]
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The census: everything that is NOT a table or a view.
#
# `assess` looked at tables and views only, so "7 of 7 objects can move" was
# true of what had been examined and overstated coverage of the estate. The
# scope statement below is carried into every report that states coverage.
# ---------------------------------------------------------------------------

_NO_CENSUS_SCOPE = (
    "**Scope: tables and views only.** No census of the rest of the estate was "
    "run, so this count is not the size of the estate — procedures, UDFs, "
    "tasks, streams, materialized and dynamic tables, stages, pipes, sequences "
    "and file formats were not examined. Run `assess` with the census enabled "
    "to find out what else is there."
)


def census_scope(plan_or_inventory: dict) -> str:
    """The one-line coverage caveat. Never silent: absence is the bug."""
    census = (plan_or_inventory or {}).get("census")
    if not census:
        return _NO_CENSUS_SCOPE
    return census.get("scope_statement") or _NO_CENSUS_SCOPE


def render_census(census: dict) -> str:
    objects = census.get("objects") or []
    kinds = census.get("kinds") or {}

    out = ["# Estate census — what is not a table or a view", "",
           census.get("scope_statement", ""), "",
           "**Nothing here is migrated by this plugin, and no equivalent is "
           "generated.** These are code, schedulers and storage definitions "
           "rather than structure. Each entry names the AIDP capability that "
           "would carry the workload — a pointer, not a promise: a "
           "plausible-but-wrong procedure translation is worse than an honest "
           "gap.", ""]

    if kinds:
        out += ["## Counts by kind", "", "| Kind | Count | Read |", "|---|---:|---|"]
        for kind, info in sorted(kinds.items()):
            count = info.get("count")
            out.append(f'| {kind.replace("_", " ").title()} | '
                       f'{count if count is not None else "*not measured*"} | '
                       f'{"yes" if info.get("readable") else "**denied**"} |')
        out.append("")

    by_effort = census.get("by_effort") or {}
    if by_effort:
        out += ["## Rewrite effort, by triage band", "",
                " · ".join(f"**{k}** {v}" for k, v in sorted(by_effort.items())),
                "", "A band, not an estimate: it says which pile an object "
                "belongs in.", ""]

    by_lang = census.get("by_language") or {}
    if by_lang:
        out += ["Handler languages: "
                + " · ".join(f"**{k}** {v}" for k, v in sorted(by_lang.items())),
                ""]

    if objects:
        out += ["## Every object, and what it would take", "",
                "| Object | Kind | Detail | Language | Effort |",
                "|---|---|---|---|---|"]
        for o in sorted(objects, key=lambda x: (x["kind"], x["source_identifier"])):
            out.append(f'| `{o["source_identifier"]}` | {o["kind"]} | '
                       f'{o.get("detail") or "—"} | {o.get("language") or "—"} | '
                       f'{o.get("effort") or "—"} |')
        out.append("")

        out += ["## Why each kind cannot move, and where it would go", ""]
        seen: set[str] = set()
        for o in objects:
            if o["kind"] in seen:
                continue
            seen.add(o["kind"])
            out += [f'**{o["kind"]}** — {o["reason"]}', ""]
            if o.get("aidp_path"):
                out += [f'AIDP path: {o["aidp_path"]}', ""]

    if census.get("unreadable"):
        out += ["## Could not be read", "",
                "These counts are a floor, not a total.", ""]
        out += [f"- {n}" for n in census["unreadable"]] + [""]

    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Security posture. The only report here with an exposure consequence, so it
# leads with the finding rather than with the inventory.
# ---------------------------------------------------------------------------

def render_security(sec: dict) -> str:
    count = sec.get("exposure_count")
    exposures = sec.get("exposures") or []
    secure_views = sec.get("secure_views") or []

    out = ["# Security posture — what protects the data, and what arrives "
           "without it", "", sec["statement"], ""]

    if count is None:
        out += ["> The check that matters could not run. Everything below is "
                "partial, and **absence of a finding here is not evidence of "
                "absence**.", ""]
    elif count or secure_views:
        out += ["**This is a data-exposure regression, not a feature gap.** "
                "The objects below are created on AIDP either way — the clone "
                "does not fail, it succeeds without the protection. Anyone who "
                "can read the target table sees what Snowflake was hiding.", ""]

    if exposures:
        out += ["## Policies that do not travel", "",
                "| Object | Column | Policy | Kind | Severity |",
                "|---|---|---|---|---|"]
        for e in exposures:
            out.append(f'| `{e["object"]}` | '
                       f'{("`" + e["column"] + "`") if e.get("column") else "*whole table*"} | '
                       f'`{e["policy"]}` | {e["policy_kind"]} | **{e["severity"]}** |')
        out.append("")
        seen: set[str] = set()
        for e in exposures:
            if e["policy_kind"] in seen:
                continue
            seen.add(e["policy_kind"])
            out += [f'**{e["policy_kind"]}** — {e["consequence"]}', "",
                    e["aidp_path"], ""]

    if secure_views:
        out += ["## Secure views", "",
                "| View | Severity |", "|---|---|"]
        out += [f'| `{v["object"]}` | **{v["severity"]}** |' for v in secure_views]
        out += ["", secure_views[0]["consequence"], "",
                secure_views[0]["aidp_path"], ""]

    pol = sec.get("policies") or {}
    if pol:
        out += ["## Policy objects defined in the source", "",
                "Defined is not the same as attached — an unattached policy "
                "protects nothing, and an attached one is listed above.", "",
                "| Kind | Count | Read |", "|---|---:|---|"]
        for label, key in (("Masking", "masking"),
                           ("Row access", "row_access"), ("Tags", "tags")):
            info = pol.get(key) or {}
            c = info.get("count")
            out.append(f'| {label} | {c if c is not None else "*not measured*"} '
                       f'| {"yes" if info.get("readable") else "**denied**"} |')
        out.append("")

    grants = sec.get("grants") or {}
    if grants.get("measured"):
        by_obj = grants.get("by_object") or {}
        out += ["## Who can read what today", "",
                f"{len(by_obj)} in-scope object(s) carry explicit grants. "
                "**No grant is replayed on the target** — AIDP roles and "
                "per-resource permissions are a separate model, so access has "
                "to be re-granted deliberately rather than copied.", ""]
        if by_obj:
            out += ["| Object | Roles |", "|---|---|"]
            for ident, entries in sorted(by_obj.items()):
                roles = sorted({e["role"] for e in entries})
                shown = ", ".join(f"`{r}`" for r in roles[:6])
                if len(roles) > 6:
                    shown += f" …and {len(roles) - 6} more"
                out.append(f"| `{ident}` | {shown} |")
            out.append("")
    else:
        out += ["## Who can read what today", "",
                f'**Not measured** — {grants.get("note", "unknown")}. So the '
                "target cannot be checked against the source's access model.",
                ""]

    if sec.get("policy_references_out_of_scope"):
        out += [f'{sec["policy_references_out_of_scope"]} policy attachment(s) '
                "exist on objects outside this migration. Context only — not a "
                "regression this migration causes.", ""]

    if sec.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in sec["unreadable"]] + [""]

    out += ["---", "",
           "This plugin **changes nothing** here and generates no equivalent. "
           "AIDP has no masking API; the equivalent is a restricted view plus "
           "ontology sensitivity classification granted per role, which is a "
           "design decision rather than a translation."]
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Pre-flight: what WOULD happen, stated before the first write.
#
# Required behaviour, not a convenience. Once both ends are known and before
# anything is created, every source -> destination mapping is on the page. A
# migration that begins without the user having seen this is one they did not
# actually approve.
# ---------------------------------------------------------------------------

def render_preflight(plan: dict, *, source: dict | None = None,
                     target: dict | None = None) -> str:
    s = plan.get("summary") or {}
    can = plan.get("can_migrate") or []
    cannot = plan.get("cannot_migrate") or []
    schemas = plan.get("schemas_to_create") or []
    catalogs = plan.get("catalogs_to_create") or []
    jobs = plan.get("silver_gold_jobs") or []
    src = source or {}

    out = ["# Pre-flight — what this migration would do", ""]

    if target is None:
        out += ["> **No AIDP target was supplied, so nothing will be created.** "
                "Everything below is what *would* happen once the datalake "
                "OCID, workspace, cluster and catalog are given.", ""]
    else:
        out += ["Read this before approving. Nothing has been created yet.", ""]

    out += ["## Source → destination", "",
            "| | Source (Snowflake) | Destination (AIDP) |", "|---|---|---|"]
    unset = "*not supplied*"
    out += [
        f'| Account / DataLake | `{src.get("account", unset)}` | '
        f'`{(target or {}).get("datalake_ocid", unset)}` |',
        f'| Region | `{src.get("region", unset)}` | *derived from the OCID* |',
        f'| Role / workspace | `{src.get("role", unset)}` | '
        f'`{(target or {}).get("workspace", unset)}` |',
        f'| Scope / catalog | {", ".join(src.get("databases") or []) or unset} | '
        f'`{(target or {}).get("catalog", unset)}` |', ""]

    out += ["## Naming", "",
            "**Destination names are lower-cased.** AIDP folds identifiers, so "
            "a schema created as `TEST_DB` is stored as `test_db`. The targets "
            "below are the names the destination will really use — planning the "
            "unfolded name would show you something that then silently "
            "differs.", "",
            "Two source objects whose names differ only by case therefore fold "
            "into one, and the run **halts** rather than merging them.", ""]

    out += [f'## {len(can)} object(s) that would be created', ""]
    if can:
        out += ["| Source | | Destination | Type | Rows | Cols |",
                "|---|---|---|---|---:|---:|"]
        for c in can:
            rows = c.get("rows")
            out.append(
                f'| `{c["source_identifier"]}` | → | `{c["target"]}` | '
                f'{c.get("object_type", "?")} | '
                f'{rows if rows is not None else "—"} | '
                f'{c.get("columns", "—")} |')
        out.append("")

    if catalogs or schemas:
        parts = []
        if catalogs:
            parts.append(f'{len(catalogs)} catalog(s): '
                         + ", ".join(f"`{c}`" for c in catalogs))
        if schemas:
            parts.append(f'{len(schemas)} schema(s): '
                         + ", ".join(f"`{a}.{b}`" for a, b in schemas))
        out += ["## Structure that would be created first", "",
                " · ".join(parts), ""]

    out += ["## What would NOT happen", "",
            "- **No data moves.** Every table arrives with its columns and "
            "**zero rows**. This is a structural clone.",
            "- **Nothing is written to Snowflake.** The source is **read-only**, "
            "enforced at the transport, whatever the credential allows.",
            "- **Nothing existing is replaced or dropped.** An object that "
            "already exists with a different structure is reported and left "
            "exactly as found.", ""]
    if jobs:
        out.append(f"- **{len(jobs)} Silver/Gold job stub(s)** are defined, "
                   f"disabled and **never triggered**. Their bodies are a "
                   f"requirement still to define.")
        out.append("")

    if cannot:
        out += [f'## {len(cannot)} object(s) that would NOT be created', "",
                "| Object | Type | Why |", "|---|---|---|"]
        out += [f'| `{c["source_identifier"]}` | {c.get("object_type", "?")} | '
                f'{c.get("reason", "?")} |' for c in cannot]
        out.append("")

    out += ["---", "",
            "Nothing above has been executed. `deploy --execute` with the "
            "target coordinates is what applies it."]
    return "\n".join(out) + "\n"


def render_stages(board: dict) -> str:
    """The stage board as a table. Read the run before executing it."""
    rows = board.get("stages") or []
    out = ["# Stages — what runs, what has run, what it found", "",
           f'Artifacts read from `{board.get("out_dir")}`. This board makes no '
           f'decisions and touches nothing.', "",
           "**Every stage is read-only except `deploy`,** which is the only one "
           "that creates anything — and it is a dry run unless `--execute` is "
           "passed with the target coordinates.", "",
           "| Stage | Needs | Status | What it found |", "|---|---|---|---|"]
    for r in rows:
        mark = " ⚠️" if r.get("attention") else ""
        writes = " **(writes)**" if r.get("writes") else ""
        out.append(f'| `{r["stage"]}`{writes} | {r["needs"]} | '
                   f'{r["status"]}{mark} | {r["found"]} |')
    out.append("")

    attention = board.get("needs_attention") or []
    if attention:
        out += ["## Needs attention", "",
                "These stages found something, or could not look. A stage that "
                "**could not look is flagged, never shown as clean** — "
                "\"0 found\" and \"we could not read it\" are opposite "
                "findings.", ""]
        for r in rows:
            if r.get("attention"):
                out.append(f'- **`{r["stage"]}`** — {r["found"]}')
        out.append("")

    if board.get("next_stage"):
        out += [f'## Next: `{board["next_stage"]}`', "",
                next(f'{r["purpose"]}' for r in rows
                     if r["stage"] == board["next_stage"]), ""]
    else:
        out += ["## Every stage has run", ""]

    out += ["---", "", "Purposes:", ""]
    out += [f'- `{r["stage"]}` — {r["purpose"]}' for r in rows]
    return "\n".join(out) + "\n"
