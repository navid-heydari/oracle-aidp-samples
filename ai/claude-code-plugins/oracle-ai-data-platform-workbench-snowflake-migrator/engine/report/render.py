"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

__all__ = ["render_inventory", "render_plan", "render_ddl_plan", "render_deploy"]


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


def render_plan(plan: dict) -> str:
    out = ["# Migration plan", "",
           f'Namespace strategy: **{plan.get("namespace_strategy")}**', ""]
    if plan.get("dependency_source"):
        out += [f'Lineage source: **{plan["dependency_source"]}** — '
                f'{plan.get("dependency_coverage_note") or ""}', ""]

    if plan.get("cycles"):
        out += ["## ⚠️ Dependency cycles", "",
                "These cannot be ordered and are excluded from the waves. They need a "
                "human decision, not an arbitrary broken edge.", ""]
        out += [f'- {", ".join(f"`{n}`" for n in c)}' for c in plan["cycles"]] + [""]

    out += ["## Waves", "",
            "Dependencies land before their dependents. Within a wave, smaller "
            "objects first.", ""]
    for i, wave in enumerate(plan.get("waves") or [], 1):
        out.append(f"### Wave {i} — {len(wave)} object(s)")
        out += [f'- `{n}` → `{plan.get("target_names", {}).get(n, "?")}` '
                f'[{plan.get("medallion_assignment", {}).get(n, "?")}]' for n in wave]
        out.append("")

    fallbacks = plan.get("fallback_assignments") or []
    if fallbacks:
        out += ["## Layer assigned by fallback — confirm before proceeding", "",
                "No naming rule matched these, so they defaulted to BRONZE. "
                "Override with an explicit mapping if that is wrong.", ""]
        out += [f"- `{n}`" for n in fallbacks] + [""]

    out += [f'## Clone targets — {len(plan.get("clone_targets") or [])} table(s)', "",
            "MVP-1 creates empty managed Delta tables. Views are inventoried and "
            "ordered but not translated.", ""]

    if plan.get("blocked"):
        out += ["## Blocked", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in plan["blocked"]]
        out.append("")
    if plan.get("unsupported"):
        out += ["## Unsupported features", ""]
        out += [f'- `{u["source_identifier"]}` — **{u["feature"]}**: {u["resolution"]}'
                for u in plan["unsupported"]]
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


def render_deploy(res: dict) -> str:
    if res.get("dry_run"):
        return ("# Deployment — DRY RUN\n\n"
                f'{res.get("statement_count", 0)} statement(s) would run; '
                "**nothing was created**.\n\nRe-run with `--execute` and the AIDP "
                "target coordinates to apply.\n")
    total = res.get("statement_count", 0)
    out = ["# Deployment result", "",
           f'Executed {res.get("executed", 0)}/{total} · '
           f'**verified {res.get("verified", 0)}/{total}**', "",
           "Verification probes each object individually: a batch can report success "
           "while statements inside it failed.", ""]
    if res.get("blocked_count"):
        out += [f'{res["blocked_count"]} object(s) were blocked and never attempted.',
                ""]
    if res.get("failed"):
        out += ["## Failed", ""]
        out += [f'- `{f["target_fqn"]}` — {f["reason"]}' for f in res["failed"]] + [""]
    if res.get("chunk_errors"):
        out += ["## Batch errors", ""] + [f"- {e}" for e in res["chunk_errors"]]
    return "\n".join(out) + "\n"
