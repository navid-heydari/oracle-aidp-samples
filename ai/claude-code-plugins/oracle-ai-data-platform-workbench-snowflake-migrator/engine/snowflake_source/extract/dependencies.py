"""Dependency edges between estate objects. Read-only.

Dual-source on purpose. OBJECT_DEPENDENCIES lives in SNOWFLAKE.ACCOUNT_USAGE and
needs a grant a customer may refuse, so building the plan step on it alone would
make the whole feature hostage to a privilege.

  1. preferred -- SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
  2. fallback  -- parse the view DDL already captured during inventory

The two are also MERGED. OBJECT_DEPENDENCIES lags DDL by up to ~3 hours, so
a view created or altered shortly before the run has no row there yet while
the query itself succeeds. A view with no ACCOUNT_USAGE edge is parsed from
its DDL instead, and `source_used` says so: `account_usage_empty` when the
view returned nothing within the inventory, `account_usage+parsed_ddl` when
it covered some views but not others. A readable-but-empty result is never
presented as authoritative lineage.

`source` is recorded per edge so a plan built on parsed DDL is never presented as
authoritative lineage.

Edge direction: {"from": dependent, "to": dependency}. `to` is created first.
Endpoints are the exact inventory `source_identifier` values, so they match
the plan's nodes: a quoted identifier keeps its case in Snowflake, and an
edge spelled any other way is silently dropped by the wave computation.
"""
from __future__ import annotations

import re
from typing import Callable

__all__ = ["extract_dependencies", "parse_view_references"]

_IDENT = r'[A-Za-z_][A-Za-z0-9_$]*|"[^"]+"'
_REF = re.compile(
    rf'\b(?:FROM|JOIN)\s+((?:{_IDENT})(?:\s*\.\s*(?:{_IDENT})){{0,2}})',
    re.IGNORECASE)


def parse_view_references(ddl: str, *, default_db: str,
                          default_schema: str) -> list[str]:
    """Extract fully-qualified referenced object names from a view body."""
    if not ddl:
        return []
    found: set[str] = set()
    for match in _REF.finditer(ddl):
        parts = [p.strip().strip('"') for p in match.group(1).split(".")]
        parts = [p for p in parts if p]
        if not parts:
            continue
        if len(parts) == 1:
            parts = [default_db, default_schema, parts[0]]
        elif len(parts) == 2:
            parts = [default_db, parts[0], parts[1]]
        found.add(".".join(p.upper() for p in parts))
    return sorted(found)


def _from_account_usage(run_sql: Callable[..., list[dict]],
                        by_upper: dict[str, str]) -> list[dict]:
    rows = run_sql(
        "select referencing_database || '.' || referencing_schema || '.' || "
        "       referencing_object_name as REFERENCING, "
        "       referenced_database || '.' || referenced_schema || '.' || "
        "       referenced_object_name as REFERENCED, "
        "       referencing_object_domain as REFERENCING_TYPE, "
        "       referenced_object_domain as REFERENCED_TYPE "
        "from snowflake.account_usage.object_dependencies")
    edges = []
    for r in rows:
        # Matched case-insensitively, emitted in the inventory's spelling.
        dependent = by_upper.get(str(r.get("REFERENCING") or "").upper())
        dependency = by_upper.get(str(r.get("REFERENCED") or "").upper())
        if dependent and dependency and dependent != dependency:
            edges.append({
                "from": dependent, "to": dependency,
                "kind": f'{r.get("REFERENCING_TYPE")}->{r.get("REFERENCED_TYPE")}',
                "source": "account_usage",
            })
    return edges


_AUTHORITATIVE_NOTE = ("ACCOUNT_USAGE.OBJECT_DEPENDENCIES: authoritative "
                       "lineage for all object types")


def _from_parsed_ddl(records: list[dict],
                     by_upper: dict[str, str]) -> tuple[list[dict], set[str]]:
    """view->object edges parsed from the DDL captured at inventory time."""
    edges: list[dict] = []
    unresolved: set[str] = set()
    for rec in records:
        if rec.get("object_type") != "VIEW":
            continue
        dependent = rec["source_identifier"]
        for ref in parse_view_references(
                rec.get("view_ddl_get_ddl") or rec.get("view_text_show") or "",
                default_db=rec["source_database"],
                default_schema=rec["source_schema"]):
            dependency = by_upper.get(ref)
            if dependency is None:
                unresolved.add(ref)
            elif dependency != dependent:
                edges.append({"from": dependent, "to": dependency,
                              "kind": "VIEW->OBJECT", "source": "parsed_ddl"})
    return edges, unresolved


def extract_dependencies(run_sql: Callable[..., list[dict]],
                         inventory: dict) -> dict:
    records = inventory.get("inventory", [])
    # Upper-cased name -> the inventory's exact spelling. Unambiguous because
    # the inventory HALTs on identifier-case collisions before this stage
    # runs (snowmig.py); were that gate ever bypassed, this map would be
    # last-wins and could attach an edge to the wrong twin.
    by_upper = {r["source_identifier"].upper(): r["source_identifier"]
                for r in records}
    views = [r for r in records if r.get("object_type") == "VIEW"]

    try:
        au_edges = _from_account_usage(run_sql, by_upper)
    except Exception as exc:
        note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES unavailable ({exc}); fell back to "
                "parsing view DDL. Covers view->object edges ONLY -- not "
                "authoritative lineage.")
        edges, unresolved = _from_parsed_ddl(views, by_upper)
        return {"edges": edges, "source_used": "parsed_ddl", "coverage_note": note,
                "unresolved_references": sorted(unresolved), "warning": None}

    covered = {e["from"] for e in au_edges}
    uncovered = [r for r in views if r["source_identifier"] not in covered]
    if not uncovered:
        return {"edges": au_edges, "source_used": "account_usage",
                "coverage_note": _AUTHORITATIVE_NOTE,
                "unresolved_references": [],
                "views_without_account_usage_edge": [], "warning": None}

    # Readable, but not populated for these views. The query succeeding is
    # not evidence that the lineage is complete: a freshly prepared estate
    # lands here, and so does a view whose only references lie outside the
    # inventory. Parse what DDL we hold and say plainly what is still
    # unordered, so the plan cannot imply an order it does not have.
    parsed, unresolved = _from_parsed_ddl(uncovered, by_upper)
    missing = sorted(r["source_identifier"] for r in uncovered)
    ordered_by_parse = {e["from"] for e in parsed}
    unordered = [i for i in missing if i not in ordered_by_parse]
    source_used = ("account_usage_empty" if not au_edges
                   else "account_usage+parsed_ddl")
    note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES was readable but returned no "
            f"edge within the inventory for {len(missing)} of {len(views)} "
            f"view(s). The view lags DDL by up to ~3 hours, so a view created "
            f"or altered shortly before this run is not in it yet. Their DDL "
            f"was parsed instead (view->object edges only) -- NOT "
            f"authoritative lineage for those views.")
    warning = (f"{len(missing)} view(s) have no ACCOUNT_USAGE lineage edge "
               f"(the view lags DDL by up to ~3 h); their DDL was parsed instead")
    if unordered:
        warning += (f". {len(unordered)} still have no edge from either source "
                    f"and are ordered by size only: {', '.join(unordered)}")
    warning += ". Re-run `deps` after the lag before relying on the wave order."
    return {"edges": au_edges + parsed, "source_used": source_used,
            "coverage_note": note, "unresolved_references": sorted(unresolved),
            "views_without_account_usage_edge": missing, "warning": warning}
