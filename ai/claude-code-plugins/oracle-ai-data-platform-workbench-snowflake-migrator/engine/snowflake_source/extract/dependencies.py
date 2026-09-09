"""Dependency edges between estate objects. Read-only.

Dual-source on purpose. OBJECT_DEPENDENCIES lives in SNOWFLAKE.ACCOUNT_USAGE and
needs a grant a customer may refuse, so building the plan step on it alone would
make the whole feature hostage to a privilege.

  1. preferred -- SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
  2. fallback  -- parse the view DDL already captured during inventory

`source` is recorded per edge so a plan built on parsed DDL is never presented as
authoritative lineage.

Edge direction: {"from": dependent, "to": dependency}. `to` is created first.
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
                        known: set[str]) -> list[dict]:
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
        dependent, dependency = r["REFERENCING"], r["REFERENCED"]
        if dependent in known and dependency in known and dependent != dependency:
            edges.append({
                "from": dependent, "to": dependency,
                "kind": f'{r.get("REFERENCING_TYPE")}->{r.get("REFERENCED_TYPE")}',
                "source": "account_usage",
            })
    return edges


def extract_dependencies(run_sql: Callable[..., list[dict]],
                         inventory: dict) -> dict:
    records = inventory.get("inventory", [])
    known = {r["source_identifier"].upper() for r in records}

    try:
        edges = _from_account_usage(run_sql, known)
        return {"edges": edges, "source_used": "account_usage",
                "coverage_note": "ACCOUNT_USAGE.OBJECT_DEPENDENCIES: authoritative "
                                 "lineage for all object types",
                "unresolved_references": []}
    except Exception as exc:
        note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES unavailable ({exc}); fell back to "
                "parsing view DDL. Covers view->object edges ONLY -- not "
                "authoritative lineage.")

    edges, unresolved = [], set()
    for rec in records:
        if rec.get("object_type") != "VIEW":
            continue
        dependent = rec["source_identifier"].upper()
        for dependency in parse_view_references(
                rec.get("view_ddl_get_ddl") or rec.get("view_text_show") or "",
                default_db=rec["source_database"],
                default_schema=rec["source_schema"]):
            if dependency == dependent:
                continue
            if dependency in known:
                edges.append({"from": dependent, "to": dependency,
                              "kind": "VIEW->OBJECT", "source": "parsed_ddl"})
            else:
                unresolved.add(dependency)
    return {"edges": edges, "source_used": "parsed_ddl", "coverage_note": note,
            "unresolved_references": sorted(unresolved)}
