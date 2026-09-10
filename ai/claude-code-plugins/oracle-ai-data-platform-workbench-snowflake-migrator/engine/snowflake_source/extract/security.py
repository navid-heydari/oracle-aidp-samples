"""What protects the data today, and what arrives without it. Read-only.

The only gap in this plugin with a data-EXPOSURE consequence. A Snowflake
column carrying a masking policy lands on AIDP as plain Delta with no policy,
so values that were masked for most roles become readable by anyone who can
read the table. Same for a row access policy: the row filter simply is not
there. Nothing in the plugin said so, and the properties were being scrubbed
silently.

REPORTS; CHANGES NOTHING, and generates no equivalent. AIDP's answer is
restricted views plus ontology sensitivity -- there is no masking REST API --
so a mechanical translation is not available even in principle, and a
half-right one would be worse than a stated gap.

The dangerous failure mode here is a FALSE NEGATIVE. If ACCOUNT_USAGE cannot be
read we cannot prove the absence of policies, so `exposure_count` is None and
the statement says the question is unanswered. It must never read as "no
policies found".
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer

__all__ = ["build_security", "POLICY_CONSEQUENCE"]

POLICY_CONSEQUENCE = {
    "MASKING_POLICY": (
        "the column arrives UNMASKED. Every role that can read the target "
        "table sees the raw value, including values Snowflake was masking for "
        "most roles."),
    "ROW_ACCESS_POLICY": (
        "the row filter is not carried over, so the target table exposes ALL "
        "rows to every role that can read it, not the subset each role could "
        "see in Snowflake."),
    "AGGREGATION_POLICY": (
        "the aggregation constraint is not carried over, so queries that "
        "Snowflake would have forced to aggregate can return individual rows."),
    "PROJECTION_POLICY": (
        "the projection constraint is not carried over, so columns Snowflake "
        "prevented from being selected become selectable."),
}

_GENERIC_CONSEQUENCE = (
    "the policy is not carried over, so whatever it restricted is "
    "unrestricted on the target.")

_AIDP_PATH = (
    "AIDP has no masking API. The equivalent is a restricted view over the "
    "table plus ontology sensitivity classification, granted per role -- a "
    "design decision, not a translation.")


def _show(run_sql, what: str, db: str) -> list[dict]:
    return run_sql(f"show {what} in database {lexer.qualify(db)}")


def _collect(run_sql, what: str, databases: list[str],
             notes: list[str]) -> dict:
    items: list[dict] = []
    readable = True
    note = ""
    for db in databases:
        try:
            for row in _show(run_sql, what, db):
                items.append({
                    "name": row.get("name"),
                    "database": row.get("database_name") or db,
                    "schema": row.get("schema_name"),
                    "kind": row.get("kind") or what,
                })
        except Exception as exc:
            readable = False
            note = str(exc)[:200]
            notes.append(f"SHOW {what.upper()} in {db}: {note}")
    return {"readable": readable, "note": note or f"{len(items)} found",
            "count": len(items) if readable else None, "items": items}


def build_security(run_sql: Callable[..., list[dict]], inventory: dict, *,
                   include_grants: bool = True) -> dict:
    notes: list[str] = []
    databases = inventory.get("databases_in_scope") or []
    records = inventory.get("inventory") or []
    in_scope = {r["source_identifier"].upper() for r in records}

    policies = {
        "masking": _collect(run_sql, "masking policies", databases, notes),
        "row_access": _collect(run_sql, "row access policies", databases, notes),
        "tags": _collect(run_sql, "tags", databases, notes),
    }

    # The load-bearing query: which objects and columns actually have a policy
    # attached. Policies existing is not the risk; policies ATTACHED to
    # something we are migrating is.
    exposures: list[dict] = []
    out_of_scope = 0
    references_readable = True
    try:
        refs = run_sql(
            "select ref_database_name REF_DATABASE_NAME, "
            "ref_schema_name REF_SCHEMA_NAME, ref_entity_name REF_ENTITY_NAME, "
            "ref_column_name REF_COLUMN_NAME, policy_kind POLICY_KIND, "
            "policy_name POLICY_NAME "
            "from snowflake.account_usage.policy_references")
    except Exception as exc:
        references_readable = False
        refs = []
        notes.append(f"ACCOUNT_USAGE.POLICY_REFERENCES: {str(exc)[:200]}")

    for r in refs:
        ident = (f'{r.get("REF_DATABASE_NAME")}.{r.get("REF_SCHEMA_NAME")}.'
                 f'{r.get("REF_ENTITY_NAME")}')
        kind = str(r.get("POLICY_KIND") or "").upper()
        if ident.upper() not in in_scope:
            out_of_scope += 1
            continue
        exposures.append({
            "object": ident,
            "column": r.get("REF_COLUMN_NAME"),
            "policy": r.get("POLICY_NAME"),
            "policy_kind": kind,
            "severity": "HIGH",
            "consequence": POLICY_CONSEQUENCE.get(kind, _GENERIC_CONSEQUENCE),
            "aidp_path": _AIDP_PATH,
        })

    # A secure view's whole point is hiding its definition and restricting
    # row visibility. Neither survives, and the view still gets created.
    secure_views = [
        {"object": r["source_identifier"], "severity": "HIGH",
         "consequence": "SECURE was not carried over. The target view's "
                        "definition is visible and Snowflake's secure-view "
                        "row-visibility guarantees do not apply.",
         "aidp_path": _AIDP_PATH}
        for r in records
        if r.get("object_type") == "VIEW"
        and str((r.get("source_metadata") or {}).get("is_secure", "")).lower()
        in ("true", "y", "yes", "on")]

    grants = {"measured": False, "by_object": {}, "note": "not requested"}
    if include_grants:
        grants = _grants(run_sql, in_scope, notes)

    count = None if not references_readable else len(exposures)
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "policies": policies,
        "policy_references_readable": references_readable,
        "policy_references_out_of_scope": out_of_scope,
        "exposures": exposures,
        "exposure_count": count,
        "secure_views": secure_views,
        "grants": grants,
        "unreadable": notes,
        "statement": _statement(count, secure_views, references_readable),
    }


def _grants(run_sql, in_scope: set[str], notes: list[str]) -> dict:
    """Who can read what today. Reported, never replayed."""
    try:
        rows = run_sql(
            "select name NAME, table_schema TABLE_SCHEMA, "
            "table_catalog DATABASE_NAME, privilege PRIVILEGE, "
            "grantee_name GRANTEE_NAME, count(*) GRANTS "
            "from snowflake.account_usage.grants_to_roles "
            "where deleted_on is null and granted_on in "
            "('TABLE', 'VIEW', 'MATERIALIZED_VIEW') "
            "group by 1, 2, 3, 4, 5")
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.GRANTS_TO_ROLES (grants): {str(exc)[:200]}")
        return {"measured": False, "by_object": {},
                "note": str(exc)[:200]}

    by_object: dict[str, list[dict]] = collections.defaultdict(list)
    for r in rows:
        ident = (f'{r.get("DATABASE_NAME")}.{r.get("TABLE_SCHEMA")}.'
                 f'{r.get("NAME")}')
        if ident.upper() not in in_scope:
            continue
        by_object[ident].append({"role": r.get("GRANTEE_NAME"),
                                 "privilege": r.get("PRIVILEGE")})
    return {"measured": True, "by_object": dict(by_object),
            "note": f"{len(by_object)} in-scope object(s) with explicit grants",
            "carried_over": False}


def _statement(count, secure_views: list[dict], references_readable: bool) -> str:
    if not references_readable:
        return ("**Policy attachments could not be read**, so whether any "
                "column is masked or any table row-filtered is UNKNOWN. This "
                "is not the same as finding none: grant "
                "`SNOWFLAKE.ACCOUNT_USAGE` and re-run before anyone concludes "
                "the estate is unprotected data.")
    parts = []
    if count:
        parts.append(
            f"**{count} exposure(s):** that many masked columns or "
            f"row-filtered tables are being migrated, and the protection does "
            f"not travel with them. Values masked in Snowflake arrive readable")
    if secure_views:
        parts.append(f"**{len(secure_views)} secure view(s)** lose SECURE")
    if not parts:
        return ("No masking, row-access, aggregation or projection policy is "
                "attached to anything being migrated, and no secure views are "
                "in scope. Nothing is protected today that the migration "
                "would strip.")
    return " · ".join(parts) + ". Resolve before the clone is used for anything real."
