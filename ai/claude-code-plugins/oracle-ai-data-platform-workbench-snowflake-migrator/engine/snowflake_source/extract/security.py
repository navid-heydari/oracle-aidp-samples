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

The dangerous failure mode here is a FALSE NEGATIVE, and it has two sources.
If ACCOUNT_USAGE cannot be read we cannot prove the absence of policies, so
`exposure_count` is None and the statement says the question is unanswered.
And ACCOUNT_USAGE.POLICY_REFERENCES -- the only attachment source -- lags up
to ~2 hours, so a policy attached shortly before the run is not in it yet
while SHOW MASKING/ROW ACCESS POLICIES already lists the policy object. When
policy objects exist and no attachment is visible, the verdict is
UNCONFIRMED, not clean. Neither case may read as "no policies found".

There is a third source, and it is the worst one: naming a policy kind the
tool never enumerated. The summary sentence used to state that no masking,
row-access, AGGREGATION or PROJECTION policy was attached while the last two
were never asked for, and the staleness tripwire summed only masking +
row_access, so a defined-but-unattached aggregation policy could not raise
UNCONFIRMED. All four kinds are now enumerated, all four feed the tripwire,
and the sentence is built from the kinds that actually answered -- a kind
whose SHOW was denied is named as "not visible to this role", never counted
as zero and never covered by a clean verdict.

Tag attachments (ACCOUNT_USAGE.TAG_REFERENCES) and grants beyond
TABLE / VIEW / MATERIALIZED_VIEW are read for the same reason: a tag count
with no attachment list is a number with no verdict, and an access model
missing every schema, database, warehouse, stage, procedure and function
grant cannot be reconstructed on the target. Both are reported; neither is
replayed.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer

__all__ = ["build_security", "POLICY_CONSEQUENCE", "POLICY_KINDS",
           "POLICY_KIND_LABELS", "GRANT_CLASSES"]

# The policy kinds this module enumerates as OBJECTS, in report order. The
# summary sentence is built from this tuple and from which of them actually
# answered, so it can never name a kind that was not asked for.
POLICY_KINDS = ("masking", "row_access", "aggregation", "projection")

POLICY_KIND_LABELS = {
    "masking": "masking",
    "row_access": "row-access",
    "aggregation": "aggregation",
    "projection": "projection",
}

# The SHOW behind each kind. All four are on the read-only allowlist.
_POLICY_SHOW = {
    "masking": "masking policies",
    "row_access": "row access policies",
    "aggregation": "aggregation policies",
    "projection": "projection policies",
}

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

# Where attachments come from, and how stale that can be. Named in the
# artifact so a clean verdict carries its own caveat.
_ATTACHMENT_SOURCE = ("SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES (lags up to "
                      "~2 hours behind DDL)")

# Tag attachments have their own view and its own identical lag. Counting tag
# OBJECTS without reading attachments made a classification-driven governance
# model look empty, which is the same false negative as above.
_TAG_ATTACHMENT_SOURCE = ("SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES (lags up to "
                          "~2 hours behind DDL)")

_TAG_CONSEQUENCE = (
    "the tag and its value do not travel, so anything keyed off this "
    "classification -- a policy, an access rule, an audit query -- has "
    "nothing to key off on the target.")

_TAG_AIDP_PATH = (
    "AIDP has no tag API verified here. The nearest equivalent is ontology "
    "sensitivity classification applied per object, which is a design "
    "decision rather than a translation.")

# What GRANTS_TO_ROLES.granted_on is asked for. Reading only TABLE / VIEW /
# MATERIALIZED_VIEW made every grant on a schema, database, warehouse, stage,
# procedure or function invisible, so the target access model could not be
# reconstructed from the report. Reported, never replayed, either way.
GRANT_CLASSES = (
    "TABLE", "VIEW", "MATERIALIZED_VIEW", "EXTERNAL_TABLE", "DYNAMIC_TABLE",
    "DATABASE", "SCHEMA", "WAREHOUSE", "STAGE", "PROCEDURE", "FUNCTION",
    "FILE_FORMAT", "SEQUENCE", "STREAM", "TASK", "PIPE", "TAG",
    "MASKING_POLICY", "ROW_ACCESS_POLICY", "AGGREGATION_POLICY",
    "PROJECTION_POLICY", "INTEGRATION",
)

# Classes that have no database of their own, so a scope filter on
# DATABASE_NAME would silently drop them.
_ACCOUNT_SCOPED_CLASSES = ("WAREHOUSE", "INTEGRATION")


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
    if readable and not items:
        # SHOW is privilege-filtered, so this zero is a statement about the
        # role. The verdict that matters comes from ACCOUNT_USAGE, which is
        # account-wide; this table is context and must not read as proof.
        note = (f"0 visible to the current role: SHOW {what.upper()} lists "
                f"only objects the role owns or holds a privilege on. The "
                f"attachment verdict comes from ACCOUNT_USAGE and is "
                f"account-wide.")
    elif not readable:
        # The count is None, and the words have to agree with it: a refused
        # SHOW means "not visible to this role", which is not a zero.
        note = (f"not visible to this role: SHOW {what.upper()} was refused "
                f"-- {note}")
    return {"readable": readable, "note": note or f"{len(items)} found",
            "count": len(items) if readable else None, "items": items}


def build_security(run_sql: Callable[..., list[dict]], inventory: dict, *,
                   include_grants: bool = True) -> dict:
    notes: list[str] = []
    databases = inventory.get("databases_in_scope") or []
    records = inventory.get("inventory") or []
    in_scope = {r["source_identifier"].upper() for r in records}

    policies = {k: _collect(run_sql, _POLICY_SHOW[k], databases, notes)
                for k in POLICY_KINDS}
    policies["tags"] = _collect(run_sql, "tags", databases, notes)

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

    # Tag OBJECTS were counted and their attachments never read, so a
    # classification-driven governance model rendered as an empty table. Same
    # unreadable handling and same latency caveat as POLICY_REFERENCES.
    tag_references = _tag_references(run_sql, in_scope, notes)

    grants = {"measured": False, "by_object": {}, "note": "not requested",
              "classes_requested": list(GRANT_CLASSES)}
    if include_grants:
        grants = _grants(run_sql, in_scope, databases, notes)

    # Policy OBJECTS that exist while POLICY_REFERENCES shows no attachment
    # anywhere. The view lags up to ~2 hours, so this is the signature of a
    # policy attached shortly before the run -- the empty attachment list is
    # then unconfirmed, not clean. A reference to an out-of-scope object
    # accounts for the policy and does not trigger it.
    defined = sum((policies[k]["count"] or 0) for k in POLICY_KINDS)
    enumerated = [POLICY_KIND_LABELS[k] for k in POLICY_KINDS
                  if policies[k]["readable"]]
    unenumerated = [POLICY_KIND_LABELS[k] for k in POLICY_KINDS
                    if not policies[k]["readable"]]
    unattached = (defined if (references_readable and not exposures
                              and out_of_scope == 0) else 0)

    count = None if not references_readable else len(exposures)
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "policies": policies,
        "policy_kinds_enumerated": enumerated,
        "policy_kinds_unenumerated": unenumerated,
        "policy_references_readable": references_readable,
        "policy_references_out_of_scope": out_of_scope,
        "policies_defined_without_attachment": unattached,
        "attachment_source": _ATTACHMENT_SOURCE,
        "exposures": exposures,
        "exposure_count": count,
        "secure_views": secure_views,
        "tag_references": tag_references,
        "grants": grants,
        "unreadable": notes,
        "statement": _statement(count, secure_views, references_readable,
                                unattached, kinds_enumerated=enumerated,
                                kinds_unenumerated=unenumerated),
    }


def _tag_references(run_sql, in_scope: set[str], notes: list[str]) -> dict:
    """Which migrated objects and columns carry a tag. Reported, never replayed.

    A tag count with no attachment list is a number with no verdict. This
    mirrors the POLICY_REFERENCES block exactly, including the rule that an
    unreadable view yields `measured: False` and a `count` of None, never 0.
    """
    try:
        rows = run_sql(
            "select tag_database TAG_DATABASE, tag_schema TAG_SCHEMA, "
            "tag_name TAG_NAME, tag_value TAG_VALUE, "
            "object_database OBJECT_DATABASE, object_schema OBJECT_SCHEMA, "
            "object_name OBJECT_NAME, column_name COLUMN_NAME, "
            "domain DOMAIN "
            "from snowflake.account_usage.tag_references")
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.TAG_REFERENCES (tag attachments): "
                     f"{str(exc)[:200]}")
        return {"measured": False, "attachments": [], "count": None,
                "out_of_scope": 0, "carried_over": False,
                "source": _TAG_ATTACHMENT_SOURCE, "note": str(exc)[:200]}

    attachments: list[dict] = []
    out_of_scope = 0
    for r in rows:
        ident = (f'{r.get("OBJECT_DATABASE")}.{r.get("OBJECT_SCHEMA")}.'
                 f'{r.get("OBJECT_NAME")}')
        if ident.upper() not in in_scope:
            out_of_scope += 1
            continue
        attachments.append({
            "object": ident,
            "column": r.get("COLUMN_NAME"),
            "domain": r.get("DOMAIN"),
            "tag": (f'{r.get("TAG_DATABASE")}.{r.get("TAG_SCHEMA")}.'
                    f'{r.get("TAG_NAME")}'),
            "value": r.get("TAG_VALUE"),
            "consequence": _TAG_CONSEQUENCE,
            "aidp_path": _TAG_AIDP_PATH,
        })
    return {"measured": True, "attachments": attachments,
            "count": len(attachments), "out_of_scope": out_of_scope,
            "carried_over": False, "source": _TAG_ATTACHMENT_SOURCE,
            "note": f"{len(attachments)} in-scope tag attachment(s)"}


def _grants(run_sql, in_scope: set[str], databases: list[str],
            notes: list[str]) -> dict:
    """Who can read what today. Reported, never replayed.

    `by_object` stays what it always was: the migrated tables and views and
    the roles that hold a privilege on them. `by_class` is everything else in
    the scoped databases -- schema, database, warehouse, stage, procedure,
    function and the rest of GRANT_CLASSES -- which used to be read as
    nothing at all. The report names the classes that were asked for, so a
    class that is absent from the answer is distinguishable from a class that
    was never in the question.
    """
    classes = ", ".join(f"'{c}'" for c in GRANT_CLASSES)
    try:
        rows = run_sql(
            "select name NAME, table_schema TABLE_SCHEMA, "
            "table_catalog DATABASE_NAME, granted_on GRANTED_ON, "
            "privilege PRIVILEGE, grantee_name GRANTEE_NAME, count(*) GRANTS "
            "from snowflake.account_usage.grants_to_roles "
            f"where deleted_on is null and granted_on in ({classes}) "
            "group by 1, 2, 3, 4, 5, 6")
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.GRANTS_TO_ROLES (grants): {str(exc)[:200]}")
        return {"measured": False, "by_object": {}, "by_class": {},
                "classes_requested": list(GRANT_CLASSES), "out_of_scope": 0,
                "note": str(exc)[:200]}

    scope = {str(d).upper() for d in databases if d}
    by_object: dict[str, list[dict]] = collections.defaultdict(list)
    classed: dict[str, dict] = {}
    out_of_scope = 0
    for r in rows:
        granted_on = str(r.get("GRANTED_ON") or "").upper() or "UNSPECIFIED"
        db = r.get("DATABASE_NAME")
        ident = ".".join(str(p) for p in (db, r.get("TABLE_SCHEMA"),
                                          r.get("NAME")) if p)
        if ident.upper() in in_scope:
            by_object[ident].append({"role": r.get("GRANTEE_NAME"),
                                     "privilege": r.get("PRIVILEGE")})
        if not _grant_in_scope(granted_on, db, r.get("NAME"), scope):
            out_of_scope += 1
            continue
        entry = classed.setdefault(
            granted_on, {"grants": 0, "roles": set(), "objects": set()})
        entry["grants"] += 1
        entry["roles"].add(r.get("GRANTEE_NAME"))
        entry["objects"].add(ident)
    by_class = {k: {"grants": v["grants"],
                    "roles": sorted(x for x in v["roles"] if x),
                    "objects": len(v["objects"])}
                for k, v in sorted(classed.items())}
    return {"measured": True, "by_object": dict(by_object),
            "by_class": by_class,
            "classes_requested": list(GRANT_CLASSES),
            "out_of_scope": out_of_scope,
            "note": f"{len(by_object)} in-scope object(s) with explicit "
                    f"grants; {len(by_class)} object class(es) seen",
            "carried_over": False}


def _grant_in_scope(granted_on: str, db, name, scope: set[str]) -> bool:
    """Is this grant about something inside the migration scope?

    A DATABASE grant names the database in NAME, not in TABLE_CATALOG, and a
    WAREHOUSE or INTEGRATION grant has no database at all -- filtering on
    TABLE_CATALOG alone would drop both classes silently.
    """
    if not scope:
        return True
    if granted_on == "DATABASE":
        return str(name or "").upper() in scope
    if db:
        return str(db).upper() in scope
    return granted_on in _ACCOUNT_SCOPED_CLASSES


_LAG = ("ACCOUNT_USAGE.POLICY_REFERENCES lags up to ~2 hours behind DDL, so "
        "a policy attached inside that window is not visible here yet")


def _join(labels) -> str:
    labels = list(labels)
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + f" or {labels[-1]}"


def _statement(count, secure_views: list[dict], references_readable: bool,
               defined_without_attachment: int = 0, *,
               kinds_enumerated=(), kinds_unenumerated=()) -> str:
    """The one sentence a reader takes away. It may only name what was asked.

    I3: "could not look" never renders as zero. The clean verdict is built
    from the kinds that actually answered, so a kind whose SHOW was denied
    cannot be covered by it -- it gets said out loud instead.
    """
    kinds_enumerated = list(kinds_enumerated)
    kinds_unenumerated = list(kinds_unenumerated)
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
    if defined_without_attachment:
        parts.append(
            f"**{defined_without_attachment} policy object(s) "
            f"({_join(kinds_enumerated) or 'no kind enumerated'}) exist in "
            f"the migrated database(s) but ACCOUNT_USAGE.POLICY_REFERENCES "
            f"lists no attachment.** {_LAG}. Treat exposure as UNCONFIRMED, "
            f"not zero: re-run `security` after the lag, or check the "
            f"attachments live (`DESCRIBE MASKING POLICY`, the "
            f"INFORMATION_SCHEMA POLICY_REFERENCES table function) before the "
            f"clone is used")
    if kinds_unenumerated:
        # Naming the kind is the whole point: a verdict that silently covers
        # a policy kind whose SHOW was denied is the clean-verdict-on-an-
        # unasked-question failure this module exists to prevent.
        parts.append(
            f"**{_join(kinds_unenumerated)} policy objects could not be "
            f"enumerated** (the SHOW was denied to this role, so they are "
            f"*not visible to this role* rather than absent). Nothing here "
            f"covers them, and the empty attachment list cannot be "
            f"corroborated for those kinds. {_LAG}. Re-run with a role that "
            f"can see them before anyone concludes the estate is unprotected "
            f"data")
    if not parts:
        return (f"No {_join(kinds_enumerated)} policy is attached to anything "
                "being migrated, and no secure views are in scope. Nothing is "
                "protected today that the migration would strip -- as of "
                f"{_LAG}.")
    return " · ".join(parts) + ". Resolve before the clone is used for anything real."
