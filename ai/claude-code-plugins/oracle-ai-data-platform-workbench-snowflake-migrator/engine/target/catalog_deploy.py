"""Deploy structure through the AIDP catalog API. I/O injected as `call`.

The SQL transport cannot be used: `POST /workspaces/<ws>/sql/execute` returns
404 against a live DataLake. This path creates schemas, then tables, then
views through the catalog CRUD API instead, which needs no Spark cluster and
has no session to lose DDL to.

Shape of the run:
  0. resolve the target catalog's TYPE and refuse EXTERNAL -- managed Delta
     cannot live in a read-only pointer, and the creates would 202-and-vanish
  1. create each distinct schema once
  2. create tables, then views -- a view's fields reference its base tables
  3. READ EACH OBJECT BACK and compare its field list to the plan

Step 3 is the point. As with the SQL path, "the create returned 200" is not the
claim; "the planned columns are there" is. A create that fails because the
object already exists is NOT a failure -- the read-back decides, because an
object that already matches the plan is indistinguishable from one we made.

TWO BEHAVIOURS LEARNED FROM A LIVE RUN, both of which broke the first attempt:

  * AIDP LOWER-CASES IDENTIFIERS. A schema created as `SNOWMIG_TESTDB`
    comes back as `lake.snowmig_testdb`. Every `schemaKey` and every
    read-back key built from the REQUESTED case was wrong, and all seven
    objects reported failed while the schema had in fact been created. So keys
    are RESOLVED from the server by listing and matching case-insensitively,
    never assumed. Field names fold too, so field comparison is
    case-insensitive -- a case fold is not a structure mismatch.

  * CREATION IS ASYNCHRONOUS AND CAN FAIL SILENTLY. POST returns 202 Accepted
    with an empty body; the object appears seconds later, or never if the async
    work fails -- and when it fails NOTHING reports it. So the read-back polls
    with a bounded backoff, and an object that never appears is a failure whose
    reason says exactly that. This is why "the create returned 2xx" cannot be
    the claim: six tables once returned 202 and not one of them existed.

  * A VIEW'S COLUMN TYPES ARE DERIVED BY THE TARGET, NOT DECLARED BY US.
    Snowflake reports a view's DECLARED output types; AIDP re-derives them from
    the SQL. Live, three aggregate columns of a 17-column view came back
    different: COUNT(*) as bigint rather than decimal(18,0), and two SUMs
    narrowed by two digits. That is a fidelity finding worth reporting loudly --
    a narrowing can overflow -- but it is NOT "someone else's object that we
    left alone", which is what a table mismatch means. The two are reported
    separately.

  * THE LIST RESPONSE OMITS FIELDS. Listing gives existence and the server's
    real key case; it carries no `tableFields` at all. Six tables were created
    correctly, as managed DELTA with the right types, and every one was
    reported a MISMATCH because the plan was compared against a fieldless list
    entry. So existence comes from the list and STRUCTURE comes from a GET on
    the resolved key.

  * DO NOT RE-CREATE AN EXISTING SCHEMA. POSTing a schema that is already
    there re-triggers async work, and table creates issued during that window
    are ACCEPTED (202) and then silently dropped. Six tables returned 202 and
    none appeared, while the identical bodies posted against a settled schema
    all landed. So the schema is resolved FIRST, created only if absent, and
    waited on until ACTIVE. A listing that FAILS refuses the deploy before
    anything is written; it is never read as "absent".

  * SCHEMA CREATION IS ASYNCHRONOUS. Creating a table immediately afterwards
    can return 409 Conflict "ongoing operation", so a 409 is retried with a
    bounded backoff rather than reported as a failure. The retry is still
    recorded, because a run that needed three attempts is worth knowing about.

  * THIS TRANSPORT CANNOT CARRY `NOT NULL`. The field entry the API takes has
    no nullability key, so a plan that declares a column NOT NULL is applied
    with less than it says. Nothing is invented: the gap is recorded per
    object in `properties_not_applied`, and DDL_PLAN.md names it per object
    too (rule R21). Column and table COMMENTs DO travel -- `fieldDescription`
    and `description` -- and are compared on the read-back, so a comment the
    target quietly dropped is reported rather than assumed applied.

`call(operation, **kwargs)` is injected so every decision here is unit-tested
with no environment.
"""
from __future__ import annotations

import datetime
import re
from typing import Callable

import time
import uuid

from .catalog_api import (
    PROPERTIES_THIS_BODY_CANNOT_CARRY, build_schema_body, build_table_body,
    build_view_body)

__all__ = ["deploy_catalog", "RefusedToExecute"]


class RefusedToExecute(RuntimeError):
    """Execution was requested without the arguments that make it safe."""


def _resolve_catalog_type(call, catalog: str) -> str | None:
    """The target catalog's `catalogType` as the server holds it, matched
    case-insensitively; None when the catalog does not exist.

    A failed LISTING is raised, not swallowed: "could not look" and "absent"
    lead to different refusals, and neither of them is "safe to write".
    """
    payload = call("list_catalogs")
    for item in payload.get("items") or []:
        name = str(item.get("displayName") or item.get("key") or "").lower()
        if name == catalog.strip().lower():
            return str(item.get("catalogType") or "").upper() or "UNKNOWN"
    return None


def _split(fqn: str) -> tuple[str, str, str]:
    catalog, schema, name = fqn.split(".", 2)
    return catalog, schema, name


def _norm(field_type, precision=None, scale=None) -> str:
    """Compare field types ignoring case and precision formatting."""
    base = str(field_type or "").strip().lower()
    if base == "decimal" and precision is not None:
        return f"decimal({int(precision)},{int(scale or 0)})"
    return base


def _find_schema(call, catalog: str, schema: str) -> dict | None:
    """The schema as the server holds it, matched case-insensitively; None
    when it is absent.

    A failed LISTING is raised, not swallowed, as in `_resolve_catalog_type`.
    Read as "absent" it would re-POST an existing schema -- the write the
    module docstring says gets the table creates after it accepted and then
    dropped -- and the 403 or 5xx behind it would be recorded nowhere.
    """
    wanted = f"{catalog}.{schema}".lower()
    payload = call("list_schemas", catalog=catalog)
    for item in payload.get("items") or []:
        if str(item.get("key") or "").lower() == wanted:
            return item
    return None


def _resolve_schema_key(call, catalog: str, schema: str) -> str | None:
    found = _find_schema(call, catalog, schema)
    return str(found.get("key")) if found else None


def _resolve_object(call, catalog: str, schema_key: str, name: str,
                    is_view: bool) -> dict | None:
    """The object as the server holds it, matched case-insensitively; None
    when it is absent. A failed listing raises: "could not look" is not
    "absent", and the two lead to different reports."""
    wanted = f"{schema_key}.{name}".lower()
    payload = call("list_views_in" if is_view else "list_tables_in",
                   catalog=catalog, schema=schema_key)
    for item in payload.get("items") or []:
        if str(item.get("key") or "").lower() == wanted:
            return item
    return None


_DECIMAL_TYPE = __import__("re").compile(r"decimal\((\d+)\s*,\s*(\d+)\)")


def _is_narrowing(declared: str, derived: str) -> bool:
    """Did the target derive a type that holds LESS than the source declared?"""
    a = _DECIMAL_TYPE.match(str(declared).lower())
    b = _DECIMAL_TYPE.match(str(derived).lower())
    if a and b:
        return int(b.group(1)) < int(a.group(1))
    # decimal -> bigint loses range above 19 digits.
    if a and str(derived).lower() in ("bigint", "int", "smallint", "tinyint"):
        return int(a.group(1)) > 18
    return False


def _diagnose_never_appeared(call, catalog: str, schema_key: str,
                             probes: list | None = None) -> bool:
    """Is this schema refusing OUR names, or refusing everything?

    A failed create permanently poisons that name in that schema -- every
    later create returns 202 and is silently dropped, and DELETE does not
    recover it. That is indistinguishable from a bad request UNLESS you try a
    name that has never failed here: if a NOVEL name lands, the schema is
    fine and the planned names are burned.

    Returns True when a novel name succeeds (so the planned names are
    poisoned), False when it does not, and None when the read-back itself
    failed -- unknown, not "the schema refuses everything". Runs at most
    once per schema, and cleans up after itself with a name that is unique
    per run -- a fixed probe name would burn itself on its first failure.
    """
    probe = f"snowmig_probe_{uuid.uuid4().hex[:8]}"
    try:
        call("create_table", catalog=catalog, schema=schema_key.split(".", 1)[-1],
             table=probe,
             body=build_table_body(catalog, schema_key.split(".", 1)[-1], probe,
                                   [{"name": "probe", "type": "STRING"}]))
    except Exception:
        return False

    try:
        landed = _resolve_object(call, catalog, schema_key, probe,
                                 False) is not None
        list_error = None
    except Exception as exc:
        # Could not look. The probe may well exist, so the delete is
        # attempted anyway -- it is never left behind because the listing
        # failed -- and the verdict is "unknown".
        landed, list_error = None, exc
    deleted = False
    if landed is not False:
        try:
            call("delete_table", catalog=catalog,
                 schema=schema_key.split(".", 1)[-1], table=probe)
            deleted = True
        except Exception:
            deleted = False
    if probes is not None:
        entry = {"name": probe, "schema": schema_key,
                 "created": landed, "deleted": deleted,
                 "note": ("removed" if deleted else
                          "NOT removed — deletes are asynchronous and "
                          "best-effort; remove it manually if it is "
                          "still there")}
        if list_error is not None:
            entry["list_error"] = str(list_error)[:200]
            entry["note"] = ("existence unknown: the listing failed; delete "
                             + ("issued" if deleted else "attempted and "
                                "refused -- remove it manually if it is there"))
        probes.append(entry)
    return landed


def _is_conflict(exc: Exception) -> bool:
    text = str(exc)
    return "409" in text or "ongoing" in text.lower()


_TRANSIENT = re.compile(r"\b5\d\d\b|\b429\b|time[d]?[ -]?out", re.IGNORECASE)


def _is_transient(exc: Exception) -> bool:
    """Worth asking again? A 5xx, a 429 or a timeout may clear on the next
    poll. A 401/403 reads the same on every attempt, and polling it would
    spend the whole verify budget per table on an answer that was known on
    the first call."""
    return bool(_TRANSIENT.search(str(exc)))


def _planned(columns: list[dict]) -> list[tuple[str, str]]:
    out = []
    for col in columns:
        field = build_table_body("c", "s", "t", [col])["tableFields"][0]
        out.append((str(field["fieldName"]).upper(),
                    _norm(field.get("fieldType"), field.get("fieldPrecision"),
                          field.get("fieldScale"))))
    return out


def _actual(payload: dict, key: str = "tableFields") -> list[tuple[str, str]]:
    # Field names are upper-cased on BOTH sides before comparing: the server
    # folds them, and a case fold is not a structure mismatch.
    return [(str(f.get("fieldName")).upper(),
             _norm(f.get("fieldType"), f.get("fieldPrecision"),
                   f.get("fieldScale")))
            for f in (payload.get(key) or [])]


def _descriptions(columns: list[dict]) -> dict[str, str]:
    """`{COLUMN: description}` as the body will SEND them.

    Built through the same body builder as the fields, so the TIMESTAMP_NTZ
    note the mapper appends is compared as sent rather than as planned.
    """
    out = {}
    for col in columns:
        field = build_table_body("c", "s", "t", [col])["tableFields"][0]
        out[str(field["fieldName"]).upper()] = str(
            field.get("fieldDescription") or "")
    return out


def _actual_descriptions(payload: dict, key: str = "tableFields"
                         ) -> dict[str, str]:
    return {str(f.get("fieldName")).upper():
            str(f.get("fieldDescription") or "")
            for f in (payload.get(key) or [])}


def _check_descriptions(out: dict, ident: str, stmt: dict,
                        columns: list[dict], payload: dict,
                        description: str) -> None:
    """Record any COMMENT the plan sent that the target did not keep.

    Structure is already verified when this runs, so nothing here changes
    that verdict: a comment is documentation, not shape. It is still
    reported, because "the plan showed it and the object does not have it"
    is the same class of defect as a dropped NOT NULL, only cheaper.
    """
    want = _descriptions(columns)
    got = _actual_descriptions(payload)
    lost = [f'{name}: planned {want[name]!r}, found {got.get(name, "")!r}'
            for name in want
            if want[name] and want[name] != got.get(name, "")]
    table_planned = str(description or "")
    table_got = str(payload.get("description") or "")
    if table_planned and table_planned != table_got:
        lost.append(f"table COMMENT: planned {table_planned!r}, found "
                    f"{table_got!r}")
    if not lost:
        return
    out["description_drift_targets"].append(ident)
    out["description_drift"].append({
        "source_identifier": ident, "target_fqn": stmt["target_fqn"],
        "reason": ("created with the planned columns, but "
                   + "; ".join(lost)
                   + ". The structure matches the plan; the documentation "
                     "the plan showed did not survive.")})


def deploy_catalog(ddl_plan: dict, *, target=None, execute: bool = False,
                   call: Callable[..., dict] | None = None,
                   retry_delays: tuple[float, ...] = (2.0, 5.0, 10.0),
                   verify_delays: tuple[float, ...] = (3.0, 5.0, 10.0, 15.0),
                   schema_wait: tuple[float, ...] = (3.0, 5.0, 10.0),
                   diagnose: bool = True,
                   timestamp_ntz_as_timestamp: bool = False) -> dict:
    all_statements = [s for s in ddl_plan.get("statements", [])]

    if target is not None:
        scope = target.catalog.upper()
        in_scope_at = {i for i, s in enumerate(all_statements)
                       if _split(s["target_fqn"])[0].upper() == scope}
        statements = [s for i, s in enumerate(all_statements) if i in in_scope_at]
        out_of_scope = [s for i, s in enumerate(all_statements)
                        if i not in in_scope_at]
    else:
        statements, out_of_scope = all_statements, []

    out = {
        "ran_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "transport": "catalog_api",
        "dry_run": not execute,
        "statements": statements,
        "statement_count": len(statements),
        "blocked_count": len(ddl_plan.get("blocked", [])),
        "catalog_in_scope": target.catalog if target is not None else None,
        "out_of_scope_count": len(out_of_scope),
        "out_of_scope_catalogs": sorted({
            _split(s["target_fqn"])[0] for s in out_of_scope}),
        # Every statement filtered out by the catalog name: this run can only
        # do nothing. Naming it here is what stops `executed: 0, errors: []`
        # from reading like a clean deploy of an empty plan.
        "matched_nothing": bool(all_statements and not statements),
        "executed": 0, "verified": 0,
        "schemas_created": [], "errors": [],
        "resolved_schema_keys": {}, "schemas_reused": [],
        "attempted_targets": [], "verified_targets": [], "failed_targets": [],
        "mismatched_targets": [], "mismatches": [],
        # A view we DID create, whose column types the engine re-derived.
        # Distinct from a mismatch: the object is ours, the types are not.
        "derived_type_drift_targets": [], "derived_type_drift": [],
        # Names the target has permanently burned. See P1 in ACTION-ITEMS.md.
        "poisoned_names": [],
        # Objects the diagnosis created. Deletes are async, so cleanup is
        # best-effort and the name is reported either way rather than left
        # silently behind in someone's catalog.
        "diagnosis_probes": [],
        "unverified_structure_targets": [], "unverified_structure": [],
        # Planned properties this TRANSPORT cannot express (NOT NULL). The
        # object is still created and still verified for structure -- but a
        # plan that said NOT NULL and a table that is nullable is exactly the
        # reviewed-versus-applied gap, so it is named per object here as well
        # as in DDL_PLAN.md rather than being visible nowhere.
        "properties_not_applied_targets": [], "properties_not_applied": [],
        # Sent with a description the target did not keep. Structure is
        # unaffected, so these still count as verified.
        "description_drift_targets": [], "description_drift": [],
        "failed": [],
        "catalog_type": None,
    }
    if not execute:
        return out

    if target is None:
        raise RefusedToExecute(
            "execute=True requires a resolved target; ask the user for the AIDP "
            "datalake OCID, workspace, cluster and catalog and pass them "
            "explicitly")
    if call is None:
        raise RefusedToExecute("execute=True requires a transport callable")

    # The catalog's TYPE gates everything below. Managed Delta cannot live in
    # an EXTERNAL catalog -- it is a registered, read-only pointer at the live
    # Snowflake source -- and the control-plane creates against one would
    # return 202 Accepted and silently produce nothing.
    try:
        catalog_type = _resolve_catalog_type(call, target.catalog)
    except Exception as exc:
        raise RefusedToExecute(
            f"could not read the type of catalog {target.catalog!r} before "
            f"writing ({str(exc)[:200]}). An EXTERNAL catalog cannot hold the "
            f"managed Delta this deploy creates, so it refuses rather than "
            f"guesses.") from exc
    if catalog_type is None:
        raise RefusedToExecute(
            f"catalog {target.catalog!r} does not exist on this DataLake. "
            f"deploy creates schemas, tables and views -- never catalogs. "
            f"Create the STANDARD catalog first; `snowmig.py catalog` only "
            f"registers EXTERNAL ones, which this deploy refuses to write "
            f"into.")
    if catalog_type == "EXTERNAL":
        raise RefusedToExecute(
            f"catalog {target.catalog!r} is EXTERNAL -- a registered, "
            f"read-only pointer at the live Snowflake source. It cannot hold "
            f"managed Delta: the creates would return 202 Accepted and "
            f"silently produce nothing. A structure clone needs a STANDARD "
            f"catalog, whose tables are created on AIDP compute via "
            f"`snowmig.py notebook`, and only when the user has explicitly "
            f"asked for one.")
    out["catalog_type"] = catalog_type
    if catalog_type == "UNKNOWN":
        out["errors"].append(
            f"catalog {target.catalog} carries no catalogType in the list "
            f"response; proceeding, but the EXTERNAL guard could not run")

    # 1. schemas: LOOK FIRST, at all of them, before writing any. Re-POSTing
    #    an existing schema re-triggers async work and silently drops the
    #    table creates that follow it -- so a schema that cannot be LISTED
    #    is not "absent", and the deploy refuses while nothing is written yet.
    resolved: dict[str, str] = {}
    schema_pairs = sorted({_split(s["target_fqn"])[:2] for s in statements})
    try:
        looked = {pair: _find_schema(call, *pair) for pair in schema_pairs}
    except Exception as exc:
        raise RefusedToExecute(
            f"could not list the schemas of catalog {target.catalog!r} before "
            f"writing ({str(exc)[:200]}). A schema that cannot be listed is "
            f"not known to be absent, and re-POSTing one that exists gets "
            f"the table creates that follow it accepted and then silently "
            f"dropped, so it refuses rather than guesses.") from exc

    def _look(catalog: str, schema: str) -> dict | None:
        # Mid-poll, the schema has already been POSTed; refusing would help
        # nothing, but the error goes on the record instead of reading as
        # "not visible yet".
        try:
            return _find_schema(call, catalog, schema)
        except Exception as exc:
            out["errors"].append(
                f"LIST SCHEMAS {catalog}.{schema}: {str(exc)[:200]}")
            return None

    for catalog, schema in schema_pairs:
        requested = f"{catalog}.{schema}"
        found = looked[(catalog, schema)]

        if found is None:
            try:
                call("create_schema", catalog=catalog, schema=schema,
                     body=build_schema_body(catalog, schema))
                out["schemas_created"].append(requested)
            except Exception as exc:
                out["errors"].append(f"CREATE SCHEMA {requested}: {exc}")
            # Creation is async: wait for it to exist AND be ACTIVE.
            for attempt in range(len(schema_wait) + 1):
                found = _look(catalog, schema)
                if found is not None and \
                        str(found.get("lifecycleState") or "ACTIVE").upper() == "ACTIVE":
                    break
                if attempt < len(schema_wait):
                    time.sleep(schema_wait[attempt])
        else:
            out["schemas_reused"].append(str(found.get("key")))
            # Present but still settling: creating tables now is what gets
            # them accepted-then-dropped.
            for attempt in range(len(schema_wait) + 1):
                if str(found.get("lifecycleState") or "ACTIVE").upper() == "ACTIVE":
                    break
                if attempt < len(schema_wait):
                    time.sleep(schema_wait[attempt])
                found = _look(catalog, schema) or found

        if found is None:
            out["errors"].append(
                f"could not resolve the server's key for schema {requested}; "
                f"falling back to the requested name")
        elif str(found.get("lifecycleState") or "ACTIVE").upper() != "ACTIVE":
            out["errors"].append(
                f"schema {requested} is "
                f'{found.get("lifecycleState")}, not ACTIVE. Creating tables '
                f"against a settling schema is what gets them accepted and "
                f"then silently dropped.")
        resolved[requested] = (str(found.get("key")) if found else requested)
    out["resolved_schema_keys"] = dict(resolved)

    # One diagnosis per schema: whether a name is burned is a property of the
    # schema, not of each object, and the probe itself writes.
    diagnosed: dict[str, bool] = {}

    # 2. tables first, then views: a view's fields reference its base tables
    ordered = ([s for s in statements if s.get("object_type") != "VIEW"]
               + [s for s in statements if s.get("object_type") == "VIEW"])

    for stmt in ordered:
        catalog, schema, name = _split(stmt["target_fqn"])
        ident = stmt.get("source_identifier")
        is_view = stmt.get("object_type") == "VIEW"
        columns = stmt.get("expected_columns") or []
        out["attempted_targets"].append(ident)

        # The key the SERVER uses, which is lower-cased.
        schema_key = resolved.get(f"{catalog}.{schema}", f"{catalog}.{schema}")
        server_schema = schema_key.split(".", 1)[1] if "." in schema_key else schema

        # Build the body FIRST. A type the API silently rejects must fail
        # here, not become an accepted-then-vanished table.
        # The source object's COMMENT. It is in the reviewed CREATE TABLE, so
        # it travels here too; it used to be dropped by defaulting to "".
        description = str(stmt.get("description") or "")
        try:
            if is_view:
                body = build_view_body(
                    catalog, server_schema, name,
                    stmt.get("view_text") or "", columns, description,
                    timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
            else:
                body = build_table_body(
                    catalog, server_schema, name, columns, description,
                    timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
        except Exception as exc:
            out["failed_targets"].append(ident)
            out["failed"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": f"cannot build a valid catalog body: {exc}"})
            continue

        # Said before the create, not discovered after it: this transport has
        # no field for nullability, so a plan that declares NOT NULL is being
        # applied with less than it says.
        not_null = [str(c.get("name")) for c in columns
                    if c.get("nullable") is False]
        if not_null:
            out["properties_not_applied_targets"].append(ident)
            out["properties_not_applied"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "property": "NOT NULL", "columns": not_null,
                "reason": (f'{", ".join(not_null)} are NOT NULL in the '
                           f"reviewed plan and are created NULLABLE here: "
                           + PROPERTIES_THIS_BODY_CANNOT_CARRY["not_null"])})

        # Schema creation is async, so a 409 here means "not settled yet".
        accepted = False
        for attempt in range(len(retry_delays) + 1):
            try:
                if is_view:
                    call("create_view", catalog=catalog, schema=server_schema,
                         view=name, body=body)
                else:
                    call("create_table", catalog=catalog, schema=server_schema,
                         table=name, body=body)
                out["executed"] += 1
                accepted = True
                break
            except Exception as exc:
                out["errors"].append(f"CREATE {stmt.get('object_type')} "
                                     f"{stmt['target_fqn']}: {exc}")
                if _is_conflict(exc) and attempt < len(retry_delays):
                    time.sleep(retry_delays[attempt])
                    continue
                break

        # 3. read it back -- this, not the create's return, is the claim.
        # Resolved by listing and matching case-insensitively, because the
        # server's key case is not the one we asked for.
        listed, list_error = None, None
        for attempt in range(len(verify_delays) + 1):
            try:
                listed = _resolve_object(call, catalog, schema_key, name,
                                         is_view)
                list_error = None
            except Exception as exc:
                # "Could not look" is not "absent". A 5xx may clear, so it
                # is polled like a slow create; a permission error reads the
                # same on every attempt and would only burn the budget.
                listed, list_error = None, exc
                if not _is_transient(exc):
                    break
            if listed is not None:
                break
            if attempt < len(verify_delays):
                time.sleep(verify_delays[attempt])
        if listed is None and list_error is not None:
            relation = "views" if is_view else "tables"
            out["errors"].append(
                f"LIST {relation.upper()} {schema_key}: {str(list_error)[:200]}")
            out["failed_targets"].append(ident)
            out["failed"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": (
                    f"the create "
                    f"{'returned 202 Accepted' if accepted else 'failed'} and "
                    f"the object could not be read back: listing {relation} "
                    f"in {schema_key} failed ({str(list_error)[:200]}). Its "
                    f"existence is UNKNOWN, not absent -- fix the listing "
                    f"permission or endpoint and re-run; no diagnosis probe "
                    f"was written.")})
            continue
        if listed is None:
            base = (f"the create returned 202 Accepted but no object matching "
                    f"{schema_key}.{name} ever appeared, and the asynchronous "
                    f"create reported nothing. ")
            # Ask the schema whether it is refusing OUR name or everything.
            # Once per schema: the answer is a property of the schema.
            if diagnose and schema_key not in diagnosed:
                diagnosed[schema_key] = _diagnose_never_appeared(
                    call, catalog, schema_key, out["diagnosis_probes"])
            verdict = diagnosed.get(schema_key)

            if verdict is True:
                out["poisoned_names"].append(stmt["target_fqn"])
                reason = base + (
                    f"A NOVEL name in {schema_key} was created successfully, "
                    f"so the schema and your request are both fine and this "
                    f"NAME IS BURNED: a create that failed here once is "
                    f"refused for ever after, and DELETE does not recover it. "
                    f"Retry into a FRESH SCHEMA -- re-running into this one "
                    f"will keep returning 202 and keep creating nothing.")
            elif verdict is False:
                reason = base + (
                    "A novel name in the same schema failed too, so this is "
                    "not a burned name: suspect the request itself (an "
                    "unsupported field type is the usual cause) or the "
                    "permissions on this catalog.")
            elif diagnose:
                reason = base + (
                    "Most often an unsupported field type. The diagnosis "
                    "probe could not be read back either, so a burned name "
                    "and a bad request cannot be told apart here -- see "
                    "diagnosis_probes for the listing error.")
            else:
                reason = base + (
                    "Most often an unsupported field type. Re-run with "
                    "diagnosis enabled to tell a burned name from a bad "
                    "request.")

            out["failed_targets"].append(ident)
            out["failed"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": reason})
            continue

        if not columns:
            out["unverified_structure_targets"].append(ident)
            out["unverified_structure"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": "exists, but the plan carried no column list to "
                          "compare it against"})
            continue

        # The list entry proves existence and gives the real key case, but it
        # carries no fields. Structure needs a GET on that key.
        server_name = str(listed.get("key") or "").rsplit(".", 1)[-1] or name
        try:
            payload = call("get_view" if is_view else "get_table",
                           catalog=catalog, schema=server_schema,
                           **{("view" if is_view else "table"): server_name})
        except Exception as exc:
            out["unverified_structure_targets"].append(ident)
            out["unverified_structure"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": f"exists, but its structure could not be read: "
                          f"{str(exc)[:200]}"})
            continue

        want = _planned(columns)
        got = _actual(payload, "viewFields" if is_view else "tableFields")
        if want == got:
            out["verified"] += 1
            out["verified_targets"].append(ident)
            # Structure is right; did the DESCRIPTIONS the plan showed
            # survive? A table's are ours to declare. A view's fields are
            # re-derived by the target, so they are not compared.
            if not is_view:
                _check_descriptions(out, ident, stmt, columns, payload,
                                    description)
        elif is_view and [n for n, _ in want] == [n for n, _ in got]:
            # Same columns in the same order, different types: the engine
            # derived them from the SQL. The view IS ours.
            drift = [(n, w, g) for (n, w), (_, g) in zip(want, got) if w != g]
            details = "; ".join(
                f"{n}: declared {w.upper()}, derived {g.upper()}"
                + (" (**NARROWED** -- values near the declared limit can "
                   "overflow)" if _is_narrowing(w, g) else "")
                for n, w, g in drift)
            out["derived_type_drift_targets"].append(ident)
            out["derived_type_drift"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "columns": [{"column": n, "declared": w, "derived": g}
                            for n, w, g in drift],
                "reason": f"created with all {len(want)} columns, but the "
                          f"target re-derived {len(drift)} column type(s) from "
                          f"the view SQL: {details}. Snowflake reports a "
                          f"view's declared output types; the target computes "
                          f"its own."})
        else:
            out["mismatched_targets"].append(ident)
            out["mismatches"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": f"planned {want}, found {got}. The object was left "
                          f"as it was found and has NOT been cloned."})
    # A gap is stated before the create, deliberately: it is a property of
    # this transport rather than something discovered afterwards. It may not
    # survive into the artifact for an object whose create then failed --
    # live 2026-09-22, `v_customer_totals` was reported as arriving NULLABLE
    # when it had not arrived at all.
    failed_idents = set(out["failed_targets"])
    out["properties_not_applied"] = [
        p for p in out["properties_not_applied"]
        if p.get("source_identifier") not in failed_idents]
    out["properties_not_applied_targets"] = [
        i for i in out["properties_not_applied_targets"]
        if i not in failed_idents]
    return out
