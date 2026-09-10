"""Deploy structure through the AIDP catalog API. I/O injected as `call`.

The SQL transport cannot be used: `POST /workspaces/<ws>/sql/execute` returns
404 against a live DataLake. This path creates schemas, then tables, then
views through the catalog CRUD API instead, which needs no Spark cluster and
has no session to lose DDL to.

Shape of the run:
  1. create each distinct schema once
  2. create tables, then views -- a view's fields reference its base tables
  3. READ EACH OBJECT BACK and compare its field list to the plan

Step 3 is the point. As with the SQL path, "the create returned 200" is not the
claim; "the planned columns are there" is. A create that fails because the
object already exists is NOT a failure -- the read-back decides, because an
object that already matches the plan is indistinguishable from one we made.

TWO BEHAVIOURS LEARNED FROM A LIVE RUN, both of which broke the first attempt:

  * AIDP LOWER-CASES IDENTIFIERS. A schema created as `TEST_DB_20260908_1529`
    comes back as `lake.test_db_20260908_1529`. Every `schemaKey` and every
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
    waited on until ACTIVE.

  * SCHEMA CREATION IS ASYNCHRONOUS. Creating a table immediately afterwards
    can return 409 Conflict "ongoing operation", so a 409 is retried with a
    bounded backoff rather than reported as a failure. The retry is still
    recorded, because a run that needed three attempts is worth knowing about.

`call(operation, **kwargs)` is injected so every decision here is unit-tested
with no environment.
"""
from __future__ import annotations

import datetime
from typing import Callable

import time
import uuid

from .catalog_api import (
    build_schema_body, build_table_body, build_view_body)

__all__ = ["deploy_catalog", "RefusedToExecute"]


class RefusedToExecute(RuntimeError):
    """Execution was requested without the arguments that make it safe."""


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
    """The schema as the server holds it, matched case-insensitively."""
    wanted = f"{catalog}.{schema}".lower()
    try:
        payload = call("list_schemas", catalog=catalog)
    except Exception:
        return None
    for item in payload.get("items") or []:
        if str(item.get("key") or "").lower() == wanted:
            return item
    return None


def _resolve_schema_key(call, catalog: str, schema: str) -> str | None:
    found = _find_schema(call, catalog, schema)
    return str(found.get("key")) if found else None


def _resolve_object(call, catalog: str, schema_key: str, name: str,
                    is_view: bool) -> dict | None:
    """The object as the server holds it, matched case-insensitively."""
    wanted = f"{schema_key}.{name}".lower()
    try:
        payload = call("list_views_in" if is_view else "list_tables_in",
                       catalog=catalog, schema=schema_key)
    except Exception:
        return None
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
    poisoned). Runs at most once per schema, and cleans up after itself with
    a name that is unique per run -- a fixed probe name would burn itself on
    its first failure.
    """
    probe = f"snowmig_probe_{uuid.uuid4().hex[:8]}"
    try:
        call("create_table", catalog=catalog, schema=schema_key.split(".", 1)[-1],
             table=probe,
             body=build_table_body(catalog, schema_key.split(".", 1)[-1], probe,
                                   [{"name": "probe", "type": "STRING"}]))
    except Exception:
        return False

    landed = _resolve_object(call, catalog, schema_key, probe, False) is not None
    deleted = False
    if landed:
        try:
            call("delete_table", catalog=catalog,
                 schema=schema_key.split(".", 1)[-1], table=probe)
            deleted = True
        except Exception:
            deleted = False
    if probes is not None:
        probes.append({"name": probe, "schema": schema_key,
                       "created": landed, "deleted": deleted,
                       "note": ("removed" if deleted else
                                "NOT removed — deletes are asynchronous and "
                                "best-effort; remove it manually if it is "
                                "still there")})
    return landed


def _is_conflict(exc: Exception) -> bool:
    text = str(exc)
    return "409" in text or "ongoing" in text.lower()


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
        "failed": [],
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

    # 1. schemas: LOOK FIRST. Re-POSTing an existing schema re-triggers async
    #    work and silently drops the table creates that follow it.
    resolved: dict[str, str] = {}
    for catalog, schema in sorted({_split(s["target_fqn"])[:2]
                                   for s in statements}):
        requested = f"{catalog}.{schema}"
        found = _find_schema(call, catalog, schema)

        if found is None:
            try:
                call("create_schema", catalog=catalog, schema=schema,
                     body=build_schema_body(catalog, schema))
                out["schemas_created"].append(requested)
            except Exception as exc:
                out["errors"].append(f"CREATE SCHEMA {requested}: {exc}")
            # Creation is async: wait for it to exist AND be ACTIVE.
            for attempt in range(len(schema_wait) + 1):
                found = _find_schema(call, catalog, schema)
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
                found = _find_schema(call, catalog, schema) or found

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
        try:
            if is_view:
                body = build_view_body(
                    catalog, server_schema, name,
                    stmt.get("view_text") or "", columns,
                    timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
            else:
                body = build_table_body(
                    catalog, server_schema, name, columns,
                    timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
        except Exception as exc:
            out["failed_targets"].append(ident)
            out["failed"].append({
                "source_identifier": ident, "target_fqn": stmt["target_fqn"],
                "reason": f"cannot build a valid catalog body: {exc}"})
            continue

        # Schema creation is async, so a 409 here means "not settled yet".
        for attempt in range(len(retry_delays) + 1):
            try:
                if is_view:
                    call("create_view", catalog=catalog, schema=server_schema,
                         view=name, body=body)
                else:
                    call("create_table", catalog=catalog, schema=server_schema,
                         table=name, body=body)
                out["executed"] += 1
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
        listed = None
        for attempt in range(len(verify_delays) + 1):
            listed = _resolve_object(call, catalog, schema_key, name, is_view)
            if listed is not None:
                break
            if attempt < len(verify_delays):
                time.sleep(verify_delays[attempt])
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
    return out
