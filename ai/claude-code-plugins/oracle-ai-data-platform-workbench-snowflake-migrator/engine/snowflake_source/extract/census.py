"""Everything in the estate that is NOT a table or a view. Read-only.

`assess` ran `SHOW TABLES` and `SHOW VIEWS` and nothing else, so the plan
reported "7 of 7 objects can move" while procedures, UDFs, tasks, streams,
stages, pipes, sequences and file formats sat there unexamined. The number was
true of what had been LOOKED AT, and overstated coverage of the estate.

NOTHING HERE IS MIGRATABLE BY THIS PLUGIN. Every entry carries
`migratable: False`, a reason, and a pointer at the AIDP capability that would
carry the workload. A pointer is not a promise: no equivalent is generated,
because a plausible-but-wrong procedure translation is worse than an honest
gap.

Two source choices worth keeping:

  * PROCEDURES and FUNCTIONS come from INFORMATION_SCHEMA, not from SHOW.
    `SHOW PROCEDURES IN SCHEMA` returns Snowflake's own built-ins -- 33 of them
    on a completely empty schema -- and INFORMATION_SCHEMA does not. It also
    carries the LANGUAGE, which is what decides the effort.
  * Reads are scoped per DATABASE, not per schema. INFORMATION_SCHEMA is a
    per-database view and SHOW accepts `IN DATABASE`, so the whole census costs
    about ten queries per database rather than ten per schema.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer

__all__ = ["KINDS", "LANGUAGE_VERDICTS", "build_census"]

# language -> what it would take on AIDP. Effort is a triage band, not an
# estimate: it says which pile the object belongs in.
LANGUAGE_VERDICTS: dict[str, dict] = {
    "SQL": {
        "effort": "MEDIUM",
        "aidp_path": "Snowflake Scripting is procedural SQL with no Spark "
                     "equivalent. Orchestration becomes an AIDP Job; the body "
                     "becomes Spark SQL in a notebook task.",
    },
    "JAVASCRIPT": {
        "effort": "HIGH",
        "aidp_path": "There is no JavaScript runtime on AIDP. The logic has to "
                     "be rewritten in Python or Spark SQL, which means it has "
                     "to be understood first, not translated.",
    },
    "PYTHON": {
        "effort": "MEDIUM",
        "aidp_path": "The closest path on AIDP. A Snowpark Python handler is "
                     "not a Spark UDF, so the body needs reworking, but the "
                     "language and most libraries carry over.",
    },
    "JAVA": {
        "effort": "MEDIUM",
        "aidp_path": "Repackage as a Spark UDF and install the JAR on the "
                     "cluster. Mechanical, but the JAR has to be rebuilt "
                     "against the Spark API.",
    },
    "SCALA": {
        "effort": "MEDIUM",
        "aidp_path": "Repackage as a Spark UDF. Scala is native to Spark, so "
                     "this is usually the least painful of the handler "
                     "languages.",
    },
}

_UNKNOWN_LANGUAGE = {
    "effort": "UNKNOWN",
    "aidp_path": "The handler language was not recognised, so no path is "
                 "proposed. Inspect it before estimating.",
}

# Each kind: where to read it, how to name it, and why it cannot migrate here.
KINDS: tuple[dict, ...] = (
    {"kind": "PROCEDURE", "source": "information_schema",
     "relation": "procedures", "name_col": "PROCEDURE_NAME",
     "schema_col": "PROCEDURE_SCHEMA", "lang_col": "PROCEDURE_LANGUAGE",
     "sig_col": "ARGUMENT_SIGNATURE", "owner_col": "PROCEDURE_OWNER",
     "reason": "stored procedures are code, not structure. AIDP has no "
               "procedure object; the workload moves to a Job or a notebook "
               "and the body has to be rewritten."},
    {"kind": "FUNCTION", "source": "information_schema",
     "relation": "functions", "name_col": "FUNCTION_NAME",
     "schema_col": "FUNCTION_SCHEMA", "lang_col": "FUNCTION_LANGUAGE",
     "sig_col": "ARGUMENT_SIGNATURE", "owner_col": "FUNCTION_OWNER",
     "reason": "a UDF is code. Spark UDFs exist but the handler contract "
               "differs, so the body has to be reworked rather than copied."},
    {"kind": "SEQUENCE", "source": "information_schema",
     "relation": "sequences", "name_col": "SEQUENCE_NAME",
     "schema_col": "SEQUENCE_SCHEMA",
     "reason": "Delta has no sequence object. Surrogate keys need a different "
               "strategy -- identity columns, a hash, or generation upstream "
               "-- and the choice changes the data."},
    {"kind": "STAGE", "source": "information_schema",
     "relation": "stages", "name_col": "STAGE_NAME",
     "schema_col": "STAGE_SCHEMA",
     "reason": "a stage points at storage and carries credentials. The AIDP "
               "equivalent is an object-storage location plus a credential in "
               "the credential store; neither is inferable from here."},
    {"kind": "FILE_FORMAT", "source": "information_schema",
     "relation": "file_formats", "name_col": "FILE_FORMAT_NAME",
     "schema_col": "FILE_FORMAT_SCHEMA",
     "reason": "file formats are named parse options for COPY. On AIDP the "
               "same options become Spark reader options at each read site."},
    {"kind": "PIPE", "source": "information_schema",
     "relation": "pipes", "name_col": "PIPE_NAME",
     "schema_col": "PIPE_SCHEMA",
     "reason": "Snowpipe is continuous ingestion. It has no AIDP object; it "
               "becomes a streaming job or a scheduled load, which is an "
               "architecture decision (see the data-movement options)."},
    {"kind": "TASK", "source": "show", "relation": "tasks",
     "reason": "a task is a scheduler. AIDP Jobs are the equivalent, but the "
               "schedule, dependencies and body all have to be re-expressed. "
               "**A task that populates a migrated table means that table "
               "stops being populated after cutover.**"},
    {"kind": "STREAM", "source": "show", "relation": "streams",
     "reason": "a stream is CDC state. Delta Change Data Feed is the nearest "
               "equivalent, but stream offsets do not transfer, so consumers "
               "restart from a new baseline."},
    {"kind": "MATERIALIZED_VIEW", "source": "show", "relation": "materialized views",
     "reason": "Snowflake maintains materialized views automatically. AIDP has "
               "no equivalent: it becomes a table plus a scheduled refresh "
               "job, which the customer then owns."},
    {"kind": "DYNAMIC_TABLE", "source": "show", "relation": "dynamic tables",
     "reason": "a dynamic table is a declarative pipeline with a target "
               "lag. AIDP has no equivalent object; it becomes a scheduled "
               "job whose cadence must be chosen deliberately."},
)


def _iso(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _read_information_schema(run_sql, db: str, spec: dict) -> list[dict]:
    cols = [spec["name_col"], spec["schema_col"]]
    for key in ("lang_col", "sig_col", "owner_col"):
        if spec.get(key):
            cols.append(spec[key])
    select = ", ".join(cols)
    return run_sql(
        f"select {select} from {lexer.qualify(db)}.information_schema."
        f'{spec["relation"]} order by 1')


def _read_show(run_sql, db: str, spec: dict) -> list[dict]:
    return run_sql(f'show {spec["relation"]} in database {lexer.qualify(db)}')


def build_census(run_sql: Callable[..., list[dict]], databases: list[str], *,
                 include_definitions: bool = False) -> dict:
    notes: list[str] = []
    kinds: dict[str, dict] = {}
    objects: list[dict] = []

    for spec in KINDS:
        kind = spec["kind"]
        count = 0
        readable = True
        note = ""
        for db in databases:
            try:
                if spec["source"] == "information_schema":
                    rows = _read_information_schema(run_sql, db, spec)
                else:
                    rows = _read_show(run_sql, db, spec)
            except Exception as exc:
                readable = False
                note = str(exc)[:200]
                notes.append(f"{kind} in {db}: {note}")
                continue
            for row in rows:
                objects.append(_entry(kind, spec, db, row,
                                      include_definitions=include_definitions))
                count += 1
        kinds[kind] = {"count": count if readable else None,
                       "readable": readable,
                       "note": note or f"{count} found"}

    by_language = collections.Counter(
        o["language"] for o in objects if o.get("language"))
    by_kind = collections.Counter(o["kind"] for o in objects)
    by_effort = collections.Counter(
        o["effort"] for o in objects if o.get("effort"))

    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "databases_in_scope": list(databases),
        "kinds": kinds,
        "objects": objects,
        "total": len(objects),
        "by_kind": dict(by_kind),
        "by_language": dict(by_language),
        "by_effort": dict(by_effort),
        "unreadable": notes,
        "scope_statement": _scope_statement(len(objects), by_kind, kinds),
    }


def _entry(kind: str, spec: dict, db: str, row: dict, *,
           include_definitions: bool) -> dict:
    if spec["source"] == "information_schema":
        name = row.get(spec["name_col"])
        schema = row.get(spec["schema_col"])
    else:
        name = row.get("name")
        schema = row.get("schema_name") or row.get("database_name") or "?"

    entry = {
        "kind": kind,
        "source_identifier": f"{db}.{schema}.{name}",
        "detail": "",
        "migratable": False,          # never true here, by construction
        "reason": spec["reason"],
        "language": None,
        "effort": None,
        "aidp_path": None,
    }

    if spec.get("sig_col") and row.get(spec["sig_col"]):
        entry["detail"] = str(row[spec["sig_col"]])
    elif spec["source"] == "show":
        state = row.get("state") or row.get("target_lag") or row.get("mode")
        entry["detail"] = f"state={state}" if state else ""

    if spec.get("lang_col"):
        language = str(row.get(spec["lang_col"]) or "").upper() or None
        verdict = LANGUAGE_VERDICTS.get(language, _UNKNOWN_LANGUAGE)
        entry["language"] = language
        entry["effort"] = verdict["effort"]
        entry["aidp_path"] = verdict["aidp_path"]
        if verdict is _UNKNOWN_LANGUAGE:
            entry["reason"] = (
                f'{spec["reason"]} Handler language {language!r} was not '
                f"recognised, so no path is proposed.")

    if include_definitions:
        for key in ("PROCEDURE_DEFINITION", "FUNCTION_DEFINITION", "text",
                    "definition"):
            if row.get(key):
                entry["definition"] = str(row[key])
                break
    return entry


def _scope_statement(total: int, by_kind, kinds: dict) -> str:
    denied = [k for k, v in kinds.items() if not v["readable"]]
    if total == 0 and not denied:
        return ("This estate contains **only tables and views** — no "
                "procedures, UDFs, tasks, streams, materialized or dynamic "
                "tables, stages, pipes, sequences or file formats were found. "
                "So the migratable count is the whole estate, not a subset "
                "of it.")
    parts = ", ".join(f"{n} {k.lower().replace('_', ' ')}(s)"
                      for k, n in sorted(by_kind.items()))
    text = (f"**{total} object(s) in this estate cannot be migrated by this "
            f"plugin**: {parts}. They are code, schedulers or storage "
            f"definitions rather than structure, so the migratable count "
            f"covers tables and views only — it is not the size of the "
            f"estate.")
    if denied:
        text += (f" **{', '.join(denied)} could not be read**, so even this "
                 f"count is a floor, not a total.")
    return text
