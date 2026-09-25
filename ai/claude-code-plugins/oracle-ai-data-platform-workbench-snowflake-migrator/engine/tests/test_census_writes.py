"""A pipe or a task that loads a migrated table must name that table.

Live 2026-09-25. The census listed PIPE_EVENTS and TSK_REFRESH_GOLD, and the
plan listed STAGING_EVENTS as migrating, and nothing connected them -- though
the census TASK paragraph itself says, in bold, that a task populating a
migrated table means the table stops being populated at cutover. Which
table is not a design question: a pipe's DEFINITION is `COPY INTO <table>`,
and a task's body is on the SHOW TASKS row. Both were already being read.

Resolution rule, live-verified the same day: executed from a session with NO
current database or schema, the task's unqualified `INSERT INTO
STAGING_EVENTS` wrote SNOWMIG_COVERAGE.CORE.STAGING_EVENTS -- the task's own
database and schema, not the caller's.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.census import build_census, written_tables
from test_census_coverage import _responses


def _pipe(definition, name="P", schema="S"):
    return {"PIPE_NAME": name, "PIPE_SCHEMA": schema, "DEFINITION": definition}


def _task(definition, name="T", schema="S"):
    return {"name": name, "schema_name": schema, "state": "started",
            "definition": definition}


def _one(census, kind):
    return next(o for o in census["objects"] if o["kind"] == kind)


# ------------------------------------------------------ what a load writes

def test_a_pipe_names_the_table_it_loads():
    census = build_census(FakeSql(_responses(**{"information_schema.pipes": [
        _pipe("COPY INTO DB.S.STAGING_EVENTS FROM @DB.S.STG "
              "FILE_FORMAT = (FORMAT_NAME = DB.S.FF)")]})), ["DB"])
    pipe = _one(census, "PIPE")
    assert pipe["writes"] == ["DB.S.STAGING_EVENTS"]
    assert "writes=DB.S.STAGING_EVENTS" in pipe["detail"]


def test_a_tasks_unqualified_table_is_the_tasks_own_schema():
    census = build_census(FakeSql(_responses(**{"show tasks": [
        _task("INSERT INTO STAGING_EVENTS (EVENT_ID) SELECT 2")]})), ["DB"])
    task = _one(census, "TASK")
    assert task["writes"] == ["DB.S.STAGING_EVENTS"]
    assert task["detail"].startswith("state=started"), task["detail"]


@pytest.mark.parametrize("sql,expected", [
    ("insert into T select 1", ["DB.S.T"]),
    ("insert overwrite into OTHER.T select 1", ["DB.OTHER.T"]),
    ("insert into X.Y.T select 1", ["X.Y.T"]),
    ('insert into "orders_edge" select 1', ["DB.S.orders_edge"]),
    ('insert into "a.b" select 1', ["DB.S.a.b"]),
    ("insert into t(a) select 1", ["DB.S.T"]),
    ("merge into T using U on T.K = U.K when matched then update set V = 1 "
     "when not matched then insert (K) values (U.K)", ["DB.S.T"]),
    ("update T set V = 1", ["DB.S.T"]),
    ("delete from T where V = 1", ["DB.S.T"]),
    ("truncate table T", ["DB.S.T"]),
    ("create or replace table T as select 1 as A", ["DB.S.T"]),
    ("begin truncate table T; insert into T select * from U; end",
     ["DB.S.T"]),
    ("begin insert into A select 1; insert into B select 2; end",
     ["DB.S.A", "DB.S.B"]),
])
def test_what_a_body_writes(sql, expected):
    assert written_tables(sql, "DB", "S") == expected


@pytest.mark.parametrize("sql", [
    "COPY INTO @DB.S.STG FROM DB.S.T",               # an unload, not a load
    "COPY INTO 's3://bucket/out/' FROM DB.S.T",
    "SELECT 'insert into T select 1'",               # data, not code
    "-- insert into T\nSELECT 1",
    "CALL DB.S.REFRESH()",                           # not readable from here
    "insert into identifier($target) select 1",
    "select * from T",
    "",
])
def test_what_is_not_a_write(sql):
    assert written_tables(sql, "DB", "S") == []


def test_a_task_that_only_calls_a_procedure_claims_no_table():
    """What a procedure writes is not on the task row. Saying nothing is the
    honest answer; `writes=` with an empty list would read as "writes none"."""
    census = build_census(FakeSql(_responses(**{"show tasks": [
        _task("CALL DB.S.REFRESH()")]})), ["DB"])
    task = _one(census, "TASK")
    assert "writes" not in task
    assert "writes=" not in task["detail"]


def test_an_unreadable_pipe_definition_costs_the_linkage_not_the_pipe():
    base = FakeSql(_responses(**{"information_schema.pipes": [
        {"PIPE_NAME": "P", "PIPE_SCHEMA": "S"}]}))

    def run_sql(sql, params=None):
        if "pipes" in sql.lower() and "definition" in sql.lower():
            raise RuntimeError("invalid identifier 'DEFINITION'")
        return base(sql, params)

    census = build_census(run_sql, ["DB"])
    assert census["kinds"]["PIPE"]["count"] == 1
    assert "writes" not in _one(census, "PIPE")
    assert "table each pipe loads" in census["kinds"]["PIPE"]["note"]


# ------------------------------------------------------- the plan's side

def _table(ident):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": "TABLE",
            "source_database": db, "source_schema": schema,
            "compatibility_status": "supported", "blocked_reasons": [],
            "row_count_exact": 1, "source_metadata": {}}


def _census(*objects):
    return {"objects": list(objects), "kinds": {}, "total": len(objects)}


def _load(kind, ident, writes):
    return {"kind": kind, "source_identifier": ident, "writes": writes,
            "reason": "", "detail": ""}


def test_a_migrated_table_fed_by_a_pipe_is_named_in_the_plan():
    from plan.build import build_plan
    inv = {"inventory": [_table("DB.S.STAGING_EVENTS"), _table("DB.S.OTHER")],
           "census": _census(
               _load("PIPE", "DB.S.PIPE_EVENTS", ["DB.S.STAGING_EVENTS"]),
               _load("TASK", "DB.S.TSK", ["DB.S.STAGING_EVENTS"]))}
    plan = build_plan(inv, {"edges": []})
    can = {c["source_identifier"]: c for c in plan["can_migrate"]}
    fed = can["DB.S.STAGING_EVENTS"]["load_warnings"]
    assert len(fed) == 2
    assert any("DB.S.PIPE_EVENTS" in w and "cutover" in w for w in fed)
    assert any("DB.S.TSK" in w for w in fed)
    assert not can["DB.S.OTHER"].get("load_warnings")
    assert plan["loads_that_stop"] == [
        {"table": "DB.S.STAGING_EVENTS", "kind": "PIPE",
         "source_identifier": "DB.S.PIPE_EVENTS"},
        {"table": "DB.S.STAGING_EVENTS", "kind": "TASK",
         "source_identifier": "DB.S.TSK"}]


def test_a_load_into_a_table_that_is_not_migrating_is_not_a_plan_warning():
    from plan.build import build_plan
    inv = {"inventory": [_table("DB.S.T")],
           "census": _census(_load("PIPE", "DB.S.P", ["DB.S.ELSEWHERE"]))}
    plan = build_plan(inv, {"edges": []})
    assert plan["loads_that_stop"] == []
    assert not plan["can_migrate"][0].get("load_warnings")


def test_a_plan_without_a_census_is_unchanged():
    from plan.build import build_plan
    plan = build_plan({"inventory": [_table("DB.S.T")]}, {"edges": []})
    assert plan["loads_that_stop"] == []


def test_the_load_warning_raises_the_risk_and_says_why():
    from plan.status import assess_risk
    level, note = assess_risk({"object_type": "TABLE", "load_warnings": [
        "loaded in Snowflake by PIPE DB.S.P; stops at cutover"]})
    assert level == "MEDIUM"
    assert "DB.S.P" in note


def test_planned_objects_lists_the_loads_that_stop():
    from plan.build import build_plan
    from report.render import render_planned_objects
    inv = {"inventory": [_table("DB.S.T")],
           "census": _census(_load("PIPE", "DB.S.P", ["DB.S.T"]))}
    md = render_planned_objects(build_plan(inv, {"edges": []}))
    head = "## Planned, but loaded by something that does not move"
    section = md[md.index(head) + len(head):].split("\n## ", 1)[0]
    assert "`DB.S.T`" in section
    assert "`DB.S.P`" in section
