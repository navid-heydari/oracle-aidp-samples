"""The Snowflake transport refuses to write, whatever the credential allows."""
import pytest

from snowflake_source.conn import READ_ONLY_VERBS, SourceWriteRefused, make_run_sql


class FakeConn:
    def __init__(self):
        self.executed = []

    def cursor(self):
        conn = self

        class Cur:
            description = [("N",)]

            def execute(self, sql, params=None):
                conn.executed.append(sql)

            def fetchall(self):
                return [(1,)]
        return Cur()


@pytest.fixture
def run_sql():
    conn = FakeConn()
    fn = make_run_sql(conn)
    fn.conn = conn
    return fn


@pytest.mark.parametrize("sql", [
    "select 1",
    "SELECT count(*) FROM t",
    "show databases",
    "SHOW TABLES IN a.b",
    "describe table t",
    "desc user u",
    "with x as (select 1) select * from x",
    "explain select 1",
    "  \n select 1",
])
def test_read_statements_are_allowed(run_sql, sql):
    assert run_sql(sql) == [{"N": 1}]


@pytest.mark.parametrize("sql", [
    "insert into t values (1)",
    "INSERT INTO t SELECT * FROM s",
    "update t set a = 1",
    "delete from t",
    "drop table t",
    "DROP DATABASE d",
    "truncate table t",
    "create table t (a int)",
    "create or replace view v as select 1",
    "alter table t add column b int",
    "merge into t using s on 1=1 when matched then update set a=1",
    "copy into @stage from t",
    "grant select on t to role r",
    "revoke select on t from role r",
    "use database d",
    "call my_proc()",
    "put file:///tmp/x @stage",
    "remove @stage/x",
    "unset query_tag",
])
def test_every_write_or_mutation_is_refused(run_sql, sql):
    with pytest.raises(SourceWriteRefused):
        run_sql(sql)
    assert run_sql.conn.executed == [], "nothing may reach Snowflake"


def test_refusal_names_the_verb_and_the_rule(run_sql):
    with pytest.raises(SourceWriteRefused, match="DROP"):
        run_sql("drop table t")
    with pytest.raises(SourceWriteRefused, match="read-only"):
        run_sql("drop table t")


def test_a_write_hidden_after_a_read_is_still_refused(run_sql):
    # Statement stacking must not smuggle a write past the first verb.
    with pytest.raises(SourceWriteRefused):
        run_sql("select 1; drop table t")
    assert run_sql.conn.executed == []


def test_comment_prefix_does_not_disguise_a_write(run_sql):
    with pytest.raises(SourceWriteRefused):
        run_sql("-- harmless\ndrop table t")
    with pytest.raises(SourceWriteRefused):
        run_sql("/* nothing to see */ delete from t")


def test_unknown_verb_is_refused_not_allowed(run_sql):
    # Default deny: a verb we do not recognise is not assumed safe.
    with pytest.raises(SourceWriteRefused, match="not a recognised read"):
        run_sql("frobnicate the_table")


def test_empty_statement_refused(run_sql):
    with pytest.raises(SourceWriteRefused):
        run_sql("   ")


def test_allowlist_is_read_verbs_only():
    assert READ_ONLY_VERBS == ("SELECT", "SHOW", "DESCRIBE", "DESC", "WITH",
                               "EXPLAIN")
