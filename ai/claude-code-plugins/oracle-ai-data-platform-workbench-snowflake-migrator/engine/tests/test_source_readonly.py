"""The Snowflake transport refuses to write, whatever the credential allows."""
import pytest

from snowflake_source import conn
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
    "with a as (select 1), b as (select 2) select * from a, b",
    "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) "
    "SELECT * FROM r",
    'with "Weird Name" as (select 1) select 1',
    "with x as (select 1) /* c */ select * from x",
    "with x /* c */ as (select 1) -- c\n select * from x",
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
    # A leading WITH is not a read by itself: the verb that matters is the
    # one after the CTE list.
    "with x as (select 1 a) insert into t select a from x",
    "with x as (select 1) delete from t",
    "with x as (select 1) update t set a = 1",
    "with x as (select 1) merge into t using x on 1=1 when matched then delete",
    "with x as (select 1) create table t2 as select * from x",
    "WITH src AS (SELECT * FROM s) INSERT INTO SALES_DB.PUBLIC.AUDIT SELECT * FROM src",
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


# --------------------------------------------------------------------------
# Scanner-backed guard (issue #18). The guard used to regex out comments and
# str.split(";"), which cannot tell code from the inside of a literal.
# --------------------------------------------------------------------------

def test_semicolon_inside_a_literal_is_one_read_not_a_smuggled_write():
    # Correctly parsed this is a single SELECT whose projection contains the
    # text "drop table t". The old splitter saw a second statement starting
    # with DROP. Both answers refuse a write; only one of them is right, and
    # the wrong one refuses legitimate reads.
    conn.assert_read_only("select 'a;drop table t' as note")


def test_comment_marker_inside_a_literal_does_not_blind_the_guard():
    # A naive `--[^\n]*` strip removes the rest of the line, which could hide
    # a real statement separator from the guard.
    conn.assert_read_only("select 'x -- y' as note")
    with pytest.raises(conn.SourceWriteRefused):
        conn.assert_read_only("select 'x -- y' as note; drop table t")


def test_statement_that_is_only_a_literal_has_no_verb_and_is_refused():
    with pytest.raises(conn.SourceWriteRefused) as exc:
        conn.assert_read_only("'drop table t'")
    assert "no leading SQL keyword" in str(exc.value)


def test_quoted_identifier_containing_a_write_verb_is_still_a_read():
    conn.assert_read_only('select 1 as "drop table t"')


def test_write_hidden_behind_a_block_comment_is_refused():
    with pytest.raises(conn.SourceWriteRefused):
        conn.assert_read_only("/* select */ delete from t")


def test_unscannable_sql_fails_closed():
    # An unterminated literal means we cannot know where statements end, so
    # the guard must refuse rather than let it through.
    with pytest.raises(conn.SourceWriteRefused) as exc:
        conn.assert_read_only("select 'oops")
    assert "could not be scanned" in str(exc.value)


def test_dollar_quoted_body_cannot_smuggle_a_write():
    conn.assert_read_only("select $$ ; drop table t $$ as body")


# --------------------------------------------------------------------------
# CTE-prefixed statements. `WITH` is on the allowlist because a CTE-SELECT is
# a read, but WITH is only the first word: `WITH x AS (...) INSERT ...` leads
# with it too. The guard looks past the CTE list to the statement's own verb.
# --------------------------------------------------------------------------

def test_cte_refusal_names_the_body_verb_and_the_rule(run_sql):
    with pytest.raises(SourceWriteRefused, match="INSERT"):
        run_sql("with x as (select 1) insert into t select 1")
    with pytest.raises(SourceWriteRefused, match="read-only"):
        run_sql("with x as (select 1) insert into t select 1")
    assert run_sql.conn.executed == []


def test_cte_with_a_parenthesised_body_is_refused_not_guessed(run_sql):
    # `with x as (select 1) (select * from x)` is a legitimate read the walker
    # does not follow. Refusing it is the documented posture: SQL the guard
    # cannot positively identify as a read does not go to Snowflake.
    with pytest.raises(SourceWriteRefused, match="no keyword"):
        run_sql("with x as (select 1) (select * from x)")
    assert run_sql.conn.executed == []


def test_write_verb_inside_a_cte_body_literal_is_data_not_the_verb(run_sql):
    # `insert` and `;` inside the CTE body are text. The body verb is the
    # first keyword at paren depth 0 after the CTE list, which is SELECT.
    sql = "with x as (select 'insert; delete' as w) select w from x"
    assert run_sql(sql) == [{"N": 1}]
    assert run_sql.conn.executed == [sql]


# ------------------------- the transport that runs INSIDE AIDP refuses too
#
# Raised by the repo owner on the review PR: conn.py was hardened against
# CTE-prefixed writes while dataplane/snowmig_source.py's pushdown() had no
# verb enforcement at all -- and that is the transport the migration
# notebooks actually run on the cluster, against the customer's live
# Snowflake, with whatever the credential allows.
#
# The guard there is deliberately STRICTER than the control plane's, not a
# second copy of it: that module runs standalone on a cluster and cannot
# import the engine's lexer, so a CTE is refused rather than followed.

from dataplane.snowmig_source import (  # noqa: E402
    PUSHDOWN_READ_VERBS, SourceWriteRefused as PushdownRefused,
    assert_pushdown_read_only)


@pytest.mark.parametrize("sql", [
    "select 1",
    "SELECT * from T",
    "  \n select a from b where c = ';'",
    "show tables in database D",
    "describe table T",
    "desc table T",
    "explain select 1",
    "-- a leading comment\nselect 1",
    "/* block */ select 1",
])
def test_a_read_is_allowed_through_the_pushdown_guard(sql):
    assert_pushdown_read_only(sql)


@pytest.mark.parametrize("sql", [
    "insert into T values (1)",
    "INSERT INTO T SELECT * FROM S",
    "update T set a = 1",
    "delete from T",
    "merge into T using S on T.a = S.a when matched then update set a = 1",
    "drop table T",
    "create table T (a int)",
    "alter table T add column b int",
    "truncate table T",
    "grant select on T to role R",
    "call my_proc()",
    "copy into @stage from T",
    "put file:///tmp/x @stage",
    "remove @stage",
    "use database D",
])
def test_every_write_is_refused_by_the_pushdown_guard(sql):
    with pytest.raises(PushdownRefused):
        assert_pushdown_read_only(sql)


def test_a_write_smuggled_behind_a_read_is_refused():
    with pytest.raises(PushdownRefused) as e:
        assert_pushdown_read_only("select 1; drop table T")
    assert "statements" in str(e.value)


def test_a_semicolon_inside_a_literal_is_one_read():
    assert_pushdown_read_only("select 'a;drop table t' as x")


def test_a_comment_marker_inside_a_literal_does_not_blind_the_guard():
    with pytest.raises(PushdownRefused):
        assert_pushdown_read_only("select '--' as x; delete from T")


def test_a_write_hidden_behind_a_comment_is_refused():
    with pytest.raises(PushdownRefused):
        assert_pushdown_read_only("-- select 1\ndelete from T")


def test_a_cte_is_refused_rather_than_analysed_on_the_cluster():
    """conn.py follows a CTE to its body. This module cannot: it has no
    lexer on the cluster, so it fails closed and says why."""
    with pytest.raises(PushdownRefused) as e:
        assert_pushdown_read_only("with x as (select 1) select * from x")
    assert "cte" in str(e.value).lower()
    assert "subquery" in str(e.value).lower()


def test_a_cte_prefixed_write_is_refused_too():
    with pytest.raises(PushdownRefused):
        assert_pushdown_read_only(
            "with x as (select 1) insert into T select * from x")


def test_an_empty_statement_is_refused():
    with pytest.raises(PushdownRefused):
        assert_pushdown_read_only("   ")


def test_the_allowlist_holds_no_write_verb():
    for verb in PUSHDOWN_READ_VERBS:
        assert verb in ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")
    assert "WITH" not in PUSHDOWN_READ_VERBS


def test_pushdown_refuses_before_it_looks_at_the_mode():
    """The refusal may not depend on configuration being right."""
    from dataplane.snowmig_source import SnowflakeSource
    src = SnowflakeSource.__new__(SnowflakeSource)
    src.mode = "external-catalog"          # would raise SourceConfigError
    with pytest.raises(PushdownRefused):
        SnowflakeSource.pushdown(src, "delete from T")
