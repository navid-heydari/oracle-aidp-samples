"""Deployment gating, batching, and per-statement verification."""
import pytest

from target.coords import resolve_target
from target.deploy import RefusedToExecute, deploy

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="bronze")


def plan(n=1):
    return {"statements": [
        {"source_identifier": f"D.S.T{i}", "target_fqn": f"bronze.S.T{i}",
         "sql": f"CREATE TABLE IF NOT EXISTS `bronze`.`S`.`T{i}` (`A` STRING)\nUSING DELTA"}
        for i in range(n)], "blocked": []}


class Recorder:
    def __init__(self, existing=None, fail_on=None):
        self.calls = []
        self.existing = existing
        self.fail_on = fail_on or ()

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        if any(f in sql for f in self.fail_on):
            raise RuntimeError("boom")
        if sql.lstrip().upper().startswith("SHOW TABLES"):
            name = sql.split("LIKE")[1].strip().strip("'")
            if self.existing is None or name in self.existing:
                return [{"tableName": name}]
            return []
        return [{"status": "ok"}]


def test_dry_run_is_the_default_and_touches_nothing():
    rec = Recorder()
    out = deploy(plan(2), run_sql=rec)
    assert out["dry_run"] is True
    assert out["executed"] == 0
    assert rec.calls == [], "dry run must not issue a single statement"
    assert len(out["statements"]) == 2


def test_execute_without_a_target_is_refused():
    with pytest.raises(RefusedToExecute, match="target"):
        deploy(plan(), execute=True, run_sql=Recorder())


def test_execute_without_run_sql_is_refused():
    with pytest.raises(RefusedToExecute, match="run_sql"):
        deploy(plan(), execute=True, target=TARGET)


def test_execute_creates_the_schema_before_the_tables():
    rec = Recorder()
    deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    joined = " || ".join(rec.calls)
    assert "CREATE SCHEMA IF NOT EXISTS" in joined
    assert joined.index("CREATE SCHEMA") < joined.index("CREATE TABLE")


def test_statements_are_batched_into_one_execution_per_chunk():
    # AIDP discards per-statement DDL on session close, so DDL is batched.
    rec = Recorder()
    deploy(plan(5), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    batches = [c for c in rec.calls if c.count("CREATE TABLE") >= 1
               and not c.lstrip().upper().startswith("SHOW")]
    assert len(batches) == 3, "5 statements at chunk_size 2 -> 3 batches"


def test_every_statement_is_probed_individually_after_its_chunk():
    # A chunk can report success while individual statements inside it failed.
    rec = Recorder()
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=3)
    probes = [c for c in rec.calls if c.lstrip().upper().startswith("SHOW TABLES")]
    assert len(probes) == 3
    assert out["verified"] == 3
    assert out["failed"] == []


def test_missing_table_after_a_successful_chunk_is_reported_failed():
    rec = Recorder(existing={"T0"})
    out = deploy(plan(2), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    assert out["verified"] == 1
    assert [f["target_fqn"] for f in out["failed"]] == ["bronze.S.T1"]


def test_already_exists_is_not_a_failure():
    rec = Recorder()
    out = deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    assert out["failed"] == []


def test_chunk_error_does_not_abort_remaining_chunks():
    rec = Recorder(existing={"T2"}, fail_on=("`T0`",))
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=1)
    assert out["verified"] == 1
    assert len(out["failed"]) == 2
    assert out["chunk_errors"], "the failing chunk must be recorded, not swallowed"


def test_blocked_statements_are_never_executed():
    p = plan(1)
    p["blocked"] = [{"source_identifier": "D.S.BAD", "reason": "VARIANT"}]
    rec = Recorder()
    out = deploy(p, execute=True, target=TARGET, run_sql=rec)
    assert "BAD" not in " ".join(rec.calls)
    assert out["blocked_count"] == 1
