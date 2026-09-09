"""Connectivity and permission smoke test. Both ends injected."""
import pytest

from target.coords import resolve_target
from plan.smoke import PROBE_SCHEMA, run_smoke

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="MYDB")


def sf_ok(sql, params=None):
    low = sql.lower()
    if "current_user" in low:
        return [{"U": "NHEYDARI", "A": "DU58131", "R": "AWS_US_EAST_2",
                 "ROLE": "ACCOUNTADMIN"}]
    if "show databases" in low:
        return [{"name": "MYDB"}]
    if "information_schema" in low:
        return [{"N": 7}]
    return [{"ok": 1}]


def aidp_ok(sql, params=None):
    low = sql.lower()
    if low.startswith("show schemas"):
        return [{"schema_name": PROBE_SCHEMA}]
    if low.startswith("show catalogs") or low.startswith("show databases"):
        return [{"catalog": "MYDB"}]
    return []


# --- source checks --------------------------------------------------------

def test_source_identity_reported():
    r = run_smoke(source_run_sql=sf_ok)
    assert r["source"]["reachable"] is True
    assert r["source"]["account"] == "DU58131"
    assert r["source"]["role"] == "ACCOUNTADMIN"


def test_source_read_checks_all_pass():
    r = run_smoke(source_run_sql=sf_ok)
    names = {c["name"]: c for c in r["source"]["checks"]}
    assert names["list databases"]["ok"] is True
    assert names["read INFORMATION_SCHEMA"]["ok"] is True
    assert all(c["ok"] for c in r["source"]["checks"])


def test_source_failure_is_captured_not_raised():
    def broken(sql, params=None):
        if "information_schema" in sql.lower():
            raise RuntimeError("insufficient privileges")
        return sf_ok(sql, params)

    r = run_smoke(source_run_sql=broken)
    bad = next(c for c in r["source"]["checks"] if not c["ok"])
    assert "insufficient privileges" in bad["detail"]
    assert r["ok"] is False


def test_unreachable_source_short_circuits():
    def dead(sql, params=None):
        raise RuntimeError("could not connect")

    r = run_smoke(source_run_sql=dead)
    assert r["source"]["reachable"] is False
    assert "could not connect" in r["source"]["error"]
    assert r["ok"] is False


# --- destination checks ---------------------------------------------------

def test_destination_skipped_when_no_target_supplied():
    r = run_smoke(source_run_sql=sf_ok)
    assert r["destination"]["skipped"] is True
    assert "not supplied" in r["destination"]["reason"].lower()


def test_destination_read_check_runs_when_target_supplied():
    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=aidp_ok)
    names = {c["name"] for c in r["destination"]["checks"]}
    assert "read target catalog" in names
    assert r["destination"]["skipped"] is False


def test_write_is_not_attempted_unless_asked():
    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=aidp_ok)
    assert r["destination"]["write_verified"] is False
    assert "not verified" in r["destination"]["write_note"].lower()
    assert not any("write" in c["name"] for c in r["destination"]["checks"])


def test_write_probe_creates_and_verifies_a_named_schema():
    calls = []

    def rec(sql, params=None):
        calls.append(sql)
        return aidp_ok(sql, params)

    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=rec,
                  write_probe=True)
    assert r["destination"]["write_verified"] is True
    assert any(f"CREATE SCHEMA IF NOT EXISTS" in c and PROBE_SCHEMA in c
               for c in calls)


def test_write_probe_never_drops_and_says_it_left_the_schema():
    calls = []

    def rec(sql, params=None):
        calls.append(sql)
        return aidp_ok(sql, params)

    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=rec,
                  write_probe=True)
    assert not any("DROP" in c.upper() for c in calls)
    # left_behind holds fully-qualified names, e.g. MYDB.snowmig_permission_probe
    assert r["destination"]["left_behind"] == [f"MYDB.{PROBE_SCHEMA}"]
    assert "remove" in r["destination"]["write_note"].lower()


def test_write_probe_failure_reported_not_raised():
    def no_write(sql, params=None):
        if sql.upper().startswith("CREATE"):
            raise RuntimeError("NotAuthorizedOrNotFound")
        return aidp_ok(sql, params)

    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=no_write,
                  write_probe=True)
    assert r["destination"]["write_verified"] is False
    assert "NotAuthorized" in r["destination"]["write_note"]
    assert r["ok"] is False


def test_write_probe_that_creates_but_cannot_verify_is_not_a_pass():
    def blind(sql, params=None):
        if sql.lower().startswith("show schemas"):
            return []          # created, but not visible
        return []

    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=blind,
                  write_probe=True)
    assert r["destination"]["write_verified"] is False


# --- overall verdict ------------------------------------------------------

def test_ok_is_true_only_when_every_run_check_passed():
    r = run_smoke(source_run_sql=sf_ok, target=TARGET, dest_run_sql=aidp_ok)
    assert r["ok"] is True


def test_probe_schema_name_is_obviously_ours():
    assert "snowmig" in PROBE_SCHEMA.lower()


def test_information_schema_probe_is_qualified_with_a_database():
    # A fresh Snowflake session has no current database, so an unqualified
    # INFORMATION_SCHEMA reference fails with 090105 even for ACCOUNTADMIN.
    seen = []

    def rec(sql, params=None):
        seen.append(sql)
        return sf_ok(sql, params)

    run_smoke(source_run_sql=rec)
    probe = next(s for s in seen if "information_schema" in s.lower())
    assert '"MYDB".information_schema' in probe


def test_explicit_database_is_used_for_the_probe():
    seen = []

    def rec(sql, params=None):
        seen.append(sql)
        return sf_ok(sql, params)

    run_smoke(source_run_sql=rec, database="CHOSEN")
    assert any('"CHOSEN".information_schema' in s for s in seen)


def test_system_databases_are_not_chosen_as_the_probe_target():
    def only_system(sql, params=None):
        if "show databases" in sql.lower():
            return [{"name": "SNOWFLAKE"}, {"name": "SNOWFLAKE_SAMPLE_DATA"}]
        return sf_ok(sql, params)

    r = run_smoke(source_run_sql=only_system)
    bad = next(c for c in r["source"]["checks"] if not c["ok"])
    assert "no non-system database" in bad["detail"]
