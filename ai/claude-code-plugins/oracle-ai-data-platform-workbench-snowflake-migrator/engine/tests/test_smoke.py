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


def test_write_probe_creates_then_removes_and_says_so():
    # Superseded: the probe used to leave its schema behind because the
    # source's no-DROP rule had been applied to the destination too.
    dest = DestRecorder()
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_run_sql=dest,
                  write_probe=True)
    assert r["destination"]["write_verified"] is True
    assert r["destination"]["left_behind"] == []
    assert "cleaned up" in r["destination"]["write_note"]

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


# ==========================================================================
# The write probe cleans up (issue #12).
#
# It could not before because the no-DROP rule was applied to the DESTINATION.
# That rule is a SOURCE guarantee: nothing is ever written to or dropped from
# Snowflake. AIDP is where this plugin legitimately creates objects, so
# removing its own probe schema there is correct.
# ==========================================================================

class DestRecorder:
    def __init__(self, *, pre_existing=False, fail_drop=False):
        self.calls: list[str] = []
        self.pre_existing, self.fail_drop = pre_existing, fail_drop
        self.created = False

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        up = sql.strip().upper()
        if up.startswith("DROP"):
            if self.fail_drop:
                raise RuntimeError("insufficient privilege to drop")
            self.created = False
            return []
        if up.startswith("CREATE SCHEMA"):
            self.created = True
            return []
        if up.startswith("SHOW SCHEMAS") and "LIKE" in up:
            if self.created or self.pre_existing:
                return [{"namespace": PROBE_SCHEMA}]
            return []
        if up.startswith("SHOW SCHEMAS"):
            return [{"namespace": "default"}]
        return []


def _target():
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="ws", cluster_id="cl", catalog="CAT")


class _Src:
    """Recording source double, so we can assert nothing but reads reach it."""

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        return sf_ok(sql, params)


def _src():
    return _Src()


def test_write_probe_drops_the_schema_it_created():
    dest = DestRecorder()
    res = run_smoke(source_run_sql=_src(), target=_target(), dest_run_sql=dest,
                    write_probe=True, database="DB")
    drops = [c for c in dest.calls if c.strip().upper().startswith("DROP")]
    assert len(drops) == 1
    assert res["destination"]["write_verified"] is True
    assert res["destination"]["left_behind"] == []
    assert "cleaned up" in res["destination"]["write_note"].lower()


def test_the_probe_only_ever_drops_its_own_schema():
    dest = DestRecorder()
    run_smoke(source_run_sql=_src(), target=_target(), dest_run_sql=dest,
              write_probe=True, database="DB")
    for call in dest.calls:
        if call.strip().upper().startswith("DROP"):
            assert PROBE_SCHEMA in call
            assert "CASCADE" not in call.upper(), "must not drop anything nested"


def test_a_pre_existing_probe_schema_is_not_dropped():
    # If it was already there, it is not ours to remove.
    dest = DestRecorder(pre_existing=True)
    res = run_smoke(source_run_sql=_src(), target=_target(), dest_run_sql=dest,
                    write_probe=True, database="DB")
    assert not [c for c in dest.calls if c.strip().upper().startswith("DROP")]
    assert res["destination"]["left_behind"] == []


def test_a_failed_cleanup_is_reported_and_names_what_is_left():
    dest = DestRecorder(fail_drop=True)
    res = run_smoke(source_run_sql=_src(), target=_target(), dest_run_sql=dest,
                    write_probe=True, database="DB")
    assert res["destination"]["left_behind"] == [f"CAT.{PROBE_SCHEMA}"]
    assert "remove it manually" in res["destination"]["write_note"]


def test_the_probe_schema_like_pattern_escapes_underscores():
    dest = DestRecorder()
    run_smoke(source_run_sql=_src(), target=_target(), dest_run_sql=dest,
              write_probe=True, database="DB")
    like = [c for c in dest.calls if "LIKE" in c.upper()][0]
    assert r"\_" in like, "the probe name is full of `_`, a LIKE wildcard"


def test_the_source_never_receives_a_write_even_during_the_write_probe():
    src = _src()
    run_smoke(source_run_sql=src, target=_target(), dest_run_sql=DestRecorder(),
              write_probe=True, database="DB")
    for call in src.calls:
        assert call.strip().split()[0].upper() in (
            "SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")
