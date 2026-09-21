"""Security posture: what protects the data today, and what arrives without it.

This is the only gap with a data-EXPOSURE consequence. A Snowflake table whose
columns carry a masking policy lands on AIDP as plain Delta with no policy, so
values that were masked for most roles become readable to anyone who can read
the table. Nothing in the plugin said so.

Reports; changes nothing. AIDP's answer is restricted views plus ontology
sensitivity -- there is no masking API -- so an equivalent cannot be generated
here even in principle.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.security import build_security


def _inv():
    return {"databases_in_scope": ["DB"],
            "inventory": [
                {"source_identifier": "DB.PUBLIC.CUSTOMERS",
                 "object_type": "TABLE", "source_database": "DB",
                 "source_schema": "PUBLIC", "source_metadata": {}},
                {"source_identifier": "DB.PUBLIC.V_SECURE",
                 "object_type": "VIEW", "source_database": "DB",
                 "source_schema": "PUBLIC",
                 "source_metadata": {"is_secure": "true"}}]}


def _responses(**over):
    base = {
        "show masking policies": [],
        "show row access policies": [],
        "show tags": [],
        "policy_references": [],
        "grants_to_roles": [],
    }
    base.update(over)
    return base


def _policy_ref(obj="CUSTOMERS", col="EMAIL", kind="MASKING_POLICY",
                name="PII_MASK"):
    return {"REF_DATABASE_NAME": "DB", "REF_SCHEMA_NAME": "PUBLIC",
            "REF_ENTITY_NAME": obj, "REF_COLUMN_NAME": col,
            "POLICY_KIND": kind, "POLICY_NAME": name}


# ------------------------------------------------------------------ exposure

def test_a_clean_estate_reports_no_exposure():
    # No secure view here: a secure view is itself a finding, so including one
    # would not be a clean estate.
    clean = {"databases_in_scope": ["DB"],
             "inventory": [{"source_identifier": "DB.PUBLIC.CUSTOMERS",
                            "object_type": "TABLE", "source_database": "DB",
                            "source_schema": "PUBLIC", "source_metadata": {}}]}
    s = build_security(FakeSql(_responses()), clean)
    assert s["exposures"] == []
    assert s["exposure_count"] == 0
    assert "no masking" in s["statement"].lower()


def test_a_masked_column_on_a_migrating_table_is_an_exposure():
    s = build_security(FakeSql(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert s["exposure_count"] == 1
    e = s["exposures"][0]
    assert e["object"] == "DB.PUBLIC.CUSTOMERS"
    assert e["column"] == "EMAIL"
    assert e["policy"] == "PII_MASK"
    assert e["severity"] == "HIGH"
    assert "unmask" in e["consequence"].lower() or "readable" in e["consequence"].lower()


def test_a_policy_on_an_object_outside_the_migration_is_not_an_exposure():
    # It is still reported as context, but it is not a regression we cause.
    s = build_security(FakeSql(_responses(
        policy_references=[_policy_ref(obj="NOT_MIGRATING")])), _inv())
    assert s["exposure_count"] == 0
    assert s["policy_references_out_of_scope"] == 1


def test_a_row_access_policy_is_an_exposure_at_row_level():
    s = build_security(FakeSql(_responses(policy_references=[
        _policy_ref(col=None, kind="ROW_ACCESS_POLICY", name="REGION_RLS")])),
        _inv())
    e = s["exposures"][0]
    assert e["policy_kind"] == "ROW_ACCESS_POLICY"
    assert e["column"] is None
    assert "row" in e["consequence"].lower()


def test_the_statement_is_loud_when_there_are_exposures():
    s = build_security(FakeSql(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert "1" in s["statement"]
    assert s["statement"].isupper() is False        # not shouting in caps
    assert "exposure" in s["statement"].lower() or "unprotected" in s["statement"].lower()


# ------------------------------------------------------------- secure views

def test_a_secure_view_is_flagged_because_secure_is_not_carried_over():
    s = build_security(FakeSql(_responses()), _inv())
    assert "DB.PUBLIC.V_SECURE" in [v["object"] for v in s["secure_views"]]
    assert s["secure_views"][0]["severity"] == "HIGH"


# ------------------------------------------------------------------- grants

def test_grants_are_summarised_per_object_when_readable():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "PUBLIC", "DATABASE_NAME": "DB",
         "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST", "GRANTS": 1}])),
        _inv())
    g = s["grants"]
    assert g["measured"] is True
    assert g["by_object"]["DB.PUBLIC.CUSTOMERS"][0]["role"] == "ANALYST"


def test_unreadable_grants_are_not_measured_rather_than_none_existing():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "grants_to_roles" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _inv())
    assert s["grants"]["measured"] is False
    assert s["grants"]["by_object"] == {}
    assert any("grant" in u.lower() for u in s["unreadable"])


def test_no_grants_are_ever_translated_into_target_ddl():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "PUBLIC", "DATABASE_NAME": "DB",
         "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST", "GRANTS": 1}])),
        _inv())
    blob = repr(s).upper()
    assert "GRANT " not in blob, "reporting grants is not the same as replaying them"


# --------------------------------------------------------------- degradation

def test_one_denied_probe_does_not_lose_the_others():
    class Partial(FakeSql):
        def __call__(self, sql, params=None):
            if "show masking policies" in sql.lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    s = build_security(Partial(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert s["exposure_count"] == 1, "policy references still read"
    assert s["policies"]["masking"]["readable"] is False


def test_when_policy_references_cannot_be_read_the_answer_is_unknown():
    # The dangerous case: we cannot prove there are no policies, so we must
    # not imply there are none.
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "policy_references" in sql.lower():
                raise RuntimeError("Insufficient privileges on ACCOUNT_USAGE")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _inv())
    assert s["exposure_count"] is None
    assert "could not" in s["statement"].lower() or "unknown" in s["statement"].lower()
    assert "no masking" not in s["statement"].lower()


# ------------------------------------------------- defined, but not seen attached
#
# The only attachment source is ACCOUNT_USAGE.POLICY_REFERENCES, which lags
# up to ~2 hours. A policy attached inside that window is invisible there
# while SHOW MASKING/ROW ACCESS POLICIES already lists the policy object. An
# estate prepared shortly before the run is exactly this case, and the
# statement must not read "nothing is protected" on it.

def _table_only_inv():
    return {"databases_in_scope": ["DB"],
            "inventory": [{"source_identifier": "DB.PUBLIC.CUSTOMERS",
                           "object_type": "TABLE", "source_database": "DB",
                           "source_schema": "PUBLIC", "source_metadata": {}}]}


def _masking_policy(name="MASK_SSN"):
    return {"name": name, "database_name": "DB", "schema_name": "PUBLIC",
            "kind": "MASKING_POLICY"}


def test_a_defined_policy_with_no_visible_attachment_is_not_reported_clean():
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()]})), _table_only_inv())
    assert s["exposure_count"] == 0, "still an int: nothing was SEEN attached"
    assert s["policies_defined_without_attachment"] == 1
    st = s["statement"].lower()
    assert "no masking" not in st
    assert "unconfirmed" in st
    assert "lag" in st
    assert "nothing is protected" not in st


def test_a_defined_row_access_policy_triggers_the_same_hedge():
    s = build_security(FakeSql(_responses(**{
        "show row access policies": [{"name": "RAP_REGION", "database_name": "DB",
                                      "schema_name": "PUBLIC",
                                      "kind": "ROW_ACCESS_POLICY"}]})),
        _table_only_inv())
    assert s["policies_defined_without_attachment"] == 1
    assert "unconfirmed" in s["statement"].lower()


def test_the_clean_statement_names_the_source_and_its_lag():
    s = build_security(FakeSql(_responses()), _table_only_inv())
    st = s["statement"].lower()
    assert "no masking" in st
    assert "policy_references" in st and "lag" in st, \
        "a clean verdict says where it looked and how stale that can be"
    assert s["policies_defined_without_attachment"] == 0
    assert "lag" in s["attachment_source"].lower()


def test_a_policy_attached_out_of_scope_does_not_trigger_the_hedge():
    # The defined policy IS accounted for: it is attached to something we are
    # not migrating, so the empty in-scope list is corroborated.
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()],
        "policy_references": [_policy_ref(obj="NOT_MIGRATING")]})),
        _table_only_inv())
    assert s["exposure_count"] == 0
    assert s["policy_references_out_of_scope"] == 1
    assert s["policies_defined_without_attachment"] == 0
    assert "unconfirmed" not in s["statement"].lower()


def test_unenumerable_policy_objects_cannot_corroborate_an_empty_attachment_list():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show masking policies" in sql.lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _table_only_inv())
    assert s["exposure_count"] == 0
    st = s["statement"].lower()
    assert "no masking" not in st
    assert "could not be enumerated" in st or "cannot corroborate" in st


def test_zero_visible_policy_objects_are_noted_as_role_filtered():
    # SHOW MASKING / ROW ACCESS POLICIES lists only policies the role owns or
    # can APPLY. The account-wide verdict comes from ACCOUNT_USAGE; this table
    # is context, and its zero must say what it is.
    s = build_security(FakeSql(_responses()), _inv())
    for key in ("masking", "row_access"):
        info = s["policies"][key]
        assert info["count"] == 0 and info["readable"] is True
        assert "visible" in info["note"], info["note"]
