"""User-supplied restrictions applied to the estate. Pure."""
import pytest

from plan.restrictions import (
    InvalidRestriction, apply_restrictions, validate_restrictions,
)


def rec(ident, kind="TABLE", rows=10, byts=100):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "row_count_exact": rows, "source_metadata": {"bytes": byts}}


RECS = [rec("D1.PUBLIC.ORDERS"), rec("D1.STAGE.TMP_X"),
        rec("D2.PUBLIC.BIG", rows=5_000_000), rec("D1.PUBLIC.V", kind="VIEW")]


def test_no_restrictions_keeps_everything():
    kept, excluded = apply_restrictions(RECS, None)
    assert len(kept) == 4 and excluded == []


def test_exclude_databases():
    kept, excluded = apply_restrictions(RECS, {"exclude_databases": ["D2"]})
    assert [e["source_identifier"] for e in excluded] == ["D2.PUBLIC.BIG"]
    assert "database D2 excluded" in excluded[0]["reason"]


def test_include_databases_is_an_allowlist():
    kept, excluded = apply_restrictions(RECS, {"include_databases": ["D2"]})
    assert [k["source_identifier"] for k in kept] == ["D2.PUBLIC.BIG"]
    assert len(excluded) == 3


def test_exclude_schemas():
    kept, _ = apply_restrictions(RECS, {"exclude_schemas": ["STAGE"]})
    assert "D1.STAGE.TMP_X" not in [k["source_identifier"] for k in kept]


def test_exclude_object_types():
    kept, excluded = apply_restrictions(RECS, {"exclude_object_types": ["VIEW"]})
    assert all(k["object_type"] == "TABLE" for k in kept)
    assert "object type VIEW excluded" in excluded[0]["reason"]


def test_max_rows():
    kept, excluded = apply_restrictions(RECS, {"max_rows": 1000})
    assert "D2.PUBLIC.BIG" in [e["source_identifier"] for e in excluded]
    assert "5000000 rows exceeds max_rows 1000" in excluded[0]["reason"]


def test_max_bytes():
    _, excluded = apply_restrictions(RECS, {"max_bytes": 50})
    assert len(excluded) == 4


def test_exclude_name_pattern():
    kept, excluded = apply_restrictions(RECS, {"exclude_name_patterns": ["^TMP_"]})
    assert "D1.STAGE.TMP_X" in [e["source_identifier"] for e in excluded]
    assert "^TMP_" in excluded[0]["reason"]


def test_explicit_object_exclusion_wins():
    _, excluded = apply_restrictions(RECS, {"exclude_objects": ["D1.PUBLIC.ORDERS"]})
    assert excluded[0]["source_identifier"] == "D1.PUBLIC.ORDERS"
    assert "explicitly excluded" in excluded[0]["reason"]


def test_object_exclusion_is_case_insensitive():
    _, excluded = apply_restrictions(RECS, {"exclude_objects": ["d1.public.orders"]})
    assert len(excluded) == 1


def test_multiple_restrictions_combine():
    kept, excluded = apply_restrictions(
        RECS, {"exclude_object_types": ["VIEW"], "max_rows": 1000})
    assert [k["source_identifier"] for k in kept] == ["D1.PUBLIC.ORDERS",
                                                      "D1.STAGE.TMP_X"]
    assert len(excluded) == 2


def test_every_exclusion_carries_a_reason():
    _, excluded = apply_restrictions(RECS, {"exclude_databases": ["D1", "D2"]})
    assert all(e["reason"] for e in excluded)
    assert all(e["restriction"] for e in excluded)


def test_unknown_restriction_key_is_rejected_not_ignored():
    # Silently ignoring a typo'd key would apply nothing and look like success.
    with pytest.raises(InvalidRestriction, match="exclude_datbases"):
        validate_restrictions({"exclude_datbases": ["D1"]})


def test_wrong_type_rejected():
    with pytest.raises(InvalidRestriction, match="list"):
        validate_restrictions({"exclude_databases": "D1"})


def test_bad_regex_rejected_early():
    with pytest.raises(InvalidRestriction, match="regex"):
        validate_restrictions({"exclude_name_patterns": ["([unclosed"]})


def test_valid_restrictions_pass_validation():
    validate_restrictions({"exclude_databases": ["A"], "max_rows": 10,
                           "exclude_name_patterns": ["^TMP_"]})
