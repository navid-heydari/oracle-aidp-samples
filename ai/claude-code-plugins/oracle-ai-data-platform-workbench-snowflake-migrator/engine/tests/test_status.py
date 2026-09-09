"""Per-object migration status and risk. Pure."""
import pytest

from plan.status import (
    MIGRATION_STATUS, RISK_LEVELS, assess_risk, migration_status,
)


def can(ident="D.S.T", kind="TABLE", rows=10, warnings=(), omitted=()):
    return {"source_identifier": ident, "object_type": kind, "rows": rows,
            "warnings": list(warnings), "omitted_properties": list(omitted)}


# --- migration status vocabulary -----------------------------------------

def test_status_vocabulary_is_fixed():
    assert MIGRATION_STATUS == ("NOT_YET_DONE", "IN_PROGRESS", "SHALLOW_CLONE",
                                "DATA_CLONE", "DONE", "BLOCKED")


def test_planned_but_not_deployed_is_not_yet_done():
    assert migration_status("D.S.T", deployed=None) == "NOT_YET_DONE"


def test_verified_structure_is_shallow_clone():
    dep = {"dry_run": False, "verified_targets": ["D.S.T"], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "SHALLOW_CLONE"


def test_attempted_but_unverified_is_in_progress():
    dep = {"dry_run": False, "attempted_targets": ["D.S.T"],
           "verified_targets": [], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "IN_PROGRESS"


def test_failed_verification_is_in_progress_not_done():
    dep = {"dry_run": False, "attempted_targets": ["D.S.T"],
           "verified_targets": [], "failed_targets": ["D.S.T"]}
    assert migration_status("D.S.T", deployed=dep) == "IN_PROGRESS"


def test_dry_run_never_reports_progress():
    dep = {"dry_run": True, "verified_targets": ["D.S.T"], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "NOT_YET_DONE"


def test_blocked_object_is_blocked():
    assert migration_status("D.S.T", deployed=None, blocked=True) == "BLOCKED"


def test_data_clone_and_done_are_never_produced_by_this_mvp():
    # The plugin moves no data, so no code path may claim otherwise.
    dep = {"dry_run": False, "verified_targets": ["D.S.T"], "failed_targets": [],
           "rows_copied": {"D.S.T": 100}}
    assert migration_status("D.S.T", deployed=dep) == "SHALLOW_CLONE"


# --- risk ----------------------------------------------------------------

def test_risk_levels_are_fixed():
    assert RISK_LEVELS == ("LOW", "MEDIUM", "HIGH")


def test_plain_mapped_table_is_low_risk():
    level, note = assess_risk(can())
    assert level == "LOW"
    assert note


def test_blocked_object_is_high_risk_with_the_reason():
    level, note = assess_risk(
        {"source_identifier": "D.S.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT"},
        blocked=True)
    assert level == "HIGH"
    assert "VARIANT" in note


def test_view_is_medium_risk_for_untranslated_sql():
    level, note = assess_risk(can(kind="VIEW"))
    assert level == "MEDIUM"
    assert "dialect" in note.lower() or "verify" in note.lower()


def test_dropped_properties_raise_risk_to_medium():
    level, note = assess_risk(can(omitted=["cluster_by=(C)"]))
    assert level == "MEDIUM"
    assert "cluster_by" in note


def test_large_table_is_flagged_for_the_later_data_phase():
    level, note = assess_risk(can(rows=500_000_000))
    assert level == "MEDIUM"
    assert "row" in note.lower()


def test_timezone_warning_raises_risk():
    level, note = assess_risk(can(warnings=["TS: TIMESTAMP_LTZ timezone semantics"]))
    assert level == "MEDIUM"
    assert "timezone" in note.lower()


def test_highest_applicable_risk_wins():
    level, _ = assess_risk(can(kind="VIEW", rows=999_999_999), blocked=True)
    assert level == "HIGH"


def test_note_is_always_a_sentence_not_empty():
    for obj in (can(), can(kind="VIEW"), can(rows=10**9)):
        _, note = assess_risk(obj)
        assert note and len(note) > 15
