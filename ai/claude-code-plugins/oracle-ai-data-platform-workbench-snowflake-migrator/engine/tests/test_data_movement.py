"""Data-movement options. MVP presents them; it executes nothing."""
import pytest

from plan.data_movement import (
    OPTIONS, NotImplementedInMvp, execute_transfer, options_for, record_choice,
)


def test_at_least_three_options_offered():
    assert len(OPTIONS) >= 3


def test_every_option_is_fully_described():
    for o in OPTIONS:
        assert o["id"] and o["name"]
        assert o["catalog_type"] in ("INTERNAL", "EXTERNAL", "BOTH", "TBD")
        assert o["etl"] and o["moves_bytes"] in (True, False, None)
        assert o["pros"] and o["cons"]
        assert o["unknowns"], "every option must state what is still unverified"
        assert o["status"] == "proposal_only"


def test_the_option_space_covers_the_named_mechanisms():
    blob = " ".join(
        f'{o["name"]} {o["etl"]} {" ".join(o["pros"])} {" ".join(o["cons"])}'
        for o in OPTIONS).lower()
    for mechanism in ("object storage", "fivetran", "external", "internal",
                      "iceberg", "interconnect"):
        assert mechanism in blob, mechanism


def test_internal_and_external_catalog_options_both_exist():
    kinds = {o["catalog_type"] for o in OPTIONS}
    assert "INTERNAL" in kinds and "EXTERNAL" in kinds


def test_at_least_one_option_moves_no_bytes():
    assert any(o["moves_bytes"] is False for o in OPTIONS)


def test_a_customer_defined_slot_exists_so_none_of_the_above_is_answerable():
    from plan.data_movement import CUSTOMER_DEFINED_ID
    assert any(o["id"] == CUSTOMER_DEFINED_ID for o in OPTIONS)


def test_options_for_historic_and_ongoing_are_distinguished():
    historic = options_for("historic")
    ongoing = options_for("ongoing")
    assert historic and ongoing
    assert {o["id"] for o in historic} != {o["id"] for o in ongoing}


def test_unknown_phase_is_rejected():
    with pytest.raises(ValueError, match="unknown phase"):
        options_for("someday")


# --- choice recording -----------------------------------------------------

def test_recording_a_choice_captures_it_without_acting():
    rec = record_choice("A1_UNLOAD_OBJECT_STORAGE", chosen_by="navid",
                        rationale="same-region unload avoids egress")
    assert rec["option_id"] == "A1_UNLOAD_OBJECT_STORAGE"
    assert rec["executed"] is False
    assert "navid" in rec["chosen_by"]
    assert rec["next_step"]


def test_recording_an_unknown_option_is_rejected():
    with pytest.raises(ValueError, match="unknown option"):
        record_choice("A9_TELEPORT", chosen_by="x", rationale="y")


def test_a_choice_requires_a_rationale():
    # The choice drives cost and wall-clock; an unexplained one is not a decision.
    with pytest.raises(ValueError, match="rationale"):
        record_choice("A1_UNLOAD_OBJECT_STORAGE", chosen_by="x", rationale="")


# --- the hard boundary ----------------------------------------------------

def test_execution_is_refused_for_every_option():
    for o in OPTIONS:
        with pytest.raises(NotImplementedInMvp) as exc:
            execute_transfer(o["id"])
        assert o["id"] in str(exc.value)


def test_the_refusal_says_what_would_have_to_be_settled_first():
    with pytest.raises(NotImplementedInMvp, match="region"):
        execute_transfer("A1_UNLOAD_OBJECT_STORAGE")
