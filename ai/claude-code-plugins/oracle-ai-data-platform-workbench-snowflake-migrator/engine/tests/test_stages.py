"""The stage board: what is supposed to run, what has run, and what it found.

Asked for so the run can be understood before it is executed. Reads only the
artifacts already on disk -- it touches no environment and makes no decisions.
"""
import json

from report.stages import STAGES, build_stage_board
from report.render import render_stages


def _write(tmp, name, payload):
    (tmp / name).write_text(json.dumps(payload))


def test_every_stage_is_listed_even_on_an_empty_directory(tmp_path):
    board = build_stage_board(tmp_path)
    assert [s["stage"] for s in board["stages"]] == [s["stage"] for s in STAGES]
    assert all(s["status"] == "NOT_RUN" for s in board["stages"])


def test_a_stage_with_its_artifact_reads_as_done(tmp_path):
    _write(tmp_path, "inventory.json",
           {"object_count": 7, "counts_by_type": {"TABLE": 6, "VIEW": 1},
            "extraction_notes": []})
    board = build_stage_board(tmp_path)
    assess = next(s for s in board["stages"] if s["stage"] == "assess")
    assert assess["status"] == "DONE"
    assert "7" in assess["found"]


def test_findings_are_summarised_per_stage(tmp_path):
    _write(tmp_path, "inventory.json", {"object_count": 7,
                                        "counts_by_type": {}, "extraction_notes": []})
    _write(tmp_path, "security.json", {"exposure_count": 2, "secure_views": [],
                                       "grants": {}})
    _write(tmp_path, "maintenance.json", {"objects_with_signals": 3,
                                          "tables": [1, 2, 3],
                                          "account_usage": {"readable": True}})
    board = build_stage_board(tmp_path)
    found = {s["stage"]: s["found"] for s in board["stages"]}
    assert "2" in found["security"]
    assert "3" in found["maintenance"]


def test_a_stage_that_found_a_problem_is_flagged(tmp_path):
    _write(tmp_path, "security.json", {"exposure_count": 2, "secure_views": [],
                                       "grants": {}})
    board = build_stage_board(tmp_path)
    sec = next(s for s in board["stages"] if s["stage"] == "security")
    assert sec["attention"] is True


def test_a_clean_stage_is_not_flagged(tmp_path):
    _write(tmp_path, "security.json", {"exposure_count": 0, "secure_views": [],
                                       "grants": {}})
    sec = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "security")
    assert sec["attention"] is False


def test_an_unreadable_probe_is_flagged_rather_than_read_as_clean(tmp_path):
    # "we could not look" must never present as "nothing found".
    _write(tmp_path, "security.json", {"exposure_count": None,
                                       "secure_views": [], "grants": {}})
    sec = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "security")
    assert sec["attention"] is True
    assert "unknown" in sec["found"].lower() or "not" in sec["found"].lower()


def test_the_board_says_which_stage_comes_next(tmp_path):
    _write(tmp_path, "inventory.json", {"object_count": 1, "counts_by_type": {},
                                        "extraction_notes": []})
    board = build_stage_board(tmp_path)
    assert board["next_stage"] == "deps"


def test_a_write_stage_is_marked_as_writing(tmp_path):
    deploy = next(s for s in STAGES if s["stage"] == "deploy")
    assert deploy["writes"] is True
    assess = next(s for s in STAGES if s["stage"] == "assess")
    assert assess["writes"] is False


def test_the_rendered_board_is_a_table_with_every_stage(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    assert "| Stage |" in md
    for s in STAGES:
        assert s["stage"] in md


def test_the_rendered_board_marks_the_only_writing_stage(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    assert "writes" in md.lower()
    assert "read-only" in md.lower()
