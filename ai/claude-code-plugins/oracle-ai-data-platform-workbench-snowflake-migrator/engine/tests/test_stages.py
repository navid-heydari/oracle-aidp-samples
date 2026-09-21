"""The stage board: what is supposed to run, what has run, and what it found.

Asked for so the run can be understood before it is executed. Reads only the
artifacts already on disk -- it touches no environment and makes no decisions.
"""
import json

from report.stages import STAGES, build_stage_board
from report.render import render_stages


def _write(tmp, name, payload):
    (tmp / name).write_text(json.dumps(payload), encoding="utf-8")


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


def test_the_rendered_board_marks_every_writing_stage(tmp_path):
    # The one-writer claim once made the (then-new) default stage invisible;
    # the writer set is now pinned so a new writer cannot ship unlisted.
    md = render_stages(build_stage_board(tmp_path))
    assert "writes" in md.lower()
    assert "read-only" in md.lower()
    assert "`provision`" in md and "`catalog`" in md and "`deploy`" in md
    writers = {s["stage"] for s in STAGES if s["writes"]}
    assert writers == {"provision", "catalog", "deploy"}


def test_a_provision_run_with_failures_is_flagged(tmp_path):
    _write(tmp_path, "provision_result.json",
           {"dry_run": False, "workspace": {"name": "acme_prod"},
            "steps": [{"step": "workspace", "action": "reused",
                       "verified": True, "detail": ""},
                      {"step": "upload", "action": "failed",
                       "verified": False, "detail": "boom"}]})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "provision")
    assert row["attention"] is True
    assert "1 failed" in row["found"]


def test_the_catalog_stage_is_on_the_board_and_reads_its_artifact(tmp_path):
    cat = next(s for s in STAGES if s["stage"] == "catalog")
    assert cat["writes"] is True
    assert cat["artifact"] == "catalog_result.json"
    _write(tmp_path, "catalog_result.json",
           {"dry_run": False, "catalog": "lake", "catalog_type": "EXTERNAL",
            "action": "created", "verified": True})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "catalog")
    assert row["status"] == "DONE"
    assert "created" in row["found"]
    assert row["attention"] is False


def test_a_catalog_registration_that_stayed_pending_is_flagged(tmp_path):
    # 202 Accepted is not the claim: a create that never became visible must
    # not read as success on the board.
    _write(tmp_path, "catalog_result.json",
           {"dry_run": False, "catalog": "lake", "catalog_type": "EXTERNAL",
            "action": "create_requested", "verified": False})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "catalog")
    assert row["attention"] is True
    assert "never became visible" in row["found"]


def test_an_optional_stage_is_never_proposed_as_next(tmp_path):
    # data-options feeds `plan` when run, but `plan` runs without it, so the
    # board must not stall on it as "next".
    _write(tmp_path, "inventory.json",
           {"object_count": 1, "counts_by_type": {}, "extraction_notes": []})
    _write(tmp_path, "dependencies.json", {"source_used": "x", "cycles": []})
    _write(tmp_path, "maintenance.json",
           {"objects_with_signals": 0, "tables": [],
            "account_usage": {"readable": True}})
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {}})
    _write(tmp_path, "compute.json", {"proposals": []})
    board = build_stage_board(tmp_path)
    assert board["next_stage"] == "plan"


# --------------------------------------------------------------------------
# "A stage that could not look is FLAGGED, never shown as clean" -- the rule
# at the top of report/stages.py, checked against the artifacts as their
# producers actually write them.
# --------------------------------------------------------------------------

def _row(tmp_path, stage):
    return next(s for s in build_stage_board(tmp_path)["stages"]
                if s["stage"] == stage)


def test_parsed_ddl_lineage_is_flagged_as_partial(tmp_path):
    # dependencies.json never carries `cycles` (that key lives in plan.json),
    # so the old rule could never fire. A graph parsed from view DDL alone is
    # partial, and the plan skill says so; the board must too.
    _write(tmp_path, "dependencies.json",
           {"edges": [], "source_used": "parsed_ddl",
            "coverage_note": "views only", "unresolved_references": ["DB.S.T"]})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "partial" in row["found"].lower()
    assert "1 unresolved" in row["found"]


def test_not_extracted_lineage_is_flagged(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [], "source_used": "not_extracted",
            "coverage_note": "manifest ingest", "unresolved_references": []})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "not extracted" in row["found"].lower()


def test_account_usage_lineage_is_clean(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [{"from": "A", "to": "B"}], "source_used": "account_usage",
            "coverage_note": "", "unresolved_references": []})
    row = _row(tmp_path, "deps")
    assert row["attention"] is False
    assert "1 edge" in row["found"]


def test_blocked_warehouses_are_counted_and_flagged(tmp_path):
    # compute.json is written from propose_all: `proposals` and `blocked`.
    _write(tmp_path, "compute.json",
           {"proposals": [{"name": "WH_A"}],
            "blocked": [{"name": "WH_B", "reason": "no metering"}]})
    row = _row(tmp_path, "compute")
    assert row["attention"] is True
    assert "1 warehouse(s) sized" in row["found"]
    assert "1 blocked" in row["found"]


def test_a_provision_step_left_unconfirmed_is_flagged(tmp_path):
    # `libraries: install_requested + restart` records verified None in
    # execute mode: not confirmed, not assumed. That is not clean.
    _write(tmp_path, "provision_result.json",
           {"dry_run": False, "workspace": {"name": "ws"},
            "steps": [{"step": "workspace", "action": "created",
                       "verified": True, "detail": ""},
                      {"step": "libraries", "action": "install_requested + restart",
                       "verified": None, "detail": "NOT confirmed, not assumed"}]})
    row = _row(tmp_path, "provision")
    assert row["attention"] is True
    assert "1 not confirmed" in row["found"]


def test_unverified_structure_and_drift_are_not_hidden_on_the_board(tmp_path):
    # The default transport (catalog_api) is the one that produces these two
    # outcomes, and the board summed only failed + mismatched.
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 4, "verified": 2,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": ["X"],
            "derived_type_drift_targets": ["Y"], "errors": []})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is True
    assert "verified 2/4" in row["found"]
    assert "1 structure not verified" in row["found"]
    assert "1 created with derived type drift" in row["found"]
    assert "deploy" in build_stage_board(tmp_path)["needs_attention"]


def test_deploy_transport_errors_are_flagged(tmp_path):
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 2, "verified": 1,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": [], "derived_type_drift_targets": [],
            "errors": [{"schema": "c.s", "error": "409 ongoing operation"}]})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is True
    assert "1 error" in row["found"]


def test_a_fully_verified_deploy_is_clean(tmp_path):
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 3, "verified": 3,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": [], "derived_type_drift_targets": [],
            "errors": []})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is False
    assert row["found"] == "**verified 3/3**"
