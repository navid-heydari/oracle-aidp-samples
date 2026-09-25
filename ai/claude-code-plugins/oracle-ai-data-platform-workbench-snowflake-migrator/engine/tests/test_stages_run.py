"""`run` writes to AIDP, and the stage board has to say so.

STAGES.md and the stage-board skill said three stages write -- provision,
catalog and deploy, each a dry run without --execute -- and "Every other
stage is read-only". `run` was never on the board: it has no --execute
(`run --execute` is an unrecognised argument), and a single `run --job
snowmig_02_copy_schema` starts a job that copies rows on the cluster, while
`run --job snowmig_01_structure` creates schemas and tables. PRIVACY.md
already listed `run` as a writer. After jobs had run, the board showed no
row for them and said "Every stage has run". The skill tells the agent to
answer a nervous user with that read-only sentence.
"""
import json
import pathlib

from report.render import render_stages
from report.stages import STAGES, build_stage_board

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _write(d, name, data):
    (d / name).write_text(json.dumps(data), encoding="utf-8")


def test_run_is_a_writing_stage_on_the_board():
    spec = next((s for s in STAGES if s["stage"] == "run"), None)
    assert spec is not None, "run is not on the stage board"
    assert spec["writes"] is True
    assert spec.get("optional"), "a structure-only clone never runs a job"


def test_the_preamble_names_run_as_a_writer_with_no_dry_run(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    preamble = md.split("| Stage |", 1)[0]
    assert "`run`" in preamble
    assert "no dry run" in preamble
    assert "Three stages write" not in preamble
    row = next(l for l in md.splitlines() if l.startswith("| `run`"))
    assert "(writes)" in row


def test_a_job_run_shows_on_the_board(tmp_path):
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": True, "ok": True,
            "status": "SUCCEEDED"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "run")
    assert row["status"] == "DONE"
    assert "snowmig_02_copy_schema" in row["found"]
    assert "SUCCESS" in row["found"]
    assert row["attention"] is False


def test_a_failed_or_unfinished_job_run_needs_attention(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": False,
            "status": "FAILED"})
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": False, "ok": False,
            "status": "RUNNING"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "run")
    assert row["attention"] is True
    assert "FAILED" in row["found"] and "STILL RUNNING" in row["found"]


def test_the_stage_board_skill_names_run_as_a_writer():
    text = (ROOT / "skills/snowflake-stage-board/SKILL.md").read_text(
        encoding="utf-8")
    assert "Three stages write" not in text
    point = text.split("Then say three things out loud:", 1)[1].split("\n2. ", 1)[0]
    assert "`run`" in point and "no dry run" in point
