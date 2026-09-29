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
    # The board now names the WORKFLOWS the `run` command starts rather than
    # the command itself, which is a finer statement of the same fact. What
    # must hold is unchanged: a stage that starts a job is on the board, it
    # is marked as writing, and it is optional.
    specs = {s["stage"]: s for s in STAGES if s.get("command") == "run"}
    assert specs, "no stage is driven by `run`"
    # Finer than "run writes": the board says WHICH job writes. Structure
    # creates schemas and tables and copy moves rows; discover and
    # reconcile only read. The defect was a writing stage being invisible,
    # and that is what must not come back.
    for name in ("structure-workflow", "copy-workflow"):
        assert specs[name]["writes"] is True, name
        assert specs[name].get("optional"), \
            "a structure-only clone never runs a job"
    for name in ("discover-workflow", "reconcile-workflow"):
        assert specs[name]["writes"] is False, f"{name} only reads"


def test_the_preamble_names_run_as_a_writer_with_no_dry_run(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    preamble = md.split("| Stage |", 1)[0]
    assert "no dry run" in preamble
    assert "Three stages write" not in preamble
    assert "`copy-workflow`" in preamble and "`structure-workflow`" in preamble
    row = next(l for l in md.splitlines()
               if l.startswith("| `copy-workflow`"))
    assert "(writes)" in row


def test_a_job_run_shows_on_the_board(tmp_path):
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": True, "ok": True,
            "status": "SUCCEEDED"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "copy-workflow")
    assert row["status"] == "DONE"
    # The job is named by the STAGE now, so the cell carries the state.
    assert "SUCCEEDED" in row["found"]
    assert row["attention"] is False


def test_a_failed_or_unfinished_job_run_needs_attention(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": False,
            "status": "FAILED"})
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": False, "ok": False,
            "status": "RUNNING"})
    rows = {r["stage"]: r for r in build_stage_board(tmp_path)["stages"]}
    assert rows["structure-workflow"]["attention"] is True
    assert "FAILED" in rows["structure-workflow"]["found"]
    assert rows["copy-workflow"]["attention"] is True
    assert "RUNNING" in rows["copy-workflow"]["found"]


def test_the_stage_board_skill_names_run_as_a_writer():
    text = (ROOT / "skills/snowflake-stage-board/SKILL.md").read_text(
        encoding="utf-8")
    assert "Three stages write" not in text
    point = text.split("Then say three things out loud:", 1)[1].split("\n2. ", 1)[0]
    assert "`run`" in point and "no dry run" in point


def test_an_unrecognised_run_state_is_not_rounded_up_to_still_running(tmp_path):
    """A status this plugin does not classify is neither done nor running.

    watch_job keeps a separate `unrecognised` flag for a non-terminal run
    whose status is not one of the ACTIVE_STATES, and cmd_run prints
    UNRECOGNISED STATE for it and exits 1, saying that reporting STILL
    RUNNING "would round it up". The board read every non-terminal run as
    STILL RUNNING, so run_snowmig_02_copy_schema.json with status
    WEIRD_STATE was on the board as "snowmig_02_copy_schema: STILL RUNNING"
    -- an agent reading it would tell the user to wait for a run that may
    never finish.
    """
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": False, "ok": False,
            "unrecognised": True, "status": "X"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "copy-workflow")
    # The point stands: the state is reported as written, never rounded up
    # to "still running" for a run that may never finish.
    assert "STILL RUNNING" not in row["found"]
    assert "X" in row["found"]
    assert row["attention"] is True
