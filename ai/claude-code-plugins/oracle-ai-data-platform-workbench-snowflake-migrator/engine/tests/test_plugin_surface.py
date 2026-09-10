"""Plugin manifest and skill/command structure."""
import json
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone", "snowflake-compute-proposal",
          "snowflake-smoke-test", "snowflake-clone-notebook",
          "snowflake-stage-board"]
COMMANDS = ["snowflake-assess", "snowflake-plan", "snowflake-soft-clone",
            "snowflake-compute", "snowflake-smoke", "snowflake-notebook"]


def frontmatter(path: pathlib.Path) -> dict:
    text = path.read_text()
    assert text.startswith("---\n"), f"{path} needs YAML frontmatter"
    block = text.split("---", 2)[1]
    out = {}
    for line in block.splitlines():
        if ":" in line and not line.startswith(" "):
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def test_manifest_is_valid_and_keeps_the_name():
    m = json.loads((ROOT / ".claude-plugin/plugin.json").read_text())
    assert m["name"] == "oracle-ai-data-platform-workbench-snowflake-migrator"
    assert "SCAFFOLD" not in m["description"]
    assert m["version"] and m["license"]


@pytest.mark.parametrize("name", SKILLS)
def test_skill_exists_with_matching_frontmatter(name):
    fm = frontmatter(ROOT / "skills" / name / "SKILL.md")
    assert fm["name"] == name
    assert len(fm["description"]) > 60, "description drives routing; make it specific"
    assert "SCAFFOLD" not in fm["description"]
    assert "Databricks" not in fm["description"]


@pytest.mark.parametrize("name", COMMANDS)
def test_command_exists(name):
    fm = frontmatter(ROOT / "commands" / f"{name}.md")
    assert fm["description"] and "SCAFFOLD" not in fm["description"]


def test_no_databricks_scaffold_survives():
    stale = [p for p in (ROOT / "skills").glob("*") if p.is_dir()
             and p.name not in SKILLS]
    assert stale == [], f"stale skill dirs: {[p.name for p in stale]}"
    assert not (ROOT / "agents").exists() or not list((ROOT / "agents").glob("*.md"))


def test_skills_invoke_the_engine_by_plugin_root():
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text()
        if "snowmig" in text:
            assert "${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py" in text, name


def test_clone_skill_states_the_runtime_coordinate_rule():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text()
    low = text.lower()
    assert "ask the user" in low
    assert "--execute" in text
    assert "dry" in low


def test_type_mapping_reference_covers_the_blocked_types():
    text = (ROOT / "references/type-mapping.md").read_text()
    for t in ["NUMBER", "TIMESTAMP_NTZ", "VARIANT", "GEOGRAPHY"]:
        assert t in text


def test_reference_documents_the_object_and_view_mapping():
    text = (ROOT / "references/type-mapping.md").read_text()
    assert "Standard Catalog" in text
    for construct in ["QUALIFY", "LATERAL FLATTEN", "LISTAGG", "DATEADD"]:
        assert construct in text, construct
    assert "secure view" in text.lower()


def test_plan_skill_documents_the_cannot_migrate_categories():
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text()
    for category in ["restriction", "unmapped_type", "snowflake_only_sql",
                     "unsupported_object"]:
        assert category in text, category
    assert "restrictions" in text.lower()


def test_clone_skill_documents_one_catalog_per_run_and_cli_backends():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text()
    low = text.lower()
    assert "one catalog per run" in low
    assert "aidp" in low and "oci" in low
    assert "empty" in low, "must say the cloned objects hold no data"
    assert "silver" in low and "never triggered" in low


def test_no_skill_still_promises_tables_only():
    # Views came into scope; a stale "tables only" line would mislead.
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text().lower()
        assert "tables only" not in text, name


def test_smoke_skill_warns_the_write_probe_leaves_a_schema():
    text = (ROOT / "skills/snowflake-smoke-test/SKILL.md").read_text()
    low = text.lower()
    assert "write-probe" in low
    assert "not remove" in low or "left behind" in low
    assert "drop" in low, "must explain why it cannot clean up"


def test_notebook_skill_says_execution_is_the_users_call():
    text = (ROOT / "skills/snowflake-clone-notebook/SKILL.md").read_text()
    low = text.lower()
    assert "do not run it for them" in low
    assert "empty" in low, "must say the tables arrive with no rows"
    assert "workspace" in low and "not in a data catalog" in low


def test_every_skill_that_can_write_states_the_no_data_guarantee():
    for name in ("snowflake-medallion-clone", "snowflake-clone-notebook"):
        low = (ROOT / "skills" / name / "SKILL.md").read_text().lower()
        assert "no data" in low or "moves no data" in low or "copies no data" in low, name


def test_dialect_translation_reference_reports_honest_coverage():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from snowflake_source.dialect.translate import coverage

    text = (ROOT / "references/dialect-translation.md").read_text()
    c = coverage()
    assert f"Implemented ({c['implemented']})" in text
    assert f"({c['declared']})" in text
    for rule_id in c["implemented_rule_ids"] + c["declared_rule_ids"]:
        assert rule_id in text, rule_id
    assert "never approximate" in text.lower()


def test_overview_states_the_source_read_only_guarantee_as_enforced():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text()
    low = text.lower()
    assert "enforced" in low
    assert "ever written to or dropped from the source" in low
    assert "assume none" in low, "no destination means no assumption"


def test_cleanup_checklist_exists_and_names_the_confidential_file():
    text = (ROOT / "CLEANUP-BEFORE-PUBLISH.md").read_text()
    assert "RAPPI-CONTEXT.md" in text
    assert "history" in text.lower(), "must say the remote history still has it"
    assert "npxbexe" in text, "must name the developer test account to remove"


def test_readme_warns_before_publishing():
    text = (ROOT / "README.md").read_text()
    assert "CLEANUP-BEFORE-PUBLISH.md" in text
    assert text.index("CLEANUP-BEFORE-PUBLISH.md") < 800, "must be near the top"


def test_assumptions_register_states_that_aidp_was_never_contacted():
    text = (ROOT / "ASSUMPTIONS.md").read_text()
    assert "Never contacted" in text
    assert "AWS_US_EAST_2" in text, "must name the Snowflake env that WAS used"
    for section in ("## A.", "## B.", "## C.", "## D.", "## E."):
        assert section in text


def test_data_movement_reference_offers_at_least_three_options():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from plan.data_movement import OPTIONS

    text = (ROOT / "references/data-movement-options.md").read_text()
    assert len(OPTIONS) >= 3
    for o in OPTIONS:
        assert o["id"] in text, o["id"]
    assert "moves no bytes" in text.lower()


def test_overview_requires_the_options_to_be_presented_always():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text()
    flat = " ".join(text.lower().split())
    assert "always present the data-movement architecture options" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_plan_skill_lists_every_option_with_a_stated_recommendation():
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text()
    for opt in ("`A1`", "`A2`", "`A3`", "`A4`", "`A5`", "`A6`"):
        assert opt in text, opt
    # Collapse whitespace: markdown line wrapping must not break a prose check.
    flat = " ".join(text.lower().split())
    assert "recommendation, not a decision" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_reference_carries_the_capability_matrix_and_build_notes():
    text = (ROOT / "references/data-movement-options.md").read_text()
    assert "Capability matrix" in text
    assert "What each option would take to build" in text
    for cap in ("historic_bulk", "ongoing_incremental", "read_without_copy"):
        assert cap in text, cap


def test_skills_present_the_open_slot_as_a_valid_answer():
    for name in ("snowflake-migrator-overview", "snowflake-migration-plan"):
        flat = " ".join((ROOT / "skills" / name / "SKILL.md")
                        .read_text().lower().split())
        assert "a6" in flat, name
        assert "never paraphrase" in flat or "never mapped" in flat, name


def test_no_skill_pushes_the_user_to_pick_from_the_listed_options():
    flat = " ".join((ROOT / "skills/snowflake-migration-plan/SKILL.md")
                    .read_text().lower().split())
    assert "real answer, not a fallback" in flat


def test_no_shipped_file_mentions_the_forked_source_platform():
    """This is a Snowflake migrator. Nothing shipped should say otherwise.

    The plugin began as a copy of a sibling plugin's layout, and three
    inherited files still described it -- PRIVACY.md named the wrong plugin and
    the wrong data flows, NOTICE named the wrong plugin and author, and the
    changelog documented files that do not exist here.

    Test files are exempt: several of them assert the ABSENCE of that scaffold
    and must be able to name what they are excluding.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    banned = ("databricks", "dbutils", "dbfs")
    # Two OSS Delta Spark settings are spelled with that legacy vendor prefix
    # and OSS honours it, so the name cannot be changed without making the
    # documentation wrong. Only the literal config prefix is exempt -- prose
    # about the other platform is still a failure.
    allowed_literals = ("spark.databricks.delta.",)
    offenders = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in (".md", ".py", ".json", ".txt", ".sql"):
            continue
        rel = path.relative_to(root)
        parts = set(rel.parts)
        if "tests" in parts or "__pycache__" in parts or ".pytest_cache" in parts:
            continue
        low = path.read_text(errors="ignore").lower()
        for literal in allowed_literals:
            low = low.replace(literal, "")
        hits = [b for b in banned if b in low]
        if hits:
            offenders.append(f"{rel}: {hits}")
    assert not offenders, "shipped files still reference the forked platform:\n" + "\n".join(offenders)


def test_every_cli_stage_is_invoked_by_at_least_one_skill():
    """No stage may be reachable only by reading the README.

    `summary` produces SUMMARY.md -- the per-object roll-up that is one of the
    plugin's headline deliverables -- and for several versions no skill or
    command mentioned it, so Claude would only have run it by accident. Same
    for `maintenance` the day it was added.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    cli = (root / "engine" / "snowmig.py").read_text()
    stages = set(re.findall(r'sub\.add_parser\(\s*"([a-z-]+)"', cli))
    assert stages, "no stages parsed -- the regex needs updating"

    invoked: set[str] = set()
    for path in list((root / "skills").rglob("SKILL.md")) + \
            list((root / "commands").glob("*.md")):
        text = path.read_text()
        invoked |= set(re.findall(r"snowmig\.py\s+([a-z-]+)", text))

    orphaned = sorted(stages - invoked)
    assert not orphaned, (
        f"these CLI stages are not invoked by any skill or command, so nothing "
        f"will ever run them: {orphaned}")
