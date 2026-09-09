"""Plugin manifest and skill/command structure."""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone"]
COMMANDS = ["snowflake-assess", "snowflake-plan", "snowflake-soft-clone"]


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
