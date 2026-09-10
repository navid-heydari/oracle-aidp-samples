"""Plugin manifest and skill/command structure."""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone", "snowflake-compute-proposal",
          "snowflake-smoke-test", "snowflake-clone-notebook"]
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
