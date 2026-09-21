"""Plugin manifest and skill/command structure."""
import json
import pathlib

import yaml
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone", "snowflake-compute-proposal",
          "snowflake-smoke-test", "snowflake-clone-notebook",
          "snowflake-stage-board", "snowflake-migrator-demo",
          "snowflake-provision-environment"]
COMMANDS = ["snowflake-assess", "snowflake-plan", "snowflake-soft-clone",
            "snowflake-compute", "snowflake-smoke", "snowflake-notebook",
            "snowflake-catalog", "snowflake-demo", "snowflake-provision"]


def frontmatter(path: pathlib.Path) -> dict:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\n"), f"{path} needs YAML frontmatter"
    block = text.split("---", 2)[1]
    # A real YAML parser, not a line splitter. Claude Code parses this block
    # as YAML; an unquoted description containing ': ' is a mapping error
    # there, and the skill then loads with EMPTY metadata. The old
    # split-on-colon reader accepted exactly that file.
    out = yaml.safe_load(block)
    assert isinstance(out, dict), f"{path}: frontmatter is not a YAML mapping"
    return out


def test_manifest_is_valid_and_keeps_the_name():
    m = json.loads((ROOT / ".claude-plugin/plugin.json").read_text(encoding="utf-8"))
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
    """Always addressed from the plugin root, never a relative path.

    Either entry point satisfies that: the launcher `bin/snowmig`, which is
    what the docs now use, or the engine directly. What must never appear is
    a path relative to wherever the user happens to be standing.
    """
    accepted = ("${CLAUDE_PLUGIN_ROOT}/bin/snowmig",
                "${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py")
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8")
        if "snowmig" in text:
            assert any(a in text for a in accepted), name


def test_clone_skill_states_the_runtime_coordinate_rule():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "ask the user" in low
    assert "--execute" in text
    assert "dry" in low


def test_type_mapping_reference_covers_the_blocked_types():
    text = (ROOT / "references/type-mapping.md").read_text(encoding="utf-8")
    for t in ["NUMBER", "TIMESTAMP_NTZ", "VARIANT", "GEOGRAPHY"]:
        assert t in text


def test_reference_documents_the_object_and_view_mapping():
    text = (ROOT / "references/type-mapping.md").read_text(encoding="utf-8")
    # A Snowflake database maps to a catalog, and which KIND of catalog is the
    # part a reader has to get right: EXTERNAL by default, Standard on request.
    assert "EXTERNAL catalog" in text
    assert "Standard catalog" in text
    for construct in ["QUALIFY", "LATERAL FLATTEN", "LISTAGG", "DATEADD"]:
        assert construct in text, construct
    assert "secure view" in text.lower()


def test_plan_skill_documents_the_cannot_migrate_categories():
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text(encoding="utf-8")
    for category in ["restriction", "unmapped_type", "snowflake_only_sql",
                     "unsupported_object"]:
        assert category in text, category
    assert "restrictions" in text.lower()


def test_clone_skill_documents_one_catalog_per_run_and_cli_backends():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "one catalog per run" in low
    assert "aidp" in low and "oci" in low
    assert "empty" in low, "must say the cloned objects hold no data"
    assert "silver" in low and "never triggered" in low


def test_no_skill_still_promises_tables_only():
    # Views came into scope; a stale "tables only" line would mislead.
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8").lower()
        assert "tables only" not in text, name


def test_smoke_skill_documents_the_write_probe_lifecycle():
    """The probe creates one schema, removes it again, and names anything a
    failed cleanup left behind. An earlier test (and the CLI help) still
    described the pre-cleanup behaviour -- "it is NOT dropped afterwards" --
    long after the code was corrected; this one pins the corrected claim."""
    text = (ROOT / "skills/snowflake-smoke-test/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "write-probe" in low
    assert "drop" in low or "remove" in low, "must say it cleans up after itself"
    assert "left behind" in low or "cleanup fails" in low, \
        "must say a failed cleanup is named, not hidden"
    assert "external" in low, \
        "must say the probe is skipped for a read-only EXTERNAL catalog"


def test_notebook_skill_says_execution_is_the_users_call():
    text = (ROOT / "skills/snowflake-clone-notebook/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "do not run it for them" in low
    assert "empty" in low, "must say the tables arrive with no rows"
    assert "workspace" in low and "not in a data catalog" in low


def test_every_skill_that_can_write_states_the_no_data_guarantee():
    for name in ("snowflake-medallion-clone", "snowflake-clone-notebook"):
        low = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8").lower()
        assert "no data" in low or "moves no data" in low or "copies no data" in low, name


def test_dialect_translation_reference_reports_honest_coverage():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from snowflake_source.dialect.translate import coverage

    text = (ROOT / "references/dialect-translation.md").read_text(encoding="utf-8")
    c = coverage()
    assert f"Implemented ({c['implemented']})" in text
    assert f"({c['declared']})" in text
    for rule_id in c["implemented_rule_ids"] + c["declared_rule_ids"]:
        assert rule_id in text, rule_id
    assert "never approximate" in text.lower()


def test_overview_states_the_source_read_only_guarantee_as_enforced():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    low = text.lower()
    assert "enforced" in low
    assert "ever written to or dropped from the source" in low
    assert "assume none" in low, "no destination means no assumption"


def test_cleanup_checklist_covers_the_confidential_docs_and_the_history():
    """The engagement docs are gone from the tree but still in git history.

    A working-tree cleanup cannot close that, so the checklist has to keep
    saying so -- without naming the customer, since this is a public sample.
    """
    text = (ROOT / "CLEANUP-BEFORE-PUBLISH.md").read_text(encoding="utf-8")
    assert "engagement docs" in text.lower()
    assert "history" in text.lower(), "must say the remote history still has it"
    assert "snowmig-config" in text, \
        "must point at the gitignored migration config -- the ONE file that " \
        "holds live credentials (there is no second config any more)"


def test_readme_warns_before_publishing():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "CLEANUP-BEFORE-PUBLISH.md" in text
    assert text.index("CLEANUP-BEFORE-PUBLISH.md") < 800, "must be near the top"


def test_assumptions_register_states_the_live_verified_split():
    """The header table must say what WAS contacted and what was not.

    An earlier version asserted the literal "AIDP: Never contacted", which
    became false the day the catalog transport was live-verified -- the test
    then ENFORCED the stale claim, and correcting the document broke the
    suite. What is invariant is the split itself: the register must name the
    live-verified surfaces and the never-executed ones, in both directions."""
    text = (ROOT / "ASSUMPTIONS.md").read_text(encoding="utf-8")
    assert "Never contacted" not in text, \
        "AIDP has been contacted; the register must say so"
    low = text.lower()
    assert "live-verified" in low, "must name what a real AIDP run proved"
    assert "still unproven" in low or "never executed" in low, \
        "must name the surfaces still unproven"
    assert "AWS_US_EAST_2" in text, "must name the Snowflake env that WAS used"
    for section in ("## A.", "## B.", "## C.", "## D.", "## E."):
        assert section in text


def test_data_movement_reference_offers_at_least_three_options():
    import sys
    sys.path.insert(0, str(ROOT / "engine"))
    from plan.data_movement import OPTIONS

    text = (ROOT / "references/data-movement-options.md").read_text(encoding="utf-8")
    assert len(OPTIONS) >= 3
    for o in OPTIONS:
        assert o["id"] in text, o["id"]
    assert "moves no bytes" in text.lower()


def test_overview_requires_the_options_to_be_presented_always():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    flat = " ".join(text.lower().split())
    assert "always present the data-movement architecture options" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_plan_skill_lists_every_option_with_a_stated_recommendation():
    text = (ROOT / "skills/snowflake-migration-plan/SKILL.md").read_text(encoding="utf-8")
    for opt in ("`A1`", "`A2`", "`A3`", "`A4`", "`A5`", "`A6`"):
        assert opt in text, opt
    # Collapse whitespace: markdown line wacmeng must not break a prose check.
    flat = " ".join(text.lower().split())
    assert "recommendation, not a decision" in flat
    assert "undecided" in flat
    assert "none of the six is implemented" in flat


def test_reference_carries_the_capability_matrix_and_build_notes():
    text = (ROOT / "references/data-movement-options.md").read_text(encoding="utf-8")
    assert "Capability matrix" in text
    assert "What each option would take to build" in text
    for cap in ("historic_bulk", "ongoing_incremental", "read_without_copy"):
        assert cap in text, cap


def test_skills_present_the_open_slot_as_a_valid_answer():
    for name in ("snowflake-migrator-overview", "snowflake-migration-plan"):
        flat = " ".join((ROOT / "skills" / name / "SKILL.md")
                        .read_text(encoding="utf-8").lower().split())
        assert "a6" in flat, name
        assert "never paraphrase" in flat or "never mapped" in flat, name


def test_no_skill_pushes_the_user_to_pick_from_the_listed_options():
    flat = " ".join((ROOT / "skills/snowflake-migration-plan/SKILL.md")
                    .read_text(encoding="utf-8").lower().split())
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
        low = path.read_text(errors="ignore", encoding="utf-8").lower()
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
    cli = (root / "engine" / "snowmig.py").read_text(encoding="utf-8")
    stages = set(re.findall(r'sub\.add_parser\(\s*"([a-z-]+)"', cli))
    assert stages, "no stages parsed -- the regex needs updating"

    invoked: set[str] = set()
    for path in list((root / "skills").rglob("SKILL.md")) + \
            list((root / "commands").glob("*.md")):
        text = path.read_text(encoding="utf-8")
        # Both invocation forms count: `snowmig.py <stage>` and the
        # launcher, `bin/snowmig <stage>`, which is what the docs now use.
        # `snowmig-test` cannot match -- the pattern needs whitespace
        # straight after the name.
        invoked |= set(re.findall(r"snowmig(?:\.py)?\s+([a-z-]+)", text))

    orphaned = sorted(stages - invoked)
    assert not orphaned, (
        f"these CLI stages are not invoked by any skill or command, so nothing "
        f"will ever run them: {orphaned}")


def test_the_readme_carries_a_runnable_from_zero_runbook():
    """Someone arriving with no context must find the order of operations.

    A fresh conversation has no memory of how the last migration was driven,
    so the sequence has to live in the repo, name the config file, and cover
    every stage that writes."""
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "How to run a migration, from zero" in text
    # The config file is the single place coordinates and secrets live.
    assert "snowmig-config.example.yaml" in text
    # Every stage a migration cannot be run without, in whichever form the
    # runbook writes the invocation (`snowmig.py assess` or `$E assess`).
    for stage in ("init-config", "preflight", "assess", "plan", "ddl",
                  "smoke", "catalog", "provision"):
        # `snowmig.py <stage>`, the launcher `snowmig <stage>`, or $E.
        assert (f"snowmig.py {stage}" in text
                or f"snowmig {stage}" in text
                or f"$E {stage}" in text), stage
    # The four in-AIDP jobs and the deliverable they produce.
    for job in ("snowmig_00_discover", "snowmig_01_structure",
                "snowmig_02_copy_schema", "snowmig_03_reconcile"):
        assert job in text, job
    assert "MIGRATION_REPORT.md" in text


def test_the_runbook_states_the_two_things_it_must_not_let_slide():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    low = text.lower()
    # A target catalog that exists as a CONTAINER only, and a cutover the
    # plugin cannot make consistent on its own.
    #
    # This used to assert the README said the target catalog is "not created
    # by this plugin". That was false: `catalog --catalog-type standard
    # --execute` creates it (runbook S4, live-verified), and the README, the
    # runbook and the CLI disagreed with each other. The invariant that
    # actually matters is the one that misleads if dropped -- the container
    # is not the structure, because a control-plane table create can return
    # 202 Accepted and create nothing.
    assert "container" in low
    assert "202 accepted" in low
    assert "cutover" in low
    assert "freeze writers" in low


def test_the_router_points_at_the_runbook():
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    assert "README.md" in text
    assert "from zero" in text.lower()
    # And names the config file as the first thing to establish.
    assert "snowmig-config.example.yaml" in text


def test_the_docs_say_where_each_credential_lives():
    """"Where do I put the URL, the user and the password?" is the question
    users actually ask, and answering it wrong once costs a leaked secret.

    Three places, and the plugin holds only one of them: Snowflake
    coordinates in the config, the Snowflake SECRET in a separate file
    referenced by path, and AIDP auth in the user's own OCI config.
    """
    for path in ("README.md",
                 "skills/snowflake-migrator-bootstrap/SKILL.md",
                 "snowmig-config.example.yaml"):
        text = (ROOT / path).read_text(encoding="utf-8")
        low = text.lower()
        # AIDP authentication is NOT this plugin's business.
        assert "~/.oci/config" in text, f"{path}: AIDP auth is the OCI config"
        # One file holds both ends, and it holds live credentials.
        assert "snowmig-config" in text, f"{path}: name the one config file"
        # And the rule that protects it.
        assert "never" in low and "chat" in low, \
            f"{path}: must say a secret is never asked for in chat"


def test_the_docs_do_not_assume_the_user_is_inside_this_repo():
    """An installed plugin has no repo and no open folder: paths come from
    CLAUDE_PLUGIN_ROOT, and the config belongs in the working directory."""
    text = (ROOT / "skills/snowflake-migrator-bootstrap/SKILL.md").read_text(encoding="utf-8")
    assert "${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py" in text
    assert "init-config" in text, "must say how to create a config from nothing"
    low = text.lower()
    assert "do not assume" in low and "inside this repo" in low
    assert "read-only" in low, \
        "must explain why the config does not live beside the plugin"


def test_the_router_forbids_doing_the_engine_s_work_by_hand():
    """The plugin's value is that a migration is deterministic and audited.

    An agent that cannot find the engine, or that finds a stage inconvenient,
    must not fall back to hand-written SQL and ad-hoc API calls: that leaves
    an estate half-migrated with no artifact saying what happened.
    """
    text = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    low = " ".join(text.lower().split())
    assert "never do by hand what a stage does" in low
    assert "do not re-implement a stage" in low
    assert "do not translate sql or types yourself" in low
    # And the explicit stop condition.
    assert "cannot be found, stop" in low
    assert "${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py" in text
    assert "never a reason to improvise" in low


# --------------------------------------------------------------------------
# "Copies no data" is true of the control plane and false of the plugin: the
# in-AIDP job snowmig_02_copy_schema INSERT-SELECTs every row when the
# operator runs it. Every surface that makes the claim has to scope it.
# --------------------------------------------------------------------------

_NO_DATA_CLAIM = re.compile(
    r"copies no data|moves no bytes|no data is moved|no rows\s+move|"
    r"none implemented|nothing below is implemented|"
    r"no code path can report that data moved", re.I)
_DATA_SURFACES = (".claude-plugin/plugin.json", ".claude-plugin/marketplace.json",
                  "NOTICE", "GAPS.md", "ASSUMPTIONS.md", "README.md",
                  "references/data-movement-options.md")


def test_no_surface_claims_the_plugin_copies_no_data_unscoped():
    paths = [ROOT / p for p in _DATA_SURFACES]
    paths += sorted((ROOT / "skills").glob("*/SKILL.md"))
    offenders = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for para in re.split(r"\n\s*\n", text):
            hit = _NO_DATA_CLAIM.search(para)
            if hit and "02_copy_schema" not in para:
                offenders.append(f"{path.relative_to(ROOT)}: {hit.group(0)!r}")
    assert not offenders, (
        "unscoped 'no data' claims (name snowmig_02_copy_schema in the same "
        "paragraph):\n" + "\n".join(offenders))


def test_manifest_and_marketplace_agree_on_the_data_claim():
    plugin = json.loads((ROOT / ".claude-plugin/plugin.json").read_text(encoding="utf-8"))
    market = json.loads((ROOT / ".claude-plugin/marketplace.json").read_text(encoding="utf-8"))
    entry = market["plugins"][0]
    assert entry["version"] == plugin["version"]
    for desc in (plugin["description"], entry["description"]):
        assert "snowmig_02_copy_schema" in desc, desc
        assert "control plane" in desc.lower(), desc


# --------------------------------------------------------------------------
# One order of operations. The overview skill (S1-S12) is the authority and
# the code enforces it: `catalog --execute` needs the workspace and cluster
# that `provision` creates. The README once ran them the other way round and
# bracketed the two coordinates as optional.
# --------------------------------------------------------------------------

def _runbook(text: str) -> str:
    start = text.index("## How to run a migration, from zero")
    end = text.index("\n## ", start + 10)
    return text[start:end]


def test_the_readme_orders_provision_before_catalog_registration():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook = _runbook(text)
    assert "Nine steps" not in text
    assert runbook.index("snowmig provision") < runbook.index("snowmig catalog --catalog")
    provision = runbook.index("Provision the migration environment")
    external = runbook.index("EXTERNAL catalog")
    assert provision < external, "provision (S1/S2) comes before the catalogs (S3/S4)"


def test_the_readme_does_not_bracket_workspace_and_cluster_for_catalog_execute():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook = _runbook(text)
    catalog_step = runbook[runbook.index("EXTERNAL catalog"):]
    catalog_step = catalog_step[:catalog_step.index("\n### ", 10)]
    assert not re.search(r"\[--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>\]",
                         catalog_step), "required for --execute; the bracket said optional"
    # And the hand-off: provision prints the keys, the operator pastes them.
    assert "aidp.workspace" in runbook and "aidp.cluster_id" in runbook
    assert "does not write them back" in runbook


def test_the_readme_labels_laptop_assess_as_an_optional_preview():
    runbook = _runbook((ROOT / "README.md").read_text(encoding="utf-8"))
    step = runbook[runbook.index("Assess the estate"):]
    step = step[:step.index("\n### ", 10)]
    low = step.lower()
    assert "optional" in low and "snowmig_00_discover" in step, \
        "the laptop assess is a preview; the migration discovers inside AIDP (S6)"


def test_the_readme_separates_copy_jobs_from_the_migration_proper():
    runbook = _runbook((ROOT / "README.md").read_text(encoding="utf-8"))
    low = " ".join(runbook.lower().split())
    assert "data migration is not run" in low, \
        "must share the overview skill's statement: S12 ends with no rows moved"
    assert runbook.index("snowmig_01_structure") < runbook.index("snowmig_02_copy_schema")


def test_the_three_runbooks_agree_on_the_step_order():
    skill = (ROOT / "skills/snowflake-migrator-overview/SKILL.md").read_text(encoding="utf-8")
    assert skill.index("| S1 |") < skill.index("| S3 |") < skill.index("| S4 |")
    arch = (ROOT / "MIGRATION-ARCHITECTURE.md").read_text(encoding="utf-8")
    table = arch[arch.index("| # | Step | Command | Writes |"):]
    table = table[:table.index("\n\n", 10)]
    assert table.index("workspace") < table.index("EXTERNAL") < table.index("INTERNAL")
    assert "snowmig_01_structure" in table
    assert "deploy --execute" not in table, \
        "the control-plane deploy is not the INTERNAL structure step (202 can create nothing)"
