"""CLI wiring. The offline subcommands are tested with no connection."""
import json

from snowmig import main

INV = {"probed_at": "t", "session": {}, "databases_in_scope": ["D"],
       "object_count": 1, "counts_by_type": {"TABLE": 1},
       "identifier_case_collisions": {}, "extraction_notes": [],
       "inventory": [{
           "source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
           "source_database": "D", "source_schema": "PUBLIC",
           "compatibility_status": "supported", "blocked_reasons": [],
           "row_count_exact": 100, "source_metadata": {"bytes": 10},
           "warnings": [],
           "columns": [{"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER",
                        "target_type": "DECIMAL(38,0)", "IS_NULLABLE": "NO",
                        "ORDINAL_POSITION": 1, "COMMENT": None}]}]}
DEPS = {"edges": [], "source_used": "parsed_ddl", "coverage_note": "views only"}


def write(tmp, name, payload):
    p = tmp / name
    p.write_text(json.dumps(payload))
    return p


def test_plan_subcommand_writes_both_artifacts(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    rc = main(["plan", "--out-dir", str(tmp_path)])
    assert rc == 0
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["clone_targets"] == ["D.PUBLIC.ORDERS"]
    assert "Wave 1" in (tmp_path / "MIGRATION_PLAN.md").read_text()


def test_plan_honours_the_strategy_flag(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path), "--namespace-strategy", "preserve-source"])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "D.PUBLIC.ORDERS"


def test_plan_exits_3_on_target_collision(tmp_path):
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D2.PUBLIC.ORDERS"
    second["source_database"] = "D2"
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 3


def test_assess_exits_3_on_case_collision(tmp_path, monkeypatch):
    inv = json.loads(json.dumps(INV))
    inv["identifier_case_collisions"] = {"D.PUBLIC.ORDERS": ["D.PUBLIC.ORDERS",
                                                             "D.PUBLIC.orders"]}
    monkeypatch.setattr("snowmig._assess_inventory", lambda args: inv)
    assert main(["assess", "--out-dir", str(tmp_path), "--account", "a",
                 "--user", "u", "--auth", "keypair", "--key-path", "/k"]) == 3


def test_ddl_subcommand_generates_sql_offline(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    rc = main(["ddl", "--out-dir", str(tmp_path)])
    assert rc == 0
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text())
    assert len(ddl["statements"]) == 1
    assert "CREATE TABLE IF NOT EXISTS" in ddl["statements"][0]["sql"]
    assert "USING DELTA" in ddl["statements"][0]["sql"]


def test_ddl_only_emits_for_tables_not_views(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V", object_type="VIEW",
                compatibility_status="requires_manual_design")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text())
    assert [s["source_identifier"] for s in ddl["statements"]] == ["D.PUBLIC.ORDERS"]


def test_deploy_defaults_to_dry_run(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 0
    res = json.loads((tmp_path / "deploy_result.json").read_text())
    assert res["dry_run"] is True and res["executed"] == 0
    assert "DRY RUN" in (tmp_path / "DEPLOY.md").read_text().upper()


def test_deploy_execute_without_coordinates_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["deploy", "--out-dir", str(tmp_path), "--execute"])
    assert rc == 1
    assert "ask the user" in capsys.readouterr().err


def test_missing_input_artifact_is_a_clear_error(tmp_path, capsys):
    assert main(["plan", "--out-dir", str(tmp_path)]) == 1
    assert "inventory.json" in capsys.readouterr().err
