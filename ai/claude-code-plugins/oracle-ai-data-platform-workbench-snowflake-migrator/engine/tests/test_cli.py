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
    assert "planned to move" in (tmp_path / "PLANNED_OBJECTS.md").read_text().lower()


def test_plan_bronze_mirrors_the_source_by_default(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "D.PUBLIC.ORDERS"
    assert plan["catalogs_to_create"] == ["D"]


def test_plan_honours_the_bronze_catalog_prefix(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path), "--bronze-catalog-prefix", "bronze"])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "bronze.D_PUBLIC.ORDERS"


def test_plan_applies_a_restrictions_file(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_databases": ["D"]}))
    main(["plan", "--out-dir", str(tmp_path), "--restrictions",
          str(tmp_path / "r.json")])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["can_migrate"] == []
    assert plan["cannot_migrate"][0]["category"] == "restriction"


def test_bad_restriction_key_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_datbases": ["D"]}))
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    assert "exclude_datbases" in capsys.readouterr().err


def test_separate_databases_no_longer_collide_under_the_bronze_mirror(tmp_path):
    # layer-catalog used to fold the database away and merge these. The mirror
    # keeps all three parts, so they are distinct by construction.
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D2.PUBLIC.ORDERS"
    second["source_database"] = "D2"
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 0
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert sorted(plan["catalogs_to_create"]) == ["D", "D2"]


def test_plan_exits_3_on_a_case_variant_collision(tmp_path):
    # The collision that CAN still happen: names differing only by case.
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D.PUBLIC.orders"
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


def test_ddl_emits_views_too_after_their_tables(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V", object_type="VIEW",
                view_ddl_get_ddl="create view V as select ORDER_ID from D.PUBLIC.ORDERS")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json",
          {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.ORDERS"}],
           "source_used": "parsed_ddl", "coverage_note": "views only"})
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text())
    idents = [s["source_identifier"] for s in ddl["statements"]]
    assert idents == ["D.PUBLIC.ORDERS", "D.PUBLIC.V"], "view emitted last"
    assert "CREATE VIEW IF NOT EXISTS" in ddl["statements"][1]["sql"]


def test_ddl_blocks_a_snowflake_only_view_with_a_reason(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V2", object_type="VIEW",
                view_ddl_get_ddl="create view V2 as select * from t "
                                 "qualify row_number() over (order by a) = 1")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert any("QUALIFY" in c["reason"] for c in plan["cannot_migrate"])


def test_deploy_defaults_to_dry_run(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 0
    res = json.loads((tmp_path / "deploy_result.json").read_text())
    assert res["dry_run"] is True and res["executed"] == 0
    assert "DRY RUN" in (tmp_path / "SOFT_CLONE_SUMMARY.md").read_text().upper()


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


def test_snowflake_and_aidp_run_sql_helpers_do_not_shadow_each_other():
    # Both modules export make_run_sql. An unqualified import of the second
    # shadowed the first and broke every Snowflake stage at runtime while every
    # unit test still passed, because the tests patch _assess_inventory.
    import inspect

    import snowmig
    from snowflake_source.conn import make_run_sql as sf_make

    assert snowmig.make_run_sql is sf_make, "the Snowflake helper must win"
    assert "backend" in inspect.signature(snowmig.make_aidp_run_sql).parameters


def test_run_sql_from_args_builds_a_snowflake_callable(tmp_path, monkeypatch):
    # Covers the path the patched-out tests skip.
    import snowmig
    captured = {}

    monkeypatch.setattr(snowmig, "build_connect_kwargs",
                        lambda *a, **k: {"account": "a"})
    monkeypatch.setattr(snowmig, "connect", lambda **kw: captured.setdefault("kw", kw))
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: "CALLABLE")

    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--account", "a", "--user", "u",
         "--auth", "keypair", "--key-path", "/k"])
    assert snowmig._run_sql_from_args(args) == "CALLABLE"
    assert captured["kw"] == {"account": "a"}


def _plan_with_two_catalogs(tmp_path):
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second.update(source_identifier="D2.PUBLIC.ORDERS", source_database="D2")
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])


def test_notebook_refuses_to_pick_a_catalog_when_the_plan_spans_several(
        tmp_path, capsys):
    _plan_with_two_catalogs(tmp_path)
    rc = main(["notebook", "--out-dir", str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "pass --catalog" in err
    assert "not assumed" in err


def test_notebook_accepts_an_explicit_catalog_choice(tmp_path):
    _plan_with_two_catalogs(tmp_path)
    assert main(["notebook", "--out-dir", str(tmp_path), "--catalog", "D2"]) == 0
    assert (tmp_path / "snowmig_shallow_clone_D2.ipynb").is_file()


def test_notebook_needs_no_catalog_flag_when_there_is_only_one(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["notebook", "--out-dir", str(tmp_path)]) == 0
