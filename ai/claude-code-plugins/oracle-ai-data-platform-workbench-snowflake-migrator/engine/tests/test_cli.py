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
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_plan_subcommand_writes_both_artifacts(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    rc = main(["plan", "--out-dir", str(tmp_path)])
    assert rc == 0
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["clone_targets"] == ["D.PUBLIC.ORDERS"]
    assert "planned to move" in (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8").lower()


def test_plan_bronze_mirrors_the_source_by_default(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "d.public.orders"
    assert plan["catalogs_to_create"] == ["d"]


def test_plan_honours_the_bronze_catalog_prefix(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path), "--bronze-catalog-prefix", "bronze"])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "bronze.d_public.orders"


def test_plan_applies_a_restrictions_file(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_databases": ["D"]}), encoding="utf-8")
    main(["plan", "--out-dir", str(tmp_path), "--restrictions",
          str(tmp_path / "r.json")])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["can_migrate"] == []
    assert plan["cannot_migrate"][0]["category"] == "restriction"


def test_bad_restriction_key_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_datbases": ["D"]}), encoding="utf-8")
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
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert sorted(plan["catalogs_to_create"]) == ["d", "d2"]


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
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text(encoding="utf-8"))
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
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text(encoding="utf-8"))
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
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert any("QUALIFY" in c["reason"] for c in plan["cannot_migrate"])


def test_deploy_defaults_to_dry_run(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 0
    res = json.loads((tmp_path / "deploy_result.json").read_text(encoding="utf-8"))
    assert res["dry_run"] is True and res["executed"] == 0
    assert "DRY RUN" in (tmp_path / "SOFT_CLONE_SUMMARY.md").read_text(encoding="utf-8").upper()


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


def test_data_options_stage_presents_options_and_implements_nothing(tmp_path):
    assert main(["data-options", "--out-dir", str(tmp_path)]) == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))
    assert payload["implemented"] is False
    assert len(payload["options"]) >= 3
    md = (tmp_path / "DATA_MOVEMENT_OPTIONS.md").read_text(encoding="utf-8")
    assert "moves no bytes" in md.lower()


def test_data_options_records_a_choice_without_executing(tmp_path):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A2_FEDERATE_EXTERNAL_CATALOG",
               "--chosen-by", "navid", "--rationale", "no bulk transfer yet"])
    assert rc == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))
    assert payload["choice"]["executed"] is False
    assert payload["choice"]["unknowns_outstanding"]


def test_data_options_choice_requires_a_rationale(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A1_UNLOAD_OBJECT_STORAGE"])
    assert rc == 1
    assert "rationale" in capsys.readouterr().err


def test_plan_always_reports_the_architecture_state(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    assert "architecture: UNDECIDED" in capsys.readouterr().out
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "Data-movement architecture" in md
    assert "A1_UNLOAD_OBJECT_STORAGE" in md and "A5_HYBRID_WAVES" in md


def test_plan_picks_up_a_recorded_architecture_choice(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A2_FEDERATE_EXTERNAL_CATALOG",
          "--chosen-by", "navid", "--rationale", "federate first"])
    main(["plan", "--out-dir", str(tmp_path)])
    assert "architecture: A2_FEDERATE_EXTERNAL_CATALOG" in capsys.readouterr().out
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["architecture_choice"]["option_id"] == "A2_FEDERATE_EXTERNAL_CATALOG"
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "federate first" in md
    assert "✅" in md, "the chosen option is marked in the table"


def test_data_options_records_a_deferral(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
               "--rationale", "platform team decides next month"])
    assert rc == 0
    assert "DEFERRED" in capsys.readouterr().out
    choice = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))["choice"]
    assert choice["deferred"] is True and choice["custom_architecture"] is None


def test_data_options_records_a_customer_architecture_verbatim(tmp_path):
    desc = tmp_path / "arch.md"
    desc.write_text("Debezium off Snowflake into OCI Streaming, then Iceberg.\n", encoding="utf-8")
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
               "--rationale", "their team already runs this",
               "--custom-name", "Kafka CDC into Iceberg",
               "--custom-description-file", str(desc)])
    assert rc == 0
    choice = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))["choice"]
    assert choice["custom_architecture"]["name"] == "Kafka CDC into Iceberg"
    assert "Debezium" in choice["custom_architecture"]["description"]
    assert choice["deferred"] is False


def test_a_custom_architecture_needs_both_flags(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "x",
               "--rationale", "y", "--custom-name", "N"])
    assert rc == 1
    assert "custom-description-file" in capsys.readouterr().err


def test_a_deferral_flows_into_the_plan_reports(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
          "--rationale", "decide later"])
    main(["plan", "--out-dir", str(tmp_path)])
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "deliberately deferred" in md.lower()
    assert "A6_CUSTOMER_DEFINED" in md
    assert "does not have to be one of the others" in md


def test_plan_stdout_distinguishes_deferred_from_undecided(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    assert "UNDECIDED" in capsys.readouterr().out

    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "n",
          "--rationale", "later"])
    main(["plan", "--out-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "DEFERRED by the customer — not a gap" in out
    assert "UNDECIDED" not in out


def test_plan_stdout_names_a_custom_architecture(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "a.md").write_text("their design", encoding="utf-8")
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "n",
          "--rationale", "r", "--custom-name", "Their Pattern",
          "--custom-description-file", str(tmp_path / "a.md")])
    main(["plan", "--out-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "Their Pattern" in out and "not assessed" in out


# --- --out-dir placement and `clean` --------------------------------------

def test_out_dir_is_honoured_before_and_after_the_subcommand(tmp_path):
    # The top-level usage line advertises `snowmig [--out-dir X] <stage>`, and
    # the subparser used to re-apply its own None default over the root value,
    # so that placement was silently discarded.
    import snowmig
    chosen = str(tmp_path / "chosen")
    after = snowmig.build_parser().parse_args(["stages", "--out-dir", chosen])
    before = snowmig.build_parser().parse_args(["--out-dir", chosen, "stages"])
    assert after.out_dir == chosen
    assert before.out_dir == chosen


def _clean_fixture(tmp_path, monkeypatch):
    import snowmig
    plugin = tmp_path / "plugin"
    default = plugin / snowmig.ARTIFACTS_DIRNAME
    default.mkdir(parents=True)
    (default / "plan.json").write_text("{}", encoding="utf-8")
    chosen = tmp_path / "chosen"
    chosen.mkdir()
    (chosen / "x").write_text("x", encoding="utf-8")
    monkeypatch.setattr(snowmig, "plugin_root", lambda: plugin)
    return default, chosen


def test_clean_refuses_a_chosen_directory_regardless_of_flag_position(
        tmp_path, monkeypatch, capsys):
    default, chosen = _clean_fixture(tmp_path, monkeypatch)
    assert main(["clean", "--out-dir", str(chosen)]) == 1
    assert "refusing" in capsys.readouterr().err
    assert (default / "plan.json").is_file()
    # Same flag, before the subcommand: the same refusal, not a deletion of
    # the default directory the operator did not name.
    assert main(["--out-dir", str(chosen), "clean"]) == 1
    assert "refusing" in capsys.readouterr().err
    assert (default / "plan.json").is_file()
    assert (chosen / "x").is_file()


def test_clean_removes_only_the_default_directory(tmp_path, monkeypatch):
    default, chosen = _clean_fixture(tmp_path, monkeypatch)
    assert main(["clean"]) == 0
    assert not default.exists()
    assert (chosen / "x").is_file()


# --- bad inputs are one `error:` line, never a traceback -------------------

def test_out_dir_that_is_a_file_exits_1_with_an_error_line(tmp_path, capsys):
    f = tmp_path / "afile.txt"
    f.write_text("not a directory", encoding="utf-8")
    assert main(["stages", "--out-dir", str(f)]) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "afile.txt" in err
    assert "Traceback" not in err


def test_list_shaped_restrictions_file_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text("[1, 2]", encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error:" in err and "JSON object" in err
    assert "Traceback" not in err


def test_unparseable_restrictions_file_names_the_file(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text("{not json", encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error:" in err and "r.json" in err


def test_corrupt_artifact_names_the_file(tmp_path, capsys):
    (tmp_path / "inventory.json").write_text("{not json", encoding="utf-8")
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "inventory.json" in err


# --- a dry run never overwrites an executed record -------------------------

EXECUTED_DEPLOY = {"dry_run": False, "executed": 4, "verified": 2,
                   "statement_count": 4, "poisoned_names": ["x"],
                   "failed_targets": ["y"], "failed": ["y"],
                   "mismatched_targets": []}


def test_dry_run_deploy_refuses_to_overwrite_an_executed_record(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    write(tmp_path, "deploy_result.json", EXECUTED_DEPLOY)
    (tmp_path / "PREFLIGHT.md").unlink(missing_ok=True)
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 1
    err = capsys.readouterr().err
    assert "EXECUTED" in err and "deploy_result.json" in err
    assert "--out-dir" in err
    kept = json.loads((tmp_path / "deploy_result.json").read_text(encoding="utf-8"))
    assert kept["dry_run"] is False and kept["poisoned_names"] == ["x"]
    # Re-reading PREFLIGHT.md is the reason people re-run a dry run; it is
    # still rendered before the refusal.
    assert (tmp_path / "PREFLIGHT.md").is_file()


def test_the_stage_board_still_shows_the_executed_deploy_after_a_refused_dry_run(
        tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    write(tmp_path, "deploy_result.json", EXECUTED_DEPLOY)
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 1
    assert main(["stages", "--out-dir", str(tmp_path)]) == 0
    board = (tmp_path / "STAGES.md").read_text(encoding="utf-8")
    deploy_row = next(l for l in board.splitlines()
                      if l.startswith("| `deploy`"))
    assert "verified 2/4" in deploy_row
    assert "DRY RUN" not in deploy_row


def test_dry_run_provision_refuses_to_overwrite_an_executed_record(
        tmp_path, capsys):
    write(tmp_path, "provision_result.json",
          {"dry_run": False, "workspace": {"name": "w"},
           "steps": [{"step": "workspace", "action": "created",
                      "verified": True, "detail": "w"}]})
    rc = main(["provision", "--out-dir", str(tmp_path), "--workspace-name",
               "w", "--skip-libraries"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "EXECUTED" in err and "provision_result.json" in err
    kept = json.loads((tmp_path / "provision_result.json").read_text(encoding="utf-8"))
    assert kept["dry_run"] is False
    assert kept["steps"][0]["verified"] is True


# --- the two opt-in writes are gated by --execute like every other write ---

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"
TARGET_FLAGS = ["--datalake-ocid", OCID, "--workspace", "ws-fake",
                "--cluster-id", "cl-fake"]


def _sf_ok(sql, params=None):
    low = sql.lower()
    if "current_user" in low:
        return [{"U": "SVC", "A": "ORGACCT", "R": "AWS_US_EAST_2",
                 "ROLE": "READER"}]
    if "show databases" in low:
        return [{"name": "SALES_DB"}]
    if "information_schema" in low:
        return [{"N": 7}]
    return []


class _DestRecorder:
    """Catalog-API double for `smoke`: one INTERNAL catalog, creates visible."""

    def __init__(self):
        self.ops: list[str] = []
        self.schemas: list[str] = []

    def __call__(self, operation, **kw):
        self.ops.append(operation)
        if operation == "list_catalogs":
            return {"items": [{"displayName": "lake", "key": "lake",
                               "catalogType": "INTERNAL"}]}
        if operation == "list_schemas":
            return {"items": [{"key": f"lake.{s}"} for s in self.schemas]}
        if operation == "create_schema":
            self.schemas.append(kw["schema"])
            return {}
        if operation == "delete_schema":
            self.schemas.remove(kw["schema"])
            return {}
        raise AssertionError(operation)


def _smoke_env(monkeypatch):
    import snowmig
    rec = _DestRecorder()
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: _sf_ok)
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    monkeypatch.setattr(snowmig, "make_call",
                        lambda target, *, backend, **kw: rec)
    return rec


def test_smoke_write_probe_without_execute_issues_no_writes(
        tmp_path, monkeypatch, capsys):
    rec = _smoke_env(monkeypatch)
    rc = main(["smoke", "--write-probe", "--out-dir", str(tmp_path),
               "--account", "a", "--user", "u", "--auth", "password",
               "--password-path", "/p", *TARGET_FLAGS, "--catalog", "lake"])
    assert rc == 0
    assert "create_schema" not in rec.ops and "delete_schema" not in rec.ops
    out = capsys.readouterr().out
    assert "dry run" in out and "--execute" in out
    smoke = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["destination"]["write_verified"] is False
    assert "--execute" in smoke["destination"]["write_note"]


def test_smoke_write_probe_with_execute_creates_then_deletes(
        tmp_path, monkeypatch):
    rec = _smoke_env(monkeypatch)
    rc = main(["smoke", "--write-probe", "--execute", "--out-dir",
               str(tmp_path), "--account", "a", "--user", "u", "--auth",
               "password", "--password-path", "/p", *TARGET_FLAGS,
               "--catalog", "lake"])
    assert rc == 0
    assert rec.ops.index("create_schema") < rec.ops.index("delete_schema")
    smoke = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["destination"]["write_verified"] is True


def _no_subprocess(monkeypatch):
    import subprocess

    def refuse(*a, **k):
        raise AssertionError("no CLI may be invoked from this test")
    monkeypatch.setattr(subprocess, "run", refuse)


def test_notebook_upload_without_execute_is_a_dry_run(
        tmp_path, monkeypatch, capsys):
    _no_subprocess(monkeypatch)
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["notebook", "--upload", "--out-dir", str(tmp_path),
               *TARGET_FLAGS, "--catalog", "d"])
    assert rc == 0
    assert (tmp_path / "snowmig_shallow_clone_d.ipynb").is_file()
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "dry run" in md.lower()
    assert "Uploaded to" not in md
    out = capsys.readouterr().out
    assert "dry run" in out and "--execute" in out


def test_notebook_upload_with_execute_is_refused_and_points_at_provision(
        tmp_path, monkeypatch, capsys):
    # The Jupyter-contents transport 200s and cannot read the file back
    # (GAPS.md 13). Refusing is honest; "uploaded" was not.
    _no_subprocess(monkeypatch)
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["notebook", "--upload", "--execute", "--out-dir",
               str(tmp_path), *TARGET_FLAGS, "--catalog", "d"])
    assert rc == 1
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "provision" in err and "run" in err
    assert "aidp notebook run" not in err
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "Uploaded to" not in md and "aidp notebook run" not in md


def test_notebook_still_generates_offline_without_upload(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["notebook", "--out-dir", str(tmp_path)]) == 0
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "aidp notebook run" not in md


def test_every_writing_subcommand_accepts_execute():
    import snowmig
    parser = snowmig.build_parser()
    for argv in (["deploy", "--execute"],
                 ["provision", "--workspace-name", "w", "--execute"],
                 ["catalog", "--execute"],
                 ["smoke", "--write-probe", "--execute"],
                 ["notebook", "--upload", "--execute"]):
        assert parser.parse_args(argv).execute is True, argv
