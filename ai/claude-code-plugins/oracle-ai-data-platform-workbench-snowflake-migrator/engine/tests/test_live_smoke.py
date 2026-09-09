"""End-to-end against a real Snowflake estate. Skipped unless SNOWMIG_LIVE=1.

Read-only. Runs assess -> deps -> plan -> ddl and asserts the pipeline holds
together on real metadata. It does NOT deploy: that needs AIDP coordinates,
which are supplied per conversation and never stored.
"""
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SNOWMIG_LIVE") != "1",
    reason="set SNOWMIG_LIVE=1 plus SNOWFLAKE_* to run against a live estate")

from snowmig import main  # noqa: E402

DB = os.environ.get("SNOWMIG_LIVE_DB", "TEST_DB_20260908_1529")


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    return tmp_path_factory.mktemp("live")


def auth_args():
    return ["--account", os.environ["SNOWFLAKE_ACCOUNT"],
            "--user", os.environ["SNOWFLAKE_USER"],
            "--auth", "keypair",
            "--key-path", os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            "--warehouse", os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")]


def test_assess_finds_tables_and_views(out):
    rc = main(["assess", "--out-dir", str(out), "--database", DB] + auth_args())
    assert rc == 0, "exit 3 would mean an identifier-case collision"
    inv = json.loads((out / "inventory.json").read_text())
    assert inv["counts_by_type"].get("TABLE", 0) >= 1
    assert inv["counts_by_type"].get("VIEW", 0) >= 1
    assert inv["extraction_notes"] == []
    # Exact counts, not SHOW estimates.
    assert all(r["row_count_exact"] is not None for r in inv["inventory"])


def test_decimal_columns_map_with_precision(out):
    inv = json.loads((out / "inventory.json").read_text())
    decimals = [c for r in inv["inventory"] for c in r["columns"]
                if (c.get("DATA_TYPE") or "").upper() == "NUMBER"]
    assert decimals, "the estate should contain NUMBER columns"
    for c in decimals:
        assert c["target_type"].startswith("DECIMAL("), c
        assert c["NUMERIC_PRECISION"] is not None


def test_timestamp_ntz_maps_to_ntz(out):
    inv = json.loads((out / "inventory.json").read_text())
    ntz = [c for r in inv["inventory"] for c in r["columns"]
           if (c.get("DATA_TYPE") or "").upper() == "TIMESTAMP_NTZ"]
    assert ntz, "the estate should contain TIMESTAMP_NTZ columns"
    assert all(c["target_type"] == "TIMESTAMP_NTZ" for c in ntz)


def test_deps_and_plan(out):
    assert main(["deps", "--out-dir", str(out)] + auth_args()) == 0
    deps = json.loads((out / "dependencies.json").read_text())
    assert deps["source_used"] in ("account_usage", "parsed_ddl")
    assert main(["plan", "--out-dir", str(out)]) == 0
    plan = json.loads((out / "plan.json").read_text())
    assert plan["waves"], "at least one wave expected"
    assert plan["clone_targets"], "tables should be clone targets"
    assert all(u["feature"] == "VIEW" for u in plan["unsupported"])


def test_view_lands_after_its_base_tables(out):
    plan = json.loads((out / "plan.json").read_text())
    deps = json.loads((out / "dependencies.json").read_text())
    if not deps["edges"]:
        pytest.skip("no edges resolved; ordering assertion not meaningful")
    wave_of = {n: i for i, w in enumerate(plan["waves"]) for n in w}
    for edge in deps["edges"]:
        if edge["from"] in wave_of and edge["to"] in wave_of:
            assert wave_of[edge["to"]] < wave_of[edge["from"]], edge


def test_ddl_generates_delta_tables_and_no_replace(out):
    assert main(["ddl", "--out-dir", str(out)]) == 0
    ddl = json.loads((out / "ddl_plan.json").read_text())
    assert ddl["statements"]
    for st in ddl["statements"]:
        assert "USING DELTA" in st["sql"]
        assert "IF NOT EXISTS" in st["sql"]
        assert "OR REPLACE" not in st["sql"]


def test_deploy_dry_run_creates_nothing(out):
    assert main(["deploy", "--out-dir", str(out)]) == 0
    res = json.loads((out / "deploy_result.json").read_text())
    assert res["dry_run"] is True and res["executed"] == 0
