"""Compiler behaviour on the fixture snapshot: what is emitted, and what is refused."""
import ast
import json

import pytest

from ocidi2aidp.compile.dataflow import FlowCompiler, _di_pattern
from ocidi2aidp.compile.graph import FlowGraph, GraphError
from ocidi2aidp.compile.pipeline import PipelineCompiler, TaskArtifact


def compile_task(snapshot, config, name, **kw):
    t = snapshot.find("tasks", name)
    return FlowCompiler(t["dataFlow"], snapshot=snapshot, config=config, name=name, key=t["key"],
                        kind=t["modelType"], strict=True, **kw).compile()


def code(res):
    return "\n".join(c.source for c in res.notebook.cells if c.kind == "code")


def codes(res):
    return {f.code for f in res.findings}


def test_every_cell_is_valid_python(snapshot, config):
    for name in ("IT_LOAD_CUSTOMERS", "IT_ORDERS_DAILY", "IT_CRM_CONTACTS", "IT_ENRICH_SENTIMENT",
                 "IT_BICC_GL", "DL_PRODUCTS"):
        res = compile_task(snapshot, config, name)
        for c in res.notebook.cells:
            if c.kind == "code":
                ast.parse(c.source)


def test_parameters_cell_is_tagged_and_carries_task_override(snapshot, config):
    res = compile_task(snapshot, config, "IT_LOAD_CUSTOMERS", overrides={"P_SINCE": "2021-01-01"})
    cell = next(c for c in res.notebook.cells if "parameters" in c.tags)
    assert '"P_SINCE": _coerce(_aidp_parameter("P_SINCE", "2021-01-01"), "TIMESTAMP")' in cell.source
    assert 'TARGET_CATALOG = _aidp_parameter("TARGET_CATALOG", "ocidi_migrated")' in cell.source


def test_no_credentials_or_urls_in_generated_code(snapshot, config):
    for name in ("IT_LOAD_CUSTOMERS", "IT_ORDERS_DAILY", "IT_CRM_CONTACTS"):
        text = code(compile_task(snapshot, config, name))
        assert "jdbc:" not in text and "password=" not in text.lower()
        assert "ocid1.vaultsecret" not in text


def test_file_source_gets_declared_schema(snapshot, config):
    text = code(compile_task(snapshot, config, "IT_LOAD_CUSTOMERS"))
    assert '.schema("`CUSTOMER_ID` INT, `FIRST_NAME` STRING' in text
    assert '.option("header", True)' in text
    assert '_os_path(SRC_OS_LANDING, "landing", "customers/customers.csv")' in text


def test_direct_field_map_resolves_keys(snapshot, config):
    text = code(compile_task(snapshot, config, "IT_LOAD_CUSTOMERS"))
    assert 'F.col("EMAIL_NORM").alias("EMAIL")' in text
    assert '_write_delta(df_TARGET_1_out, _tbl(TARGET_CATALOG, "sales_dw", "dim_customer"), ' \
           '"merge", keys=["CUSTOMER_ID"])' in text


def test_incremental_source_and_watermark_commit(snapshot, config):
    res = compile_task(snapshot, config, "IT_ORDERS_DAILY")
    text = code(res)
    assert '_wm_prev = _wm_get(WM_TASK, "SOURCE_ORDERS")' in text
    assert "_wm_commit(WM_TASK)" in text
    assert res.watermarks == [{"source": "SOURCE_ORDERS", "column": "UPDATED_AT"}]
    assert "TG08_OVERWRITE_INCREMENTAL" in codes(res)


def test_targets_written_in_load_order(snapshot, config):
    text = code(compile_task(snapshot, config, "IT_ORDERS_DAILY"))
    assert text.index('"large_orders"') < text.index('"fact_orders_daily"')


def test_join_lookup_split_aggregate(snapshot, config):
    res = compile_task(snapshot, config, "IT_ORDERS_DAILY")
    text = code(res)
    assert 'F.expr("l.`CUSTOMER_ID` = r.`CUSTOMER_ID`"), "inner"' in text
    assert "_ocidi_row" in text and "row_number" in text           # RETURN_ANY de-duplication
    assert 'F.lit("UNKNOWN")' in text                               # null fill
    assert "df_SPLIT_SIZE_UNMATCHED = df_EXPR_ENRICH.filter(~(_c_SPLIT_SIZE_0))" in text
    assert '.groupBy("ORDER_DATE", "COUNTRY", "CATEGORY").agg(' in text
    assert {"JN03_DUPLICATE_COLUMNS", "SR10_INCREMENTAL"} <= codes(res)


def test_union_distinct_sort_and_jdbc_source(snapshot, config):
    res = compile_task(snapshot, config, "IT_CRM_CONTACTS")
    text = code(res)
    assert "unionByName" in text and ".distinct()" in text and 'F.col("UPDATED_AT").desc()' in text
    assert 'aidputils.secrets.get(name=SRC_PG_CRM, key="password")' in text
    assert any(f.code == "SR01_SOURCE_ACCESS" and "pg_crm" in f.message for f in res.findings)


def test_untranslatable_expression_becomes_a_work_order(snapshot, config):
    res = compile_task(snapshot, config, "IT_CRM_CONTACTS")
    assert len(res.fallbacks) == 1
    order = res.fallbacks[0]
    assert order["kind"] == "expression" and "ORA_HASH" in order["reason"]
    assert order["node"]["label"] == "EXPR_HASH"
    assert order["inputs"][0]["from"] == "UNION_ALL"
    text = code(res)
    assert f"# >>> {order['id']}" in text and "raise NotImplementedError" in text
    # downstream is still compiled, from the stub's output
    assert "df_DISTINCT_1 = df_EXPR_HASH.distinct()" in text


def test_oci_function_is_a_work_order_and_fusion_source_is_manual(snapshot, config):
    fn = compile_task(snapshot, config, "IT_ENRICH_SENTIMENT")
    assert fn.fallbacks[0]["kind"] == "oci_function"
    gl = compile_task(snapshot, config, "IT_BICC_GL")
    assert not gl.fallbacks
    assert any(f.severity == "manual" and "BICC" in f.message for f in gl.findings)


def test_work_order_ids_are_stable_across_runs(snapshot, config):
    a = compile_task(snapshot, config, "IT_CRM_CONTACTS").fallbacks[0]["id"]
    b = compile_task(snapshot, config, "IT_CRM_CONTACTS").fallbacks[0]["id"]
    assert a == b


def test_cycle_is_reported_not_crashed(snapshot, config):
    flow = json.loads(json.dumps(snapshot.find("tasks", "DL_PRODUCTS")["dataFlow"]))
    src, tgt = flow["nodes"]
    tgt["outputLinks"] = [{"key": "back", "port": "p", "toLinks": ["back-in"]}]
    src["inputLinks"] = [{"key": "back-in", "port": "q", "fromLink": "back"}]
    with pytest.raises(GraphError, match="cycle"):
        FlowGraph(flow).order()
    res = FlowCompiler(flow, snapshot=snapshot, config=config, name="X", key="x",
                       kind="INTEGRATION_TASK").compile()
    assert res.failed and any(f.code == "DF01_GRAPH" for f in res.findings)


def test_map_by_pattern():
    pattern, repl = _di_pattern("*NAME", "TGT_$1", False)
    import re
    assert re.sub(pattern, repl, "FIRSTNAME") == "TGT_FIRST"
    assert re.fullmatch(pattern, "firstname")           # case-insensitive by default
    pattern, _ = _di_pattern("(?c)*NAME", "X", False)
    assert not re.fullmatch(pattern, "firstname")


def test_pipeline_dependencies_runif_retries(snapshot):
    pl = snapshot.find("pipelines", "PL_NIGHTLY")
    res = PipelineCompiler(pl, lambda ref: TaskArtifact(notebook_path=f"/nb/{ref['name']}")).compile()
    tasks = {t["taskKey"]: t for t in res.tasks}
    assert set(tasks) == {"LOAD_CUSTOMERS", "LOAD_PRODUCTS", "ORDERS_DAILY", "REFRESH_STATS",
                          "NOTIFY_FAILURE"}
    assert [d["taskKey"] for d in tasks["ORDERS_DAILY"]["dependsOn"]] == ["LOAD_CUSTOMERS",
                                                                           "LOAD_PRODUCTS"]
    assert tasks["NOTIFY_FAILURE"]["runIf"] == "ALL_FAILED"
    assert tasks["ORDERS_DAILY"]["maxRetries"] == 2
    assert tasks["ORDERS_DAILY"]["minRetryIntervalMillis"] == 300_000
    assert not res.manual


def test_pipeline_decision_branch_is_left_out_and_manual(snapshot):
    pl = snapshot.find("pipelines", "PL_WEEKLY")
    res = PipelineCompiler(pl, lambda ref: TaskArtifact(notebook_path="/nb")).compile()
    assert [t["taskKey"] for t in res.tasks] == ["ENRICH"]
    assert res.manual
    assert {"PL02_DECISION", "PL03_BEHIND_DECISION"} <= {f.code for f in res.findings}


def test_nested_pipeline_is_inlined(snapshot):
    inner = snapshot.find("pipelines", "PL_NIGHTLY")
    outer = json.loads(json.dumps(snapshot.find("pipelines", "PL_WEEKLY")))
    # Replace the decision branch with: START -> NIGHTLY (nested) -> ENRICH
    nodes = {n["name"]: n for n in outer["nodes"]}
    enrich = nodes["ENRICH"]
    enrich["operator"]["task"] = {"modelType": "PIPELINE_TASK", "name": "PT_NIGHTLY"}
    outer["nodes"] = [nodes["START_1"], enrich]

    def resolve(ref):
        if ref.get("modelType") == "PIPELINE_TASK":
            return TaskArtifact(pipeline=inner)
        return TaskArtifact(notebook_path=f"/nb/{ref['name']}")
    res = PipelineCompiler(outer, resolve).compile()
    keys = [t["taskKey"] for t in res.tasks]
    assert "ENRICH__ORDERS_DAILY" in keys and len(keys) == 5
