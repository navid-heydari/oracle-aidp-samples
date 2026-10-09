"""The LLM path: prompts, the validator, splicing, and the headless provider (mocked)."""
from types import SimpleNamespace

import pytest

from ocidi2aidp.fallback.apply import apply
from ocidi2aidp.fallback.providers import run_anthropic
from ocidi2aidp.fallback.validate import extract_code, validate
from ocidi2aidp.fallback.workorder import load_orders, render_prompt, response_path
from ocidi2aidp.notebook import cell_text, read_ipynb
from ocidi2aidp.report import Report

ORDER = {"id": "FB_x", "function": "_fb_FB_x", "kind": "expression"}


def ok(body="    return inputs['in0']"):
    return f"def _fb_FB_x(spark, inputs, params):\n    from pyspark.sql import functions as F\n{body}\n"


def test_valid_answer_passes_and_assumptions_are_collected():
    v = validate(ok("    # ASSUMPTION: bucket values differ from Oracle\n    return inputs['in0']"),
                 ORDER)
    assert v.ok and not v.declined and v.assumptions == ["bucket values differ from Oracle"]


@pytest.mark.parametrize("bad,why", [
    ("import os\n" + ok(), "import of 'os'"),
    (ok("    return eval('1')"), "eval"),
    (ok("    open('/etc/passwd')"), "open"),
    (ok("    spark.conf.set('a', 'b')"), "session configuration"),
    (ok("    url = 'jdbc:oracle:thin:@db'"), "JDBC URL"),
    (ok("    password = 'hunter2'"), "credential literal"),
    (ok("    return spark._jvm.java.sql.DriverManager"), "JVM access"),
    (ok("    aidputils.secrets.get(name='a', key='b')"), "aidputils"),
    (ok("    import urllib.request"), "urllib"),
    ("def other(spark, inputs, params):\n    return 1\n", "exactly one top-level function"),
    ("def _fb_FB_x(spark, inputs):\n    return 1\n", "signature"),
    (ok() + "X = 1\n", "only the function"),
    ("def _fb_FB_x(spark, inputs, params)\n    return 1\n", "does not parse"),
    (ok("    return ().__class__.__subclasses__()"), "dunder"),
])
def test_validator_refuses(bad, why):
    v = validate(bad, ORDER)
    assert not v.ok
    assert any(why in e for e in v.errors), v.errors


def test_capabilities_are_per_kind():
    sql = {"id": "FB_x", "function": "_fb_FB_x", "kind": "sql_task"}
    body = ok("    c = spark._jvm.java.sql.DriverManager.getConnection(\n"
              "        aidputils.secrets.get(name='adw', key='url'),\n"
              "        aidputils.secrets.get(name='adw', key='user'),\n"
              "        aidputils.secrets.get(name='adw', key='password'))\n    c.close()")
    assert validate(body, sql).ok
    assert not validate(body, ORDER).ok
    rest = {"id": "FB_x", "function": "_fb_FB_x", "kind": "rest_task"}
    assert validate(ok("    import urllib.request\n    return None"), rest).ok


def test_only_raising_is_a_decline_not_an_error():
    v = validate("def _fb_FB_x(spark, inputs, params):\n    \"\"\"doc\"\"\"\n"
                 "    raise NotImplementedError('function code unavailable')\n", ORDER)
    assert v.ok and v.declined


def test_extract_code_takes_the_fenced_block():
    assert extract_code("Here:\n```python\ndef f():\n    pass\n```\nthanks") == "def f():\n    pass\n"
    assert extract_code("def f(): pass") == "def f(): pass\n"


def test_prompt_contains_contract_and_raw_json(migrated):
    out, _ = migrated
    order = next(o for o in load_orders(out) if o["kind"] == "expression")
    prompt = render_prompt(order)
    assert f"def {order['function']}(spark, inputs, params):" in prompt
    assert "ORA_HASH" in prompt and '"EMAIL_BUCKET"' in prompt
    assert "any network access" in prompt and "aidputils" in prompt
    sql = next(o for o in load_orders(out) if o["kind"] == "sql_task")
    assert "spark._jvm.java.sql.DriverManager" in render_prompt(sql)
    assert "other than that one JDBC connection" in render_prompt(sql)


def _answers(out):
    orders = {o["kind"]: o for o in load_orders(out)}
    expr, fn, sql = orders["expression"], orders["oci_function"], orders["sql_task"]
    response_path(out, expr["id"]).write_text(
        "```python\n"
        f"def {expr['function']}(spark, inputs, params):\n"
        "    from pyspark.sql import functions as F\n"
        "    # ASSUMPTION: Spark hash() buckets differ from ORA_HASH buckets\n"
        "    return inputs['in0'].withColumn('EMAIL_BUCKET', F.pmod(F.hash('EMAIL'), F.lit(16)))\n"
        "```\n")
    response_path(out, fn["id"]).write_text(
        f"def {fn['function']}(spark, inputs, params):\n"
        "    raise NotImplementedError('the OCI Function code is not available')\n")
    response_path(out, sql["id"]).write_text(f"def {sql['function']}(spark, inputs, params):\n"
                                             "    import os\n    return None\n")
    return expr, fn, sql


def test_apply_splices_declines_and_rejects(fresh_migration):
    out, _ = fresh_migration
    expr, fn, sql = _answers(out)
    outcomes = {o.id: o for o in apply(out, author="test")}
    assert outcomes[expr["id"]].state == "applied"
    assert outcomes[fn["id"]].state == "declined"
    assert outcomes[sql["id"]].state == "rejected"
    report = Report.load(out)
    by_name = {r.name: r for r in report.results if r.kind == "notebook"}
    assert by_name["IT_CRM_CONTACTS"].status == "llm_assisted"
    assert by_name["IT_ENRICH_SENTIMENT"].status == "manual"
    assert by_name["SQL_REFRESH_STATS"].status == "fallback_pending"
    nb = read_ipynb(out / by_name["IT_CRM_CONTACTS"].artifacts[0])
    text = "\n".join(cell_text(c) for c in nb["cells"])
    assert "F.pmod(F.hash('EMAIL'), F.lit(16))" in text
    assert f"# LLM-ASSISTED -- REVIEW REQUIRED [{expr['id']}]" in text
    assert "raise NotImplementedError" not in text.split(f"# >>> {expr['id']}")[1].split(
        f"# <<< {expr['id']}")[0]
    # idempotent: applying again leaves one copy
    apply(out, author="test")
    nb = read_ipynb(out / by_name["IT_CRM_CONTACTS"].artifacts[0])
    assert "\n".join(cell_text(c) for c in nb["cells"]).count("F.pmod(") == 1


class _FakeMessages:
    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    def create(self, **kw):
        self.calls.append(kw)
        return self.replies.pop(0)


def test_anthropic_provider_request_shape_and_refusal(fresh_migration):
    pytest.importorskip("anthropic")
    out, _ = fresh_migration
    orders = load_orders(out)
    good = SimpleNamespace(stop_reason="end_turn",
                           content=[SimpleNamespace(type="text", text="```python\ndef f(): pass\n```")])
    refused = SimpleNamespace(stop_reason="refusal", content=[])
    messages = _FakeMessages([good, refused, good])
    client = SimpleNamespace(beta=SimpleNamespace(messages=messages))
    res = run_anthropic(out, client=client)
    states = [s for _, s, _ in res]
    assert states == ["written", "refused", "written"]
    call = messages.calls[0]
    assert call["model"] == "claude-opus-5-5"
    assert call["fallbacks"] == "default" and call["betas"] == ["server-side-fallback-2026-07-01"]
    assert call["output_config"] == {"effort": "high"}
    assert "thinking" not in call and "temperature" not in call
    assert response_path(out, orders[0]["id"]).exists()
    assert not response_path(out, orders[1]["id"]).exists()
    # existing answers are not overwritten unless asked; the refused one is retried
    messages.replies.append(refused)
    again = {fid: s for fid, s, _ in run_anthropic(out, client=client)}
    assert again[orders[0]["id"]] == "skipped" and again[orders[1]["id"]] == "refused"
    assert len(messages.calls) == 4
