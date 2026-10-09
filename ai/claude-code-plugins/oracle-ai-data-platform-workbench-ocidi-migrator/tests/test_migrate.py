"""End to end, offline: snapshot -> output directory."""
import json

from ocidi2aidp.migrate import IN_PROGRESS
from ocidi2aidp.report import Report
from ocidi2aidp.verify import quartz_problem, verify


def results(report, kind):
    return {r.name: r for r in report.results if r.kind == kind}


def test_published_copy_wins_and_design_only_is_flagged(migrated):
    out, report = migrated
    nbs = results(report, "notebook")
    assert nbs["IT_ORDERS_DAILY"].extra["published"] is True
    assert nbs["IT_CRM_CONTACTS"].extra["published"] is False
    assert any(f.code == "MG01_DESIGN_ONLY" for f in nbs["IT_CRM_CONTACTS"].findings)


def test_statuses(migrated):
    _, report = migrated
    nbs = results(report, "notebook")
    assert nbs["IT_LOAD_CUSTOMERS"].status == "ok"
    assert nbs["IT_CRM_CONTACTS"].status == "fallback_pending"
    assert nbs["SQL_REFRESH_STATS"].status == "fallback_pending"
    assert nbs["IT_BICC_GL"].status == "manual"
    assert nbs["OCIDF_SCORE"].status == "manual"
    assert results(report, "job")["PT_WEEKLY"].status == "manual"


def test_jobs_have_the_live_verified_shape(migrated):
    out, report = migrated
    body = json.loads((out / "jobs/PT_NIGHTLY.job.json").read_text())
    assert set(body) == {"name", "description", "path", "maxConcurrentRuns", "tasks", "schedule"}
    assert body["schedule"] == {"quartzCronExpression": "0 30 2 * * ?",
                                "timezoneId": "America/Chicago", "pauseStatus": "PAUSED"}
    for t in body["tasks"]:
        assert t["type"] == "NOTEBOOK_TASK" and t["notebookPath"].startswith("/Workspace/ocidi/")
        assert isinstance(t["dependsOn"], list)


def test_standalone_jobs_only_for_published_or_unreferenced_tasks(migrated):
    _, report = migrated
    jobs = set(results(report, "job"))
    assert {"IT_LOAD_CUSTOMERS", "IT_ORDERS_DAILY", "DL_PRODUCTS", "IT_ENRICH_SENTIMENT",
            "PT_NIGHTLY", "PT_WEEKLY", "IT_BICC_GL"} == jobs   # SQL/REST tasks only via pipelines


def test_disabled_task_schedule_still_attached_paused(migrated):
    out, report = migrated
    body = json.loads((out / "jobs/IT_ENRICH_SENTIMENT.job.json").read_text())
    assert body["schedule"]["quartzCronExpression"] == "0 15 0/2 * * ?"
    job = results(report, "job")["IT_ENRICH_SENTIMENT"]
    assert any("DISABLED in OCI-DI" in f.message for f in job.findings)
    assert any(f.code == "JB03_NOTEBOOK_STATUS" for f in job.findings)


def test_ddl_watermark_seed_reconcile_sources(migrated):
    out, report = migrated
    sql = (out / "ddl/setup.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS `{catalog}`.`sales_dw`.`dim_customer`" in sql
    assert "`CUSTOMER_ID` DECIMAL(10,0)" in sql and "USING DELTA" in sql
    assert "`{catalog}`.`ocidi_control`.`watermarks`" in sql
    seed = (out / "watermarks/seed.sql").read_text()
    assert "'IT_ORDERS_DAILY' AS task, 'SOURCE_ORDERS' AS source" in seed
    assert "TIMESTAMP'2026-10-01 06:00:00'" in seed
    rec = json.loads((out / "reconcile/reconcile.ipynb").read_text())
    pairs = next("".join(c["source"]) for c in rec["cells"] if "PAIRS =" in "".join(c["source"]))
    assert '"dim_customer"' in pairs and "SRC_ADW_SALES" in pairs
    assert "| PG_CRM |" in (out / "sources.md").read_text()


def test_report_roundtrip_and_review(migrated):
    out, report = migrated
    again = Report.load(out)
    assert again.counts() == report.counts()
    review = (out / "REVIEW.md").read_text()
    assert "## notebook: `IT_BICC_GL` -- manual" in review
    assert not (out / IN_PROGRESS).exists()
    assert "instance_id" in json.dumps(again.config) and "ocid1" not in json.dumps(again.config)


def test_verify_has_no_failures(migrated):
    out, _ = migrated
    grades = {c.artifact: c.grade for c in verify(out)}
    assert "FAIL" not in grades.values()
    assert grades["notebooks/SALES/Ingest/IT_LOAD_CUSTOMERS.ipynb"] == "PASS"
    assert grades["notebooks/SALES/Ingest/IT_CRM_CONTACTS.ipynb"] == "REVIEW"


def test_verify_catches_broken_artifacts(fresh_migration):
    out, _ = fresh_migration
    job = out / "jobs/IT_ORDERS_DAILY.job.json"
    body = json.loads(job.read_text())
    body["tasks"][0]["notebookPath"] = "/Workspace/elsewhere.ipynb"
    body["schedule"] = {"quartzCronExpression": "0 0 1 * * *", "timezoneId": "UTC",
                        "pauseStatus": "PAUSED"}
    job.write_text(json.dumps(body))
    nb = out / "notebooks/SALES/Ingest/DL_PRODUCTS.ipynb"
    data = json.loads(nb.read_text())
    data["cells"][3]["source"] = ["x = (\n", "password = 'hunter2'\n"]
    nb.write_text(json.dumps(data))
    checks = {c.artifact: c for c in verify(out)}
    assert checks["jobs/IT_ORDERS_DAILY.job.json"].grade == "FAIL"
    assert checks["notebooks/SALES/Ingest/DL_PRODUCTS.ipynb"].grade == "FAIL"


def test_quartz_rules():
    assert quartz_problem("0 0 1 * * ?") == ""
    assert quartz_problem("0 0 1 ? * MON") == ""
    assert quartz_problem("0 0 1 * * *")
    assert quartz_problem("0 1 * * *")


def test_rerun_keeps_fallback_answers(fresh_migration, snapshot_path, config):
    from ocidi2aidp.migrate import migrate
    out, report = fresh_migration
    fid = next(r for r in report.results if r.fallbacks).fallbacks[0]
    (out / f"fallback/{fid}.py").write_text("def x(): pass\n")
    migrate(snapshot_path, out, config, strict=True)
    assert (out / f"fallback/{fid}.py").read_text() == "def x(): pass\n"


def test_only_filter(tmp_path, snapshot_path, config):
    from ocidi2aidp.migrate import migrate
    rep = migrate(snapshot_path, tmp_path / "o", config, only={"IT_LOAD_CUSTOMERS"})
    assert {r.name for r in rep.results if r.kind == "notebook"} == {"IT_LOAD_CUSTOMERS"}
