"""The migration summary table: name, rows, risk, migration status."""
from report.render import render_summary

PLAN = {
    "bronze_mapping": "Snowflake database -> AIDP Standard Catalog",
    "target_names": {"D.PUBLIC.ORDERS": "D.PUBLIC.ORDERS",
                     "D.PUBLIC.V": "D.PUBLIC.V"},
    "can_migrate": [
        {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "D.PUBLIC.ORDERS", "rows": 100, "columns": 41},
        {"source_identifier": "D.PUBLIC.V", "object_type": "VIEW",
         "target": "D.PUBLIC.V", "rows": 100, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "D.PUBLIC.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT"}],
    "silver_gold_jobs": [
        {"name": "silver_D_PUBLIC", "layer": "SILVER", "reads_from": "D.PUBLIC",
         "enabled": False, "trigger": "MANUAL_NEVER_TRIGGERED",
         "body_status": "placeholder"}],
    "catalogs_to_create": ["D"],
    "schemas_to_create": [["D", "PUBLIC"]],
    "summary": {"objects_inventoried": 3, "can_migrate": 2, "cannot_migrate": 1,
                "tables": 1, "views": 1, "catalogs": 1, "schemas": 1,
                "silver_gold_jobs": 1, "cannot_by_category": {"unmapped_type": 1}},
}
INV = {"session": {"A": "DU58131", "R": "AWS_US_EAST_2", "ROLE": "ACCOUNTADMIN",
                   "V": "10.32.102"},
       "databases_in_scope": ["D"]}
DEPLOYED = {"dry_run": False, "attempted_targets": ["D.PUBLIC.ORDERS", "D.PUBLIC.V"],
            "verified_targets": ["D.PUBLIC.ORDERS"], "failed_targets": ["D.PUBLIC.V"],
            "catalog_in_scope": "D"}


# --- source -> destination ------------------------------------------------

def test_source_and_destination_are_stated_briefly():
    md = render_summary(PLAN, INV, None, None)
    assert "DU58131" in md and "AWS_US_EAST_2" in md
    assert "->" in md or "→" in md


def test_destination_shows_the_target_when_supplied():
    md = render_summary(PLAN, INV, DEPLOYED,
                        {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.a",
                         "workspace": "ws-1", "cluster_id": "cl-1", "catalog": "D"})
    assert "ocid1.aidataplatform.oc1.iad.a" in md
    assert "cl-1" in md


def test_destination_says_not_supplied_when_absent():
    md = render_summary(PLAN, INV, None, None)
    assert "not supplied" in md.lower()


# --- the table ------------------------------------------------------------

def test_table_has_the_four_required_columns():
    md = render_summary(PLAN, INV, None, None)
    header = next(l for l in md.splitlines() if l.startswith("| Object"))
    for col in ("Object", "Rows", "Risk", "Migration status"):
        assert col in header, header


def test_row_counts_shown_per_object():
    md = render_summary(PLAN, INV, None, None)
    assert "| 100 " in md


def test_status_is_not_yet_done_before_any_deploy():
    md = render_summary(PLAN, INV, None, None)
    assert "NOT_YET_DONE" in md


def test_status_becomes_shallow_clone_once_verified():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    orders = next(l for l in md.splitlines() if "D.PUBLIC.ORDERS" in l)
    assert "SHALLOW_CLONE" in orders


def test_unverified_object_shows_in_progress():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    view = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "IN_PROGRESS" in view


def test_blocked_object_shows_blocked_and_high_risk():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "D.PUBLIC.J" in l)
    assert "BLOCKED" in row and "HIGH" in row


def test_view_carries_medium_risk():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "MEDIUM" in row


def test_views_and_tables_are_both_in_the_table():
    md = render_summary(PLAN, INV, None, None)
    assert "TABLE" in md and "VIEW" in md


# --- jobs -----------------------------------------------------------------

def test_jobs_appear_in_the_same_summary_format():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "silver_D_PUBLIC" in l)
    assert "JOB" in row
    assert "NOT_YET_DONE" in row


def test_job_risk_notes_it_is_a_placeholder():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "silver_D_PUBLIC" in l)
    assert "placeholder" in row.lower() or "never triggered" in row.lower()


# --- the no-data guarantee ------------------------------------------------

def test_summary_states_no_data_was_moved():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    low = md.lower()
    assert "no data" in low
    assert "DATA_CLONE" in md, "the vocabulary is shown so the gap is visible"


def test_status_counts_rolled_up():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    assert "SHALLOW_CLONE" in md and "BLOCKED" in md


# --- smoke report ---------------------------------------------------------

def test_smoke_report_shows_both_ends_and_the_verdict():
    from report.render import render_smoke
    md = render_smoke({
        "ok": True,
        "source": {"reachable": True, "user": "U", "role": "R", "account": "A",
                   "region": "REG",
                   "checks": [{"name": "list databases", "ok": True,
                               "detail": "1 database(s) visible"}]},
        "destination": {"skipped": False, "catalog": "D", "cluster_id": "cl",
                        "checks": [{"name": "read target catalog", "ok": True,
                                    "detail": "2 schema(s)"}],
                        "write_verified": True,
                        "write_note": "verified: created D.probe. remove it",
                        "left_behind": ["D.probe"]}})
    assert "PASS" in md
    assert "list databases" in md and "read target catalog" in md
    assert "D.probe" in md
    assert "DROP" in md, "must say why it cannot clean up"


def test_smoke_report_states_when_destination_was_skipped():
    from report.render import render_smoke
    md = render_smoke({"ok": True,
                       "source": {"reachable": True, "checks": []},
                       "destination": {"skipped": True,
                                       "reason": "coordinates not supplied"}})
    assert "Skipped" in md and "not supplied" in md
