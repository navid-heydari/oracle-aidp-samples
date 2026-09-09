"""The two headline reports: what is planned to move, and what got created."""
from report.render import (
    render_compute, render_planned_objects, render_soft_clone_summary,
)

PLAN = {
    "built_at": "2026-09-09T00:00:00+00:00",
    "bronze_catalog_prefix": None,
    "bronze_mapping": "Snowflake database -> AIDP Standard Catalog, schema -> schema",
    "waves": [["D.PUBLIC.ORDERS"], ["D.PUBLIC.ORDERS_VW"]],
    "cycles": [],
    "target_names": {"D.PUBLIC.ORDERS": "D.PUBLIC.ORDERS",
                     "D.PUBLIC.ORDERS_VW": "D.PUBLIC.ORDERS_VW"},
    "clone_targets": ["D.PUBLIC.ORDERS", "D.PUBLIC.ORDERS_VW"],
    "can_migrate": [
        {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "D.PUBLIC.ORDERS", "rows": 100, "columns": 41},
        {"source_identifier": "D.PUBLIC.ORDERS_VW", "object_type": "VIEW",
         "target": "D.PUBLIC.ORDERS_VW", "rows": 100, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "D.PUBLIC.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT semi-structured"},
        {"source_identifier": "D.PUBLIC.V2", "object_type": "VIEW",
         "category": "snowflake_only_sql", "reason": "uses QUALIFY (no Spark equiv)"},
        {"source_identifier": "D2.S.X", "object_type": "TABLE",
         "category": "restriction", "reason": "database D2 excluded (restriction: exclude_databases)"}],
    "restrictions_applied": {"exclude_databases": ["D2"]},
    "catalogs_to_create": ["D"],
    "schemas_to_create": [["D", "PUBLIC"]],
    "silver_gold_jobs": [
        {"name": "silver_D_PUBLIC", "layer": "SILVER", "enabled": False,
         "trigger": "MANUAL_NEVER_TRIGGERED", "reads_from": "D.PUBLIC",
         "body_status": "placeholder"},
        {"name": "gold_D_PUBLIC", "layer": "GOLD", "enabled": False,
         "trigger": "MANUAL_NEVER_TRIGGERED", "reads_from": "D.PUBLIC",
         "body_status": "placeholder"}],
    "dependency_source": "account_usage",
    "dependency_coverage_note": "authoritative",
    "summary": {"objects_inventoried": 5, "can_migrate": 2, "cannot_migrate": 3,
                "tables": 1, "views": 1, "catalogs": 1, "schemas": 1,
                "silver_gold_jobs": 2,
                "cannot_by_category": {"unmapped_type": 1, "snowflake_only_sql": 1,
                                       "restriction": 1}},
}


# --- "what are the objects planned to move" ------------------------------

def test_planned_report_leads_with_the_headline_counts():
    md = render_planned_objects(PLAN)
    assert "2" in md and "3" in md
    assert "planned to move" in md.lower()


def test_can_migrate_table_lists_object_type_and_target():
    md = render_planned_objects(PLAN)
    assert "D.PUBLIC.ORDERS" in md and "TABLE" in md
    assert "D.PUBLIC.ORDERS_VW" in md and "VIEW" in md


def test_cannot_migrate_gives_a_brief_reason_per_object():
    md = render_planned_objects(PLAN)
    assert "VARIANT" in md
    assert "QUALIFY" in md
    assert "database D2 excluded" in md


def test_cannot_migrate_is_grouped_by_category():
    md = render_planned_objects(PLAN)
    for cat in ("unmapped_type", "snowflake_only_sql", "restriction"):
        assert cat in md


def test_restrictions_in_force_are_shown():
    md = render_planned_objects(PLAN)
    assert "exclude_databases" in md


def test_bronze_mapping_is_stated_explicitly():
    md = render_planned_objects(PLAN)
    assert "Standard Catalog" in md


def test_silver_gold_jobs_shown_as_created_but_not_triggered():
    md = render_planned_objects(PLAN)
    assert "silver_D_PUBLIC" in md
    assert "not triggered" in md.lower() or "never triggered" in md.lower()


def test_catalogs_the_user_must_precreate_are_called_out():
    md = render_planned_objects(PLAN)
    assert "catalogs_to_create" in md.lower() or "Catalogs" in md


def test_waves_shown_with_views_last():
    md = render_planned_objects(PLAN)
    assert md.index("Wave 1") < md.index("Wave 2")


# --- "what has been created in the soft clone" ---------------------------

def test_soft_clone_summary_dry_run_says_nothing_created():
    md = render_soft_clone_summary(PLAN, {"dry_run": True, "statement_count": 2,
                                          "executed": 0, "verified": 0,
                                          "failed": [], "chunk_errors": [],
                                          "blocked_count": 0,
                                          "catalog_in_scope": "D",
                                          "out_of_scope_count": 0,
                                          "out_of_scope_catalogs": []})
    assert "DRY RUN" in md.upper()
    assert "nothing was created" in md.lower()


def test_soft_clone_summary_reports_verified_not_executed():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 2, "executed": 2, "verified": 1,
        "failed": [{"target_fqn": "D.PUBLIC.ORDERS_VW",
                    "reason": "not present after its chunk reported completion"}],
        "chunk_errors": [], "blocked_count": 0, "catalog_in_scope": "D",
        "out_of_scope_count": 0, "out_of_scope_catalogs": []})
    assert "verified" in md.lower()
    assert "1/2" in md or "1 of 2" in md
    assert "D.PUBLIC.ORDERS_VW" in md


def test_soft_clone_summary_says_the_tables_are_empty():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 2, "executed": 2, "verified": 2,
        "failed": [], "chunk_errors": [], "blocked_count": 0,
        "catalog_in_scope": "D", "out_of_scope_count": 0,
        "out_of_scope_catalogs": []})
    assert "empty" in md.lower() and "no data" in md.lower()


def test_soft_clone_summary_flags_out_of_scope_catalogs():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 1, "executed": 1, "verified": 1,
        "failed": [], "chunk_errors": [], "blocked_count": 0,
        "catalog_in_scope": "D", "out_of_scope_count": 4,
        "out_of_scope_catalogs": ["D2", "D3"]})
    assert "D2" in md and "D3" in md
    assert "separate" in md.lower() or "not deployed" in md.lower()


# --- compute proposal -----------------------------------------------------

def test_compute_report_lists_warehouses_and_proposals():
    md = render_compute({
        "warehouse_count": 1, "max_concurrent_clusters": 2,
        "total_source_nodes": 4, "observed_credits_total": 600.0,
        "credits_basis": "observed", "cost_model": None,
        "cost_note": "No cost model: a credit price was not supplied.",
        "proposals": [{"name": "W", "source_size": "Medium", "source_nodes": 4,
                       "worker_count": 2, "autoscale_max_workers": 4,
                       "worker_ocpus": 8, "worker_shape_family": "VM.Standard.E5.Flex",
                       "shape_confirmation_required": True, "notes": "confirm shape"}],
        "blocked": [{"name": "ODD", "reason": "unrecognised size 'Nano'"}]})
    assert "W" in md and "Medium" in md
    assert "VM.Standard.E5.Flex" in md
    assert "confirm" in md.lower()
    assert "ODD" in md and "Nano" in md
    assert "credit price" in md.lower()
