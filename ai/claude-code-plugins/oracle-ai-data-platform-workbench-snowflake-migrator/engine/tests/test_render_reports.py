"""The two headline reports: what is planned to move, and what got created."""
from snowflake_source.extract.census import KINDS
from report.render import (
    render_census, render_ddl_plan, render_maintenance,
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


# --------------------------------------------------------------------------
# Mismatch and unverified structure must be visible, not folded into
# "verified" or "failed" (issue #2).
# --------------------------------------------------------------------------

def test_soft_clone_summary_reports_a_structure_mismatch_prominently():
    res = {"dry_run": False, "statement_count": 2, "executed": 2, "verified": 1,
           "catalog_in_scope": "CAT",
           "failed": [], "chunk_errors": [],
           "mismatches": [{"source_identifier": "D.S.T", "target_fqn": "CAT.S.T",
                           "reason": "position 1: planned ID DECIMAL(38,0), "
                                     "found ID STRING"}],
           "unverified_structure": []}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "Structure differs" in md
    assert "CAT.S.T" in md
    assert "not been cloned" in md.lower() or "left as found" in md.lower()


def test_soft_clone_summary_reports_unverified_structure_separately():
    res = {"dry_run": False, "statement_count": 1, "executed": 1, "verified": 0,
           "catalog_in_scope": "CAT", "failed": [], "chunk_errors": [],
           "mismatches": [],
           "unverified_structure": [
               {"source_identifier": "D.S.T", "target_fqn": "CAT.S.T",
                "reason": "exists, but its structure could not be read"}]}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "Structure not verified" in md
    assert "CAT.S.T" in md


def test_verified_wording_says_structure_not_just_existence():
    res = {"dry_run": False, "statement_count": 1, "executed": 1, "verified": 1,
           "catalog_in_scope": "CAT", "failed": [], "chunk_errors": [],
           "mismatches": [], "unverified_structure": []}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "column" in md.lower()


def test_ddl_plan_reports_deferred_maintenance_settings():
    plan = {"statements": [
        {"source_identifier": "DB.SC.T", "object_type": "TABLE",
         "target_fqn": "CAT.SC.T", "sql": "CREATE TABLE ...",
         "rules_applied": [], "warnings": [], "omitted_properties": [],
         "deferred_properties": [
             {"property": "cluster_by", "value": "(ORDER_DATE, STORE_ID)",
              "aidp_equivalent": "Delta liquid clustering (`CLUSTER BY`) or "
                                 "`OPTIMIZE … ZORDER BY`"}]}],
        "blocked": []}
    md = render_ddl_plan(plan)
    assert "Maintenance and layout" in md
    assert "cluster_by" in md
    assert "(ORDER_DATE, STORE_ID)" in md
    assert "ZORDER" in md
    # And it must say plainly that nothing was applied.
    assert "not applied" in md.lower()


# --------------------------------------------------------------------------
# The maintenance report (M2).
# --------------------------------------------------------------------------

def _maint(**over):
    base = {
        "probed_at": "2026-09-10T00:00:00+00:00", "history_days": 30,
        "table_parameters_probed": False,
        "retention": {"account": {"data_retention_time_in_days": 1,
                                  "max_data_extension_time_in_days": 14,
                                  "set_at": "default"},
                      "databases": {"DB": 1}, "schemas": {"DB.PUBLIC": 1}},
        "account_usage": {"readable": True, "note": "summarised over 30 day(s)"},
        "tables": [], "objects_with_signals": 0,
        "no_equivalent": [{"capability": "Fail-safe", "snowflake": "7 days",
                           "impact": "No AIDP equivalent"}],
        "unreadable": [],
    }
    base.update(over)
    return base


def _mt(**over):
    base = {"source_identifier": "DB.PUBLIC.ORDERS", "cluster_by": "(ORDER_DATE)",
            "clustered": True, "automatic_clustering": True,
            "change_tracking": False, "search_optimization": False,
            "search_optimization_bytes": None, "retention_days": 7,
            "retention_set_at": "table", "retention_inherited_value": 1,
            "rows": 4_000_000, "bytes": 10**9,
            "reclustering": {"measured": True, "events": 12, "credits": 34.5,
                             "bytes_reclustered": 1, "rows_reclustered": 1},
            "dml_churn": {"measured": True, "rows_added": 10, "rows_removed": 5,
                          "rows_updated": 5, "rows_rewritten": 10, "windows": 30},
            "signals": [{"signal": "clustering key in use", "detail": "d",
                         "aidp_equivalent": "liquid clustering",
                         "aidp_requires": "a scheduled job"}]}
    base.update(over)
    return base


def test_maintenance_report_lists_tables_with_signals():
    md = render_maintenance(_maint(tables=[_mt()], objects_with_signals=1))
    assert "DB.PUBLIC.ORDERS" in md
    assert "clustering key in use" in md
    assert "scheduled job" in md


def test_maintenance_report_states_nothing_was_applied():
    md = render_maintenance(_maint(tables=[_mt()])).lower()
    assert "applies nothing" in md, "the report must say it changed nothing"
    assert "none applied" in md
    assert "proposes no cadence" in md


def test_unmeasured_history_is_never_shown_as_zero():
    md = render_maintenance(_maint(
        account_usage={"readable": False, "note": "Insufficient privileges"},
        tables=[_mt(reclustering={"measured": False, "events": None,
                                  "credits": None, "bytes_reclustered": None,
                                  "rows_reclustered": None})]))
    assert "not measured" in md.lower()
    assert "Insufficient privileges" in md
    # The credits column must not read as a real zero.
    assert "| 0 " not in md


def test_capabilities_with_no_equivalent_are_named():
    md = render_maintenance(_maint())
    assert "Fail-safe" in md
    assert "No equivalent" in md or "no equivalent" in md


def test_a_clean_estate_says_so_rather_than_rendering_an_empty_table():
    md = render_maintenance(_maint(tables=[_mt(
        clustered=False, cluster_by="", automatic_clustering=False,
        retention_set_at="inherited", signals=[])], objects_with_signals=0))
    assert "no maintenance" in md.lower() or "nothing" in md.lower()


# --------------------------------------------------------------------------
# The census scope statement must reach the reports that state coverage.
# "7 of 7 objects can move" was true of what was looked at.
# --------------------------------------------------------------------------

_CENSUS = {
    "total": 3, "by_kind": {"PROCEDURE": 2, "TASK": 1},
    "by_language": {"SQL": 1, "JAVASCRIPT": 1}, "by_effort": {"HIGH": 1},
    "kinds": {"PROCEDURE": {"count": 2, "readable": True, "note": "2 found"},
              "TASK": {"count": 1, "readable": True, "note": "1 found"}},
    "objects": [
        {"kind": "PROCEDURE", "source_identifier": "DB.SC.SP_LOAD",
         "detail": "(A VARCHAR)", "migratable": False, "reason": "code",
         "language": "JAVASCRIPT", "effort": "HIGH", "aidp_path": "rewrite"},
        {"kind": "TASK", "source_identifier": "DB.SC.T_NIGHTLY",
         "detail": "state=started", "migratable": False,
         # The real reason from KINDS, so this tests the shipped text.
         "reason": next(k["reason"] for k in KINDS if k["kind"] == "TASK"),
         "language": None, "effort": None, "aidp_path": None}],
    "unreadable": [],
    "scope_statement": "**3 object(s) in this estate cannot be migrated by "
                       "this plugin**: 2 procedure(s), 1 task(s).",
}


def test_planned_objects_carries_the_scope_statement():
    plan = dict(PLAN)
    plan["census"] = _CENSUS
    md = render_planned_objects(plan)
    assert "cannot be migrated by this plugin" in md
    assert "procedure(s)" in md


def test_planned_objects_without_a_census_says_scope_was_not_examined():
    # Silence is the bug being fixed: if the census did not run, the coverage
    # claim must not read as if the whole estate was examined.
    md = render_planned_objects(dict(PLAN))
    assert "tables and views" in md.lower()


def test_the_census_report_lists_objects_with_their_effort():
    md = render_census(_CENSUS)
    assert "DB.SC.SP_LOAD" in md
    assert "JAVASCRIPT" in md
    assert "HIGH" in md
    assert "DB.SC.T_NIGHTLY" in md


def test_the_census_report_says_nothing_is_migratable():
    md = render_census(_CENSUS).lower()
    assert "cannot" in md or "not migrat" in md
    assert "no equivalent is generated" in md or "generates no" in md


def test_a_task_gets_the_cutover_warning():
    md = render_census(_CENSUS)
    assert "stops being populated" in md or "stops being" in md
