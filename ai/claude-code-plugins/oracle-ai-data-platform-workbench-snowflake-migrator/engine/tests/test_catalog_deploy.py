"""Deploy structure through the catalog API. I/O injected as `call`."""
import pytest

from target.catalog_deploy import deploy_catalog
from target.coords import resolve_target

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="lake")


def _plan(n=1, views=0):
    stmts = [{"source_identifier": f"DB.PUBLIC.T{i}", "object_type": "TABLE",
              "target_fqn": f"lake.DB.T{i}", "sql": "CREATE TABLE ...",
              "expected_columns": [{"name": "A", "type": "STRING"}]}
             for i in range(n)]
    stmts += [{"source_identifier": f"DB.PUBLIC.V{i}", "object_type": "VIEW",
               "target_fqn": f"lake.DB.V{i}", "sql": "CREATE VIEW ...",
               "view_text": "select 1 as a",
               "expected_columns": [{"name": "A", "type": "STRING"}]}
              for i in range(views)]
    return {"statements": stmts, "blocked": []}


class Recorder:
    """Injected transport double. Records every operation."""

    def __init__(self, *, exists=(), fail_on=()):
        self.ops: list[tuple] = []
        self.created: set[str] = set(exists)
        self.fail_on = set(fail_on)

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        cat = kw.get("catalog")
        name = kw.get("table") or kw.get("view") or kw.get("schema")
        if operation in ("create_table", "create_view", "create_schema"):
            if name in self.fail_on:
                raise RuntimeError(f"boom on {name}")
            self.created.add(name)
            return {"key": f'{cat}.{kw.get("schema")}.{name}'}
        if operation == "list_schemas":
            # Only reports schemas that have actually been created, so a fresh
            # Recorder starts empty like a real catalog.
            return {"items": [{"key": f"{cat}.{s}", "lifecycleState": "ACTIVE"}
                              for s in ("DB",) if s in self.created]}
        if operation in ("list_tables_in", "list_views_in"):
            # A real list entry carries no fields.
            return {"items": [{"key": f'{kw["schema"]}.{n}'}
                              for n in sorted(self.created)]}
        if operation in ("get_table", "get_view"):
            if name not in self.created:
                raise RuntimeError("404 not found")
            fields = [{"fieldName": "A", "fieldType": "string"}]
            return {"key": name, "tableFields": fields, "viewFields": fields}
        raise AssertionError(f"unexpected operation {operation}")


def test_dry_run_calls_nothing():
    rec = Recorder()
    out = deploy_catalog(_plan(2), target=TARGET, execute=False, call=rec, retry_delays=(), verify_delays=())
    assert rec.ops == []
    assert out["dry_run"] is True
    assert out["statement_count"] == 2


def test_execute_creates_the_schema_before_its_tables():
    rec = Recorder()
    deploy_catalog(_plan(2), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    kinds = [o[0] for o in rec.ops]
    # It LOOKS first -- re-POSTing an existing schema drops the table creates
    # that follow -- then creates it once if absent.
    assert kinds[0] == "list_schemas"
    assert kinds.count("create_schema") == 1, "one schema, created once"
    assert kinds.index("create_schema") < kinds.index("create_table")
    assert kinds.count("create_table") == 2


def test_views_are_created_after_tables():
    rec = Recorder()
    deploy_catalog(_plan(2, views=1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    kinds = [o[0] for o in rec.ops]
    assert kinds.index("create_view") > max(
        i for i, k in enumerate(kinds) if k == "create_table")


def test_each_object_is_verified_by_reading_it_back():
    rec = Recorder()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    # Resolved by LISTING, not by GET-on-a-guessed-key: the server's key case
    # is not the one we asked for.
    assert "list_tables_in" in [o[0] for o in rec.ops]
    assert out["verified_targets"] == ["DB.PUBLIC.T0"]
    assert out["verified"] == 1


def test_a_structure_mismatch_on_read_back_is_not_verified():
    class Mismatch(Recorder):
        def __call__(self, operation, **kw):
            out = super().__call__(operation, **kw)
            if operation == "get_table":
                return {"key": "T0",
                        "tableFields": [{"fieldName": "DIFFERENT",
                                         "fieldType": "string"}]}
            return out

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Mismatch(), retry_delays=(), verify_delays=())
    assert out["verified_targets"] == []
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]
    assert "DIFFERENT" in out["mismatches"][0]["reason"]


def test_a_create_failure_does_not_abort_the_rest():
    rec = Recorder(fail_on={"T0"})
    out = deploy_catalog(_plan(3), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    assert out["verified"] == 2
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]
    assert out["errors"], "the failure must be recorded, not swallowed"


def test_an_existing_object_is_reported_not_replaced():
    # The create fails because it already exists; the read-back still matches
    # the plan, so it counts as verified. An object that already matches is
    # indistinguishable from one we made.
    rec = Recorder(exists={"T0"}, fail_on={"T0"})
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    # Create failed because it exists; the read-back still matches the plan.
    assert out["verified_targets"] == ["DB.PUBLIC.T0"]
    assert any("already" in n.lower() or "boom" in n.lower()
               for n in out["errors"])


def test_execute_without_a_target_is_refused():
    with pytest.raises(Exception):
        deploy_catalog(_plan(1), target=None, execute=True, call=Recorder(), retry_delays=(), verify_delays=())


def test_out_of_scope_catalogs_are_not_touched():
    plan = _plan(1)
    plan["statements"].append(
        {"source_identifier": "DB2.PUBLIC.X", "object_type": "TABLE",
         "target_fqn": "other_catalog.DB2.X", "sql": "",
         "expected_columns": [{"name": "A", "type": "STRING"}]})
    rec = Recorder()
    out = deploy_catalog(plan, target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    assert out["out_of_scope_count"] == 1
    assert all(o[1].get("catalog") == "lake" for o in rec.ops)


def test_no_row_data_is_ever_sent():
    rec = Recorder()
    deploy_catalog(_plan(2, views=1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    blob = repr(rec.ops).upper()
    for banned in ("INSERT", "VALUES ", "COPY ", "MERGE "):
        assert banned not in blob


# ==========================================================================
# AIDP LOWER-CASES IDENTIFIERS. Verified live: a schema created as
# "TEST_DB_20260908_1529" comes back as "lake.test_db_20260908_1529", so every
# schemaKey and read-back key built from the REQUESTED case was wrong and all
# seven objects reported failed.
#
# The fix is to discover the case rather than assume it.
# ==========================================================================

class Folding:
    """Transport double that lower-cases names, as the real API does."""

    def __init__(self, *, conflict_times=0):
        self.ops: list[tuple] = []
        self.schemas: dict[str, str] = {}
        self.tables: dict[str, dict] = {}
        self.conflict_times = conflict_times

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        cat = kw.get("catalog")
        if operation == "create_schema":
            key = f'{cat}.{kw["schema"]}'.lower()
            self.schemas[key] = key
            return {"key": key}
        if operation == "list_schemas":
            return {"items": [{"key": k} for k in self.schemas]}
        if operation in ("create_table", "create_view"):
            if self.conflict_times > 0:
                self.conflict_times -= 1
                raise RuntimeError("backend returned 409 Conflict: ongoing operation")
            name = kw.get("table") or kw.get("view")
            key = f'{cat}.{kw["schema"]}.{name}'.lower()
            fields = kw["body"].get("tableFields") or kw["body"].get("viewFields")
            self.tables[key] = {"key": key, "tableFields": fields,
                                "viewFields": fields}
            return {"key": key}
        if operation in ("list_tables_in", "list_views_in"):
            return {"items": [dict(v) for k, v in self.tables.items()
                              if k.startswith(kw["schema"].lower() + ".")]}
        if operation in ("get_table", "get_view"):
            name = kw.get("table") or kw.get("view")
            key = f'{cat}.{kw["schema"]}.{name}'.lower()
            full = self.tables.get(key)
            if full is None:
                raise RuntimeError("404 not found")
            return dict(full)
        raise AssertionError(f"unexpected op {operation}")


def test_the_schema_key_is_resolved_from_the_server_not_assumed():
    fold = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    # Every table body must carry the key the SERVER reported, lower-cased.
    creates = [kw for op, kw in fold.ops if op == "create_table"]
    assert creates[0]["body"]["schemaKey"] == "lake.db"
    assert out["resolved_schema_keys"] == {"lake.DB": "lake.db"}


def test_objects_are_verified_despite_the_case_change():
    fold = Folding()
    out = deploy_catalog(_plan(2), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 2, out["failed"] + out["mismatches"]
    assert out["failed_targets"] == []


def test_a_view_is_verified_despite_the_case_change():
    fold = Folding()
    out = deploy_catalog(_plan(1, views=1), target=TARGET, execute=True,
                         call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 2


def test_field_names_are_compared_case_insensitively():
    # The server lower-cases field names too; that is a case fold, not a
    # structure mismatch.
    fold = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == []


def test_a_409_conflict_is_retried_rather_than_reported_as_failure():
    # Schema creation is asynchronous, so the first table create can land
    # while the schema is still settling.
    fold = Folding(conflict_times=2)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold,
                         retry_delays=(0, 0, 0))
    assert out["verified"] == 1
    assert any("409" in e for e in out["errors"]), "the retry must be visible"


def test_retries_are_bounded_and_the_failure_is_reported():
    fold = Folding(conflict_times=99)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold,
                         retry_delays=(0, 0), verify_delays=())
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]


def test_a_genuine_structure_difference_is_still_a_mismatch():
    class Wrong(Folding):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                return {"key": "t0",
                        "tableFields": [{"fieldName": "different",
                                         "fieldType": "string"}]}
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Wrong(), retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]


# ==========================================================================
# Creation is ASYNCHRONOUS. Verified live: POST returns 202 Accepted with an
# empty body, and the object appears seconds later -- or never, if the async
# work fails, which it does silently. So the read-back must POLL, and a
# never-appearing object is a failure with a real explanation.
# ==========================================================================

class Delayed(Folding):
    """Appears only after `appear_after` list calls, as an async create does."""

    def __init__(self, *, appear_after=2, never=False):
        super().__init__()
        self.list_calls = 0
        self.appear_after = appear_after
        self.never = never

    def __call__(self, operation, **kw):
        if operation in ("list_tables_in", "list_views_in"):
            self.list_calls += 1
            if self.never or self.list_calls <= self.appear_after:
                self.ops.append((operation, kw))
                return {"items": []}
        return super().__call__(operation, **kw)


def test_the_read_back_polls_until_the_object_appears():
    d = Delayed(appear_after=2)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                         verify_delays=(0, 0, 0))
    assert out["verified"] == 1, out["failed"]
    assert d.list_calls >= 3, "it must have polled, not asked once"


def test_an_object_that_never_appears_is_a_failure_with_a_reason():
    d = Delayed(never=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                         verify_delays=(0, 0))
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]
    reason = out["failed"][0]["reason"].lower()
    assert "202" in reason or "async" in reason or "accepted" in reason


def test_polling_is_bounded():
    d = Delayed(never=True)
    deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                   verify_delays=(0, 0))
    assert d.list_calls <= 4, "polling must not loop forever"


def test_a_blocked_column_type_is_reported_before_any_create():
    # timestamp_ntz is silently rejected by the API, so it must be caught
    # here rather than becoming an accepted-then-vanished table.
    plan = {"statements": [
        {"source_identifier": "DB.PUBLIC.T", "object_type": "TABLE",
         "target_fqn": "lake.DB.T", "sql": "",
         "expected_columns": [{"name": "TS", "type": "TIMESTAMP_NTZ"}]}],
        "blocked": []}
    fold = Folding()
    out = deploy_catalog(plan, target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T"]
    assert "timestamp_ntz" in out["failed"][0]["reason"].lower()
    assert not any(o[0] == "create_table" for o in fold.ops), \
        "an unbuildable body must never be POSTed"


# ==========================================================================
# Do not re-create a schema that already exists.
#
# Verified live: POSTing a schema that is already there re-triggers async
# work, and table creates issued during that window are ACCEPTED (202) and
# then silently dropped -- six tables returned 202 and none appeared, while
# the identical bodies posted against a settled schema all landed.
#
# So: resolve first, create only if absent, and wait for ACTIVE.
# ==========================================================================

class Settled(Folding):
    """Schema already exists and is ACTIVE."""

    def __init__(self, state="ACTIVE"):
        super().__init__()
        self.schemas["lake.db"] = "lake.db"
        self.state = state

    def __call__(self, operation, **kw):
        if operation == "list_schemas":
            self.ops.append((operation, kw))
            return {"items": [{"key": "lake.db", "lifecycleState": self.state}]}
        return super().__call__(operation, **kw)


def test_an_existing_schema_is_not_re_created():
    s = Settled()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=())
    assert not any(op == "create_schema" for op, _ in s.ops), \
        "re-POSTing an existing schema drops the table creates that follow"
    assert out["schemas_created"] == []
    assert out["schemas_reused"] == ["lake.db"]
    assert out["verified"] == 1


def test_a_missing_schema_is_still_created():
    f = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=f,
                         retry_delays=(), verify_delays=())
    assert any(op == "create_schema" for op, _ in f.ops)
    assert out["schemas_created"] == ["lake.DB"]


def test_a_schema_that_is_not_active_is_waited_for():
    s = Settled(state="CREATING")
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=(),
                         schema_wait=(0, 0))
    # It never became ACTIVE, so this is reported rather than pushed through.
    assert any("ACTIVE" in e or "active" in e for e in out["errors"])


# ==========================================================================
# The LIST response omits tableFields; GET-by-key includes them.
#
# Verified live: six tables were created correctly as managed DELTA with the
# right field types, and the plugin called all six a MISMATCH because it
# compared the plan against a list entry that carries no fields at all.
# Existence comes from the list (which is how the real key case is found);
# STRUCTURE has to come from a GET on that key.
# ==========================================================================

class Summarised(Folding):
    """List returns summaries with no fields; GET returns the full object."""

    def __call__(self, operation, **kw):
        if operation in ("list_tables_in", "list_views_in"):
            self.ops.append((operation, kw))
            return {"items": [{"key": k} for k in self.tables]}   # no fields
        return super().__call__(operation, **kw)


def test_structure_is_read_with_a_get_not_from_the_list():
    s = Summarised()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=())
    assert out["verified"] == 1, out["mismatches"] + out["failed"]
    assert any(op == "get_table" for op, _ in s.ops), \
        "the list carries no fields, so structure needs a GET"


def test_a_get_that_fails_leaves_the_structure_unverified_not_mismatched():
    class NoGet(Summarised):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                raise RuntimeError("DESCRIBE unavailable")
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=NoGet(),
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == []
    assert out["unverified_structure_targets"] == ["DB.PUBLIC.T0"]


def test_a_full_type_string_from_the_server_still_compares_equal():
    # The server returns fieldType "decimal(38,0)" AND fieldPrecision 38;
    # the plan says fieldType "decimal" with precision "38". Same type.
    class FullType(Summarised):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                self.ops.append((operation, kw))
                return {"key": "x", "tableFields": [
                    {"fieldName": "a", "fieldType": "string",
                     "fieldPrecision": 20, "fieldScale": 0}]}
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=FullType(),
                         retry_delays=(), verify_delays=())
    assert out["verified"] == 1, out["mismatches"]


# ==========================================================================
# A VIEW's column types are DERIVED by the target engine, not declared by us.
#
# Verified live: RAPPI_ORDER_360_VW was created correctly with all 17 columns,
# and three aggregate columns came back with different types --
#   ITEM_COUNT           decimal(18,0) -> bigint
#   TOTAL_ITEM_QUANTITY  decimal(22,0) -> decimal(20,0)
#   ITEM_TOTAL_AMOUNT    decimal(30,2) -> decimal(28,2)
# because Snowflake reports a view's DECLARED output types while AIDP
# re-derives them from the SQL.
#
# That is a fidelity finding worth reporting loudly -- decimal(22,0) to
# decimal(20,0) is a narrowing -- but it is NOT "someone else's object that we
# left alone", which is what a table mismatch means. The two must not be
# reported as the same thing.
# ==========================================================================

def _view_plan(found_types):
    return {"statements": [
        {"source_identifier": "DB.PUBLIC.V", "object_type": "VIEW",
         "target_fqn": "lake.DB.V", "sql": "", "view_text": "select 1 as a",
         "expected_columns": [{"name": "N", "type": "DECIMAL(22,0)"}]}],
        "blocked": []}, found_types


class DerivedTypes(Folding):
    def __init__(self, found):
        super().__init__()
        self.found = found

    def __call__(self, operation, **kw):
        if operation in ("get_table", "get_view"):
            self.ops.append((operation, kw))
            return {"key": "v", "viewFields": self.found,
                    "tableFields": self.found}
        return super().__call__(operation, **kw)


def test_a_view_whose_types_were_re_derived_is_not_called_uncloned():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == [], \
        "the view WAS created; it is not someone else's object"
    assert out["derived_type_drift_targets"] == ["DB.PUBLIC.V"]
    drift = out["derived_type_drift"][0]
    assert "derive" in drift["reason"].lower()
    assert "NOT been cloned" not in drift["reason"]


def test_the_drift_names_the_columns_and_both_types():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["derived_type_drift"][0]["reason"]
    assert "N" in reason and "DECIMAL(22,0)" in reason.upper()
    assert "DECIMAL(20,0)" in reason.upper()


def test_a_narrowing_is_called_out_as_an_overflow_risk():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert "narrow" in out["derived_type_drift"][0]["reason"].lower()


def test_a_view_with_missing_or_extra_columns_is_still_a_mismatch():
    # Column DRIFT is derivation. A different column LIST is not.
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "somethingelse",
                          "fieldType": "decimal(22,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.V"]
    assert out["derived_type_drift_targets"] == []


def test_a_table_type_difference_is_still_a_hard_mismatch():
    # Tables are created FROM our field list, so a type difference there means
    # the object is not ours.
    call = DerivedTypes([{"fieldName": "A", "fieldType": "int"}])
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]
    assert out["derived_type_drift_targets"] == []


# ==========================================================================
# Tell the user WHY nothing appeared (P1).
#
# A failed async create permanently poisons that name in that schema: every
# later create returns 202 and is silently dropped, and DELETE does not
# recover it. The signature is distinguishable -- a NOVEL name in the same
# schema succeeds -- so the plugin probes once and says which situation the
# user is in, instead of leaving them hunting a body problem that is not
# there.
# ==========================================================================

class NeverAppears(Folding):
    """Creates are accepted; the planned name never shows up.

    `probe_works` decides whether a novel name in the same schema succeeds,
    which is exactly what separates a poisoned name from a broken request.
    """

    def __init__(self, *, probe_works=True):
        super().__init__()
        self.probe_works = probe_works

    def __call__(self, operation, **kw):
        if operation == "create_table":
            name = kw.get("table") or ""
            self.ops.append((operation, kw))
            if name.startswith("snowmig_probe_") and self.probe_works:
                key = f'{kw["catalog"]}.{kw["schema"]}.{name}'.lower()
                # The real API returns the FULL key, not a bare name.
                self.tables[key] = {"key": key, "tableFields": []}
            return {}
        return super().__call__(operation, **kw)


def test_a_poisoned_name_is_named_as_such():
    call = NeverAppears(probe_works=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "poison" in reason.lower() or "burned" in reason.lower()
    assert "fresh schema" in reason.lower() or "new schema" in reason.lower()
    assert out["poisoned_names"] == ["lake.DB.T0"]


def test_when_a_novel_name_also_fails_the_diagnosis_is_different():
    call = NeverAppears(probe_works=False)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "poison" not in reason.lower()
    assert out["poisoned_names"] == []
    # A novel name failing too points at the request or the permissions.
    assert "request" in reason.lower() or "permission" in reason.lower()


def test_the_probe_uses_a_unique_name_and_is_cleaned_up():
    call = NeverAppears(probe_works=True)
    deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                   retry_delays=(), verify_delays=())
    probes = [kw.get("table") for op, kw in call.ops
              if op == "create_table" and str(kw.get("table", "")).startswith(
                  "snowmig_probe_")]
    assert len(probes) == 1
    assert len(set(probes)) == 1
    # It must delete what it made -- and a fixed name would itself get burned.
    assert any(op == "delete_table" for op, _ in call.ops)


def test_the_probe_runs_once_per_schema_not_once_per_object():
    call = NeverAppears(probe_works=True)
    deploy_catalog(_plan(4), target=TARGET, execute=True, call=call,
                   retry_delays=(), verify_delays=())
    probes = [kw for op, kw in call.ops
              if op == "create_table" and str(kw.get("table", "")).startswith(
                  "snowmig_probe_")]
    assert len(probes) == 1, "one diagnosis per schema is enough"
    assert len(call.ops) < 40, "the diagnosis must not multiply the work"


def test_the_probe_can_be_disabled():
    call = NeverAppears(probe_works=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=(), diagnose=False)
    assert not any(str(kw.get("table", "")).startswith("snowmig_probe_")
                   for _, kw in call.ops)
    assert out["poisoned_names"] == []


def test_a_successful_run_never_probes():
    f = Folding()
    deploy_catalog(_plan(2), target=TARGET, execute=True, call=f,
                   retry_delays=(), verify_delays=())
    assert not any(str(kw.get("table", "")).startswith("snowmig_probe_")
                   for _, kw in f.ops), "nothing failed, so nothing to diagnose"
