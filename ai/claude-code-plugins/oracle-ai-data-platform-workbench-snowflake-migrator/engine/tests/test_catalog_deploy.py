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
            return {"items": [{"key": f"{cat}.{s}"} for s in ("DB",)]}
        if operation in ("list_tables_in", "list_views_in"):
            fields = [{"fieldName": "A", "fieldType": "string"}]
            return {"items": [
                {"key": f'{kw["schema"]}.{n}', "tableFields": fields,
                 "viewFields": fields}
                for n in sorted(self.created)]}
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
    assert kinds[0] == "create_schema"
    assert kinds.count("create_schema") == 1, "one schema, created once"
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
            if operation == "list_tables_in":
                return {"items": [{"key": f'{kw["schema"]}.T0',
                                   "tableFields": [{"fieldName": "DIFFERENT",
                                                    "fieldType": "string"}]}]}
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
            if operation in ("list_tables_in", "list_views_in"):
                return {"items": [{"key": "lake.db.t0",
                                   "tableFields": [{"fieldName": "different",
                                                    "fieldType": "string"}]}]}
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
