"""Provisioning: workspace, cluster, libraries, scripts folder, jobs.

The transport is injected, so every decision — look-first, name translation,
poll-the-read-back, record-don't-swallow — is tested with no environment.
"""
import json
import pathlib

import pytest

from target.provision_api import (
    ProvisionBackendUnsupported, build_driver_notebook, build_job_body,
    build_library_items, build_provision_command, content_path,
)
from target.stage_notebooks import STAGES, build_stage_notebook
from target.provisioning import (
    BACKUP_FOLDER, JOB_SPECS, PLAN_FOLDER, REPORTS_FOLDER, SCRIPTS_FOLDER,
    provision, render_provision,
)

OCID = "ocid1.aidataplatform.oc1.iad.a"


class Fake:
    """Provisioning transport double. Starts empty, like a fresh tenancy."""

    def __init__(self, *, workspaces=(), clusters=(), jobs=(), fail=(),
                 appear_after=0):
        self.ops: list[tuple] = []
        self.workspaces = [{"displayName": w, "key": f"ws-{w}"}
                           for w in workspaces]
        self.clusters = [{"displayName": c, "key": f"cl-{c}"}
                         for c in clusters]
        self.jobs = [{"name": j, "key": f"job-{j}"} for j in jobs]
        self.contents: dict[str, dict] = {}
        self.fail = set(fail)
        self.appear_after = appear_after
        self._ws_lists = 0

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation in self.fail:
            raise RuntimeError(f"denied: {operation}")
        if operation == "list_workspaces":
            self._ws_lists += 1
            if self._ws_lists <= self.appear_after:
                return {"items": []}
            return {"items": list(self.workspaces)}
        if operation == "create_workspace":
            name = kw["body"]["displayName"]
            self.workspaces.append({"displayName": name, "key": f"ws-{name}"})
            return {}
        if operation == "list_clusters":
            return {"items": list(self.clusters)}
        if operation == "create_cluster":
            name = kw["body"]["displayName"]
            self.clusters.append({"displayName": name, "key": f"cl-{name}"})
            return {}
        if operation in ("install_libraries", "restart_cluster"):
            return {}
        if operation == "list_libraries":
            return {"items": []}
        if operation == "create_ws_folder":
            self.contents[kw["path"]] = {"type": "FOLDER"}
            return {}
        if operation == "upload_ws_file":
            # Capture the BYTES at upload time. The provisioner writes each
            # notebook to a temp file and unlinks it immediately after, so a
            # double that only remembers the path has nothing to read later.
            try:
                body = pathlib.Path(kw["local_path"]).read_text(encoding="utf-8")
            except OSError:
                body = None
            self.contents[kw["path"]] = {"type": "FILE",
                                         "local": kw["local_path"],
                                         "body": body}
            return {}
        if operation == "list_ws_objects":
            prefix = kw["path"] + "/"
            return {"items": [{"path": k, "displayName": k.rsplit("/", 1)[-1]}
                              for k in self.contents
                              if k.startswith(prefix)]}
        if operation == "list_jobs":
            return {"items": list(self.jobs)}
        if operation == "create_job":
            self.jobs.append({"name": kw["body"]["name"],
                              "key": f'job-{kw["body"]["name"]}'})
            return {}
        raise AssertionError(f"unexpected op {operation}")


@pytest.fixture()
def scripts(tmp_path):
    out = []
    for name in ("00_discover_snowflake.py", "01_create_structure.py",
                 "02_copy_schema.py", "03_reconcile.py"):
        path = tmp_path / name
        path.write_text("# script body\n", encoding="utf-8")
        out.append(path)
    return out


def test_a_dry_run_calls_nothing_and_plans_everything(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="Acme PROD", scripts=scripts,
                    execute=False)
    assert fake.ops == []
    assert res["dry_run"] is True
    kinds = {s["step"] for s in res["steps"]}
    assert {"workspace", "cluster", "upload", "job"} <= kinds


def test_the_workspace_name_is_translated_before_any_create(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="Acme PROD — Migração",
                    scripts=scripts, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_workspace"]
    assert created == ["acme_prod_migracao"]
    assert res["workspace"]["renamed"] is True
    assert res["workspace"]["notes"], "a silent rename is not attributable"


def test_an_existing_workspace_is_refused_not_adopted(scripts):
    """A migration creates its own environment, so its blast radius is known.

    Adopting a workspace somebody else made means the migration's objects sit
    among strangers' and cannot be torn down as a unit. A taken name is a
    collision for the user to resolve, never a shortcut.
    """
    fake = Fake(workspaces=("acme",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    ops = [op for op, _ in fake.ops]
    assert "create_workspace" not in ops
    assert any(s["step"] == "workspace" and s["action"] == "name_taken"
               for s in res["steps"])
    assert any(s["step"] == "halt" for s in res["steps"])
    # Nothing downstream was attempted.
    assert "create_cluster" not in ops
    assert not any(s["step"] == "job" for s in res["steps"])


def test_reuse_existing_opts_back_in_explicitly(scripts):
    """The escape hatch exists, but it has to be asked for by name."""
    fake = Fake(workspaces=("acme",), clusters=("migration_assets",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True)
    ops = [op for op, _ in fake.ops]
    assert "create_workspace" not in ops
    assert "create_cluster" not in ops
    assert any(s["step"] == "workspace" and s["action"] == "reused"
               for s in res["steps"])
    assert any(s["step"] == "job" for s in res["steps"])


def test_a_created_workspace_is_polled_until_visible(scripts):
    fake = Fake(appear_after=2)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0, 0))
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created"
    assert ws["verified"] is True


def test_a_workspace_that_never_appears_halts_the_run(scripts):
    class NeverVisible(Fake):
        def __call__(self, operation, **kw):
            if operation == "list_workspaces":
                self.ops.append((operation, kw))
                return {"items": []}
            return super().__call__(operation, **kw)

    fake = NeverVisible()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0,))
    assert any(s["step"] == "halt" for s in res["steps"])
    assert not any(op == "create_cluster" for op, _ in fake.ops), \
        "nothing else may be attempted against a workspace that is not there"


def test_stage_notebooks_are_uploaded_and_read_back(scripts):
    # The data plane ships as `.ipynb` ONLY. AIDP types a workspace object by
    # extension -- a `.py` uploaded with --type NOTEBOOK is stored as a FILE
    # -- and a job task needs a NOTEBOOK, so a `.py` on the workspace could
    # never be run as a job.
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert len(notebooks) == len(JOB_SPECS)
    assert all(s["verified"] is True for s in notebooks)
    assert f"{SCRIPTS_FOLDER}/00_discover_snowflake.ipynb" in fake.contents
    assert not any(k.endswith(".py") for k in fake.contents), \
        "no .py may reach the workspace: it would be stored as a FILE"


def test_a_failed_upload_is_recorded_and_the_run_continues(scripts):
    fake = Fake(
                fail={"upload_ws_file"})
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert notebooks, "a failed upload must not silently abandon the rest"
    # A job without its notebook would be born broken, so when the upload
    # fails the job is deliberately NOT created — and both facts are on the
    # record.
    assert all(s["verified"] is False for s in notebooks)
    assert not any(s["step"] == "job" for s in res["steps"])


def test_jobs_point_straight_at_the_stage_notebook(scripts):
    # No driver wrapper: the job runs the stage notebook itself, so the code
    # a user opens in the console is the code that runs.
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=(), external_catalog="snowflake_ext",
              target_catalog="lake")
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_job"]
    assert {b["name"] for b in bodies} == {s["name"] for s in JOB_SPECS}
    copy = next(b for b in bodies if b["name"] == "snowmig_02_copy_schema")
    task = copy["tasks"][0]
    assert task["type"] == "NOTEBOOK_TASK"
    assert task["notebookPath"] == f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"
    assert task["source"] == "WORKSPACE"
    assert task["cluster"] == {"clusterKey": "cl-migration_assets"}
    uploaded = [kw for op, kw in fake.ops
                if op == "upload_ws_file"
                and kw.get("object_type") == "NOTEBOOK"]
    assert len(uploaded) == len(JOB_SPECS)


def test_this_run_s_coordinates_are_written_into_the_params_cell(scripts):
    # Job `parameters` reach a notebook neither as argv nor as env (probed
    # live), so the coordinates have to be IN the notebook.
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=(), external_catalog="snowflake_ext",
              target_catalog="lake")
    nb = json.loads(
        fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"])
    params = "".join(nb["cells"][1]["source"])
    assert "'target-catalog': 'lake'" in params
    assert "'source-catalog': 'snowflake_ext'" in params


def test_a_stage_notebook_turns_sys_exit_into_a_real_verdict():
    # main() RETURNS a code; a cell that raises SystemExit is reported as a
    # FAILED task even when the work succeeded -- live, a fully successful
    # discovery came back failed for exactly that reason.
    nb = build_stage_notebook(STAGES[0])
    run = "".join(nb["cells"][-1]["source"])
    assert "code = main(ARGV)" in run
    assert "raise RuntimeError" in run, \
        "a non-zero exit must still fail the job"
    body = "".join(nb["cells"][-2]["source"])
    assert "__main__" not in body, \
        "the main guard must be stripped, or SystemExit escapes"


def test_an_existing_job_is_reused(scripts):
    fake = Fake(
                jobs=("snowmig_00_discover",))
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=())
    created = [kw["body"]["name"] for op, kw in fake.ops
               if op == "create_job"]
    assert "snowmig_00_discover" not in created


def test_libraries_come_from_the_requirements_file(scripts, tmp_path):
    req = tmp_path / "requirements-aidp.txt"
    req.write_text("# comment\nsnowflake-connector-python>=4.7.0\n", encoding="utf-8")
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              requirements=req, execute=True, delays=())
    ops = [op for op, _ in fake.ops]
    assert "install_libraries" in ops
    assert "restart_cluster" in ops, "the doc requires a restart"
    body = next(kw["body"] for op, kw in fake.ops
                if op == "install_libraries")
    assert body["items"][0]["package"].startswith("snowflake-connector")


def test_an_all_comments_requirements_file_installs_nothing(scripts, tmp_path):
    req = tmp_path / "requirements-aidp.txt"
    req.write_text("# nothing enabled\n", encoding="utf-8")
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              requirements=req, execute=True, delays=())
    assert not any(op == "install_libraries" for op, _ in fake.ops)


def test_the_report_carries_the_unverified_warning():
    res = provision(call=None, workspace_name="acme", scripts=[],
                    execute=False)
    md = render_provision(res)
    assert "DRY RUN" in md
    assert "not yet live-verified" in md


# --- provision_api ---------------------------------------------------------

def test_commands_use_the_documented_api_family():
    cmd = build_provision_command("oci_raw", "list_workspaces", OCID)
    uri = cmd[cmd.index("--target-uri") + 1]
    assert "/20260430/aiDataPlatforms/" in uri
    assert "dataLakes" not in uri


def test_the_aidp_cli_backend_is_refused_not_guessed():
    with pytest.raises(ProvisionBackendUnsupported):
        build_provision_command("aidp_cli", "list_workspaces", OCID)


def test_a_file_upload_goes_through_the_validated_cli_surface():
    # workspace-object create with @local-path: the live-verified upload.
    cmd = build_provision_command(
        "oci_raw", "upload_ws_file", OCID, workspace="ws",
        path="backup-snowflake-migration/scripts/x.py", local_path="/tmp/x.py")
    assert cmd[:3] == ["aidp", "workspace-object", "create"]
    assert "@/tmp/x.py" in cmd and "--is-overwrite" in cmd
    assert not any(a.startswith("/backup") for a in cmd), \
        "workspace paths are relative; a leading slash is a live 400"


def test_library_items_refuse_an_empty_change():
    with pytest.raises(ValueError):
        build_library_items()


def test_job_bodies_match_the_live_verified_task_shape():
    body = build_job_body("j", notebook_path="s/run_j.ipynb", cluster_key="cl")
    assert "schedule" not in body, "migrations are driven runs, not crons"
    assert body["maxConcurrentRuns"] == 1
    task = body["tasks"][0]
    # Live-verified: runIf and a per-task cluster are required; NOTEBOOK_TASK
    # takes notebookPath + source, and is the shape that actually RAN
    # (PYTHON_TASK failed file resolution on the validated build).
    assert task["type"] == "NOTEBOOK_TASK"
    assert task["notebookPath"] == "s/run_j.ipynb"
    assert task["source"] == "WORKSPACE"
    assert task["runIf"] == "ALL_SUCCESS"
    assert task["cluster"] == {"clusterKey": "cl"}


def test_content_paths_are_absolute_and_clean():
    assert content_path("a/", "/b", "c.py") == "/a/b/c.py"


# --------------------------------------------------------------------------
# The driver notebook is the only thing an AIDP job actually runs, so its
# contract is load-bearing: it must put the scripts' own folder on sys.path
# (they import a shared module from there) and read off the /Workspace mount.
# --------------------------------------------------------------------------

def test_the_driver_notebook_makes_the_shared_module_importable():
    src = "".join(build_driver_notebook(
        "backup-snowflake-migration/scripts/00_discover_snowflake.py",
        {})["cells"][0]["source"])
    assert "sys.path.insert(0, os.path.dirname(SCRIPT))" in src, \
        "the scripts import snowmig_source from beside themselves"
    assert src.count("/Workspace/") >= 1, \
        "the workspace tree is mounted at /Workspace on the cluster"


# --------------------------------------------------------------------------
# One AIDP compute cluster per Snowflake warehouse, named after it.
#
# The requirement was explicit: same names, AIDP DEFAULT config, no sizing
# decision now. So the warehouse's size is reported and deliberately not
# translated -- a Snowflake size is not a Spark shape, and COMPUTE_PROPOSAL.md
# keeps that a decision rather than a silent default.
# --------------------------------------------------------------------------

_WAREHOUSES = [{"name": "WH_ETL", "size": "Medium"},
               {"name": "WH BI Team", "size": "X-Small"}]


def test_a_cluster_is_created_for_each_warehouse_with_its_own_name(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_cluster"
               and kw["body"]["displayName"] != "migration_assets"]
    # The awkward name is translated, and the translation is reported.
    assert created == ["wh_etl", "wh_bi_team"]
    mirrored = {t["warehouse"]: t for t in res["warehouse_clusters"]}
    assert mirrored["WH BI Team"]["name"] == "wh_bi_team"
    assert mirrored["WH BI Team"]["renamed"] is True
    assert mirrored["WH BI Team"]["notes"]


def test_the_warehouse_size_is_reported_and_not_translated(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_cluster"]
    # Every mirrored cluster gets the SAME default shape: no sizing is
    # inferred from the warehouse.
    assert len({json.dumps(b["driverConfig"], sort_keys=True)
                for b in bodies}) == 1
    assert res["warehouse_clusters"][0]["source_size"] == "Medium"
    md = render_provision(res)
    assert "Medium" in md
    assert "NOT carried over" in md, \
        "the report must say the sizing decision was left open"


def test_an_existing_warehouse_cluster_is_not_adopted(scripts):
    fake = Fake(clusters=("wh_etl",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_cluster"
               and kw["body"]["displayName"] != "migration_assets"]
    assert created == ["wh_bi_team"], "the existing one is left alone"
    taken = [s for s in res["steps"]
             if s["step"] == "warehouse-cluster"
             and s["action"] == "name_taken"]
    assert len(taken) == 1, "the collision is reported, not silently adopted"
    assert taken[0]["verified"] is False
    assert "not this migration's" in taken[0]["detail"]


def test_one_failed_warehouse_cluster_does_not_stop_the_others(scripts):
    class OneFails(Fake):
        def __call__(self, operation, **kw):
            if operation == "create_cluster" and \
                    kw["body"]["displayName"] == "wh_etl":
                self.ops.append((operation, kw))
                raise RuntimeError("quota exceeded")
            return super().__call__(operation, **kw)

    fake = OneFails()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    steps = {s["detail"].split(" -> ")[0]: s for s in res["steps"]
             if s["step"] == "warehouse-cluster"}
    assert steps["WH_ETL"]["verified"] is False
    assert "quota exceeded" in steps["WH_ETL"]["detail"]
    assert steps["WH BI Team"]["verified"] is True
    # And the migration's own jobs are unaffected: these are the customer's
    # compute, not the migration's.
    assert any(s["step"] == "job" for s in res["steps"])


def test_mirroring_never_touches_the_migration_cluster_binding(scripts):
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_job"]
    assert {b["tasks"][0]["cluster"]["clusterKey"] for b in bodies} == \
        {"cl-migration_assets"}


def test_a_dry_run_lists_the_warehouse_clusters_it_would_create(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=False)
    assert fake.ops == []
    planned = [s for s in res["steps"] if s["step"] == "warehouse-cluster"]
    assert len(planned) == 2
    assert all(s["verified"] is None for s in planned)
    assert "NOT carried over" in planned[0]["detail"]


def test_a_nameless_warehouse_entry_is_skipped_not_guessed(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=[{"size": "Medium"}], execute=True,
                    delays=())
    assert res["warehouse_clusters"] == []
    assert not any(op == "create_cluster"
                   and kw["body"]["displayName"] != "migration_assets"
                   for op, kw in fake.ops)


def test_the_cluster_body_carries_both_driver_and_worker_config():
    """Live-enumerated by the API: a cluster needs driverConfig AND
    workerConfig (shape + min/max worker count), and the shape is an AIDP
    compute family -- an OCI VM shape like VM.Standard.E4.Flex is rejected."""
    from target.provision_api import DEFAULT_SHAPE, build_cluster_body
    body = build_cluster_body("wh_etl")
    assert body["driverConfig"]["driverShape"] == DEFAULT_SHAPE
    worker = body["workerConfig"]
    assert worker["workerShape"] == DEFAULT_SHAPE
    assert worker["minWorkerCount"] >= 1
    assert worker["maxWorkerCount"] >= worker["minWorkerCount"]
    assert not DEFAULT_SHAPE.startswith("VM."), \
        "an OCI VM shape is not an AIDP compute shape"
    # Also required, and also learned from a 400: the RUNTIME version.
    assert body["clusterRuntimeConfig"]["sparkVersion"].startswith("3.5")


def test_the_dry_run_previews_notebooks_not_their_python_sources():
    """AIDP types a workspace object by extension: `.py` lands as a FILE and no
    job can run it, so execute uploads a generated `.ipynb` per stage and never
    the `engine/dataplane/` sources. The dry run used to list those sources,
    advertising an upload that never happens."""
    out = provision(call=None, workspace_name="ws",
                    scripts=[pathlib.Path("00_discover_snowflake.py")],
                    execute=False)
    uploads = [s["detail"] for s in out["steps"] if s["step"] == "upload"]
    assert uploads, "the dry run must preview the stage uploads"
    assert all(".ipynb" in d for d in uploads), uploads
    assert not any(d.endswith(".py") for d in uploads), uploads


def test_provisioning_creates_the_backup_folder_the_runbook_writes_into(scripts):
    """S6 backs the manifest up before any stage reads it, and S9 backs the full
    plan up before scope is reduced. Only scripts/, plan/ and reports/ were
    created, so the first backup had nowhere to land."""
    fake = Fake()
    provision(call=fake, workspace_name="ws", scripts=scripts, execute=True,
              delays=())
    folders = [kw["path"] for op, kw in fake.ops if op == "create_ws_folder"]
    assert BACKUP_FOLDER in folders, folders


# --- pagination on the provisioning transport -------------------------------
#
# `oci raw-request` surfaces `opc-next-page` under `headers`; the transport
# used to drop it, so jobs.in_flight_runs read one page of jobRuns and could
# miss the very run it exists to guard against.

def _paged_proc(pages):
    """`pages`: page token (None first) -> (items, next token)."""
    import types
    asked = []

    def fake(cmd):
        uri = cmd[cmd.index("--target-uri") + 1] if "--target-uri" in cmd \
            else " ".join(cmd)
        asked.append(uri)
        token = uri.rsplit("page=", 1)[1] if "page=" in uri else None
        items, nxt = pages[token]
        env = {"data": {"items": items}, "status": "200 OK"}
        if nxt:
            env["headers"] = {"opc-next-page": nxt}
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout=json.dumps(env))

    return fake, asked


def test_list_job_runs_follows_the_next_page():
    from target.provisioning import make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "run-1", "endTime": 1}], "P2"),
                               "P2": ([{"key": "run-2"}], None)})
    call = make_provision_call(OCID, run_process=fake)
    out = call("list_job_runs", workspace="ws", job_key="j")
    assert [i["key"] for i in out["items"]] == ["run-1", "run-2"]
    assert len(asked) == 2
    assert "jobKey=j&sortBy=timeCreated" in asked[0] and "page=" not in asked[0]
    assert asked[1].endswith("jobKey=j&sortBy=timeCreated&page=P2")


def test_in_flight_runs_sees_a_run_on_page_two():
    from target import jobs
    from target.provisioning import make_provision_call
    fake, _ = _paged_proc({None: ([{"key": "run-1", "endTime": 123}], "P2"),
                           "P2": ([{"key": "run-2", "endTime": None}], None)})
    call = make_provision_call(OCID, run_process=fake)
    assert jobs.in_flight_runs(call, workspace="ws", job_key="j") == ["run-2"]


def test_a_single_page_listing_is_one_request():
    from target.provisioning import make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "ws-1"}], None)})
    call = make_provision_call(OCID, run_process=fake)
    assert call("list_workspaces")["items"] == [{"key": "ws-1"}]
    assert len(asked) == 1 and "page=" not in asked[0]


def test_a_repeating_token_is_a_transport_error_not_a_loop():
    from target.provisioning import ProvisionTransportError, make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "a"}], "P2"),
                               "P2": ([{"key": "b"}], "P2")})
    call = make_provision_call(OCID, run_process=fake)
    with pytest.raises(ProvisionTransportError, match="list_jobs"):
        call("list_jobs", workspace="ws")
    assert len(asked) <= 3


def test_a_workspace_object_listing_with_a_next_page_is_refused_not_truncated():
    """list_ws_objects rides the aidp CLI, whose paging flags are unknown. A
    truncated listing would read uploads as not visible -- or worse, as
    absent; refusing names the problem."""
    import types
    from target.provisioning import ProvisionTransportError, make_provision_call

    def fake(cmd):
        assert cmd[0] == "aidp"
        return types.SimpleNamespace(
            returncode=0, stderr="",
            stdout='Response:\n' + json.dumps(
                {"data": {"items": [{"path": "a/b"}]},
                 "headers": {"opc-next-page": "P2"}}))

    call = make_provision_call(OCID, run_process=fake)
    with pytest.raises(ProvisionTransportError, match="page"):
        call("list_ws_objects", workspace="ws", path="a")


def test_provision_list_commands_carry_the_page_token():
    for op, kw in (("list_workspaces", {}),
                   ("list_clusters", {"workspace": "ws"}),
                   ("list_libraries", {"workspace": "ws", "cluster": "cl"}),
                   ("list_jobs", {"workspace": "ws"}),
                   ("list_task_runs", {"workspace": "ws", "run_key": "r"})):
        plain = build_provision_command("oci_raw", op, OCID, **kw)
        assert "page=" not in " ".join(plain)
        assert build_provision_command("oci_raw", op, OCID, page=None,
                                       **kw) == plain
        uri = build_provision_command("oci_raw", op, OCID, page="T2", **kw)
        uri = uri[uri.index("--target-uri") + 1]
        assert uri.endswith("page=T2") and uri.count("?") == 1, (op, uri)


# --- the cluster POST fails: the workspace record must survive -------------
#
# GAPS.md records that a cluster POSTed before the workspace reports ACTIVE
# is a 409 "ongoing operation". That POST was not guarded, so the
# ProvisionTransportError propagated out of provision(), cmd_provision never
# wrote provision_result.json or PROVISION.md, and the workspace created two
# seconds earlier was on record nowhere -- the next run halted on name_taken
# and called it someone else's.

_409 = ("create_cluster failed (exit 0): 409 Conflict ongoing operation on "
        "workspace")


class ClusterConflicts(Fake):
    """The first `fail_times` cluster POSTs raise `text`, then it works."""

    def __init__(self, fail_times, text=_409, **kw):
        super().__init__(**kw)
        self.fail_times = fail_times
        self.text = text

    def __call__(self, operation, **kw):
        if operation == "create_cluster" and self.fail_times:
            self.fail_times -= 1
            self.ops.append((operation, kw))
            from target.provisioning import ProvisionTransportError
            raise ProvisionTransportError(self.text)
        return super().__call__(operation, **kw)


def test_a_409_on_the_cluster_post_is_retried_then_recorded(scripts):
    fake = ClusterConflicts(2)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))
    cluster = [s for s in res["steps"] if s["step"] == "cluster"]
    assert [s["action"] for s in cluster] == ["retried", "retried", "created"]
    assert [s["verified"] for s in cluster] == [None, None, True]
    assert [op for op, _ in fake.ops].count("create_cluster") == 3
    assert any(s["step"] == "job" for s in res["steps"]), \
        "a retry that succeeds is not a halt"


def test_a_cluster_post_that_keeps_failing_returns_the_partial_record(scripts):
    fake = ClusterConflicts(99)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))          # returns, no raise
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("workspace", "created", True) in steps
    assert ("cluster", "failed", False) in steps
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "--reuse-existing" in halt["detail"]
    assert "ws-acme" in halt["detail"], "the record names the key"
    assert res["workspace"]["key"] == "ws-acme"
    ops = [op for op, _ in fake.ops]
    assert "upload_ws_file" not in ops and "create_job" not in ops


def test_a_non_conflict_cluster_error_is_not_retried(scripts):
    fake = ClusterConflicts(
        99, text="create_cluster failed (exit 1): 403 NotAuthorized")
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))
    assert [op for op, _ in fake.ops].count("create_cluster") == 1
    actions = {(s["step"], s["action"]) for s in res["steps"]}
    assert ("cluster", "failed") in actions
    assert ("cluster", "retried") not in actions
    assert any(s["step"] == "halt" for s in res["steps"])


def test_a_failed_cluster_listing_is_recorded_not_raised(scripts):
    fake = Fake(fail={"list_clusters"})
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("workspace", "created", True) in steps
    assert ("cluster", "failed", False) in steps
    assert any(s["step"] == "halt" for s in res["steps"])
    assert "create_cluster" not in [op for op, _ in fake.ops], \
        "could not look is not absent"


class Settling(Fake):
    """A created workspace reports CREATING until the `active_after`-th
    listing, then ACTIVE -- the way the live API behaves for a few seconds
    after the POST returns."""

    def __init__(self, active_after):
        super().__init__()
        self.active_after = active_after
        self.lists = 0

    def __call__(self, operation, **kw):
        if operation == "create_workspace":
            self.ops.append((operation, kw))
            name = kw["body"]["displayName"]
            self.workspaces.append({"displayName": name, "key": f"ws-{name}",
                                    "lifecycleState": "CREATING"})
            return {}
        if operation == "list_workspaces":
            self.lists += 1
            if self.lists >= self.active_after:
                for w in self.workspaces:
                    w["lifecycleState"] = "ACTIVE"
        return super().__call__(operation, **kw)


def test_the_workspace_is_waited_on_until_active(scripts):
    fake = Settling(active_after=4)      # 1 look-first + 3 polls
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0, 0))
    ops = [op for op, _ in fake.ops]
    before_cluster = ops[:ops.index("create_cluster")]
    assert before_cluster.count("list_workspaces") == 4, \
        "the cluster POST waits for ACTIVE, not just for visibility"
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created" and ws["verified"] is True


def test_a_workspace_that_stays_creating_is_recorded_and_not_halted(scripts):
    fake = Settling(active_after=10 ** 6)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0,))
    assert "create_cluster" in [op for op, _ in fake.ops], \
        "a slow ACTIVE is not a reason to stop; the 409 retry covers it"
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created" and "CREATING" in ws["detail"]


def test_the_name_taken_halt_names_the_operator_s_own_orphan(scripts):
    fake = Fake(workspaces=("acme",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "previous run" in halt["detail"].lower()
    assert "--reuse-existing" in halt["detail"]


# --- --source-config: only the snowflake: block travels, and it is said so --
#
# The operator's whole migration config used to be appended to plan_files and
# uploaded verbatim: the Snowflake password or PEM AND the aidp: block
# (DataLake OCID, target coordinates), as a workspace object readable by every
# member and every cluster, with a PROVISION.md row that said only
# "snowmig-config.yaml -> .../plan/snowmig-config.yaml". A `key_path:` config
# went up unchanged too, and failed five minutes later on the cluster with a
# FileNotFoundError for a laptop path.

_FAKE_PASSWORD = "FAKE-PASSWORD-not-real-123"
_FAKE_PEM = "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n"


def _config(tmp_path, **snowflake):
    import yaml
    block = {"account": "ACME-TEST", "user": "READER", "warehouse": "WH",
             "database": "DB", "auth": "password", "password": _FAKE_PASSWORD}
    block.update(snowflake)
    block = {k: v for k, v in block.items() if v is not None}
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(yaml.safe_dump({
        "snowflake": block,
        "aidp": {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.fakefakefake",
                 "catalog": "lake"}}), encoding="utf-8")
    return cfg


def test_the_source_config_upload_carries_only_the_snowflake_block(scripts,
                                                                    tmp_path):
    cfg = _config(tmp_path)
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    plan_files=[cfg], source_config=cfg, execute=True,
                    delays=())
    uploads = [kw for op, kw in fake.ops if op == "upload_ws_file"]
    assert not any(kw["local_path"] == str(cfg) for kw in uploads), \
        "the operator's file itself never travels"
    assert f"{PLAN_FOLDER}/snowmig-config.yaml" not in fake.contents
    remote = f"{PLAN_FOLDER}/snowmig-config.json"
    body = json.loads(fake.contents[remote]["body"])
    assert set(body) == {"snowflake"}
    assert body["snowflake"]["password"] == _FAKE_PASSWORD
    blob = json.dumps(body)
    assert "aidp" not in blob and "datalake_ocid" not in blob
    assert not pathlib.Path(fake.contents[remote]["local"]).exists(), \
        "the derived copy does not outlive the upload"
    assert res["credential_objects"] == [remote]
    step = next(s for s in res["steps"]
                if s["step"] == "upload" and remote in s["detail"])
    assert step["verified"] is True and "CREDENTIAL" in step["detail"]
    # The notebooks read the derived copy off the mount.
    nb = json.loads(
        fake.contents[f"{SCRIPTS_FOLDER}/00_discover_snowflake.ipynb"]["body"])
    params = "".join(nb["cells"][1]["source"])
    mount = REPORTS_FOLDER.rsplit("/", 1)[0]
    assert f"'source-config': '{mount}/plan/snowmig-config.json'" in params


def test_the_dry_run_names_the_credential_object(scripts, tmp_path):
    cfg = _config(tmp_path)
    res = provision(call=None, workspace_name="acme", scripts=scripts,
                    plan_files=[cfg], source_config=cfg, execute=False)
    remote = f"{PLAN_FOLDER}/snowmig-config.json"
    assert res["credential_objects"] == [remote]
    details = [s["detail"] for s in res["steps"] if s["step"] == "upload"]
    assert any("CREDENTIAL" in d and remote in d for d in details), details
    assert not any(d.endswith("snowmig-config.yaml") for d in details), \
        "the raw file is not previewed as an upload"
    md = render_provision(res)
    assert "Credential placed on the workspace" in md and remote in md
    assert _FAKE_PASSWORD not in md


def test_a_plan_file_that_is_not_the_source_config_has_no_credential_wording(
        scripts, tmp_path):
    plan = tmp_path / "plan.json"
    plan.write_text("{}", encoding="utf-8")
    res = provision(call=None, workspace_name="acme", scripts=scripts,
                    plan_files=[plan], execute=False)
    assert res["credential_objects"] == []
    assert not any("CREDENTIAL" in s["detail"] for s in res["steps"])
    assert "Credential placed" not in render_provision(res)


def test_a_path_form_secret_is_refused_before_any_upload(scripts, tmp_path):
    from migration_config import ConfigError
    pem = tmp_path / "rsa_key.p8"
    pem.write_text(_FAKE_PEM, encoding="utf-8")
    cfg = _config(tmp_path, auth="keypair", key_path=str(pem), password=None)
    fake = Fake()
    for execute in (False, True):
        with pytest.raises(ConfigError) as exc:
            provision(call=fake if execute else None, workspace_name="acme",
                      scripts=scripts, source_config=cfg, execute=execute,
                      delays=())
        message = str(exc.value)
        assert "key_path" in message and "inline" in message.lower()
        assert "/Workspace" in message
    assert fake.ops == [], "refused before anything reached AIDP"


def test_an_inline_secret_config_is_accepted(scripts, tmp_path):
    cfg = _config(tmp_path, auth="keypair", private_key=_FAKE_PEM,
                  password=None)
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    source_config=cfg, execute=True, delays=())
    assert any(s["step"] == "job" for s in res["steps"])
    body = json.loads(
        fake.contents[f"{PLAN_FOLDER}/snowmig-config.json"]["body"])
    assert body["snowflake"]["private_key"] == _FAKE_PEM
