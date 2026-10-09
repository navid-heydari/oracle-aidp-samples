"""provision and publish against a fake AIDP client."""
import pytest

from ocidi2aidp.aidp.client import AidpClient, AidpError
from ocidi2aidp.aidp.provision import provision
from ocidi2aidp.aidp.publish import PublishError, plan_publish, publish


class FakeAidp:
    instance_id = "ocid1.aidataplatform.oc1.iad.test"

    def __init__(self, workspaces=(), catalogs=(), objects=None, jobs=(), clusters=("c1",)):
        self._ws = [dict(w) for w in workspaces]
        self._cats = list(catalogs)
        self.objects = {k: set(v) for k, v in (objects or {}).items()}
        self._jobs = [{"name": j} for j in jobs]
        self._clusters = [{"key": c} for c in clusters]
        self.created_ws, self.created_cats, self.notebooks, self.made, self.job_bodies = \
            [], [], {}, [], []

    object_path = staticmethod(AidpClient.object_path)

    def workspaces(self):
        return self._ws

    def workspace(self, key):
        return {"key": key, "lifecycleState": "ACTIVE"}

    def create_workspace(self, name, description):
        self.created_ws.append(name)
        ws = {"key": "ws-new", "displayName": name, "lifecycleState": "CREATING"}
        self._ws.append(ws)
        return ws

    def catalogs(self):
        return [{"displayName": c, "catalogType": "INTERNAL"} for c in self._cats]

    def create_catalog(self, name, description):
        self.created_cats.append(name)

    def names_in(self, ws, folder):
        return self.objects.get(folder, set())

    def mkdir(self, ws, folder):
        self.made.append(folder)

    def put_notebook(self, ws, path, ipynb):
        assert ipynb["nbformat"] == 4
        self.notebooks[path] = ipynb

    def jobs(self, ws):
        return self._jobs

    def create_job(self, ws, body):
        self.job_bodies.append(body)
        return f"job-{len(self.job_bodies)}"

    def clusters(self, ws):
        return self._clusters


def test_provision_dry_run_creates_nothing(config):
    fake = FakeAidp()
    res = provision(config, client=fake)
    assert [(s.what, s.state) for s in res.steps] == [("workspace", "would-create"),
                                                       ("catalog", "would-create")]
    assert not fake.created_ws and not fake.created_cats


def test_provision_apply_creates_workspace_named_ocidi_migrated(config):
    fake = FakeAidp()
    res = provision(config, client=fake, apply=True, poll=0)
    assert fake.created_ws == ["ocidi_migrated"] and fake.created_cats == ["ocidi_migrated"]
    assert res.workspace_key == "ws-new"
    assert [s.state for s in res.steps] == ["created", "created"]


def test_provision_reuses_existing(config):
    fake = FakeAidp(workspaces=[{"key": "ws1", "displayName": "ocidi_migrated",
                                 "lifecycleState": "ACTIVE"}], catalogs=["ocidi_migrated"])
    res = provision(config, client=fake, apply=True)
    assert res.workspace_key == "ws1" and not fake.created_ws and not fake.created_cats
    assert {s.state for s in res.steps} == {"exists"}


def test_publish_plan_is_a_dry_run(migrated, config):
    out, _ = migrated
    plan, log = publish(out, config)
    assert len(plan.notebooks) == 11 and len(plan.jobs) == 7
    assert plan.notebooks[0]["remote"].endswith("_setup/00_setup.ipynb")
    assert not log.uploaded


def test_publish_apply_uploads_then_creates_jobs(fresh_migration, config):
    out, _ = fresh_migration
    fake = FakeAidp()
    plan, log = publish(out, config, client=fake, workspace_key="ws", cluster_key="c1", apply=True)
    assert len(log.uploaded) == 11 and len(log.created_jobs) == 7 and not log.errors
    assert "/Workspace/ocidi/SALES/Marts" in fake.made
    for body in fake.job_bodies:
        assert all(t["cluster"] == {"clusterKey": "c1"} for t in body["tasks"])
        assert all(t["notebookPath"] in fake.notebooks for t in body["tasks"])
    assert (out / "publish.json").exists()


def test_publish_without_cluster_uploads_notebooks_but_creates_no_job(fresh_migration, config):
    out, _ = fresh_migration
    fake = FakeAidp()
    _, log = publish(out, config, client=fake, workspace_key="ws", apply=True)
    assert len(log.uploaded) == 11 and not fake.job_bodies
    assert len(log.refused_jobs) == 7
    assert all("cluster must not be null" in r["why"] for r in log.refused_jobs)


def test_second_publish_with_cluster_creates_jobs_on_notebooks_it_uploaded(fresh_migration,
                                                                          config):
    out, _ = fresh_migration
    fake = FakeAidp()
    publish(out, config, client=fake, workspace_key="ws", apply=True)     # no cluster yet
    present = {}
    for path in fake.notebooks:
        parent, _, name = path.rpartition("/")
        present.setdefault(parent, set()).add(name)
    again = FakeAidp(objects=present)
    _, log = publish(out, config, client=again, workspace_key="ws", cluster_key="c1", apply=True)
    assert not log.uploaded and len(log.skipped) == 11
    assert len(log.created_jobs) == 7 and not log.refused_jobs


def test_publish_never_overwrites_and_refuses_dependent_jobs(fresh_migration, config):
    out, _ = fresh_migration
    fake = FakeAidp(objects={"/Workspace/ocidi/SALES/Ingest": {"IT_LOAD_CUSTOMERS.ipynb"}},
                    jobs=["IT_ORDERS_DAILY"])
    _, log = publish(out, config, client=fake, workspace_key="ws", cluster_key="c1", apply=True)
    assert "/Workspace/ocidi/SALES/Ingest/IT_LOAD_CUSTOMERS.ipynb" not in fake.notebooks
    refused = {r["job"] for r in log.refused_jobs}
    assert {"IT_LOAD_CUSTOMERS", "PT_NIGHTLY"} <= refused
    assert any(s.get("job") == "IT_ORDERS_DAILY" for s in log.skipped)


def test_publish_prefix_moves_notebooks_and_renames_jobs(migrated, config):
    out, _ = migrated
    plan = plan_publish(out, config, prefix="alice")
    assert all(n["remote"].startswith("/Workspace/ocidi/alice/") for n in plan.notebooks)
    assert all(j["name"].startswith("alice_") for j in plan.jobs)
    for j in plan.jobs:
        assert all(p.startswith("/Workspace/ocidi/alice/") for p in j["notebooks"])
    with pytest.raises(PublishError):
        plan_publish(out, config, prefix="bad prefix")


def test_publish_rejects_unknown_cluster(fresh_migration, config):
    out, _ = fresh_migration
    with pytest.raises(PublishError, match="not one of this workspace's clusters"):
        publish(out, config, client=FakeAidp(), workspace_key="ws", cluster_key="nope", apply=True)


def test_mkdir_sends_the_body_the_cli_requires(monkeypatch):
    seen = {}

    def fake_run(self, args, body=None):
        seen["args"] = args
        return {"status": 200, "data": []}
    monkeypatch.setattr(AidpClient, "run", fake_run)
    AidpClient(instance_id="x").mkdir("ws", "/Workspace/ocidi/SALES")
    assert seen["args"] == ["workspace-object", "create", "ws", "--path", '"ocidi/SALES"',
                            "--type", '"FOLDER"', "--body", ""]


def test_client_requires_instance():
    with pytest.raises(AidpError):
        AidpClient(instance_id="")
    assert AidpClient.object_path("/Workspace/a/b.ipynb") == "a/b.ipynb"
