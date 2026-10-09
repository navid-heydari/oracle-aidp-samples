"""extract against a fake OCI-DI REST API, then migrate what it wrote."""
import json

from ocidi2aidp.extract.extractor import DiClient, extract, redact
from ocidi2aidp.snapshot import Snapshot

WS = "ocid1.disworkspace.oc1.iad.test"


def fake_api(snapshot_dir):
    """Serve the fixture snapshot as the REST API would, two items per page."""
    snap = Snapshot.load(snapshot_dir)
    base = f"/workspaces/{WS}"
    calls = []
    flat = {"projects": "projects", "folders": "folders", "dataAssets": "data_assets",
            "connections": "connections", "dataFlows": "data_flows", "tasks": "tasks",
            "pipelines": "pipelines", "functionLibraries": "function_libraries",
            "applications": "applications"}
    app_kinds = {"publishedObjects": "published_objects", "schedules": "schedules",
                 "taskSchedules": "task_schedules"}

    def page(items, params):
        start = int(params.get("page") or 0)
        nxt = {"opc-next-page": str(start + 2)} if start + 2 < len(items) else {}
        return {"items": items[start:start + 2]}, nxt

    def transport(url, params):
        path = url.split("/20200430", 1)[1]
        calls.append((path, dict(params)))
        if path == base:
            return {"id": WS, "displayName": "SALES_DI"}, {}
        rest = path[len(base) + 1:].split("/")
        if rest[0] == "connections" and len(rest) == 1:
            items = [c for c in snap.all("connections").values()
                     if (c.get("parentRef") or {}).get("parent") == params.get("dataAssetKey")]
            return page(items, params)
        if rest[0] == "functionLibraries" and len(rest) >= 3:
            items = [u for u in snap.all("user_defined_functions").values()
                     if (u.get("parentRef") or {}).get("parent") == rest[1]]
            return (page(items, params) if len(rest) == 3 else
                    (snap.get("user_defined_functions", rest[3]), {}))
        if rest[0] == "applications" and len(rest) >= 3:
            app, kind = rest[1], rest[2]
            if kind == "taskRuns":
                runs = [dict(r, taskKey=r["taskKey"]) for r in snap.all("task_runs").values()]
                runs.append({"key": "failed-run", "status": "ERROR", "taskKey": "po-orders-daily"})
                return page(runs, params)
            objs = [o for o in snap.all(app_kinds[kind]).values()
                    if o.get("_applicationKey") == app]
            if len(rest) == 3:
                return page(objs, params)
            return snap.get(app_kinds[kind], rest[3]), {}
        kind = flat[rest[0]]
        if len(rest) == 1:
            return page(list(snap.all(kind).values()), params)
        return snap.get(kind, rest[1]), {}

    return transport, calls


def test_extract_writes_a_loadable_snapshot(tmp_path, snapshot_path, config):
    transport, calls = fake_api(snapshot_path)
    client = DiClient("us-ashburn-1", transport=transport)
    manifest = extract(client, WS, tmp_path / "snap", log=lambda *_: None)
    assert manifest["counts"]["tasks"] == 11 and manifest["counts"]["published_objects"] == 6
    assert manifest["region"] == "us-ashburn-1"
    assert any(p.endswith("/tasks/t-orders-daily") and q.get("expandReferences") == "true"
               for p, q in calls)
    assert any(q.get("page") for _, q in calls)                    # pagination followed
    snap = Snapshot.load(tmp_path / "snap")
    assert snap.last_success_run("app-prod", "po-orders-daily")["status"] == "SUCCESS"
    from ocidi2aidp.migrate import migrate
    rep = migrate(tmp_path / "snap", tmp_path / "out", config, strict=True)
    assert rep.counts()["failed"] == 0


def test_redaction_keeps_vault_references():
    conn = {"modelType": "ORACLE_ADWC_CONNECTION", "username": "U", "password": "p@ss",
            "passwordSecret": {"value": "inline", "secretConfig": {
                "modelType": "OCI_VAULT_SECRET_CONFIG", "secretId": "ocid1.vaultsecret.x"}},
            "credentialFileContent": "UEsDBB...", "connectionProperties": [
                {"name": "privateKey", "value": "-----BEGIN"}]}
    out = redact(conn)
    assert out["password"] == "<redacted>" and out["credentialFileContent"] == "<redacted>"
    assert out["passwordSecret"]["value"] == "<redacted>"
    assert out["passwordSecret"]["secretConfig"]["secretId"] == "ocid1.vaultsecret.x"
    assert out["username"] == "U"
    assert "p@ss" not in json.dumps(out)
