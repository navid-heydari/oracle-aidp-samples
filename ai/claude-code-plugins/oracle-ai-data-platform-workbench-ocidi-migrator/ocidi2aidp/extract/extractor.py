"""Walk one OCI-DI workspace and write every object, verbatim, to a snapshot.

Raw JSON on purpose: the typed SDK drops fields of models it does not know
(the Table Function operator has none in 2.165.1), and the compiler needs
those fields.

Paths (OCI-DI REST API ``/20200430``; the same calls the SDK makes):

  GET /workspaces/{ws}
  GET /workspaces/{ws}/projects | /folders | /dataAssets | /connections?dataAssetKey=
  GET /workspaces/{ws}/dataFlows/{key}?expandReferences=true
  GET /workspaces/{ws}/tasks/{key}?expandReferences=true
  GET /workspaces/{ws}/pipelines/{key}?expandReferences=true
  GET /workspaces/{ws}/functionLibraries/{lib}/userDefinedFunctions
  GET /workspaces/{ws}/applications/{app}/publishedObjects/{key}?expandReferences=true
  GET /workspaces/{ws}/applications/{app}/schedules | /taskSchedules | /taskRuns

Secrets: connection objects can carry passwords, wallets and private keys.
Every value under a key that names a secret is replaced with ``<redacted>``
before anything is written; OCI Vault secret OCIDs (``secretId``) are kept,
because they are references, not values.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Callable, Optional

from .. import __version__

API = "20200430"
_SECRET_KEYS = {"password", "passphrase", "passPhrase", "privateKey", "privateKeyPassword",
                "credentialFileContent", "walletPassword", "secret", "token", "clientSecret",
                "value", "keyContent", "sasToken", "accessKey", "secretKey"}
_SAFE_UNDER_SECRET = {"secretId", "modelType", "secretConfig"}


def redact(obj, *, under_secret: bool = False):
    """Replace secret values; keep structure and Vault references."""
    if isinstance(obj, dict):
        out = {}
        sensitive = under_secret or obj.get("modelType") in ("SENSITIVE_ATTRIBUTE",)
        for k, v in obj.items():
            if k in _SAFE_UNDER_SECRET:
                out[k] = redact(v, under_secret=False) if isinstance(v, (dict, list)) else v
            elif (k in _SECRET_KEYS and not isinstance(v, (dict, list)) and v not in (None, "")
                  and (k != "value" or sensitive)):
                out[k] = "<redacted>"
            elif k.lower().endswith(("password", "secret")) and isinstance(v, dict):
                out[k] = redact(v, under_secret=True)
            else:
                out[k] = redact(v, under_secret=under_secret)
        return out
    if isinstance(obj, list):
        return [redact(v, under_secret=under_secret) for v in obj]
    return obj


class DiClient:
    """Signed GETs against the OCI-DI REST API. ``transport`` is injectable for tests."""

    def __init__(self, region: str, *, profile: str = "DEFAULT", transport: Optional[Callable] = None):
        self.base = f"https://dataintegration.{region}.oci.oraclecloud.com/{API}"
        self.transport = transport or self._signed_transport(profile, region)

    @staticmethod
    def _signed_transport(profile: str, region: str) -> Callable:
        try:
            import oci
            import requests
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("extract needs the oci package: pip install oci") from exc
        cfg = oci.config.from_file(profile_name=profile)
        if "security_token_file" in cfg:
            token = Path(cfg["security_token_file"]).expanduser().read_text().strip()
            key = oci.signer.load_private_key_from_file(cfg["key_file"])
            signer = oci.auth.signers.SecurityTokenSigner(token, key)
        else:
            signer = oci.signer.Signer(cfg["tenancy"], cfg["user"], cfg["fingerprint"],
                                       cfg["key_file"], cfg.get("pass_phrase"))
        session = requests.Session()

        def transport(url: str, params: dict):
            resp = session.get(url, params=params, auth=signer, timeout=120)
            if resp.status_code == 404:
                return None, {}
            resp.raise_for_status()
            return resp.json(), dict(resp.headers)
        return transport

    def get(self, path: str, **params) -> Optional[dict]:
        data, _ = self.transport(self.base + path, params)
        return data

    def list(self, path: str, **params) -> list:
        items, page = [], None
        for _ in range(1000):
            q = dict(params, limit=100)
            if page:
                q["page"] = page
            data, headers = self.transport(self.base + path, q)
            if data is None:
                return items
            batch = data.get("items", data if isinstance(data, list) else [])
            items.extend(batch)
            page = next((v for k, v in headers.items() if k.lower() == "opc-next-page"), None)
            if not page:
                return items
        raise RuntimeError(f"{path}: still paging after 1000 pages")


def extract(client: DiClient, workspace_id: str, out_dir, *, log=print) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ws_path = f"/workspaces/{workspace_id}"
    counts = {}

    def put(kind, obj, app=None, name=None):
        d = out / kind / app if app else out / kind
        d.mkdir(parents=True, exist_ok=True)
        key = name or obj.get("key") or obj.get("identifier")
        (d / f"{key}.json").write_text(json.dumps(redact(obj), indent=2) + "\n", encoding="utf-8")
        counts[kind] = counts.get(kind, 0) + 1

    workspace = client.get(ws_path) or {}
    (out / "workspace.json").write_text(json.dumps(redact(workspace), indent=2) + "\n")
    for kind, path in (("projects", "/projects"), ("folders", "/folders")):
        for item in client.list(ws_path + path):
            put(kind, client.get(f"{ws_path}{path}/{item['key']}") or item)
    for asset in client.list(ws_path + "/dataAssets"):
        full = client.get(f"{ws_path}/dataAssets/{asset['key']}") or asset
        put("data_assets", full)
        for conn in client.list(ws_path + "/connections", dataAssetKey=asset["key"]):
            put("connections", client.get(f"{ws_path}/connections/{conn['key']}") or conn)
    for kind, path in (("data_flows", "/dataFlows"), ("tasks", "/tasks"), ("pipelines", "/pipelines")):
        for item in client.list(ws_path + path):
            put(kind, client.get(f"{ws_path}{path}/{item['key']}", expandReferences="true") or item)
        log(f"  {kind}: {counts.get(kind, 0)}")
    for lib in client.list(ws_path + "/functionLibraries"):
        put("function_libraries", lib)
        for udf in client.list(f"{ws_path}/functionLibraries/{lib['key']}/userDefinedFunctions"):
            put("user_defined_functions",
                client.get(f"{ws_path}/functionLibraries/{lib['key']}/userDefinedFunctions/"
                           f"{udf['key']}") or udf)
    for app in client.list(ws_path + "/applications"):
        akey = app["key"]
        put("applications", client.get(f"{ws_path}/applications/{akey}") or app)
        base = f"{ws_path}/applications/{akey}"
        for po in client.list(base + "/publishedObjects"):
            put("published_objects", client.get(f"{base}/publishedObjects/{po['key']}",
                                                expandReferences="true") or po, akey)
        for s in client.list(base + "/schedules"):
            put("schedules", client.get(f"{base}/schedules/{s['key']}") or s, akey)
        for ts in client.list(base + "/taskSchedules"):
            put("task_schedules", client.get(f"{base}/taskSchedules/{ts['key']}") or ts, akey)
        latest = {}
        for run in client.list(base + "/taskRuns", sortBy="TIME_CREATED", sortOrder="DESC"):
            task_key = run.get("taskKey") or (run.get("parentRef") or {}).get("parent")
            if run.get("status") == "SUCCESS" and task_key and task_key not in latest:
                latest[task_key] = run
        for task_key, run in latest.items():
            put("task_runs", run, akey, task_key)
    manifest = {"workspace_id": workspace_id, "workspace_name": workspace.get("displayName"),
                "region": client.base.split(".")[1], "tool": f"ocidi2aidp {__version__}",
                "extracted_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "counts": counts}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
