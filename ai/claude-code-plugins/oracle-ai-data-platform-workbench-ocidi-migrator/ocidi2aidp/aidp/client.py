"""Talk to AIDP through the official ``aidp`` CLI.

``aidp`` already solves OCI request signing, endpoint resolution and profile
handling; shelling out to it keeps the ``oci`` SDK out of this package's
required dependencies. The calling conventions below (option values passed as
JSON literals, ``--body @file``, pagination through ``opc-next-page``, API
errors carried in a JSON payload whose ``status`` is >= 400) are the ones the
Fabric migrator verified against a live workspace (2026-09/10).
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

TIMEOUT_SECONDS = 180
PAGE_SIZE = 100
MAX_PAGES = 200


class AidpUnavailable(Exception):
    """The ``aidp`` CLI is not installed."""


class AidpError(Exception):
    def __init__(self, message, *, status=None, code=None):
        super().__init__(message)
        self.status = status
        self.code = code


def _literal(value: str) -> str:
    """aidp-cli parses option values as JSON: pass strings as JSON strings."""
    return json.dumps(value)


class AidpClient:
    def __init__(self, *, instance_id: str, region: str = "", profile: str = "DEFAULT",
                 auth: str = "api_key", executable: str = "aidp", timeout: int = TIMEOUT_SECONDS):
        if not instance_id:
            raise AidpError("no AIDP instance (DataLake OCID) configured; set aidp.instance_id or "
                            "aidp.instance_name in ocidi-config, or OCIDI_AIDP_INSTANCE_ID")
        self.instance_id = instance_id
        self.region = region
        self.profile = profile
        self.auth = auth
        self.executable = executable
        self.timeout = timeout

    @staticmethod
    def available(executable: str = "aidp") -> bool:
        return shutil.which(executable) is not None

    # ---------------------------------------------------------------- core
    def run(self, args: list, body=None) -> dict:
        found = shutil.which(self.executable)
        if found is None:
            raise AidpUnavailable(f"{self.executable!r} not found on PATH; install the AIDP CLI "
                                  f"(pip install aidp-cli)")
        command = [found] + [str(a) for a in args] + ["--instance-id", self.instance_id,
                                                      "--profile", self.profile,
                                                      "--auth", self.auth,
                                                      "--timeout", str(self.timeout)]
        if self.region:
            command += ["--region", self.region]
        scratch = None
        try:
            if body is not None:
                scratch = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                                      encoding="utf-8")
                json.dump(body, scratch)
                scratch.close()
                command += ["--body", f"@{scratch.name}"]
            done = subprocess.run(command, capture_output=True, text=True,
                                  timeout=self.timeout + 30)
        except subprocess.TimeoutExpired as exc:
            raise AidpError(f"aidp timed out after {self.timeout}s: {' '.join(args[:2])}") from exc
        finally:
            if scratch is not None:
                Path(scratch.name).unlink(missing_ok=True)
        text = done.stdout or ""
        if "{" not in text and "{" in (done.stderr or ""):
            text = done.stderr
        start = text.find("{")
        if start < 0:
            raise AidpError(f"aidp returned no JSON (exit {done.returncode}): "
                            f"{(done.stderr or text).strip()[:300]}")
        try:
            payload = json.loads(text[start:text.rindex("}") + 1])
        except ValueError as exc:
            raise AidpError(f"aidp returned unparseable JSON: {text[:200]!r}") from exc
        status = payload.get("status")
        if isinstance(status, int) and status >= 400:
            raise AidpError(f"AIDP {status} {payload.get('code', '')}: "
                            f"{payload.get('message', '')}".strip(), status=status,
                            code=payload.get("code"))
        return payload

    def pages(self, args: list) -> list:
        found, page = [], None
        for _ in range(MAX_PAGES):
            payload = self.run(list(args) + ["--limit", str(PAGE_SIZE)]
                               + (["--page", _literal(page)] if page else []))
            data = payload.get("data") or {}
            items = data.get("items") if isinstance(data, dict) else data
            found.extend(items if isinstance(items, list) else [])
            headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else {}
            page = next((v for k, v in headers.items()
                         if str(k).lower() == "opc-next-page" and v), None)
            if not page:
                return found
        raise AidpError(f"{' '.join(args[:2])} still paging after {MAX_PAGES} pages")

    # ---------------------------------------------------------- workspaces
    def workspaces(self) -> list:
        payload = self.run(["workspace", "list"])
        data = payload.get("data") or {}
        return data.get("items", []) if isinstance(data, dict) else data

    def workspace(self, key: str) -> dict:
        return self.run(["workspace", "get", key]).get("data") or {}

    def create_workspace(self, display_name: str, description: str) -> dict:
        return self.run(["workspace", "create"], body={"displayName": display_name,
                                                       "description": description}).get("data") or {}

    # ------------------------------------------------------------ catalogs
    def catalogs(self) -> list:
        return self.pages(["catalog", "list"])

    def create_catalog(self, display_name: str, description: str) -> dict:
        return self.run(["catalog", "create"], body={"displayName": display_name,
                                                     "catalogType": "INTERNAL",
                                                     "description": description}).get("data") or {}

    # ------------------------------------------------------------- objects
    @staticmethod
    def object_path(path: str) -> str:
        """``/Workspace/a/b.ipynb`` -> ``a/b.ipynb`` (the object API's relative form)."""
        rel = path.strip("/")
        if rel == "Workspace" or rel.startswith("Workspace/"):
            rel = rel[len("Workspace"):].lstrip("/")
        return rel

    def names_in(self, workspace_key: str, folder: str) -> set:
        try:
            items = self.pages(["workspace-object", "list", workspace_key,
                                "--path", _literal(self.object_path(folder) or "/")])
        except AidpError as exc:
            if exc.status == 404:
                return set()
            raise
        return {str(i.get("displayName") or str(i.get("path", "")).rsplit("/", 1)[-1])
                for i in items if isinstance(i, dict)}

    def mkdir(self, workspace_key: str, folder: str) -> None:
        """Create one folder. ``--body`` is required even for a folder, and an
        empty one is what works (live, aidp CLI 4.2.1, 2026-10-09: without it the
        CLI exits 2 "Missing required flag --body"; with '' the API answers 200)."""
        try:
            self.run(["workspace-object", "create", workspace_key,
                      "--path", _literal(self.object_path(folder)), "--type", _literal("FOLDER"),
                      "--body", ""])
        except AidpError as exc:
            if exc.status == 409 or "exist" in str(exc).lower():
                return
            raise

    def put_notebook(self, workspace_key: str, path: str, ipynb: dict) -> dict:
        return self.run(["notebook", "update-content", workspace_key, path],
                        body={"type": "notebook", "format": "json", "content": ipynb})

    # ---------------------------------------------------------------- jobs
    def jobs(self, workspace_key: str) -> list:
        return self.pages(["workflow", "list-jobs", workspace_key])

    def create_job(self, workspace_key: str, body: dict) -> str:
        payload = self.run(["workflow", "create-job", workspace_key], body=body)
        key = (payload.get("data") or {}).get("key")
        if not key:
            raise AidpError(f"create-job returned no key: {str(payload)[:200]}")
        return key

    def clusters(self, workspace_key: str) -> list:
        return self.pages(["cluster", "list", workspace_key])
