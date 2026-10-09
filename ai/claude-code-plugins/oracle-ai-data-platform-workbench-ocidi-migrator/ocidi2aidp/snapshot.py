"""The offline input every step after ``extract`` reads.

A snapshot is a directory of raw OCI-DI JSON, one object per file, laid out as
described in references/design.md. ``Snapshot.load`` also accepts an OCI-DI
export (``.zip`` or an unpacked directory); its layout is undocumented, so the
export loader classifies every JSON object it finds by ``modelType`` and is
reported as unverified.
"""
from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

# Directory name -> kind. Application-scoped kinds live one level deeper,
# under the application key.
FLAT_KINDS = ("projects", "folders", "data_assets", "connections", "data_flows", "tasks",
              "pipelines", "function_libraries", "user_defined_functions", "applications")
APP_KINDS = ("published_objects", "schedules", "task_schedules", "task_runs")

TASK_TYPES = ("INTEGRATION_TASK", "DATA_LOADER_TASK", "PIPELINE_TASK", "SQL_TASK",
              "OCI_DATAFLOW_TASK", "REST_TASK")


class SnapshotError(Exception):
    pass


@dataclass
class Snapshot:
    root: str
    manifest: dict = field(default_factory=dict)
    objects: dict = field(default_factory=dict)      # kind -> {key: obj}
    source_format: str = "snapshot"                  # snapshot | export

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, path) -> "Snapshot":
        path = Path(path)
        if path.is_file() and path.suffix == ".zip":
            return cls._from_export_zip(path)
        if not path.is_dir():
            raise SnapshotError(f"no snapshot at {path}")
        if (path / "manifest.json").is_file():
            return cls._from_snapshot_dir(path)
        return cls._from_export_objects(path, cls._json_files(path))

    @classmethod
    def _from_snapshot_dir(cls, path: Path) -> "Snapshot":
        snap = cls(root=str(path), manifest=_read_json(path / "manifest.json"))
        for kind in FLAT_KINDS:
            snap.objects[kind] = {}
            for f in sorted((path / kind).glob("*.json")) if (path / kind).is_dir() else []:
                obj = _read_json(f)
                snap.objects[kind][obj.get("key") or f.stem] = obj
        for kind in APP_KINDS:
            snap.objects[kind] = {}
            base = path / kind
            if not base.is_dir():
                continue
            for app_dir in sorted(p for p in base.iterdir() if p.is_dir()):
                for f in sorted(app_dir.glob("*.json")):
                    obj = _read_json(f)
                    obj.setdefault("_applicationKey", app_dir.name)
                    key = obj.get("key") or f.stem
                    if kind == "task_runs":
                        key = f"{app_dir.name}/{f.stem}"
                    snap.objects[kind][key] = obj
        return snap

    @staticmethod
    def _json_files(path: Path) -> Iterator[tuple[str, bytes]]:
        for f in sorted(path.rglob("*.json")):
            yield str(f.relative_to(path)), f.read_bytes()

    @classmethod
    def _from_export_zip(cls, path: Path) -> "Snapshot":
        with zipfile.ZipFile(path) as zf:
            files = [(n, zf.read(n)) for n in sorted(zf.namelist()) if n.endswith(".json")]
            # An export may nest further zips (one per object).
            for n in sorted(zf.namelist()):
                if n.endswith(".zip"):
                    with zipfile.ZipFile(io.BytesIO(zf.read(n))) as inner:
                        files += [(f"{n}/{m}", inner.read(m)) for m in sorted(inner.namelist())
                                  if m.endswith(".json")]
        return cls._from_export_objects(path, files)

    @classmethod
    def _from_export_objects(cls, path: Path, files) -> "Snapshot":
        snap = cls(root=str(path), source_format="export",
                   manifest={"source": "export", "path": str(path),
                             "warning": "OCI-DI export layout is undocumented; objects were "
                                        "classified by modelType"})
        for kind in FLAT_KINDS + APP_KINDS:
            snap.objects[kind] = {}
        for name, data in files:
            try:
                obj = json.loads(data)
            except ValueError:
                continue
            for item in _walk_objects(obj):
                kind = _classify(item)
                if kind:
                    snap.objects[kind].setdefault(item.get("key") or name, item)
        return snap

    # -------------------------------------------------------------- queries
    def all(self, kind: str) -> dict:
        return self.objects.get(kind, {})

    def get(self, kind: str, key: str) -> Optional[dict]:
        return self.objects.get(kind, {}).get(key)

    def find(self, kind: str, ref) -> Optional[dict]:
        """By key, identifier, or name -- in that order."""
        if ref is None:
            return None
        if isinstance(ref, dict):
            for attr in ("key", "identifier", "name"):
                hit = self.find(kind, ref.get(attr)) if ref.get(attr) else None
                if hit:
                    return hit
            return None
        items = self.objects.get(kind, {})
        if ref in items:
            return items[ref]
        for attr in ("identifier", "name"):
            for obj in items.values():
                if obj.get(attr) == ref:
                    return obj
        return None

    def data_asset_for(self, ref) -> Optional[dict]:
        return self.find("data_assets", ref)

    def tasks_of_type(self, *types: str) -> list[dict]:
        return [t for t in self.all("tasks").values() if t.get("modelType") in types]

    def published_tasks(self) -> list[dict]:
        return list(self.all("published_objects").values())

    def task_schedules_for(self, published_key: str) -> list[dict]:
        out = []
        for ts in self.all("task_schedules").values():
            target = ts.get("publishedObjectKey") or ((ts.get("parentRef") or {}).get("parent"))
            if target == published_key:
                out.append(ts)
        return out

    def last_success_run(self, application_key: str, task_key: str) -> Optional[dict]:
        return self.get("task_runs", f"{application_key}/{task_key}")

    def folder_path(self, obj: dict) -> list[str]:
        """Project/folder names above an object, outermost first."""
        names, seen = [], set()
        parent = (obj.get("parentRef") or {}).get("parent")
        while parent and parent not in seen:
            seen.add(parent)
            node = self.get("folders", parent) or self.get("projects", parent)
            if not node:
                break
            names.append(node.get("name") or node.get("identifier") or parent)
            parent = (node.get("parentRef") or {}).get("parent")
        return list(reversed(names))

    def counts(self) -> dict:
        return {k: len(v) for k, v in self.objects.items() if v}


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise SnapshotError(f"{path} is not valid JSON: {exc}") from exc


def _walk_objects(obj):
    """Top-level objects in an export file: the object itself, or list members."""
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield item
    elif isinstance(obj, dict):
        if isinstance(obj.get("items"), list):
            for item in obj["items"]:
                if isinstance(item, dict):
                    yield item
        else:
            yield obj


def _classify(obj: dict) -> Optional[str]:
    kind = obj.get("modelType", "")
    if kind in TASK_TYPES:
        return "tasks"
    if kind == "DATA_FLOW" or ("nodes" in obj and kind in ("", "DATAFLOW")):
        return "data_flows"
    if kind == "PIPELINE":
        return "pipelines"
    if kind.endswith("_DATA_ASSET"):
        return "data_assets"
    if kind.endswith("_CONNECTION"):
        return "connections"
    if kind == "PROJECT":
        return "projects"
    if kind == "FOLDER":
        return "folders"
    if kind == "FUNCTION_LIBRARY":
        return "function_libraries"
    if kind in ("USER_DEFINED_FUNCTION", "DIS_USER_DEFINED_FUNCTION"):
        return "user_defined_functions"
    if kind in ("APPLICATION", "DIS_APPLICATION"):
        return "applications"
    if kind == "SCHEDULE":
        return "schedules"
    if kind == "TASK_SCHEDULE":
        return "task_schedules"
    return None
