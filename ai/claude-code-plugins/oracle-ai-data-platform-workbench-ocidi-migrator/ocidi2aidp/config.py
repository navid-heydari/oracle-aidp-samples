"""Migration configuration: one file, every field optional.

Looked up, first match wins: ``--config PATH``; ``./ocidi-config.yaml``;
``./ocidi-config.json``. YAML needs PyYAML; JSON always works. Environment
variables override the AIDP connection fields so a credential-adjacent value
(an OCID) never has to be committed:

  OCIDI_AIDP_INSTANCE_ID, OCIDI_AIDP_REGION, OCIDI_AIDP_PROFILE, OCIDI_AIDP_AUTH,
  OCIDI_AIDP_WORKSPACE_KEY, OCIDI_AIDP_CLUSTER_KEY,
  OCIDI_DI_WORKSPACE_ID, OCIDI_DI_REGION, OCIDI_DI_PROFILE
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

DEFAULT_WORKSPACE_NAME = "ocidi_migrated"
DEFAULT_TARGET_CATALOG = "ocidi_migrated"
DEFAULT_CONTROL_SCHEMA = "ocidi_control"
DEFAULT_NOTEBOOK_ROOT = "/Workspace/ocidi"
CONFIG_NAMES = ("ocidi-config.yaml", "ocidi-config.yml", "ocidi-config.json")


@dataclass
class AidpConfig:
    instance_id: str = ""            # DataLake OCID of the AIDP instance 
    instance_name: str = ""          # display name, used only to resolve instance_id
    region: str = ""
    profile: str = "DEFAULT"
    auth: str = "api_key"
    workspace_name: str = DEFAULT_WORKSPACE_NAME
    workspace_key: str = ""
    cluster_key: str = ""
    notebook_root: str = DEFAULT_NOTEBOOK_ROOT


@dataclass
class DiConfig:
    workspace_id: str = ""
    region: str = ""
    profile: str = "DEFAULT"


@dataclass
class TargetConfig:
    catalog: str = DEFAULT_TARGET_CATALOG
    control_schema: str = DEFAULT_CONTROL_SCHEMA
    # DI schema (database schema, or Object Storage bucket) name -> AIDP schema.
    schema_map: dict = field(default_factory=dict)


@dataclass
class MigrationConfig:
    aidp: AidpConfig = field(default_factory=AidpConfig)
    di: DiConfig = field(default_factory=DiConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    # DI data asset name -> {"access": "catalog"|"path"|"jdbc", "value": "..."}
    #   catalog: AIDP catalog name the asset is registered as (external catalog)
    #   path:    URI root, may contain {bucket} and {namespace}
    #   jdbc:    AIDP credential name holding url/user/password (read via aidputils.secrets)
    sources: dict = field(default_factory=dict)
    job_prefix: str = ""
    default_timezone: str = ""
    fallback_provider: str = "session"      # session | anthropic
    fallback_model: str = "claude-opus-5-5"

    def to_json(self) -> dict:
        data = asdict(self)
        # Never echo connection identifiers into report.json.
        for key in ("instance_id", "workspace_key", "cluster_key"):
            if data["aidp"].get(key):
                data["aidp"][key] = "<set>"
        if data["di"].get("workspace_id"):
            data["di"]["workspace_id"] = "<set>"
        return data


def _read(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover - depends on env
            raise RuntimeError(f"{path} is YAML but PyYAML is not installed; "
                               f"pip install pyyaml, or use ocidi-config.json") from exc
        return yaml.safe_load(text) or {}
    return json.loads(text or "{}")


def _merge(dc, values: dict):
    for key, value in (values or {}).items():
        if not hasattr(dc, key):
            raise ValueError(f"unknown config key {key!r} in {type(dc).__name__}")
        current = getattr(dc, key)
        if value is None and isinstance(current, (dict, str)):
            continue    # a YAML key with only commented-out children
        if hasattr(current, "__dataclass_fields__") and isinstance(value, dict):
            _merge(current, value)
        else:
            setattr(dc, key, value)
    return dc


def load_config(path: str | os.PathLike | None = None, *, cwd: Path | None = None) -> MigrationConfig:
    cfg = MigrationConfig()
    cwd = Path(cwd or Path.cwd())
    chosen = Path(path) if path else next((cwd / n for n in CONFIG_NAMES if (cwd / n).is_file()), None)
    if chosen is not None:
        if not chosen.is_file():
            raise FileNotFoundError(f"config file not found: {chosen}")
        _merge(cfg, _read(chosen))
    env = os.environ
    for var, (section, key) in {
        "OCIDI_AIDP_INSTANCE_ID": ("aidp", "instance_id"),
        "OCIDI_AIDP_REGION": ("aidp", "region"),
        "OCIDI_AIDP_PROFILE": ("aidp", "profile"),
        "OCIDI_AIDP_AUTH": ("aidp", "auth"),
        "OCIDI_AIDP_WORKSPACE_KEY": ("aidp", "workspace_key"),
        "OCIDI_AIDP_CLUSTER_KEY": ("aidp", "cluster_key"),
        "OCIDI_DI_WORKSPACE_ID": ("di", "workspace_id"),
        "OCIDI_DI_REGION": ("di", "region"),
        "OCIDI_DI_PROFILE": ("di", "profile"),
    }.items():
        if env.get(var):
            setattr(getattr(cfg, section), key, env[var])
    return cfg
