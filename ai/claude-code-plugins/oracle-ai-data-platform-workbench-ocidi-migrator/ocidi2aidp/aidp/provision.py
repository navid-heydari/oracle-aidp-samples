"""``provision``: make sure the AIDP side exists -- a dry run unless ``apply``.

1. Resolve the DataLake (AIDP instance): ``aidp.instance_id``, or look up
   ``aidp.instance_name`` with OCI Search.
2. Workspace ``aidp.workspace_name`` (default ``ocidi_migrated``): reuse it
   if it exists, otherwise create it and wait until ACTIVE.
3. Catalog ``target.catalog`` (default ``ocidi_migrated``): an INTERNAL
   catalog for the migrated Delta tables; reuse or create. ``--skip-catalog``
   leaves it alone.

Schemas and tables are created by ``ddl/00_setup.ipynb`` on a cluster -- they
need Spark, not the control plane.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field

from .client import AidpClient, AidpError


@dataclass
class Step:
    what: str
    state: str          # exists | would-create | created | skipped | failed
    detail: str = ""


@dataclass
class ProvisionResult:
    instance_id: str = ""
    workspace_key: str = ""
    catalog: str = ""
    steps: list = field(default_factory=list)

    def to_json(self) -> dict:
        return {"instance_id": self.instance_id, "workspace_key": self.workspace_key,
                "catalog": self.catalog, "steps": [asdict(s) for s in self.steps]}


def resolve_instance(config) -> str:
    """The DataLake OCID, from config or by display name through OCI Search."""
    if config.aidp.instance_id:
        return config.aidp.instance_id
    name = config.aidp.instance_name
    if not name:
        raise AidpError("set aidp.instance_id (DataLake OCID) or aidp.instance_name "
                        "in ocidi-config")
    try:
        import oci
    except ImportError as exc:
        raise AidpError("resolving an instance by name needs the oci package "
                        "(pip install oci), or set aidp.instance_id") from exc
    cfg = oci.config.from_file(profile_name=config.aidp.profile or "DEFAULT")
    if config.aidp.region:
        cfg["region"] = config.aidp.region
    client = oci.resource_search.ResourceSearchClient(cfg)
    safe = name.replace("'", "")
    details = oci.resource_search.models.StructuredSearchDetails(
        query=f"query AiDataPlatform resources where displayName = '{safe}'",
        type="Structured", matching_context_type="NONE")
    items = client.search_resources(details).data.items
    active = [i for i in items if (i.lifecycle_state or "").upper() == "ACTIVE"]
    if not active:
        raise AidpError(f"no ACTIVE AI Data Platform instance named {name!r} is visible to "
                        f"profile {config.aidp.profile}")
    if len(active) > 1:
        raise AidpError(f"{len(active)} AI Data Platform instances are named {name!r}; set "
                        f"aidp.instance_id")
    return active[0].identifier


def provision(config, *, apply: bool = False, client: AidpClient = None,
              skip_catalog: bool = False, wait_seconds: int = 900, poll: int = 15) -> ProvisionResult:
    res = ProvisionResult()
    res.instance_id = resolve_instance(config) if client is None else client.instance_id
    client = client or AidpClient(instance_id=res.instance_id, region=config.aidp.region,
                                  profile=config.aidp.profile, auth=config.aidp.auth)
    name = config.aidp.workspace_name
    ws = next((w for w in client.workspaces() if w.get("displayName") == name
               and (w.get("lifecycleState") or "ACTIVE") not in ("DELETED", "DELETING")), None)
    if ws:
        res.workspace_key = ws.get("key", "")
        res.steps.append(Step("workspace", "exists", f"{name} ({ws.get('lifecycleState')})"))
    elif not apply:
        res.steps.append(Step("workspace", "would-create", name))
    else:
        created = client.create_workspace(name, "OCI Data Integration migration target "
                                                "(created by ocidi2aidp)")
        res.workspace_key = created.get("key", "")
        state = created.get("lifecycleState", "")
        deadline = time.monotonic() + wait_seconds
        while res.workspace_key and state not in ("ACTIVE", "FAILED") and \
                time.monotonic() < deadline:
            time.sleep(poll)
            state = client.workspace(res.workspace_key).get("lifecycleState", "")
        if not res.workspace_key:
            # Some create calls return an async operation, not the workspace.
            ws = next((w for w in client.workspaces() if w.get("displayName") == name), None)
            res.workspace_key = (ws or {}).get("key", "")
            state = (ws or {}).get("lifecycleState", state)
        res.steps.append(Step("workspace", "created" if state == "ACTIVE" else "failed",
                              f"{name}: {state or 'unknown state'}"))
    cat_name = config.target.catalog
    res.catalog = cat_name
    if skip_catalog:
        res.steps.append(Step("catalog", "skipped", cat_name))
        return res
    cats = client.catalogs()
    cat = next((c for c in cats if c.get("displayName") == cat_name or c.get("name") == cat_name),
               None)
    if cat:
        res.steps.append(Step("catalog", "exists", f"{cat_name} ({cat.get('catalogType', '?')})"))
    elif not apply:
        res.steps.append(Step("catalog", "would-create", f"{cat_name} (INTERNAL)"))
    else:
        client.create_catalog(cat_name, "Delta tables migrated from OCI Data Integration")
        res.steps.append(Step("catalog", "created", f"{cat_name} (INTERNAL)"))
    return res
