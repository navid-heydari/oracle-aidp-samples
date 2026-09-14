"""Create the AIDP target catalog itself. `call(operation, **kwargs)` injected,
same convention as `catalog_deploy.py`, so every decision here is unit-tested
with no environment.

DEFAULT IS EXTERNAL/SNOWFLAKE, NOT STANDARD. An EXTERNAL catalog is a
registered, read-only pointer at the live Snowflake source -- no managed
storage, no copy, nothing to keep in sync. A STANDARD catalog holds managed
Delta tables the migrator writes into directly, which is real storage with
real blast radius, and this module refuses to build one however it is asked
for -- see `RefusedToExecute` below for why, and for where the Standard path
actually is.
"""
from __future__ import annotations

import time
from typing import Callable

from .catalog_api import build_catalog_body

__all__ = ["RefusedToExecute", "ensure_catalog"]


class RefusedToExecute(RuntimeError):
    """A STANDARD catalog was asked for without going through its real path."""


_STANDARD_REFUSAL = (
    "a STANDARD catalog was requested. This module only ever registers "
    "EXTERNAL catalogs -- a STANDARD catalog holds managed Delta tables that "
    "this migrator would have to write into directly, and that table "
    "creation belongs on AIDP compute (a Spark cluster), not the control-plane "
    "catalog CRUD API: it is easier to track and debug there instead of going "
    "back and forth over OCI/HTTP. Generate the shallow-clone script with "
    "`snowmig.py notebook`, upload it to the workspace `Shared/` directory, "
    "and run it on the cluster -- see the `snowflake-clone-notebook` skill.")


def _find_catalog(call, display_name: str) -> dict | None:
    """The catalog as the server holds it, matched case-insensitively.

    A failed LISTING is raised, not swallowed. "We could not look" and "it is
    not there" lead to opposite decisions: swallowing the first one made a
    transient read error read as "absent", and the next step is a create.
    """
    wanted = display_name.strip().lower()
    payload = call("list_catalogs")
    for item in payload.get("items") or []:
        name = str(item.get("displayName") or item.get("key") or "").lower()
        if name == wanted:
            return item
    return None


def _poll_for_catalog(call, display_name: str,
                      delays: tuple[float, ...]) -> dict | None:
    """Read the catalog back until it appears, or the budget runs out.

    Creation is ASYNCHRONOUS and can fail SILENTLY: POST returns 202 Accepted
    with an empty body and no `opc-work-request-id`, so there is no waiter and
    the return value is not the claim -- the object appears seconds later, or
    never. `catalog_deploy.py` learned this the hard way; the same rule applies
    here. Reading back exactly once, immediately, reports every SUCCESSFUL
    create as pending.

    A listing that fails mid-poll is treated as "not visible yet" rather than
    as a failed create, because it is not evidence either way.
    """
    for attempt in range(len(delays) + 1):
        try:
            found = _find_catalog(call, display_name)
        except Exception:
            found = None
        if found is not None:
            return found
        if attempt < len(delays):
            time.sleep(delays[attempt])
    return None


def ensure_catalog(*, display_name: str, call: Callable[..., dict],
                   catalog_type: str = "EXTERNAL",
                   source_type: str | None = "SNOWFLAKE",
                   connection: dict | None = None, description: str = "",
                   properties: dict | None = None,
                   verify_delays: tuple[float, ...] = (3.0, 5.0, 10.0, 15.0),
                   ) -> dict:
    """Create `display_name` if absent, or report the existing catalog.

    A STANDARD catalog is refused outright, however it is asked for: its path
    is the notebook run on AIDP compute, not this function.
    """
    if catalog_type == "STANDARD":
        raise RefusedToExecute(_STANDARD_REFUSAL)
    if catalog_type != "EXTERNAL":
        raise RefusedToExecute(
            f"unknown catalog_type {catalog_type!r}; this module only creates "
            f"EXTERNAL catalogs")

    found = _find_catalog(call, display_name)
    if found is not None:
        return {"catalog": display_name, "action": "reused",
                "catalog_type": found.get("catalogType", catalog_type),
                "key": found.get("key") or found.get("displayName")}

    body = build_catalog_body(display_name, catalog_type="EXTERNAL",
                              source_type=source_type, description=description,
                              connection=connection, properties=properties)
    call("create_catalog", body=body)

    created = _poll_for_catalog(call, display_name, verify_delays)
    return {"catalog": display_name,
           "action": "created" if created is not None else "create_requested",
           "catalog_type": catalog_type,
           "key": (created or {}).get("key") or display_name,
           "verified": created is not None}
