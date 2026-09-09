"""AIDP target coordinates.

DELIBERATELY INERT. This module cannot discover a target and cannot persist one.
It reads no environment variable, no config file, and no cache, and it writes
nothing to disk -- a test asserts that by inspecting this file's source.

Coordinates arrive as function arguments, supplied in the conversation that uses
them. Absent coordinates are a hard error instructing the caller to ask the user.

An unmapped region short-code errors rather than defaulting: guessing a region
would point a write at the wrong tenancy.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["MissingTarget", "REGIONS", "Target", "region_from_ocid", "resolve_target"]

REGIONS = {
    "iad": "us-ashburn-1", "phx": "us-phoenix-1", "fra": "eu-frankfurt-1",
    "lhr": "uk-london-1", "bom": "ap-mumbai-1", "hyd": "ap-hyderabad-1",
    "sin": "ap-singapore-1", "nrt": "ap-tokyo-1", "syd": "ap-sydney-1",
    "gru": "sa-saopaulo-1", "yyz": "ca-toronto-1", "icn": "ap-seoul-1",
}


class MissingTarget(RuntimeError):
    """One or more AIDP target coordinates were not supplied."""


@dataclass(frozen=True)
class Target:
    datalake_ocid: str
    workspace: str
    cluster_id: str
    catalog: str


def resolve_target(*, datalake_ocid: str | None = None, workspace: str | None = None,
                   cluster_id: str | None = None,
                   catalog: str | None = None) -> Target:
    supplied = {"datalake_ocid": datalake_ocid, "workspace": workspace,
                "cluster_id": cluster_id, "catalog": catalog}
    missing = [k for k, v in supplied.items() if not (v and str(v).strip())]
    if missing:
        raise MissingTarget(
            "AIDP target coordinates not supplied: " + ", ".join(sorted(missing)) +
            ". These are never read from the environment or a config file -- ask the "
            "user for them and pass them explicitly.")
    return Target(**{k: str(v).strip() for k, v in supplied.items()})


def region_from_ocid(ocid: str) -> str:
    parts = (ocid or "").split(".")
    if len(parts) < 5 or parts[0] != "ocid1":
        raise ValueError(f"malformed OCID: {ocid!r}")
    code = parts[3]
    if code not in REGIONS:
        raise ValueError(
            f"unmapped OCI region short-code {code!r}; add it to REGIONS rather than "
            "defaulting -- guessing a region would target the wrong tenancy")
    return REGIONS[code]
