"""Medallion mapping. Pure functions, zero I/O.

BRONZE IS A MIRROR, NOT AN ASSIGNMENT. The required mapping is structural:

    Snowflake database  ->  AIDP Standard Catalog
    Snowflake schema    ->  AIDP schema
    Snowflake table     ->  AIDP table
    Snowflake view      ->  AIDP view

So Bronze is an identity transform on the three-part name. AIDP supports
three-level namespaces, so nothing has to be flattened and nothing can collide --
which is why there is no layer-assignment heuristic here any more. An earlier
design assigned each object a layer by name matching; that was wrong for this
requirement and produced a `bronze.<schema>.<table>` shape that silently dropped
the source database.

SILVER AND GOLD ARE REQUIREMENT-DRIVEN. Their content depends on transformations
nobody has specified yet, so this module does not invent any. It emits one job
per layer per source schema, created but **never triggered**, as the place that
logic will later live. Fabricating transformation SQL would ship logic no one
asked for and no one can review.
"""
from __future__ import annotations

import collections
import re

__all__ = ["LAYERS", "UnknownStrategy", "bronze_target", "layer_jobs",
           "detect_target_collisions"]

LAYERS = ("BRONZE", "SILVER", "GOLD")

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class UnknownStrategy(ValueError):
    """An identifier or option is not usable as an AIDP catalog/schema name."""


def bronze_target(source_db: str, source_schema: str, object_name: str, *,
                  catalog_prefix: str | None = None) -> str:
    """Bronze target name for a source object.

    Default (`catalog_prefix=None`) is the required 1:1 mirror:
    `<database>.<schema>.<object>`, i.e. database becomes the Standard Catalog.

    `catalog_prefix` supports deployments that want ONE bronze catalog instead of
    catalog-per-database; the source database is then folded into the schema name
    so distinct databases still cannot merge.
    """
    for part in (source_db, source_schema, object_name):
        if not part or not str(part).strip():
            raise UnknownStrategy(f"empty name component in "
                                  f"{source_db!r}.{source_schema!r}.{object_name!r}")
    if catalog_prefix is None:
        return f"{source_db}.{source_schema}.{object_name}"
    if not _SAFE_IDENT.match(catalog_prefix):
        raise UnknownStrategy(
            f"catalog_prefix {catalog_prefix!r} is not a valid identifier")
    return f"{catalog_prefix}.{source_db}_{source_schema}.{object_name}"


def layer_jobs(scopes: list[tuple[str, str]]) -> list[dict]:
    """One SILVER and one GOLD job per (database, schema) scope.

    Created, disabled, and never triggered. The body is an explicit placeholder:
    what belongs there is a requirement, not something to guess.
    """
    jobs: list[dict] = []
    for db, schema in scopes:
        for layer in ("SILVER", "GOLD"):
            jobs.append({
                "name": f"{layer.lower()}_{db}_{schema}",
                "layer": layer,
                "source_database": db,
                "source_schema": schema,
                "reads_from": f"{db}.{schema}",
                "enabled": False,
                "schedule": None,
                "trigger": "MANUAL_NEVER_TRIGGERED",
                "body_status": "placeholder",
                "body_note": (
                    f"{layer} transformation logic is a requirement to be defined "
                    "with the customer. This job is created as the place it will "
                    "live; it is disabled and is never triggered by the migrator."),
            })
    return jobs


def detect_target_collisions(mapping: dict[str, str]) -> dict[str, list[str]]:
    """Two source objects mapping to one target name. Empty result means safe.

    Bronze cannot collide by construction, but this still runs: a
    `catalog_prefix` deployment, or a source estate with case-variant names, can
    produce one. A collision halts rather than being resolved by guessing.
    """
    buckets: dict[str, list[str]] = collections.defaultdict(list)
    for source, target in mapping.items():
        buckets[target.upper()].append(source)
    return {mapping[v[0]]: sorted(v) for v in buckets.values() if len(v) > 1}
