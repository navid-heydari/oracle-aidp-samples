"""Medallion layer assignment and target namespace strategies. Pure.

"Generic unless it was provided": a generic name heuristic decides the layer by
default, and an explicitly-supplied mapping overrides it. An object that matches
nothing falls back to BRONZE and is REPORTED as a fallback -- never silently
assigned -- so the user can correct it before anything is created.

Strategy is a parameter, not a structural commitment, so the naming decision is
not a one-way door.
"""
from __future__ import annotations

import collections

__all__ = ["LAYERS", "STRATEGIES", "UnknownStrategy", "assign_layer",
           "detect_target_collisions", "target_name"]

LAYERS = ("BRONZE", "SILVER", "GOLD")
STRATEGIES = ("layer-catalog", "preserve-source", "layer-flattened")

# Order matters: GOLD and SILVER are checked before BRONZE so that a name like
# "gold_staging" resolves to GOLD rather than matching BRONZE's "stg".
_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("GOLD", ("gold", "mart", "dm_", "datamart", "reporting", "report")),
    ("SILVER", ("silver", "curated", "clean", "conformed")),
    ("BRONZE", ("bronze", "raw", "stg", "staging", "landing")),
)


class UnknownStrategy(ValueError):
    """Namespace strategy is not one of STRATEGIES."""


def _match(text: str) -> str | None:
    low = text.lower()
    for layer, needles in _RULES:
        if any(n in low for n in needles):
            return layer
    return None


def assign_layer(source_db: str, source_schema: str,
                 user_map: dict[str, str] | None = None, *,
                 source_identifier: str | None = None) -> tuple[str, str]:
    if user_map:
        candidates = []
        if source_identifier:
            candidates += [source_identifier, source_identifier.upper()]
        candidates += [f"{source_db}.{source_schema}", source_db]
        for cand in candidates:
            if cand in user_map:
                layer = user_map[cand].upper()
                if layer not in LAYERS:
                    raise ValueError(
                        f"invalid layer {user_map[cand]!r} for {cand!r}; "
                        f"expected one of {LAYERS}")
                return layer, "user_provided"

    for text in (source_db, source_schema):
        hit = _match(text)
        if hit:
            return hit, "matched_rule"
    return "BRONZE", "fallback"


def target_name(source_db: str, source_schema: str, object_name: str,
                layer: str, strategy: str) -> str:
    if strategy not in STRATEGIES:
        raise UnknownStrategy(
            f"unknown namespace strategy {strategy!r}; expected {STRATEGIES}")
    if strategy == "preserve-source":
        return f"{source_db}.{source_schema}.{object_name}"
    if strategy == "layer-catalog":
        return f"{layer.lower()}.{source_schema}.{object_name}"
    return f"{layer.lower()}.{source_db}_{source_schema}.{object_name}"


def detect_target_collisions(mapping: dict[str, str]) -> dict[str, list[str]]:
    """Two source objects mapping to one target name. Empty result means safe."""
    buckets: dict[str, list[str]] = collections.defaultdict(list)
    for source, target in mapping.items():
        buckets[target.upper()].append(source)
    return {mapping[v[0]]: sorted(v) for v in buckets.values() if len(v) > 1}
