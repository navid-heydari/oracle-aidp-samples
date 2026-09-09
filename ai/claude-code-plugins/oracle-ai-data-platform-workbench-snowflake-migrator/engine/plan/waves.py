"""Topological layering by Kahn's algorithm. Pure functions, zero I/O.

Edge {"from": dependent, "to": dependency} means `from` needs `to` to exist
first, so dependencies land in earlier waves.

Cycles are REPORTED, never broken by picking an arbitrary edge to drop. Objects
in a cycle are excluded from the waves and surfaced for a human decision.
"""
from __future__ import annotations

import collections
from typing import Callable

__all__ = ["compute_waves"]


def compute_waves(nodes: list[str], edges: list[dict],
                  sort_key: Callable[[str], tuple] | None = None) -> dict:
    node_set = set(nodes)
    key = sort_key or (lambda n: (n,))

    dependents: dict[str, set[str]] = collections.defaultdict(set)
    indegree: dict[str, int] = {n: 0 for n in node_set}
    seen: set[tuple[str, str]] = set()

    for edge in edges:
        dependent, dependency = edge["from"], edge["to"]
        if dependent not in node_set or dependency not in node_set:
            continue
        if dependent == dependency or (dependent, dependency) in seen:
            continue
        seen.add((dependent, dependency))
        dependents[dependency].add(dependent)
        indegree[dependent] += 1

    waves: list[list[str]] = []
    ready = sorted((n for n in node_set if indegree[n] == 0), key=key)
    placed: set[str] = set()

    while ready:
        waves.append(ready)
        placed.update(ready)
        nxt: list[str] = []
        for node in ready:
            for dependent in dependents[node]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    nxt.append(dependent)
        ready = sorted(nxt, key=key)

    stuck = node_set - placed
    cycles = [sorted(stuck)] if stuck else []
    return {"waves": waves, "cycles": cycles}
