"""Names AIDP accepts, derived deterministically from OCI-DI names.

AIDP job names must start with a letter and contain only letters, digits and
underscores (live create-job refusal, recorded by the Fabric migrator,
2026-09). Delta identifiers are emitted lower-case and unquoted-safe so the
generated SQL never needs backticks for a table name.
"""
from __future__ import annotations

import re

JOB_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_NOT_WORD = re.compile(r"[^A-Za-z0-9_]+")


def job_name(name: str, prefix: str = "") -> str:
    cleaned = _NOT_WORD.sub("_", str(name or "")).strip("_") or "job"
    if not cleaned[0].isalpha():
        cleaned = "job_" + cleaned
    if prefix:
        cleaned = f"{prefix.strip('_')}_{cleaned}"
    return cleaned


def task_key(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(name or "")).strip("_")
    return cleaned or "task"


def ident(name: str) -> str:
    """A Delta schema/table/column-safe lower-case identifier."""
    cleaned = _NOT_WORD.sub("_", str(name or "")).strip("_").lower() or "x"
    if cleaned[0].isdigit():
        cleaned = "t_" + cleaned
    return cleaned


def py_ident(name: str) -> str:
    """A Python identifier fragment for a DataFrame variable."""
    cleaned = _NOT_WORD.sub("_", str(name or "")).strip("_") or "node"
    if cleaned[0].isdigit():
        cleaned = "n_" + cleaned
    return cleaned


def file_stem(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name or "")).strip("_.") or "object"
    return cleaned


def unique(base: str, taken: set) -> str:
    candidate, n = base, 2
    while candidate in taken:
        candidate = f"{base}_{n}"
        n += 1
    taken.add(candidate)
    return candidate
