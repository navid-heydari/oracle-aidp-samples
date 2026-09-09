"""Per-object migration status and migration risk. Pure, zero I/O.

The status vocabulary is deliberately closed, and two of its values --
DATA_CLONE and DONE -- are UNREACHABLE in this version. The plugin moves no data,
so no code path may report that it did. They exist so the vocabulary does not
have to change when a data phase is added.
"""
from __future__ import annotations

__all__ = ["MIGRATION_STATUS", "RISK_LEVELS", "assess_risk", "migration_status"]

MIGRATION_STATUS = ("NOT_YET_DONE", "IN_PROGRESS", "SHALLOW_CLONE",
                    "DATA_CLONE", "DONE", "BLOCKED")
RISK_LEVELS = ("LOW", "MEDIUM", "HIGH")

# Above this, the later data phase needs a wave/staging plan of its own.
_LARGE_ROWS = 100_000_000


def migration_status(identifier: str, *, deployed: dict | None,
                     blocked: bool = False) -> str:
    if blocked:
        return "BLOCKED"
    if not deployed or deployed.get("dry_run"):
        return "NOT_YET_DONE"
    if identifier in set(deployed.get("verified_targets") or []):
        # Structure only. DATA_CLONE/DONE are never returned here: this plugin
        # copies no rows, and claiming otherwise would be a false report.
        return "SHALLOW_CLONE"
    if identifier in set(deployed.get("attempted_targets") or []):
        return "IN_PROGRESS"
    return "NOT_YET_DONE"


def assess_risk(obj: dict, *, blocked: bool = False) -> tuple[str, str]:
    """Return (level, one-sentence note) for one object."""
    if blocked:
        reason = obj.get("reason") or "cannot be migrated"
        return "HIGH", f"Cannot migrate: {reason}"

    notes: list[str] = []
    level = "LOW"

    if obj.get("object_type") == "VIEW":
        level = "MEDIUM"
        notes.append("view SQL is carried over without dialect translation; "
                     "verify its result against the source")

    omitted = obj.get("omitted_properties") or []
    if omitted:
        level = "MEDIUM"
        notes.append("source properties dropped with no Delta equivalent: "
                     + ", ".join(omitted))

    warnings = obj.get("warnings") or []
    tz = [w for w in warnings if "timezone" in w.lower()]
    if tz:
        level = "MEDIUM"
        notes.append("timezone semantics differ for one or more columns")
    other = [w for w in warnings if w not in tz]
    if other:
        level = "MEDIUM"
        notes.append(f"{len(other)} column warning(s) recorded")

    rows = obj.get("rows")
    if rows is not None and rows >= _LARGE_ROWS:
        level = "MEDIUM"
        notes.append(f"{rows:,} rows: the later data phase will need its own "
                     "staging and wave plan")

    if not notes:
        notes.append("all column types mapped, no properties dropped; structure "
                     "clones cleanly")
    return level, ". ".join(n[0].upper() + n[1:] for n in notes) + "."
