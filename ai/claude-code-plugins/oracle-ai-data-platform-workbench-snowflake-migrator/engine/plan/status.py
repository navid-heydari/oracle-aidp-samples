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
    if identifier in set(deployed.get("mismatched_targets") or []):
        # Present in AIDP, but not the object we planned -- and the DDL is
        # CREATE IF NOT EXISTS, so it was left exactly as it was found. This is
        # BLOCKED, not cloned: something else owns that name.
        return "BLOCKED"
    if identifier in set(deployed.get("verified_targets") or []):
        # Structure only. DATA_CLONE/DONE are never returned here: this plugin
        # copies no rows, and claiming otherwise would be a false report.
        return "SHALLOW_CLONE"
    if identifier in set(deployed.get("derived_type_drift_targets") or []):
        # The view exists and is ours; the target derived some column types
        # from the SQL rather than taking ours. Structure-cloned, with a
        # fidelity caveat -- not BLOCKED, and not silently clean either.
        return "SHALLOW_CLONE"
    if identifier in set(deployed.get("unverified_structure_targets") or []):
        # It exists, but its columns were never compared, so "cloned" is not a
        # claim we have earned.
        return "IN_PROGRESS"
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
        # HIGH, not MEDIUM: an untranslated or mistranslated view CREATES
        # SUCCESSFULLY and then returns wrong numbers. A loud failure would be
        # safer than this, so it gets the higher level.
        level = "HIGH"
        notes.append("view SQL is carried over without full dialect "
                     "translation; it will create successfully even if the "
                     "semantics differ, so verify its result against the "
                     "source before anyone relies on it")

    omitted = obj.get("omitted_properties") or []
    if omitted:
        level = "MEDIUM"
        notes.append("source properties dropped with no AIDP equivalent: "
                     + ", ".join(omitted))

    deferred = obj.get("deferred_properties") or []
    if deferred:
        level = "MEDIUM"
        notes.append(
            "source maintenance/layout settings not applied on the target: "
            + ", ".join(f'{d["property"]}={d["value"]}' for d in deferred))

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
