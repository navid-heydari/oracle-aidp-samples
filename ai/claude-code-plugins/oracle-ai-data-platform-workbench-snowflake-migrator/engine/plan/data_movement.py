"""Data-movement options for the later phase. PLACEHOLDER -- presents, never acts.

This MVP moves no bytes. What it does is lay out the realistic ways bytes could
move later, with the trade-offs and the unknowns attached, so the choice is made
deliberately rather than defaulting to whichever path someone happened to build
first. The choice drives cost, wall-clock, and whether the migration can run
unattended, so it belongs to the customer.

`execute_transfer` exists only to refuse, loudly, and to name what would have to
be settled before any of this becomes real. Every option is marked
`proposal_only`.
"""
from __future__ import annotations

import datetime

__all__ = ["OPTIONS", "NotImplementedInMvp", "execute_transfer", "options_for",
           "record_choice"]


class NotImplementedInMvp(NotImplementedError):
    """Data movement is out of scope. This exists to refuse, not to defer quietly."""


OPTIONS: tuple[dict, ...] = (
    {
        "id": "A1_UNLOAD_OBJECT_STORAGE",
        "name": "Bulk unload to object storage, land as managed Delta",
        "catalog_type": "INTERNAL",
        "phase": ("historic",),
        "moves_bytes": True,
        "etl": ("Snowflake COPY INTO @stage as Parquet -> cloud object storage -> "
                "OCI Object Storage (Interconnect or replication) -> Spark read -> "
                "managed Delta in an INTERNAL catalog"),
        "pros": [
            "Parquet carries DECIMAL natively, so NUMBER(p,s) survives if the "
            "round trip is verified",
            "unloading to storage in the SAME cloud region avoids Snowflake "
            "egress charges",
            "lands as managed Delta, which is the only writable AIDP catalog type",
            "resumable and re-runnable per table, so it suits waves",
        ],
        "cons": [
            "the largest moving part: staging, parallelism, throttling and "
            "reconciliation all have to be built",
            "Snowflake has no native OCI external-stage target, so it implies an "
            "S3/Azure/GCS hop plus a transfer into OCI",
            "wall-clock at PB scale is measured in days, not hours",
        ],
        "unknowns": [
            "which region the source account is actually in -- this decides "
            "whether same-region unload and Interconnect apply at all",
            "whether NUMBER(p,s) survives unload -> Parquet -> Delta with exact "
            "precision (never yet measured)",
            "transfer throughput actually achievable into OCI Object Storage",
        ],
    },
    {
        "id": "A2_FEDERATE_EXTERNAL_CATALOG",
        "name": "Federate: read Snowflake in place through an EXTERNAL catalog",
        "catalog_type": "EXTERNAL",
        "phase": ("historic", "ongoing"),
        "moves_bytes": False,
        "etl": ("AIDP registers Snowflake as an EXTERNAL, read-only catalog over "
                "JDBC; queries read through. Copy only the subset that needs to be "
                "resident"),
        "pros": [
            "no bulk transfer, so no staging, no egress and no wall-clock risk",
            "available immediately once credentials exist",
            "good for a coexistence period, and for validating results against "
            "the source before anything is copied",
            "reduces the migration to only the objects that genuinely must move",
        ],
        "cons": [
            "EXTERNAL catalogs are READ-ONLY in AIDP: nothing can be written back, "
            "so this is not a destination",
            "Snowflake keeps running and keeps costing credits -- it defers spend "
            "rather than removing it",
            "predicate and aggregation pushdown is limited, so large scans pull "
            "rows across the wire repeatedly",
            "the native connector is read-only in AIDP 4.0, which rules out "
            "dual-write and write-back reconciliation",
        ],
        "unknowns": [
            "how much aggregation actually pushes down to Snowflake",
            "whether the target AIDP version's Snowflake connector supports the "
            "auth the customer can grant",
        ],
    },
    {
        "id": "A3_REDIRECT_INGESTION",
        "name": "Redirect ingestion at the source (Fivetran and pipelines)",
        "catalog_type": "BOTH",
        "phase": ("ongoing",),
        "moves_bytes": True,
        "etl": ("Stop feeding Snowflake for the migrated domains. Point Fivetran "
                "at an Oracle destination (ADW/ALH, Beta) or via Kafka -> OCI "
                "Streaming -> Spark consumer -> Delta. Historic data is handled "
                "separately by A1"),
        "pros": [
            "forward-only: new data arrives in AIDP from day one, so the copied "
            "history stops growing",
            "removes the double-write period that a long bulk migration implies",
            "reuses the existing ingestion tooling rather than replacing it",
        ],
        "cons": [
            "Fivetran has NO AIDP destination and no generic protocol destination; "
            "its object-storage destination is AWS-only",
            "the Oracle destination is Beta, so volume and object-type coverage "
            "need confirming",
            "the Kafka route means owning the sink -- more moving parts we operate",
            "solves nothing for historic data on its own",
        ],
        "unknowns": [
            "Fivetran Oracle-destination Beta limits at the customer's ingest rate",
            "whether OCI Streaming's SASL_SSL is compatible with Fivetran's Kafka "
            "destination",
        ],
    },
    {
        "id": "A4_ICEBERG_INTEROP",
        "name": "Iceberg interop: share storage instead of copying",
        "catalog_type": "EXTERNAL",
        "phase": ("historic", "ongoing"),
        "moves_bytes": False,
        "etl": ("Snowflake writes Iceberg tables to external object storage; AIDP "
                "reads the same files through an Iceberg catalog. No duplication "
                "for tables already in, or convertible to, Iceberg format"),
        "pros": [
            "no copy at all for Iceberg-format tables -- one set of files, two "
            "engines",
            "avoids the precision and fidelity risk of an unload round trip "
            "entirely",
            "a genuine coexistence story rather than a cutover",
        ],
        "cons": [
            "only applies to Iceberg tables; native Snowflake FDN tables must be "
            "converted first, which is itself a rewrite of the data",
            "storage stays wherever Snowflake writes it, so cross-cloud latency "
            "and egress may persist",
            "feature maturity on both sides needs verifying against the actual "
            "versions in play",
        ],
        "unknowns": [
            "what share of the estate is or could be Iceberg rather than native",
            "whether the target AIDP version can read Iceberg from the "
            "customer's storage account",
        ],
    },
    {
        "id": "A5_HYBRID_WAVES",
        "name": "Hybrid: federate first, copy selectively, redirect forward",
        "catalog_type": "BOTH",
        "phase": ("historic", "ongoing"),
        "moves_bytes": True,
        "etl": ("A2 for immediate read access and result validation, A1 for the "
                "objects usage data shows actually matter, A3 to stop the history "
                "growing. Iceberg (A4) wherever it already applies"),
        "pros": [
            "avoids an all-or-nothing cutover and lets value land before the bulk "
            "transfer finishes",
            "usage data decides what is worth copying, so the expensive path runs "
            "on the smallest possible set",
            "each wave is independently reversible",
        ],
        "cons": [
            "the most governance: two live systems, a moving boundary, and lineage "
            "spanning both",
            "needs the usage profile to be available, which needs ACCOUNT_USAGE",
            "hardest to explain to stakeholders",
        ],
        "unknowns": [
            "everything A1, A2 and A3 do not yet know, plus how long a coexistence "
            "window the business will accept",
        ],
    },
)

_PHASES = ("historic", "ongoing")
_BY_ID = {o["id"]: o for o in OPTIONS}

for _o in OPTIONS:                      # every option is a proposal, nothing more
    _o["status"] = "proposal_only"


def options_for(phase: str) -> list[dict]:
    if phase not in _PHASES:
        raise ValueError(f"unknown phase {phase!r}; expected one of {_PHASES}")
    return [o for o in OPTIONS if phase in o["phase"]]


def record_choice(option_id: str, *, chosen_by: str, rationale: str) -> dict:
    """Record which option the customer picked. Records only -- acts on nothing."""
    if option_id not in _BY_ID:
        raise ValueError(f"unknown option {option_id!r}; expected one of "
                         f"{sorted(_BY_ID)}")
    if not (rationale or "").strip():
        raise ValueError(
            "a rationale is required: this choice drives cost, wall-clock and "
            "whether the migration can run unattended, so an unexplained pick is "
            "not a decision")
    option = _BY_ID[option_id]
    return {
        "option_id": option_id,
        "option_name": option["name"],
        "chosen_by": chosen_by,
        "rationale": rationale,
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "executed": False,
        "unknowns_outstanding": list(option["unknowns"]),
        "next_step": ("Retire the unknowns above with a hand-run spike on one "
                      "representative table before any tooling is built. This "
                      "plugin does not implement data movement."),
    }


def execute_transfer(option_id: str, **_kwargs):
    """Always refuses. Data movement is out of scope for this plugin."""
    option = _BY_ID.get(option_id)
    unknowns = option["unknowns"] if option else [
        "the source region, and whether NUMBER(p,s) survives a round trip"]
    raise NotImplementedInMvp(
        f"{option_id}: data movement is not implemented and this plugin moves no "
        "bytes. Before it could, these must be settled: "
        + "; ".join(unknowns)
        + ". Record the chosen option with record_choice() and run a manual spike "
          "first.")
