"""Snowflake warehouse -> AIDP Spark cluster proposal. Pure, zero I/O.

Snowflake warehouse sizing is a clean doubling series: X-Small is 1 node and 1
credit/hour, and every step doubles both. That much is documented and safe to
encode.

What is NOT encoded is an exact OCI compute shape SKU. Shape availability varies
by tenancy and region, so every proposal carries
`shape_confirmation_required: True` and names a shape FAMILY rather than
asserting a SKU that may not exist in the customer's region.

Cost is only computed when the caller supplies a credit price. Snowflake credit
prices vary by edition and region; defaulting one would produce a number that
looks authoritative and is not.
"""
from __future__ import annotations

__all__ = ["CREDITS_PER_HOUR", "NODES_PER_SIZE", "propose_cluster", "propose_all"]

# Documented Snowflake series: each size doubles.
NODES_PER_SIZE = {
    "X-Small": 1, "Small": 2, "Medium": 4, "Large": 8, "X-Large": 16,
    "2X-Large": 32, "3X-Large": 64, "4X-Large": 128, "5X-Large": 256,
    "6X-Large": 512,
}
CREDITS_PER_HOUR = dict(NODES_PER_SIZE)

# A Snowflake "server" is ~8 vCPU / 16 GB. Spark workers are provisioned larger,
# so a worker absorbs several Snowflake nodes' worth of compute.
_VCPU_PER_SOURCE_NODE = 8
_MEM_GB_PER_SOURCE_NODE = 16
_VCPU_PER_WORKER = 16
_SHAPE_FAMILY = "VM.Standard.E5.Flex (or the tenancy's current standard flex family)"


def propose_cluster(warehouse: dict) -> dict:
    """Propose a standard-sized Spark cluster for one warehouse."""
    size = warehouse.get("size")
    if size not in NODES_PER_SIZE:
        return {"name": warehouse.get("name"), "blocked": True,
                "reason": f"unrecognised Snowflake warehouse size {size!r}; "
                          "refusing to guess a compute equivalent"}

    nodes = NODES_PER_SIZE[size]
    vcpu = nodes * _VCPU_PER_SOURCE_NODE
    workers = max(1, vcpu // _VCPU_PER_WORKER)
    max_clusters = max(1, int(warehouse.get("max_cluster_count") or 1))

    return {
        "name": warehouse.get("name"),
        "blocked": False,
        "source_size": size,
        "source_nodes": nodes,
        "source_vcpu_equivalent": vcpu,
        "source_memory_gb_equivalent": nodes * _MEM_GB_PER_SOURCE_NODE,
        "driver_shape_family": _SHAPE_FAMILY,
        "worker_shape_family": _SHAPE_FAMILY,
        "worker_ocpus": _VCPU_PER_WORKER // 2,   # OCPU = 2 vCPU on x86 flex shapes
        "worker_count": workers,
        "autoscale_min_workers": workers,
        "autoscale_max_workers": workers * max_clusters,
        "auto_suspend_seconds": warehouse.get("auto_suspend_seconds"),
        "shape_confirmation_required": True,
        "notes": (
            f"{size} = {nodes} Snowflake node(s) ~= {vcpu} vCPU. Proposed "
            f"{workers} worker(s); autoscale max {workers * max_clusters} to cover "
            f"max_cluster_count={max_clusters}. Confirm the shape family and OCPU "
            "availability against the target tenancy and region before use."),
    }


def propose_all(warehouses: list[dict], *,
                credit_price_usd: float | None = None) -> dict:
    proposals, blocked = [], []
    for wh in warehouses:
        p = propose_cluster(wh)
        (blocked if p.get("blocked") else proposals).append(p)

    observed = [w.get("observed_credits") for w in warehouses
                if w.get("observed_credits") is not None]
    credits_total = sum(observed) if observed else None
    days = next((w.get("observed_days") for w in warehouses
                 if w.get("observed_credits") is not None), None)

    cost_model, cost_note = None, ""
    if credit_price_usd is None:
        cost_note = ("No cost model: a credit price was not supplied. Snowflake "
                     "credit prices vary by edition and region, so one is never "
                     "assumed. Pass --credit-price to compute spend.")
    elif credits_total is None:
        cost_note = ("No cost model: credit consumption was not observable "
                     "(ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY unavailable), so "
                     "there is nothing to price.")
    else:
        per_day = credits_total / days if days else credits_total
        cost_model = {
            "credit_price_usd": credit_price_usd,
            "observed_credits": credits_total,
            "observed_days": days,
            "snowflake_monthly_usd": round(per_day * 30 * credit_price_usd, 2),
            "snowflake_annual_usd": round(per_day * 365 * credit_price_usd, 2),
            "basis": "observed WAREHOUSE_METERING_HISTORY",
            "aidp_comparison": (
                "AIDP cost depends on the confirmed cluster shapes and their "
                "running hours, which cannot be derived from Snowflake metering "
                "alone. Price the proposed shapes against the tenancy rate card "
                "once the shape family is confirmed."),
        }

    return {
        "warehouse_count": len(warehouses),
        "max_concurrent_clusters": sum(
            max(1, int(w.get("max_cluster_count") or 1)) for w in warehouses),
        "total_source_nodes": sum(
            NODES_PER_SIZE.get(w.get("size"), 0) for w in warehouses),
        "observed_credits_total": credits_total,
        "credits_basis": "observed" if observed else "declared_size_only",
        "cost_model": cost_model,
        "cost_note": cost_note,
        "proposals": proposals,
        "blocked": blocked,
    }
