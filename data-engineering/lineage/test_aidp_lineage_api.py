"""
Conformance tests for the AIDP Semantic Catalog **lineage** API.

Purpose: prove, from live responses, that the lineage feature is actually released and
reachable in this tenancy — and separately, report honestly whether the lineage *graph*
is populated for our own tables.

The suite is deliberately split:

  Part A — API EXISTENCE (must be green)
      Proves the endpoint is deployed, authenticated, and enforcing its documented
      request contract. This is the part that answers "is lineage released?".

  Part B — GRAPH POPULATION (xfail = known open gap)
      Proves whether lineage *data* exists for our tables. These are marked xfail
      because no `anchorNode` value is currently accepted. They are NOT deleted or
      skipped: if lineage becomes populated (or Oracle documents the id format) they
      flip to XPASS and the suite tells you the gap closed.

Run:
    pytest test_aidp_lineage_api.py -v
    pytest test_aidp_lineage_api.py -v -m existence     # just the proof-of-release
    pytest test_aidp_lineage_api.py -v -rX              # show why Part B is blocked

Requires: oci, requests, pytest  ·  a working ~/.oci/config profile.
"""
import json
import os

import oci
import pytest
import requests

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
PROFILE = os.environ.get("AIDP_PROFILE", "DEFAULT")
REGION = os.environ.get("AIDP_REGION", "us-ashburn-1")
DATALAKE = os.environ.get(
    "AIDP_DATALAKE",
    "ocid1.aidataplatform.oc1.iad.amaaaaaau3k3eoaa3ww7q3tq6gc7lqo63leue6tgv2xitc7rz3oqnvqdfvgq",
)
SCHEMA_KEY = os.environ.get("AIDP_SCHEMA", "default.lin_demo")
ANCHOR_TABLE = os.environ.get("AIDP_ANCHOR_TABLE", "default.lin_demo.mart_customer_revenue")

# The lineage API lives on the DATA-PLANE host at API version 20260430 — NOT on
# aidp.{region}.../20240831, which is the older generation with no lineage surface.
DP_HOST = "https://datalake.%s.oci.oraclecloud.com" % REGION
DP_VERSION = "20260430"
DP_BASE = "%s/%s/aiDataPlatforms/%s" % (DP_HOST, DP_VERSION, DATALAKE)

LEGACY_BASE = "https://aidp.%s.oci.oraclecloud.com/20240831/dataLakes/%s" % (REGION, DATALAKE)

TIMEOUT = 60


# --------------------------------------------------------------------------------------
# Signed-request helpers
# --------------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def signer():
    cfg = oci.config.from_file(profile_name=PROFILE)
    oci.config.validate_config(cfg)
    return oci.signer.Signer(
        tenancy=cfg["tenancy"],
        user=cfg["user"],
        fingerprint=cfg["fingerprint"],
        private_key_file_location=cfg["key_file"],
        pass_phrase=cfg.get("pass_phrase"),
    )


def _req(signer, method, url, body=None):
    """Signed request -> (status_code, parsed_json_or_text)."""
    kwargs = {"timeout": TIMEOUT, "auth": signer}
    if body is not None:
        kwargs["data"] = json.dumps(body)
        kwargs["headers"] = {"Content-Type": "application/json"}
    r = requests.request(method, url, **kwargs)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, r.text


def fetch_lineage(signer, **overrides):
    """POST actions/fetchLineage with a valid-by-schema body."""
    body = {
        "anchorNode": ANCHOR_TABLE,
        "maxDepth": 3,
        "level": "ENTITY",
        "direction": "BOTH",
        "shouldIncludeEdges": True,
    }
    body.update(overrides)
    body = {k: v for k, v in body.items() if v is not _OMIT}
    return _req(signer, "POST", DP_BASE + "/actions/fetchLineage", body)


class _Omit:
    def __repr__(self):
        return "<omitted>"


_OMIT = _Omit()


# ======================================================================================
# Part A — API EXISTENCE.  These prove the lineage feature is released and live.
# ======================================================================================
pytestmark = []


@pytest.mark.existence
def test_A0_auth_works_on_dataplane_host(signer):
    """Baseline: we are authenticated on the data-plane host.

    Without this, a 4xx from the lineage route would be ambiguous (auth vs route).
    """
    code, body = _req(signer, "GET", DP_BASE + "/catalogs?limit=1")
    assert code == 200, "expected 200 listing catalogs, got %s: %s" % (code, body)


@pytest.mark.existence
def test_A1_fetchLineage_route_exists(signer):
    """POST actions/fetchLineage is DEPLOYED.

    A deployed-but-validating route answers 400 InvalidParameter. A missing route
    answers 404 NotAuthorizedOrNotFound (see test_A2 for the control).
    """
    code, body = fetch_lineage(signer)
    assert code != 404, "lineage route missing (404): %s" % body
    assert code == 400, "expected 400 from body validation, got %s: %s" % (code, body)
    assert body.get("code") == "InvalidParameter", body
    # It reached the operation's own parameter validation — proof of a real handler.
    assert "anchorNode" in body.get("message", ""), body


@pytest.mark.existence
def test_A2_control_bogus_action_is_404(signer):
    """Control that gives test_A1 its meaning.

    A nonexistent sibling action under the same base returns 404, so A1's 400 is a
    genuine discrimination between "route exists" and "route absent" — not an artifact
    of how this service reports errors.
    """
    code, body = _req(
        signer, "POST", DP_BASE + "/actions/zzzNotARealLineageAction", {"anchorNode": "x"}
    )
    assert code == 404, "expected 404 for a bogus action, got %s: %s" % (code, body)


@pytest.mark.existence
def test_A3_exportLineage_route_exists(signer):
    """The second documented lineage operation (CSV export) is also deployed."""
    code, body = _req(
        signer,
        "POST",
        DP_BASE + "/actions/exportLineage",
        {"anchorNode": ANCHOR_TABLE, "direction": "UPSTREAM"},
    )
    assert code != 404, "exportLineage route missing (404): %s" % body
    assert code == 400 and body.get("code") == "InvalidParameter", (code, body)


@pytest.mark.existence
def test_A4_request_contract_is_enforced_server_side(signer):
    """The server enforces the documented schema, distinguishing missing from invalid.

    Omitting anchorNode yields a *different* message than supplying a bad one. Only a
    real implementation of this operation can make that distinction.
    """
    code_missing, body_missing = fetch_lineage(signer, anchorNode=_OMIT)
    assert code_missing == 400, (code_missing, body_missing)
    assert "must not be null" in body_missing.get("message", ""), body_missing

    code_bad, body_bad = fetch_lineage(signer, anchorNode="definitely-not-a-node")
    assert code_bad == 400, (code_bad, body_bad)
    assert body_bad.get("message") == "Invalid anchorNode", body_bad

    assert body_missing["message"] != body_bad["message"], (
        "server must distinguish missing from invalid anchorNode"
    )


@pytest.mark.existence
@pytest.mark.parametrize(
    "field,value",
    [
        ("level", "COLUMN"),          # column-level lineage is a released capability
        ("level", "ENTITY"),
        ("direction", "UPSTREAM"),
        ("direction", "DOWNSTREAM"),
        ("direction", "BOTH"),
    ],
)
def test_A5_documented_enums_are_accepted(signer, field, value):
    """Documented enum values pass schema validation.

    Each reaches anchorNode resolution ("Invalid anchorNode") rather than being rejected
    as a bad enum — so the server implements these options, including level=COLUMN.
    """
    code, body = fetch_lineage(signer, **{field: value})
    assert code == 400, (code, body)
    assert body.get("message") == "Invalid anchorNode", (
        "%s=%s was rejected before anchor resolution: %s" % (field, value, body)
    )


@pytest.mark.existence
def test_A6_invalid_enum_is_rejected(signer):
    """Complement to A5: a bogus enum fails *earlier* than anchor resolution.

    This proves A5 is meaningful — the server really parses these fields rather than
    ignoring them and always complaining about anchorNode.
    """
    code, body = fetch_lineage(signer, direction="SIDEWAYS")
    assert code == 400, (code, body)
    assert body.get("message") != "Invalid anchorNode", (
        "bogus enum should be rejected as an enum, not fall through to anchor: %s" % body
    )


@pytest.mark.existence
def test_A7_lineage_absent_from_legacy_api_generation(signer):
    """Documents *why* lineage looks missing if you probe the old surface.

    The previous generation (aidp.{region} + /20240831/dataLakes) has no lineage route,
    while still serving /catalogs. Anyone concluding "AIDP has no lineage API" from that
    host is probing a generation behind.
    """
    code_cat, _ = _req(signer, "GET", LEGACY_BASE + "/catalogs")
    assert code_cat == 200, "legacy base should still serve catalogs (%s)" % code_cat

    code_lin, _ = _req(signer, "GET", LEGACY_BASE + "/lineage")
    assert code_lin == 404, "legacy generation unexpectedly has /lineage (%s)" % code_lin


# ======================================================================================
# Part B — GRAPH POPULATION.  Known open gap: no anchorNode value is accepted yet.
# ======================================================================================
ANCHOR_CANDIDATES = [
    "default.lin_demo.mart_customer_revenue",           # table key (as ListTables returns)
    "mart_customer_revenue",                            # bare display name
    "hive.lin_demo.mart_customer_revenue",              # catalogGuid-qualified
    "default.cat/lin_demo.db/mart_customer_revenue",    # storage-style path
    "TABLE:default.lin_demo.mart_customer_revenue",     # type-prefixed
    DATALAKE,                                           # the platform OCID itself
]

BLOCKED = (
    "No accepted anchorNode format known: every candidate returns "
    "400 'Invalid anchorNode'. Either the lineage graph is not populated for this "
    "DataLake, or the node-id format is undocumented (CLI docs leave anchorNode's "
    "description empty). Open question for Oracle."
)


@pytest.mark.population
def test_B0_report_all_anchor_candidates(signer):
    """Diagnostic, always green: records what every candidate id form returns.

    This is the evidence to hand Oracle, and the tripwire that shows the moment any
    form starts resolving.
    """
    results = {}
    for cand in ANCHOR_CANDIDATES:
        code, body = fetch_lineage(signer, anchorNode=cand)
        msg = body.get("message") if isinstance(body, dict) else str(body)[:80]
        results[cand] = (code, msg)

    print("\n  anchorNode candidate probe:")
    for cand, (code, msg) in results.items():
        print("    %-52s -> %s %s" % (cand[:52], code, msg))

    accepted = [c for c, (code, _) in results.items() if code == 200]
    if accepted:
        print("\n  *** an anchorNode form is NOW ACCEPTED: %s ***" % accepted)
    # Always passes: this test reports, it does not gate.
    assert results, "expected probe results"


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B1_entity_lineage_returns_graph(signer):
    """Entity-level lineage for a known table returns nodes (and edges)."""
    code, body = fetch_lineage(signer, level="ENTITY", direction="BOTH")
    assert code == 200, "fetchLineage failed: %s %s" % (code, body)
    assert "nodes" in body, "EntityLineage must carry nodes: %s" % body
    assert body["nodes"], "lineage graph is empty for %s" % ANCHOR_TABLE


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B2_upstream_contains_expected_sources(signer):
    """UPSTREAM of the mart reaches stg_orders and raw_customers.

    Mirrors the DAG asserted from the Spark plan in Verify_Data_Lineage.ipynb, so a green
    run here means the platform's own graph agrees with what actually executed.
    """
    code, body = fetch_lineage(signer, level="ENTITY", direction="UPSTREAM", maxDepth=3)
    assert code == 200, (code, body)
    names = {n.get("qualifiedName") or n.get("displayName") for n in body.get("nodes", [])}
    assert any("stg_orders" in (n or "") for n in names), names
    assert any("raw_customers" in (n or "") for n in names), names


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B3_column_level_lineage_returns_links(signer):
    """COLUMN-level lineage returns column nodes and edges.

    Target claim: mart.revenue traces back to stg_orders.amount.
    """
    code, body = fetch_lineage(
        signer, level="COLUMN", direction="UPSTREAM", maxDepth=3, shouldIncludeEdges=True
    )
    assert code == 200, (code, body)
    assert body.get("links"), "expected column-level edges: %s" % body


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B4_export_lineage_returns_csv(signer):
    """exportLineage returns a CSV document for the anchor."""
    code, body = _req(
        signer,
        "POST",
        DP_BASE + "/actions/exportLineage",
        {"anchorNode": ANCHOR_TABLE, "direction": "UPSTREAM"},
    )
    assert code == 200, (code, body)
    assert isinstance(body, str) and "," in body, "expected CSV text, got: %r" % (body,)
