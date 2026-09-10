"""Connectivity and permission smoke test across both ends.

Source needs READ. Destination needs READ and, to be proven, WRITE.

Proving write means actually writing, so the probe creates a clearly-named
schema at the DESTINATION and then removes it again.

The no-DROP rule is a SOURCE guarantee -- nothing is ever written to or dropped
from Snowflake, whatever the credential permits. It does not extend to AIDP,
which is where this plugin legitimately creates objects, so cleaning up its own
probe schema there is correct rather than forbidden. An earlier version applied
the source rule to the destination and therefore could not clean up, which is
why the probe had to stay off.

It is still opt-in, because it writes. The DROP names exactly one constant
schema, is never CASCADE, and is skipped if the schema was already there -- a
pre-existing schema is not ours to remove. If cleanup fails, the report names
what was left.

Both ends take an injected run_sql, so this is unit-testable with no connection.
Every check captures its own failure: one denied privilege should not hide the
result of the others.
"""
from __future__ import annotations

__all__ = ["PROBE_SCHEMA", "run_smoke"]

PROBE_SCHEMA = "snowmig_permission_probe"


def _like_literal(name: str) -> str:
    """Escape LIKE wildcards. The probe name is full of `_`, which matches any
    single character, so an unescaped pattern is not the name."""
    return (name.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
                .replace("'", "\\'"))

# Excluded when auto-picking a database to probe: their INFORMATION_SCHEMA is
# not representative of the customer's own objects.
_SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})


def _check(name: str, fn) -> dict:
    try:
        detail = fn()
        return {"name": name, "ok": True, "detail": detail or "ok"}
    except Exception as exc:
        return {"name": name, "ok": False, "detail": str(exc)[:300]}


def run_smoke(*, source_run_sql, target=None, dest_run_sql=None,
              write_probe: bool = False, database: str | None = None) -> dict:
    source: dict = {"reachable": False, "checks": []}
    try:
        ident = source_run_sql(
            "select current_user() U, current_account() A, current_region() R, "
            "current_role() ROLE")[0]
        source.update(reachable=True, user=ident.get("U"), account=ident.get("A"),
                      region=ident.get("R"), role=ident.get("ROLE"))
    except Exception as exc:
        source["error"] = str(exc)[:300]
        return {"ok": False, "source": source,
                "destination": {"skipped": True,
                                "reason": "source is unreachable"}}

    # INFORMATION_SCHEMA must be QUALIFIED: a fresh session has no current
    # database, so an unqualified reference fails with 090105 even for
    # ACCOUNTADMIN. Probe a real database rather than relying on session state.
    def _probe_database() -> str:
        if database:
            return database
        for row in source_run_sql("show databases"):
            if row["name"] not in _SYSTEM_DBS:
                return row["name"]
        raise RuntimeError("no non-system database is visible to this role")

    def _read_information_schema() -> str:
        db = _probe_database()
        count = source_run_sql(
            f'select count(*) N from "{db}".information_schema.tables')[0]["N"]
        return f"{count} table(s) readable in {db}.INFORMATION_SCHEMA"

    source["checks"] = [
        _check("list databases",
               lambda: f'{len(source_run_sql("show databases"))} database(s) visible'),
        _check("read INFORMATION_SCHEMA", _read_information_schema),
    ]

    destination: dict
    if target is None or dest_run_sql is None:
        destination = {
            "skipped": True,
            "reason": ("AIDP target coordinates were not supplied, so the "
                       "destination was not checked"),
            "checks": [], "write_verified": False,
            "write_note": "not verified: no target supplied", "left_behind": []}
    else:
        destination = {"skipped": False, "checks": [], "write_verified": False,
                       "left_behind": [],
                       "catalog": target.catalog, "cluster_id": target.cluster_id}
        destination["checks"].append(_check(
            "read target catalog",
            lambda: f'{len(dest_run_sql(f"SHOW SCHEMAS IN `{target.catalog}`"))} '
                    "schema(s) visible"))

        if not write_probe:
            destination["write_note"] = (
                "not verified: the write probe is opt-in because it writes. It "
                "creates one schema and removes it again. Re-run with "
                "--write-probe to prove write access.")
        else:
            probe_fqn = f"{target.catalog}.{PROBE_SCHEMA}"
            pre_existing = False
            try:
                # Already there? Then it is not ours to remove afterwards.
                pre_existing = bool(dest_run_sql(
                    f"SHOW SCHEMAS IN `{target.catalog}` "
                    f"LIKE '{_like_literal(PROBE_SCHEMA)}'"))
                dest_run_sql(f"CREATE SCHEMA IF NOT EXISTS "
                             f"`{target.catalog}`.`{PROBE_SCHEMA}`")
                visible = dest_run_sql(
                    f"SHOW SCHEMAS IN `{target.catalog}` "
                    f"LIKE '{_like_literal(PROBE_SCHEMA)}'")
                if visible:
                    destination["write_verified"] = True
                    destination["checks"].append(
                        {"name": "write probe schema", "ok": True,
                         "detail": f"{probe_fqn} created and visible"})
                    if pre_existing:
                        destination["left_behind"] = []
                        destination["write_note"] = (
                            f"verified: {probe_fqn} was already present, so it "
                            f"was left alone rather than removed.")
                    else:
                        # Exactly one constant schema, never CASCADE.
                        try:
                            dest_run_sql(
                                f"DROP SCHEMA IF EXISTS "
                                f"`{target.catalog}`.`{PROBE_SCHEMA}`")
                            destination["left_behind"] = []
                            destination["write_note"] = (
                                f"verified: created {probe_fqn} and cleaned up "
                                f"after itself. Nothing was left behind.")
                            destination["checks"].append(
                                {"name": "write probe cleanup", "ok": True,
                                 "detail": f"{probe_fqn} dropped"})
                        except Exception as exc:
                            destination["left_behind"] = [probe_fqn]
                            destination["write_note"] = (
                                f"verified write, but cleanup failed "
                                f"({str(exc)[:120]}). {probe_fqn} still exists — "
                                f"remove it manually.")
                            destination["checks"].append(
                                {"name": "write probe cleanup", "ok": False,
                                 "detail": str(exc)[:200]})
                else:
                    destination["write_note"] = (
                        "not verified: the CREATE returned without error but the "
                        "schema is not visible, so write is unproven")
                    destination["checks"].append(
                        {"name": "write probe schema", "ok": False,
                         "detail": "created without error but not visible"})
            except Exception as exc:
                destination["write_note"] = (
                    f"not verified: {str(exc)[:200]}")
                destination["checks"].append(
                    {"name": "write probe schema", "ok": False,
                     "detail": str(exc)[:200]})

    all_checks = source["checks"] + destination.get("checks", [])
    return {"ok": bool(all_checks) and all(c["ok"] for c in all_checks),
            "source": source, "destination": destination}
