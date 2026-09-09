"""Deploy generated DDL to AIDP. Dry-run unless explicitly told otherwise.

Two AIDP behaviours drive the shape of this module:
  * Per-statement DDL is silently discarded when the session closes, so DDL is
    batched into one execution per chunk.
  * A chunk can report success while individual statements inside it failed, so
    every statement's object is probed individually afterwards. The chunk's own
    return value is not trusted.

A chunk error does not abort the run: the remaining chunks are attempted and the
error is recorded, because a partial deployment that is accurately reported is
more useful than an aborted one that is not.

SCOPED TO ONE CATALOG PER RUN. Bronze mirrors the source, so a multi-database
estate produces one AIDP Standard Catalog per Snowflake database. Rather than
fan out across all of them from a single confirmation, a run deploys only the
statements belonging to `target.catalog` and reports the rest as out of scope.
Deploying a second catalog is a second explicit invocation.
"""
from __future__ import annotations

import datetime
from typing import Callable

from .ddl import build_create_schema

__all__ = ["deploy", "RefusedToExecute"]


class RefusedToExecute(RuntimeError):
    """Execution was requested without the arguments that make it safe."""


def _split_fqn(target_fqn: str) -> tuple[str, str, str]:
    catalog, schema, table = target_fqn.split(".", 2)
    return catalog, schema, table


def deploy(ddl_plan: dict, *, target=None, execute: bool = False,
           run_sql: Callable[..., list[dict]] | None = None,
           chunk_size: int = 25) -> dict:
    all_statements = [s for s in ddl_plan.get("statements", []) if s.get("sql")]
    blocked_count = len(ddl_plan.get("blocked", []))

    # Scope to the confirmed catalog. Out-of-scope objects are reported, not run.
    if target is not None:
        scope = target.catalog.upper()
        statements = [s for s in all_statements
                      if _split_fqn(s["target_fqn"])[0].upper() == scope]
        out_of_scope = [s for s in all_statements if s not in statements]
    else:
        statements, out_of_scope = all_statements, []

    out = {
        "ran_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "dry_run": not execute,
        "statements": statements,
        "statement_count": len(statements),
        "blocked_count": blocked_count,
        "catalog_in_scope": target.catalog if target is not None else None,
        "out_of_scope_count": len(out_of_scope),
        "out_of_scope_catalogs": sorted({
            _split_fqn(s["target_fqn"])[0] for s in out_of_scope}),
        "executed": 0, "verified": 0, "failed": [], "chunk_errors": [],
        # Per-source-identifier lists so plan/status.py can report a status per
        # object rather than only an aggregate count.
        "attempted_targets": [], "verified_targets": [], "failed_targets": [],
    }
    if not execute:
        return out

    if target is None:
        raise RefusedToExecute(
            "execute=True requires a resolved target; ask the user for the AIDP "
            "datalake OCID, workspace, cluster and catalog and pass them explicitly")
    if run_sql is None:
        raise RefusedToExecute("execute=True requires a run_sql callable")

    schemas = {_split_fqn(s["target_fqn"])[:2] for s in statements}
    for catalog, schema in sorted(schemas):
        try:
            run_sql(build_create_schema(catalog, schema))
        except Exception as exc:
            out["chunk_errors"].append(f"CREATE SCHEMA {catalog}.{schema}: {exc}")

    for start in range(0, len(statements), chunk_size):
        chunk = statements[start:start + chunk_size]
        batch = ";\n".join(s["sql"] for s in chunk)
        out["attempted_targets"] += [s.get("source_identifier") for s in chunk]
        try:
            run_sql(batch)
            out["executed"] += len(chunk)
        except Exception as exc:
            out["chunk_errors"].append(
                f"chunk {start // chunk_size}: {str(exc)[:300]}")

        # Never trust the chunk's own result. Probe each object.
        for stmt in chunk:
            catalog, schema, table = _split_fqn(stmt["target_fqn"])
            try:
                rows = run_sql(
                    f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'")
                if rows:
                    out["verified"] += 1
                    out["verified_targets"].append(stmt.get("source_identifier"))
                else:
                    out["failed_targets"].append(stmt.get("source_identifier"))
                    out["failed"].append({
                        "source_identifier": stmt.get("source_identifier"),
                        "target_fqn": stmt["target_fqn"],
                        "reason": "not present after its chunk reported completion"})
            except Exception as exc:
                out["failed_targets"].append(stmt.get("source_identifier"))
                out["failed"].append({
                    "source_identifier": stmt.get("source_identifier"),
                    "target_fqn": stmt["target_fqn"],
                    "reason": f"existence probe failed: {str(exc)[:200]}"})
    return out
