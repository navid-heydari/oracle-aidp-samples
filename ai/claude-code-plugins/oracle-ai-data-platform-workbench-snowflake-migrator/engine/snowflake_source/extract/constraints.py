"""Table constraints for one Snowflake database. I/O injected as `run_sql`.

Why this exists: the DDL audit trail asserted that PK/FK/UNIQUE were
"captured in the inventory" (rule R20), and nothing captured them. A rule
that states a falsehood is worse than a missing rule, because it is the part
of the report a reviewer trusts to be mechanical.

Three SHOW statements per database, all on the read-only transport's
allowlist:

    SHOW PRIMARY KEYS IN DATABASE <db>
    SHOW UNIQUE KEYS IN DATABASE <db>
    SHOW IMPORTED KEYS IN DATABASE <db>

SHOW is used rather than `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` because these
statements carry the COLUMN NAMES and their ordinal position
(`key_sequence`), which is the whole point: "ORDERS has a primary key" is not
actionable, "ORDERS(ORDER_ID, LINE_NO) is the primary key" is.
`IMPORTED KEYS` is the child-side view, so a foreign key is recorded on the
table that HAS it.

WHAT IS NOT HERE, and why:
  * CHECK. Snowflake does not support CHECK constraints at all -- UNIQUE,
    PRIMARY KEY, FOREIGN KEY and NOT NULL are the whole set -- so the one
    constraint class Delta genuinely ENFORCES has nothing to carry over.
    That is a fact about the source, not a gap in this reader, and the DDL
    rule says so rather than leaving a reader to assume CHECK was missed.
  * NOT NULL. It is a column property and comes from
    `INFORMATION_SCHEMA.COLUMNS.is_nullable`, which the column reader
    already selects and the DDL emits.

Nothing here decides anything: it reports what the source declares. Snowflake
does not ENFORCE PK/UNIQUE/FK either (they are metadata, and RELY governs
whether the optimiser trusts them), and `rely` is carried so that is
visible rather than assumed.
"""
from __future__ import annotations

from typing import Callable

from ..dialect import lexer

__all__ = ["build_constraints", "CONSTRAINT_STATEMENTS"]

CONSTRAINT_STATEMENTS = ("show primary keys in database",
                         "show unique keys in database",
                         "show imported keys in database")


def _get(row: dict, key: str):
    """One SHOW cell, whatever case the driver returned the key in."""
    if key in row:
        return row[key]
    for k, v in row.items():
        if str(k).lower() == key:
            return v
    return None


def _sequence(row: dict) -> int:
    try:
        return int(_get(row, "key_sequence") or 0)
    except (TypeError, ValueError):
        return 0


def _keyed(rows: list[dict], constraint_type: str, prefix: str = "") -> dict:
    """Group key rows into one entry per constraint, columns in key order."""
    grouped: dict[tuple, dict] = {}
    for row in rows:
        db = _get(row, f"{prefix}database_name")
        schema = _get(row, f"{prefix}schema_name")
        table = _get(row, f"{prefix}table_name")
        if not (db and schema and table):
            continue
        name = (_get(row, "constraint_name") or _get(row, "fk_name")
                or _get(row, "pk_name") or constraint_type)
        ident = f"{db}.{schema}.{table}"
        entry = grouped.setdefault((ident, name), {
            "constraint_type": constraint_type, "name": str(name),
            "columns": [], "rely": _get(row, "rely"), "_rows": []})
        entry["_rows"].append(row)
    out: dict[tuple, dict] = {}
    for key, entry in grouped.items():
        rows_in_order = sorted(entry.pop("_rows"), key=_sequence)
        entry["columns"] = [str(_get(r, f"{prefix}column_name"))
                            for r in rows_in_order
                            if _get(r, f"{prefix}column_name") is not None]
        out[key] = entry
    return out


def build_constraints(run_sql: Callable[..., list[dict]], db: str,
                      notes: list[str] | None = None) -> dict[str, list[dict]]:
    """`{source_identifier: [constraint, ...]}` for one database.

    A read that fails is recorded in `notes` and the others still run: a role
    that cannot see one of these views must not cost the estate the other
    two, and "not read" must not read as "none declared".
    """
    by_table: dict[str, list[dict]] = {}
    qualified = lexer.qualify(db)

    def _read(statement: str) -> list[dict]:
        try:
            return run_sql(f"{statement} {qualified}")
        except Exception as exc:
            if notes is not None:
                notes.append(f"{db} {statement}: {str(exc)[:200]}")
            return []

    for statement, constraint_type in (
            ("show primary keys in database", "PRIMARY KEY"),
            ("show unique keys in database", "UNIQUE")):
        for (ident, _), entry in _keyed(_read(statement),
                                        constraint_type).items():
            by_table.setdefault(ident, []).append(entry)

    # IMPORTED KEYS is the CHILD side: the table that holds the foreign key.
    fk_rows = _read("show imported keys in database")
    for (ident, _), entry in _keyed(fk_rows, "FOREIGN KEY", "fk_").items():
        rows = [r for r in fk_rows
                if f'{_get(r, "fk_database_name")}.{_get(r, "fk_schema_name")}'
                   f'.{_get(r, "fk_table_name")}' == ident
                and str(_get(r, "fk_name") or "FOREIGN KEY") == entry["name"]]
        rows.sort(key=_sequence)
        first = rows[0] if rows else {}
        entry["references"] = ".".join(
            str(_get(first, f"pk_{part}_name") or "")
            for part in ("database", "schema", "table"))
        entry["referenced_columns"] = [str(_get(r, "pk_column_name"))
                                       for r in rows
                                       if _get(r, "pk_column_name") is not None]
        by_table.setdefault(ident, []).append(entry)

    for entries in by_table.values():
        entries.sort(key=lambda e: (e["constraint_type"], e["name"]))
    return by_table
