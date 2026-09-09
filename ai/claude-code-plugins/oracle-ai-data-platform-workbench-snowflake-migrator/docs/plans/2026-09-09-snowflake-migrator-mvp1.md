# Snowflake → AIDP Migrator MVP-1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Claude Code plugin that investigates a Snowflake estate, plans its migration, and structurally clones its tables into a medallion layout on AIDP — schema only, no data movement.

**Architecture:** A linear artifact pipeline. Four JSON artifacts (`inventory.json` → `dependencies.json` → `plan.json` → `ddl_plan.json`) are the only interface between stages, so `snowflake_source/` and `target/` never import each other. All deterministic logic lives in pure Python functions with I/O injected as callables, which makes the whole engine testable offline from recorded fixtures. Five thin skills orchestrate and interpret; they contain no logic.

**Tech Stack:** Python 3.11 · `snowflake-connector-python` 4.7+ · `cryptography` · `oci` · `requests` · `websocket-client` · `pytest`

**Spec:** `docs/specs/2026-09-09-mvp1-design.md` (in this plugin)

## Global Constraints

- **Python 3.11.** The venv is `/Users/nheydari/Workspace/oracle/snowflake_migrator/.venv`. Do NOT use the default `python3` — it is a pyenv 3.14 shim, PEP-668 managed, and loses packages between sessions.
- **Plugin name stays `oracle-ai-data-platform-workbench-snowflake-migrator`.** Do not rename.
- **Read-only against Snowflake, always.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL`. No DDL, no DML, no session mutation beyond `USE`.
- **No target coordinates may be discovered or persisted.** No code path reads AIDP coordinates from an environment variable, config file, or cache; none writes them to disk. They are function arguments only.
- **Dry-run is the default.** Any write path requires `--execute` plus all four of `--datalake-ocid`, `--workspace`, `--cluster-id`, `--catalog` in the same invocation.
- **Fail loudly on anything unrecognized.** An unmapped type, property, or construct produces `blocked` / `requires_manual_design` with the reason named. Never a silent default or an approximation.
- **Precision and scale come from `INFORMATION_SCHEMA`, never inferred from sampled data.**
- **No `CREATE OR REPLACE`, no `DROP`.** Use `CREATE TABLE IF NOT EXISTS`; HTTP 409 means "exists", not failure.
- **Secrets never appear** in artifacts, reports, logs, or conversational output. Auth takes a file path or a token read from a file.
- **Every pure module takes injected I/O** (a `run_sql` callable or plain data) so it unit-tests with no live connection.
- **Tests live beside the engine** at `engine/tests/`, run with `pytest engine/tests -v`.

---

## File Structure

| Path | Responsibility |
|---|---|
| `engine/requirements.txt` | Runtime deps |
| `engine/requirements-dev.txt` | `pytest` only |
| `engine/snowflake_source/conn.py` | Auth + connection; the only module that opens a Snowflake socket |
| `engine/snowflake_source/dialect/types.py` | Snowflake type → Spark/Delta type. Pure |
| `engine/snowflake_source/dialect/identifiers.py` | Case-form capture + collision detection. Pure |
| `engine/snowflake_source/extract/catalog.py` | Estate inventory. I/O injected |
| `engine/snowflake_source/extract/dependencies.py` | Dual-source dependency edges. I/O injected |
| `engine/snowflake_source/corpus/` | Test-estate generator + validator (relocated) |
| `engine/plan/waves.py` | Topological layers + cycle detection. Pure |
| `engine/plan/medallion.py` | Layer assignment + namespace strategies. Pure |
| `engine/target/coords.py` | Runtime-only target resolution |
| `engine/target/ddl.py` | `CREATE TABLE` generation with rule audit. Pure |
| `engine/target/deploy.py` | Dry-run-default deployment, batched replay + per-statement probe |
| `engine/target/aidp_executor.py` | **From fork, unchanged.** OCI-signed REST + Jupyter WS |
| `engine/target/cluster_session.py` | **From fork, unchanged** |
| `engine/target/cluster_lifecycle.py` | **From fork, unchanged** |
| `engine/report/render.py` | Artifact → markdown. Pure |
| `engine/snowmig.py` | CLI: `assess` · `deps` · `plan` · `ddl` · `deploy`. Thin argparse over the above |
| `skills/*/SKILL.md` × 5 | Orchestration instructions. No logic |
| `commands/*.md` × 3 | User-facing entry points |
| `references/type-mapping.md` | The type table, for humans |

---

## Task 1: Prune the fork and lay the engine skeleton

**Files:**
- Delete: everything under `engine/scripts/` **except** `aidp_executor.py`, `cluster_session.py`, `cluster_lifecycle.py`
- Delete: `engine/aidp_compat/` (all 21 modules), `engine/schemas/`, `engine/run_migration.sh`, `engine/setup.py`
- Delete: `skills/` (all 10 Databricks scaffold dirs), `commands/` (all 4), `agents/` (both)
- Delete: `references/cli-map.md`, `references/ddl-rewrite-rules.md`, `references/gotchas.md`, `references/job-report-format.md`
- Move: `engine/scripts/{aidp_executor,cluster_session,cluster_lifecycle}.py` → `engine/target/`
- Move: `snowflake_source/corpus/*` → `engine/snowflake_source/corpus/`
- Create: `engine/requirements.txt`, `engine/requirements-dev.txt`, `engine/__init__.py`, and `__init__.py` in `snowflake_source/`, `snowflake_source/dialect/`, `snowflake_source/extract/`, `plan/`, `target/`, `report/`, `tests/`
- Test: `engine/tests/test_skeleton.py`

**Interfaces:**
- Consumes: nothing
- Produces: the `engine.*` package tree that every later task imports

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_skeleton.py
"""The engine package tree exists and the fork's dead weight is gone."""
import importlib
import pathlib

import pytest

ENGINE = pathlib.Path(__file__).resolve().parents[1]

PACKAGES = [
    "snowflake_source",
    "snowflake_source.dialect",
    "snowflake_source.extract",
    "plan",
    "target",
    "report",
]

# Fork modules that MUST survive the prune -- the OCI transport layer.
KEPT = [
    "target/aidp_executor.py",
    "target/cluster_session.py",
    "target/cluster_lifecycle.py",
]

# Dead weight for MVP-1: no notebook migration, no data movement, no dbutils.
PRUNED = [
    "scripts/job_migrate.py",
    "scripts/agent_migrate.py",
    "scripts/build_dag.py",
    "scripts/cell_analyzer.py",
    "scripts/fuse_scanner.py",
    "scripts/acceptance_contract.py",
    "scripts/extract_catalog_databricks.py",
    "aidp_compat",
    "schemas",
    "run_migration.sh",
    "setup.py",
]


@pytest.mark.parametrize("pkg", PACKAGES)
def test_package_importable(pkg):
    assert importlib.import_module(pkg) is not None


@pytest.mark.parametrize("rel", KEPT)
def test_transport_modules_kept(rel):
    assert (ENGINE / rel).is_file(), f"{rel} must survive the prune"


@pytest.mark.parametrize("rel", PRUNED)
def test_dead_weight_pruned(rel):
    assert not (ENGINE / rel).exists(), f"{rel} is dead weight for MVP-1"


def test_corpus_relocated():
    assert (ENGINE / "snowflake_source/corpus/00_rappi_setup.sql").is_file()
    assert (ENGINE / "snowflake_source/corpus/validate.py").is_file()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_skeleton.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source'`, plus failures on every `test_dead_weight_pruned`.

- [ ] **Step 3: Create the package tree and prune**

```bash
cd engine

# Keep the three transport modules, drop the other 34 scripts.
mkdir -p target
git mv scripts/aidp_executor.py    target/ 2>/dev/null || mv scripts/aidp_executor.py    target/
git mv scripts/cluster_session.py  target/ 2>/dev/null || mv scripts/cluster_session.py  target/
git mv scripts/cluster_lifecycle.py target/ 2>/dev/null || mv scripts/cluster_lifecycle.py target/
rm -rf scripts aidp_compat schemas run_migration.sh setup.py

mkdir -p snowflake_source/dialect snowflake_source/extract snowflake_source/corpus plan report tests
mv ../snowflake_source/corpus/00_rappi_setup.sql snowflake_source/corpus/
mv ../snowflake_source/corpus/validate.py        snowflake_source/corpus/
rm -rf ../snowflake_source

for d in . snowflake_source snowflake_source/dialect snowflake_source/extract plan target report tests; do
  touch "$d/__init__.py"
done

# Databricks scaffold: every skill, command and agent is Databricks content.
cd ..
rm -rf skills commands agents
rm -f references/cli-map.md references/ddl-rewrite-rules.md \
      references/gotchas.md references/job-report-format.md
mkdir -p skills commands
```

```
# engine/requirements.txt
snowflake-connector-python>=4.7.0
cryptography>=42.0.0
oci>=2.130.0
requests>=2.31.0
websocket-client>=1.6.0
```

```
# engine/requirements-dev.txt
-r requirements.txt
pytest>=8.0.0
```

```ini
# engine/pytest.ini
[pytest]
testpaths = tests
pythonpath = .
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_skeleton.py -v`
Expected: PASS — all parametrized cases green.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(snowflake-migrator): prune Databricks fork to the MVP-1 engine skeleton"
```

---

## Task 2: Type mapping

**Files:**
- Create: `engine/snowflake_source/dialect/types.py`
- Test: `engine/tests/test_types.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `@dataclass(frozen=True) TypeMapping(spark_type: str | None, blocked: bool, reason: str | None, warning: str | None)`
  - `map_type(data_type: str, *, precision: int | None = None, scale: int | None = None, char_length: int | None = None) -> TypeMapping`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_types.py
"""Snowflake -> Spark/Delta type mapping. Pure; no connection."""
import pytest

from snowflake_source.dialect.types import TypeMapping, map_type


@pytest.mark.parametrize("dt", ["NUMBER", "DECIMAL", "NUMERIC"])
def test_number_preserves_precision_and_scale(dt):
    m = map_type(dt, precision=18, scale=2)
    assert m.spark_type == "DECIMAL(18,2)"
    assert m.blocked is False


def test_number_scale_zero_is_still_decimal():
    # NUMBER(38,0) is Snowflake's default integer. Mapping it to BIGINT would
    # overflow: 38 digits does not fit in 64 bits.
    assert map_type("NUMBER", precision=38, scale=0).spark_type == "DECIMAL(38,0)"


def test_number_without_precision_is_blocked_not_guessed():
    m = map_type("NUMBER")
    assert m.blocked is True
    assert "precision" in m.reason.lower()


def test_timestamp_ntz_maps_to_ntz_not_timestamp():
    # Spark's bare TIMESTAMP is session-timezone-dependent; NTZ is not.
    m = map_type("TIMESTAMP_NTZ")
    assert m.spark_type == "TIMESTAMP_NTZ"
    assert m.warning is None


@pytest.mark.parametrize("dt", ["TIMESTAMP_LTZ", "TIMESTAMP_TZ"])
def test_zoned_timestamps_map_with_a_warning(dt):
    m = map_type(dt)
    assert m.spark_type == "TIMESTAMP"
    assert m.warning is not None and "timezone" in m.warning.lower()


@pytest.mark.parametrize("dt,expected", [
    ("TEXT", "STRING"), ("VARCHAR", "STRING"), ("CHAR", "STRING"),
    ("BOOLEAN", "BOOLEAN"), ("DATE", "DATE"), ("BINARY", "BINARY"),
    ("FLOAT", "DOUBLE"), ("DOUBLE", "DOUBLE"),
])
def test_direct_mappings(dt, expected):
    assert map_type(dt).spark_type == expected


def test_varchar_length_is_recorded_as_a_warning():
    m = map_type("TEXT", char_length=100)
    assert m.spark_type == "STRING"
    assert "100" in m.warning


@pytest.mark.parametrize("dt", ["VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY"])
def test_semistructured_and_geo_are_blocked(dt):
    m = map_type(dt)
    assert m.blocked is True
    assert m.spark_type is None
    assert m.reason


def test_unknown_type_is_blocked_never_defaulted():
    m = map_type("SOME_FUTURE_TYPE")
    assert m.blocked is True
    assert "unmapped" in m.reason.lower()


def test_case_and_whitespace_insensitive():
    assert map_type("  number ", precision=5, scale=2).spark_type == "DECIMAL(5,2)"


def test_mapping_is_immutable():
    with pytest.raises(Exception):
        map_type("DATE").spark_type = "STRING"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_types.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source.dialect.types'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/snowflake_source/dialect/types.py
"""Snowflake type -> Spark/Delta type. Pure functions, zero I/O.

Precision and scale are always passed in from INFORMATION_SCHEMA.COLUMNS and are
never inferred from sampled data: NUMBER is Snowflake's default numeric type, and
getting its scale wrong does not raise -- it silently changes values.

An unmapped type is BLOCKED, never approximated.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["TypeMapping", "map_type"]


@dataclass(frozen=True)
class TypeMapping:
    spark_type: str | None
    blocked: bool = False
    reason: str | None = None
    warning: str | None = None


_DIRECT = {
    "TEXT": "STRING", "VARCHAR": "STRING", "CHAR": "STRING", "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "BINARY": "BINARY", "VARBINARY": "BINARY",
    "FLOAT": "DOUBLE", "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE",
    "DOUBLE": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "REAL": "DOUBLE",
    "TIME": "STRING",
}

_NUMERIC = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
            "SMALLINT", "TINYINT", "BYTEINT"}

_BLOCKED = {
    "VARIANT": "semi-structured; needs an explicit struct/map/array target design",
    "OBJECT": "semi-structured; needs an explicit struct/map target design",
    "ARRAY": "semi-structured; needs an explicit array target design",
    "GEOGRAPHY": "no Spark/Delta target type",
    "GEOMETRY": "no Spark/Delta target type",
}


def map_type(data_type: str, *, precision: int | None = None,
             scale: int | None = None, char_length: int | None = None) -> TypeMapping:
    if data_type is None:
        return TypeMapping(None, True, "missing data_type")
    key = " ".join(data_type.strip().upper().split())

    if key in _BLOCKED:
        return TypeMapping(None, True, f"{key}: {_BLOCKED[key]}")

    if key in _NUMERIC:
        if precision is None:
            return TypeMapping(
                None, True,
                f"{key} with no numeric_precision from INFORMATION_SCHEMA; "
                "refusing to guess precision")
        return TypeMapping(f"DECIMAL({precision},{scale if scale is not None else 0})")

    if key == "TIMESTAMP_NTZ":
        return TypeMapping("TIMESTAMP_NTZ")
    if key in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"):
        return TypeMapping(
            "TIMESTAMP",
            warning=f"{key} -> Spark TIMESTAMP: timezone semantics differ; "
                    "Spark TIMESTAMP is session-timezone-dependent")

    if key in _DIRECT:
        warning = None
        if _DIRECT[key] == "STRING" and char_length is not None:
            warning = (f"declared length {char_length} is not enforced by Delta; "
                       "recorded only")
        return TypeMapping(_DIRECT[key], warning=warning)

    return TypeMapping(None, True, f"unmapped Snowflake type: {key}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_types.py -v`
Expected: PASS — all cases green.

- [ ] **Step 5: Commit**

```bash
git add engine/snowflake_source/dialect/types.py engine/tests/test_types.py
git commit -m "feat(snowflake-migrator): Snowflake to Spark type mapping, unmapped types blocked"
```

---

## Task 3: Identifier case form and collision detection

**Files:**
- Create: `engine/snowflake_source/dialect/identifiers.py`
- Test: `engine/tests/test_identifiers.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `case_form(name: str) -> str` returning `"UPPER_UNQUOTED"` or `"MIXED_QUOTED_CASE_SENSITIVE"`
  - `detect_collisions(identifiers: list[str]) -> dict[str, list[str]]` — key is the upper-cased identifier, value the distinct originals; empty dict means safe
  - `assert_safe_identifier(name: str) -> str` — returns the name, raises `UnsafeIdentifier` otherwise
  - `class UnsafeIdentifier(ValueError)`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_identifiers.py
"""Identifier case policy and collision detection. Pure; no connection."""
import pytest

from snowflake_source.dialect.identifiers import (
    UnsafeIdentifier, assert_safe_identifier, case_form, detect_collisions,
)


def test_upper_is_unquoted_form():
    assert case_form("CUSTOMERS") == "UPPER_UNQUOTED"


@pytest.mark.parametrize("name", ["customers", "Customers", "cUSTOMERS"])
def test_non_upper_is_case_sensitive_form(name):
    assert case_form(name) == "MIXED_QUOTED_CASE_SENSITIVE"


def test_digits_and_underscores_are_upper_form():
    assert case_form("ORDER_ITEMS_2024") == "UPPER_UNQUOTED"


def test_no_collision_returns_empty():
    assert detect_collisions(["DB.S.A", "DB.S.B"]) == {}


def test_same_name_twice_is_not_a_collision():
    # The same object listed twice is a duplicate, not two colliding objects.
    assert detect_collisions(["DB.S.A", "DB.S.A"]) == {}


def test_case_variants_collide():
    got = detect_collisions(["DB.S.customers", "DB.S.CUSTOMERS"])
    assert list(got) == ["DB.S.CUSTOMERS"]
    assert sorted(got["DB.S.CUSTOMERS"]) == ["DB.S.CUSTOMERS", "DB.S.customers"]


def test_three_way_collision_lists_all():
    got = detect_collisions(["A.B.c", "A.B.C", "A.B.C "])
    assert len(got["A.B.C"]) >= 2


@pytest.mark.parametrize("bad", ["", "   ", "a`b", "a\tb", "a\nb", "a\x00b", None])
def test_unsafe_identifiers_rejected(bad):
    with pytest.raises(UnsafeIdentifier):
        assert_safe_identifier(bad)


def test_safe_identifier_returned_unchanged():
    assert assert_safe_identifier("ORDER_DIMENSIONS") == "ORDER_DIMENSIONS"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_identifiers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source.dialect.identifiers'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/snowflake_source/dialect/identifiers.py
"""Snowflake identifier case policy.

Unquoted Snowflake identifiers fold to UPPER; quoted ones preserve case and are
case-SENSITIVE. So `customers` is really CUSTOMERS, while "customers" is a
DIFFERENT object. Spark/Delta folds to lower by default, so naive lowercasing
merges them and loses data with no error.

The form is captured at extraction time -- SHOW output tells us which was used --
and a collision HALTS rather than being resolved by guessing.
"""
from __future__ import annotations

import collections

__all__ = ["UnsafeIdentifier", "assert_safe_identifier", "case_form",
           "detect_collisions"]

_FORBIDDEN = set('`"\'') | {c for c in map(chr, range(32))}


class UnsafeIdentifier(ValueError):
    """Identifier is empty, quoted, or contains whitespace/control characters."""


def case_form(name: str) -> str:
    return "UPPER_UNQUOTED" if name == name.upper() else "MIXED_QUOTED_CASE_SENSITIVE"


def assert_safe_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise UnsafeIdentifier(f"empty or non-string identifier: {name!r}")
    if any(c in _FORBIDDEN or c.isspace() for c in name):
        raise UnsafeIdentifier(f"identifier contains unsafe characters: {name!r}")
    return name


def detect_collisions(identifiers: list[str]) -> dict[str, list[str]]:
    """Group identifiers that differ only by case. Empty result means safe."""
    buckets: dict[str, set[str]] = collections.defaultdict(set)
    for raw in identifiers:
        buckets[raw.strip().upper()].add(raw)
    return {k: sorted(v) for k, v in buckets.items() if len(v) > 1}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_identifiers.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/snowflake_source/dialect/identifiers.py engine/tests/test_identifiers.py
git commit -m "feat(snowflake-migrator): identifier case policy with halting collision detector"
```

---

## Task 4: Snowflake connection and auth

**Files:**
- Create: `engine/snowflake_source/conn.py`
- Test: `engine/tests/test_conn.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `class AuthError(RuntimeError)`
  - `build_connect_kwargs(auth: str, *, account: str, user: str | None = None, role: str | None = None, warehouse: str | None = None, database: str | None = None, key_path: str | None = None, key_passphrase: str | None = None, pat_path: str | None = None, password_path: str | None = None) -> dict`
  - `load_private_key_der(path: str, passphrase: str | None = None) -> bytes`
  - `connect(**kwargs)` — opens the connection; the only socket-opening function in the engine
  - `make_run_sql(conn) -> Callable[[str, dict | None], list[dict]]` — the injected I/O every extract module takes

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_conn.py
"""Auth kwarg construction. Pure; opens no socket."""
import pytest

from snowflake_source.conn import AuthError, build_connect_kwargs

ACC = "npxbexe-op03637"


def test_keypair_requires_a_key_path():
    with pytest.raises(AuthError, match="key_path"):
        build_connect_kwargs("keypair", account=ACC, user="u")


def test_pat_requires_a_token_file():
    with pytest.raises(AuthError, match="pat_path"):
        build_connect_kwargs("pat", account=ACC, user="u")


def test_password_requires_a_password_file():
    with pytest.raises(AuthError, match="password_path"):
        build_connect_kwargs("password", account=ACC, user="u")


def test_account_is_always_required():
    with pytest.raises(AuthError, match="account"):
        build_connect_kwargs("keypair", account=None, user="u", key_path="/k")


def test_user_required_except_for_externalbrowser():
    with pytest.raises(AuthError, match="user"):
        build_connect_kwargs("keypair", account=ACC, key_path="/k")
    kw = build_connect_kwargs("externalbrowser", account=ACC)
    assert "user" not in kw


def test_externalbrowser_sets_the_authenticator():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u")
    assert kw["authenticator"] == "externalbrowser"


def test_pat_reads_the_token_from_a_file(tmp_path):
    f = tmp_path / "pat"
    f.write_text("  tok-abc123  \n")
    kw = build_connect_kwargs("pat", account=ACC, user="u", pat_path=str(f))
    assert kw["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN"
    assert kw["password"] == "tok-abc123", "token must be stripped"


def test_password_read_from_file_not_taken_inline(tmp_path):
    f = tmp_path / "pw"
    f.write_text("s3cret\n")
    kw = build_connect_kwargs("password", account=ACC, user="u", password_path=str(f))
    assert kw["password"] == "s3cret"


def test_optional_session_context_passed_through():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u",
                              role="R", warehouse="W", database="D")
    assert (kw["role"], kw["warehouse"], kw["database"]) == ("R", "W", "D")


def test_unset_session_context_is_omitted_not_none():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u")
    assert "role" not in kw and "warehouse" not in kw and "database" not in kw


def test_unknown_auth_mode_rejected():
    with pytest.raises(AuthError, match="unknown"):
        build_connect_kwargs("magic", account=ACC, user="u")


def test_missing_secret_file_is_a_clear_error(tmp_path):
    with pytest.raises(AuthError, match="not readable"):
        build_connect_kwargs("pat", account=ACC, user="u",
                             pat_path=str(tmp_path / "nope"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_conn.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source.conn'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/snowflake_source/conn.py
"""Snowflake connection and auth. The only module here that opens a socket.

Secrets are read from FILES, never taken as inline arguments, so they cannot end
up in shell history, process listings, or an artifact. Nothing in this module
logs or returns a credential.

Auth modes:
  keypair          -- key_path (+ key_passphrase). Preferred; also what AIDP's
                      native Snowflake connector uses, so it is not throwaway setup.
  pat              -- pat_path. Scoped, expiring, revocable.
  password         -- password_path.
  externalbrowser  -- SSO. Needs a SAML IdP on the account; a plain Snowflake
                      account returns 390190.
"""
from __future__ import annotations

import pathlib
from typing import Any, Callable

__all__ = ["AuthError", "build_connect_kwargs", "load_private_key_der",
           "connect", "make_run_sql"]

_MODES = {"keypair", "pat", "password", "externalbrowser"}


class AuthError(RuntimeError):
    """Auth arguments are missing, contradictory, or unreadable."""


def _read_secret_file(path: str, label: str) -> str:
    p = pathlib.Path(path).expanduser()
    try:
        return p.read_text().strip()
    except OSError as exc:
        raise AuthError(f"{label} not readable at {path}: {exc.strerror}") from exc


def load_private_key_der(path: str, passphrase: str | None = None) -> bytes:
    from cryptography.hazmat.primitives import serialization
    p = pathlib.Path(path).expanduser()
    try:
        raw = p.read_bytes()
    except OSError as exc:
        raise AuthError(f"private key not readable at {path}: {exc.strerror}") from exc
    key = serialization.load_pem_private_key(
        raw, password=passphrase.encode() if passphrase else None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())


def build_connect_kwargs(auth: str, *, account: str, user: str | None = None,
                         role: str | None = None, warehouse: str | None = None,
                         database: str | None = None, key_path: str | None = None,
                         key_passphrase: str | None = None,
                         pat_path: str | None = None,
                         password_path: str | None = None) -> dict[str, Any]:
    if auth not in _MODES:
        raise AuthError(f"unknown auth mode {auth!r}; expected one of {sorted(_MODES)}")
    if not account:
        raise AuthError("account is required")
    if not user and auth != "externalbrowser":
        raise AuthError(f"user is required for auth mode {auth!r}")

    kw: dict[str, Any] = {"account": account, "client_session_keep_alive": False}
    if user:
        kw["user"] = user
    for value, key in ((role, "role"), (warehouse, "warehouse"), (database, "database")):
        if value:
            kw[key] = value

    if auth == "externalbrowser":
        kw["authenticator"] = "externalbrowser"
    elif auth == "keypair":
        if not key_path:
            raise AuthError("keypair auth requires key_path")
        kw["private_key"] = load_private_key_der(key_path, key_passphrase)
    elif auth == "pat":
        if not pat_path:
            raise AuthError("pat auth requires pat_path")
        kw["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
        kw["password"] = _read_secret_file(pat_path, "PAT file")
    elif auth == "password":
        if not password_path:
            raise AuthError("password auth requires password_path")
        kw["password"] = _read_secret_file(password_path, "password file")
    return kw


def connect(**kwargs):
    import snowflake.connector
    return snowflake.connector.connect(**kwargs)


def make_run_sql(conn) -> Callable[..., list[dict]]:
    """Return the injected-I/O callable every extract module consumes.

    Signature: run_sql(sql, params=None) -> list[dict]
    """
    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return run_sql
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_conn.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/snowflake_source/conn.py engine/tests/test_conn.py
git commit -m "feat(snowflake-migrator): file-based auth for keypair, PAT, password and SSO"
```

---

## Task 5: Estate inventory extraction

**Files:**
- Create: `engine/snowflake_source/extract/catalog.py`
- Create: `engine/tests/fake_sql.py`
- Test: `engine/tests/test_catalog.py`

**Interfaces:**
- Consumes: `map_type` (Task 2); `case_form`, `detect_collisions` (Task 3)
- Produces:
  - `build_inventory(run_sql, databases: list[str] | None = None) -> dict` — the `inventory.json` payload of spec §3.1
  - `SYSTEM_DBS: frozenset[str]`
- Test helper produced for later tasks: `tests/fake_sql.py::FakeSql(responses: dict[str, list[dict]])`, callable as `run_sql(sql, params)`, matching the first key that is a case-insensitive substring of the SQL, and recording `.calls`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/fake_sql.py
"""A run_sql test double. Matches canned responses by SQL substring."""
from __future__ import annotations


class FakeSql:
    def __init__(self, responses: dict[str, list[dict]]):
        self.responses = responses
        self.calls: list[str] = []

    def __call__(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append(sql)
        flat = " ".join(sql.split()).lower()
        for needle, rows in self.responses.items():
            if needle.lower() in flat:
                return rows
        raise AssertionError(f"FakeSql has no canned response for: {flat[:160]}")
```

```python
# engine/tests/test_catalog.py
"""Inventory extraction with injected I/O. No Snowflake connection."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.catalog import SYSTEM_DBS, build_inventory

SESSION = [{"U": "NHEYDARI", "A": "DU58131", "R": "AWS_US_EAST_2",
            "ROLE": "ACCOUNTADMIN", "WH": "COMPUTE_WH", "V": "10.32.102"}]

COLUMNS = [
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 1,
     "COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
     "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 2,
     "COLUMN_NAME": "PAID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": 18, "NUMERIC_SCALE": 2,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 3,
     "COLUMN_NAME": "PAYLOAD", "DATA_TYPE": "VARIANT", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
]


def base_responses(**over):
    r = {
        "current_user()": SESSION,
        "show databases": [{"name": "MYDB"}, {"name": "SNOWFLAKE"},
                           {"name": "SNOWFLAKE_SAMPLE_DATA"}],
        "show schemas": [{"name": "PUBLIC"}, {"name": "INFORMATION_SCHEMA"}],
        "information_schema.columns": COLUMNS,
        "show tables": [{"name": "ORDERS", "rows": 99, "bytes": 4096,
                         "created_on": "2026-09-08", "comment": None,
                         "cluster_by": None}],
        "show views": [{"name": "ORDERS_VW", "text": "select * from ORDERS",
                        "created_on": "2026-09-08", "comment": None,
                        "is_secure": "false"}],
        "count(*)": [{"N": 100}],
        "get_ddl": [{"D": "create or replace view ORDERS_VW as select * from ORDERS;"}],
    }
    r.update(over)
    return r


def test_system_databases_are_excluded():
    inv = build_inventory(FakeSql(base_responses()))
    assert inv["databases_in_scope"] == ["MYDB"]
    assert "SNOWFLAKE" in SYSTEM_DBS


def test_information_schema_is_not_walked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["source_schema"] == "PUBLIC" for r in inv["inventory"])


def test_finds_one_table_and_one_view():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert inv["counts_by_type"] == {"TABLE": 1, "VIEW": 1}
    assert inv["object_count"] == 2


def test_row_count_uses_exact_count_not_show_estimate():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] == 100, "must be count(*), not SHOW's 99"
    assert table["source_metadata"]["bytes"] == 4096


def test_precision_and_scale_carried_from_information_schema():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    paid = next(c for c in table["columns"] if c["COLUMN_NAME"] == "PAID")
    assert (paid["NUMERIC_PRECISION"], paid["NUMERIC_SCALE"]) == (18, 2)


def test_type_mapping_recorded_and_variant_blocked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    m = {c["COLUMN_NAME"]: c["target_type"] for c in table["columns"]}
    assert m["ORDER_ID"] == "DECIMAL(38,0)"
    assert m["PAID"] == "DECIMAL(18,2)"
    assert m["PAYLOAD"] is None
    assert table["compatibility_status"] == "blocked"
    assert any("VARIANT" in b for b in table["blocked_reasons"])


def test_table_with_all_types_mapped_is_supported():
    cols = [c for c in COLUMNS if c["COLUMN_NAME"] != "PAYLOAD"]
    inv = build_inventory(
        FakeSql(base_responses(**{"information_schema.columns": cols})),
        databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["compatibility_status"] == "supported"


def test_view_keeps_both_ddl_forms_verbatim():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    view = next(r for r in inv["inventory"] if r["object_type"] == "VIEW")
    assert view["view_text_show"] == "select * from ORDERS"
    assert "create or replace view" in view["view_ddl_get_ddl"]
    assert view["compatibility_status"] == "requires_manual_design"


def test_case_form_captured_per_object():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["identifier_case_form"] == "UPPER_UNQUOTED" for r in inv["inventory"])


def test_case_collision_is_surfaced():
    r = base_responses(**{"show tables": [
        {"name": "ORDERS", "rows": 1, "bytes": 1},
        {"name": "orders", "rows": 1, "bytes": 1}]})
    inv = build_inventory(FakeSql(r), databases=["MYDB"])
    assert inv["identifier_case_collisions"], "must report, not silently merge"


def test_count_failure_is_recorded_not_fatal():
    class Flaky(FakeSql):
        def __call__(self, sql, params=None):
            if "count(*)" in sql.lower():
                raise RuntimeError("warehouse suspended")
            return super().__call__(sql, params)

    inv = build_inventory(Flaky(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] is None
    assert "warehouse suspended" in table["row_count_error"]


def test_schema_listing_failure_is_noted_and_extraction_continues():
    class NoSchemas(FakeSql):
        def __call__(self, sql, params=None):
            if "show schemas" in sql.lower():
                raise RuntimeError("insufficient privileges")
            return super().__call__(sql, params)

    inv = build_inventory(NoSchemas(base_responses()), databases=["MYDB"])
    assert inv["inventory"] == []
    assert any("insufficient privileges" in n for n in inv["extraction_notes"])


def test_migration_status_starts_at_discovered():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["migration_status"] == "discovered" for r in inv["inventory"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_catalog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source.extract.catalog'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/snowflake_source/extract/catalog.py
"""Read-only Snowflake estate inventory. I/O injected as `run_sql`.

Issues only SHOW / SELECT / GET_DDL. Cannot modify the estate.

Three things are captured HERE rather than reconstructed later, because they
cannot be recovered afterwards:
  * identifier_case_form -- SHOW output tells us which form was used
  * numeric precision/scale -- from INFORMATION_SCHEMA, never from sampled data
  * view SQL, verbatim -- both SHOW VIEWS.text and GET_DDL()

A per-object failure is recorded in extraction_notes and extraction continues.
"no objects" and "extraction failed" are different outcomes and must not be
conflated.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect.identifiers import case_form, detect_collisions
from ..dialect.types import map_type

__all__ = ["build_inventory", "SYSTEM_DBS"]

SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})

_META_KEYS = ("rows", "bytes", "created_on", "comment", "cluster_by", "is_dynamic",
              "is_iceberg", "is_secure", "is_materialized", "owner",
              "change_tracking", "retention_time")


def _jsonable(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return value


def build_inventory(run_sql: Callable[..., list[dict]],
                    databases: list[str] | None = None) -> dict:
    notes: list[str] = []
    session = run_sql(
        "select current_user() U, current_account() A, current_region() R, "
        "current_role() ROLE, current_warehouse() WH, current_version() V")[0]

    if not databases:
        databases = [r["name"] for r in run_sql("show databases")
                     if r["name"] not in SYSTEM_DBS]

    inventory: list[dict] = []
    for db in databases:
        try:
            schemas = [r["name"] for r in run_sql(f'show schemas in database "{db}"')
                       if r["name"] != "INFORMATION_SCHEMA"]
        except Exception as exc:
            notes.append(f"database {db}: {exc}")
            continue

        cols_by_obj: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
        try:
            for c in run_sql(
                    f'select table_schema, table_name, ordinal_position, column_name, '
                    f'data_type, is_nullable, numeric_precision, numeric_scale, '
                    f'character_maximum_length, datetime_precision, comment '
                    f'from "{db}".information_schema.columns '
                    f'order by table_schema, table_name, ordinal_position'):
                cols_by_obj[(c["TABLE_SCHEMA"], c["TABLE_NAME"])].append(c)
        except Exception as exc:
            notes.append(f'{db}.information_schema.columns: {exc}')

        for schema in schemas:
            for kind, show in (("TABLE", "tables"), ("VIEW", "views")):
                try:
                    objects = run_sql(f'show {show} in schema "{db}"."{schema}"')
                except Exception as exc:
                    notes.append(f"{db}.{schema} {show}: {exc}")
                    continue
                for obj in objects:
                    inventory.append(
                        _record(run_sql, db, schema, kind, obj,
                                cols_by_obj.get((schema, obj["name"]), [])))

    collisions = detect_collisions([r["source_identifier"] for r in inventory])
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "session": {k: _jsonable(v) for k, v in session.items()},
        "databases_in_scope": databases,
        "object_count": len(inventory),
        "counts_by_type": dict(collections.Counter(r["object_type"] for r in inventory)),
        "identifier_case_collisions": collisions,
        "extraction_notes": notes,
        "inventory": inventory,
    }


def _record(run_sql, db: str, schema: str, kind: str, obj: dict,
            columns: list[dict]) -> dict:
    name = obj["name"]
    blocked_reasons: list[str] = []
    warnings: list[str] = []

    enriched = []
    for c in columns:
        m = map_type(c.get("DATA_TYPE"),
                     precision=c.get("NUMERIC_PRECISION"),
                     scale=c.get("NUMERIC_SCALE"),
                     char_length=c.get("CHARACTER_MAXIMUM_LENGTH"))
        if m.blocked:
            blocked_reasons.append(f'{c["COLUMN_NAME"]}: {m.reason}')
        if m.warning:
            warnings.append(f'{c["COLUMN_NAME"]}: {m.warning}')
        enriched.append({**c, "target_type": m.spark_type})

    rec = {
        "source_identifier": f"{db}.{schema}.{name}",
        "object_type": kind,
        "source_database": db,
        "source_schema": schema,
        "identifier_case_form": case_form(name),
        "migration_status": "discovered",
        "compatibility_status": "blocked" if blocked_reasons else "supported",
        "blocked_reasons": blocked_reasons,
        "warnings": warnings,
        "evidence_location": f"show {kind.lower()}s in {db}.{schema}",
        "columns": enriched,
        "source_metadata": {k: _jsonable(obj[k]) for k in _META_KEYS if k in obj},
    }

    try:
        rec["row_count_exact"] = run_sql(
            f'select count(*) N from "{db}"."{schema}"."{name}"')[0]["N"]
    except Exception as exc:
        rec["row_count_exact"] = None
        rec["row_count_error"] = str(exc)[:200]

    if kind == "VIEW":
        rec["view_text_show"] = obj.get("text")
        try:
            rec["view_ddl_get_ddl"] = run_sql(
                "select get_ddl('view', %(f)s) D",
                {"f": f'"{db}"."{schema}"."{name}"'})[0]["D"]
        except Exception as exc:
            rec["view_ddl_error"] = str(exc)[:200]
        # MVP-1 clones tables only. The existing Databricks rewriter raises
        # UnsupportedDDL(R15_VIEW_DEFERRED) for every view; there is no prior art.
        rec["compatibility_status"] = "requires_manual_design"
        rec["risk_level"] = "high"
        rec["recommended_approach"] = (
            "View translation is out of MVP-1 scope. Source SQL captured verbatim "
            "so a later translation can be diffed against it.")
    return rec
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_catalog.py -v`
Expected: PASS — 13 tests green.

- [ ] **Step 5: Commit**

```bash
git add engine/snowflake_source/extract/catalog.py engine/tests/test_catalog.py engine/tests/fake_sql.py
git commit -m "feat(snowflake-migrator): estate inventory with exact row counts and case capture"
```

---

## Task 6: Dependency edges, dual-source

**Files:**
- Create: `engine/snowflake_source/extract/dependencies.py`
- Test: `engine/tests/test_dependencies.py`

**Interfaces:**
- Consumes: `run_sql` (Task 4); `inventory.json` payload (Task 5)
- Produces:
  - `parse_view_references(ddl: str, *, default_db: str, default_schema: str) -> list[str]` — fully-qualified upper-cased names
  - `extract_dependencies(run_sql, inventory: dict) -> dict` — the `dependencies.json` payload of spec §3.2. Edge shape is `{"from": dependent, "to": dependency, "kind": ..., "source": "account_usage" | "parsed_ddl"}`, i.e. **`from` depends on `to`**, so `to` must be created first.

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_dependencies.py
"""Dependency extraction: ACCOUNT_USAGE preferred, parsed DDL as fallback."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.dependencies import (
    extract_dependencies, parse_view_references,
)

# The real view from the test estate: 4 fully-qualified names, 3 LEFT JOINs.
REAL_VIEW = """
create or replace view RAPPI_ORDER_360_VW(ORDER_ID, ITEM_COUNT) as
  SELECT o.ORDER_ID, COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM TEST_DB_20260908_1529.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.CUSTOMER_DIMENSIONS c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.STORE_DIMENSIONS s
    ON o.STORE_ID = s.STORE_ID
  LEFT JOIN TEST_DB_20260908_1529.PUBLIC.ORDER_ITEMS_FACT i
    ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;
"""


def test_parses_all_four_qualified_references():
    got = parse_view_references(REAL_VIEW, default_db="D", default_schema="S")
    assert got == [
        "TEST_DB_20260908_1529.PUBLIC.CUSTOMER_DIMENSIONS",
        "TEST_DB_20260908_1529.PUBLIC.ORDER_DIMENSIONS",
        "TEST_DB_20260908_1529.PUBLIC.ORDER_ITEMS_FACT",
        "TEST_DB_20260908_1529.PUBLIC.STORE_DIMENSIONS",
    ]


def test_bare_name_qualified_with_defaults():
    assert parse_view_references("select * from orders",
                                 default_db="D", default_schema="S") == ["D.S.ORDERS"]


def test_two_part_name_qualified_with_default_db():
    assert parse_view_references("select * from sales.orders",
                                 default_db="D", default_schema="S") == ["D.SALES.ORDERS"]


def test_subquery_after_from_is_not_a_reference():
    got = parse_view_references("select * from (select 1) t",
                                default_db="D", default_schema="S")
    assert got == []


def test_duplicate_references_deduplicated():
    sql = "select * from D.S.A join D.S.A b on 1=1"
    assert parse_view_references(sql, default_db="D", default_schema="S") == ["D.S.A"]


def test_account_usage_is_preferred_when_readable():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"}]}
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "D.S.V", "REFERENCED": "D.S.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, inv)
    assert out["source_used"] == "account_usage"
    assert out["edges"] == [{"from": "D.S.V", "to": "D.S.T",
                             "kind": "VIEW->TABLE", "source": "account_usage"}]


def test_falls_back_to_parsed_ddl_when_account_usage_denied():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "object_dependencies" in sql.lower():
                raise RuntimeError("Object does not exist or not authorized")
            return super().__call__(sql, params)

    out = extract_dependencies(Denied({}), inv)
    assert out["source_used"] == "parsed_ddl"
    assert out["edges"][0]["source"] == "parsed_ddl"
    assert "not authorized" in out["coverage_note"]


def test_fallback_drops_edges_to_objects_outside_the_inventory():
    # A reference we never inventoried cannot be planned, so it must not become
    # a phantom node in the wave graph.
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T join OTHER.X.Y on 1=1"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    out = extract_dependencies(Denied({}), inv)
    assert out["edges"] == []
    assert any("OTHER.X.Y" in n for n in out["unresolved_references"])


def test_tables_produce_no_edges_in_fallback_mode():
    inv = {"inventory": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                          "source_database": "D", "source_schema": "S"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    assert extract_dependencies(Denied({}), inv)["edges"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_dependencies.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowflake_source.extract.dependencies'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/snowflake_source/extract/dependencies.py
"""Dependency edges between estate objects. Read-only.

Dual-source on purpose. OBJECT_DEPENDENCIES lives in SNOWFLAKE.ACCOUNT_USAGE and
needs a grant a customer may refuse, so building the plan step on it alone would
make the whole feature hostage to a privilege.

  1. preferred -- SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
  2. fallback  -- parse the view DDL already captured during inventory

`source` is recorded per edge so a plan built on parsed DDL is never presented as
authoritative lineage.

Edge direction: {"from": dependent, "to": dependency}. `to` is created first.
"""
from __future__ import annotations

import re
from typing import Callable

__all__ = ["extract_dependencies", "parse_view_references"]

_IDENT = r'[A-Za-z_][A-Za-z0-9_$]*|"[^"]+"'
_REF = re.compile(
    rf'\b(?:FROM|JOIN)\s+((?:{_IDENT})(?:\s*\.\s*(?:{_IDENT})){{0,2}})',
    re.IGNORECASE)


def parse_view_references(ddl: str, *, default_db: str,
                          default_schema: str) -> list[str]:
    """Extract fully-qualified referenced object names from a view body."""
    if not ddl:
        return []
    found: set[str] = set()
    for match in _REF.finditer(ddl):
        parts = [p.strip().strip('"') for p in match.group(1).split(".")]
        parts = [p for p in parts if p]
        if not parts:
            continue
        if len(parts) == 1:
            parts = [default_db, default_schema, parts[0]]
        elif len(parts) == 2:
            parts = [default_db, parts[0], parts[1]]
        found.add(".".join(p.upper() for p in parts))
    return sorted(found)


def _from_account_usage(run_sql: Callable[..., list[dict]],
                        known: set[str]) -> list[dict]:
    rows = run_sql(
        "select referencing_database || '.' || referencing_schema || '.' || "
        "       referencing_object_name as REFERENCING, "
        "       referenced_database || '.' || referenced_schema || '.' || "
        "       referenced_object_name as REFERENCED, "
        "       referencing_object_domain as REFERENCING_TYPE, "
        "       referenced_object_domain as REFERENCED_TYPE "
        "from snowflake.account_usage.object_dependencies")
    edges = []
    for r in rows:
        dependent, dependency = r["REFERENCING"], r["REFERENCED"]
        if dependent in known and dependency in known and dependent != dependency:
            edges.append({
                "from": dependent, "to": dependency,
                "kind": f'{r.get("REFERENCING_TYPE")}->{r.get("REFERENCED_TYPE")}',
                "source": "account_usage",
            })
    return edges


def extract_dependencies(run_sql: Callable[..., list[dict]],
                         inventory: dict) -> dict:
    records = inventory.get("inventory", [])
    known = {r["source_identifier"].upper() for r in records}

    try:
        edges = _from_account_usage(run_sql, known)
        return {"edges": edges, "source_used": "account_usage",
                "coverage_note": "ACCOUNT_USAGE.OBJECT_DEPENDENCIES: authoritative "
                                 "lineage for all object types",
                "unresolved_references": []}
    except Exception as exc:
        note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES unavailable ({exc}); fell back to "
                "parsing view DDL. Covers view->object edges ONLY -- not "
                "authoritative lineage.")

    edges, unresolved = [], set()
    for rec in records:
        if rec.get("object_type") != "VIEW":
            continue
        dependent = rec["source_identifier"].upper()
        for dependency in parse_view_references(
                rec.get("view_ddl_get_ddl") or rec.get("view_text_show") or "",
                default_db=rec["source_database"],
                default_schema=rec["source_schema"]):
            if dependency == dependent:
                continue
            if dependency in known:
                edges.append({"from": dependent, "to": dependency,
                              "kind": "VIEW->OBJECT", "source": "parsed_ddl"})
            else:
                unresolved.add(dependency)
    return {"edges": edges, "source_used": "parsed_ddl", "coverage_note": note,
            "unresolved_references": sorted(unresolved)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_dependencies.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/snowflake_source/extract/dependencies.py engine/tests/test_dependencies.py
git commit -m "feat(snowflake-migrator): dual-source dependency extraction with no privilege dependency"
```

---

## Task 7: Topological waves and cycle detection

**Files:**
- Create: `engine/plan/waves.py`
- Test: `engine/tests/test_waves.py`

**Interfaces:**
- Consumes: edge list from Task 6
- Produces: `compute_waves(nodes: list[str], edges: list[dict], *, sort_key: Callable[[str], tuple] | None = None) -> dict` returning `{"waves": [[str]], "cycles": [[str]]}`. Dependencies land in earlier waves than their dependents. Nodes in a cycle appear in `cycles`, never in `waves`.

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_waves.py
"""Topological wave computation. Pure."""
from plan.waves import compute_waves


def e(dependent, dependency):
    return {"from": dependent, "to": dependency}


def test_no_edges_means_one_wave():
    out = compute_waves(["A", "B"], [])
    assert out["waves"] == [["A", "B"]]
    assert out["cycles"] == []


def test_dependency_lands_before_its_dependent():
    out = compute_waves(["V", "T"], [e("V", "T")])
    assert out["waves"] == [["T"], ["V"]]


def test_three_level_chain():
    out = compute_waves(["A", "B", "C"], [e("C", "B"), e("B", "A")])
    assert out["waves"] == [["A"], ["B"], ["C"]]


def test_diamond_collapses_to_three_waves():
    out = compute_waves(["TOP", "L", "R", "BASE"],
                        [e("TOP", "L"), e("TOP", "R"), e("L", "BASE"), e("R", "BASE")])
    assert out["waves"] == [["BASE"], ["L", "R"], ["TOP"]]


def test_within_a_wave_nodes_are_sorted_deterministically():
    out = compute_waves(["Z", "A", "M"], [])
    assert out["waves"] == [["A", "M", "Z"]]


def test_custom_sort_key_orders_within_a_wave():
    sizes = {"A": 30, "B": 10, "C": 20}
    out = compute_waves(["A", "B", "C"], [], sort_key=lambda n: (sizes[n], n))
    assert out["waves"] == [["B", "C", "A"]], "smallest first"


def test_two_node_cycle_reported_not_waved():
    out = compute_waves(["A", "B"], [e("A", "B"), e("B", "A")])
    assert out["waves"] == []
    assert sorted(out["cycles"][0]) == ["A", "B"]


def test_cycle_isolated_and_the_rest_still_planned():
    out = compute_waves(["A", "B", "OK"], [e("A", "B"), e("B", "A")])
    assert out["waves"] == [["OK"]]
    assert sorted(out["cycles"][0]) == ["A", "B"]


def test_self_edge_is_ignored_not_a_cycle():
    out = compute_waves(["A"], [e("A", "A")])
    assert out["waves"] == [["A"]]
    assert out["cycles"] == []


def test_edges_to_unknown_nodes_are_ignored():
    out = compute_waves(["A"], [e("A", "GHOST")])
    assert out["waves"] == [["A"]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_waves.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'plan.waves'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/plan/waves.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_waves.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/plan/waves.py engine/tests/test_waves.py
git commit -m "feat(snowflake-migrator): topological waves with cycles reported not broken"
```

---

## Task 8: Medallion layer assignment and namespace strategies

**Files:**
- Create: `engine/plan/medallion.py`
- Test: `engine/tests/test_medallion.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `LAYERS = ("BRONZE", "SILVER", "GOLD")`
  - `STRATEGIES = ("layer-catalog", "preserve-source", "layer-flattened")`
  - `assign_layer(source_db: str, source_schema: str, user_map: dict[str, str] | None = None, *, source_identifier: str | None = None) -> tuple[str, str]` returning `(layer, basis)` where basis is `"user_provided" | "matched_rule" | "fallback"`
  - `target_name(source_db: str, source_schema: str, object_name: str, layer: str, strategy: str) -> str`
  - `detect_target_collisions(mapping: dict[str, str]) -> dict[str, list[str]]` — key is the target name, value the source identifiers colliding on it
  - `class UnknownStrategy(ValueError)`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_medallion.py
"""Layer assignment and target naming. Pure."""
import pytest

from plan.medallion import (
    LAYERS, STRATEGIES, UnknownStrategy, assign_layer, detect_target_collisions,
    target_name,
)


@pytest.mark.parametrize("name,expected", [
    ("BRONZE_PROD", "BRONZE"), ("raw_events", "BRONZE"), ("STG_ORDERS", "BRONZE"),
    ("landing", "BRONZE"), ("SILVER_PROD", "SILVER"), ("curated", "SILVER"),
    ("conformed_x", "SILVER"), ("GOLD_PROD", "GOLD"), ("datamart", "GOLD"),
    ("dm_finance", "GOLD"), ("reporting", "GOLD"),
])
def test_heuristic_matches_layer_names(name, expected):
    layer, basis = assign_layer(name, "PUBLIC")
    assert (layer, basis) == (expected, "matched_rule")


def test_schema_name_matches_when_database_does_not():
    assert assign_layer("ANALYTICS", "gold_marts")[0] == "GOLD"


def test_database_wins_over_schema():
    assert assign_layer("GOLD_PROD", "staging")[0] == "GOLD"


def test_unmatched_falls_back_to_bronze_and_says_so():
    layer, basis = assign_layer("ANALYTICS", "PUBLIC")
    assert (layer, basis) == ("BRONZE", "fallback")


def test_user_map_overrides_the_heuristic():
    layer, basis = assign_layer("GOLD_PROD", "PUBLIC",
                                {"GOLD_PROD.PUBLIC.T": "SILVER"},
                                source_identifier="GOLD_PROD.PUBLIC.T")
    assert (layer, basis) == ("SILVER", "user_provided")


def test_user_map_accepts_a_database_level_key():
    layer, basis = assign_layer("ANALYTICS", "PUBLIC", {"ANALYTICS": "GOLD"},
                                source_identifier="ANALYTICS.PUBLIC.T")
    assert (layer, basis) == ("GOLD", "user_provided")


def test_user_map_value_is_validated():
    with pytest.raises(ValueError, match="PLATINUM"):
        assign_layer("D", "S", {"D": "PLATINUM"}, source_identifier="D.S.T")


def test_layer_catalog_is_the_default_shape():
    assert target_name("MYDB", "SALES", "ORDERS", "BRONZE",
                       "layer-catalog") == "bronze.SALES.ORDERS"


def test_preserve_source_keeps_all_three_parts():
    assert target_name("MYDB", "SALES", "ORDERS", "GOLD",
                       "preserve-source") == "MYDB.SALES.ORDERS"


def test_layer_flattened_folds_db_into_schema():
    assert target_name("MYDB", "SALES", "ORDERS", "SILVER",
                       "layer-flattened") == "silver.MYDB_SALES.ORDERS"


def test_unknown_strategy_rejected():
    with pytest.raises(UnknownStrategy):
        target_name("D", "S", "T", "BRONZE", "whatever")


def test_layer_catalog_collides_across_databases():
    # This is exactly why the strategy needs a collision check: layer-catalog
    # drops the source database, so two DBs sharing schema.table merge.
    mapping = {
        "DB1.PUBLIC.ORDERS": target_name("DB1", "PUBLIC", "ORDERS", "BRONZE", "layer-catalog"),
        "DB2.PUBLIC.ORDERS": target_name("DB2", "PUBLIC", "ORDERS", "BRONZE", "layer-catalog"),
    }
    got = detect_target_collisions(mapping)
    assert got == {"bronze.PUBLIC.ORDERS": ["DB1.PUBLIC.ORDERS", "DB2.PUBLIC.ORDERS"]}


def test_preserve_source_never_collides():
    mapping = {
        "DB1.PUBLIC.ORDERS": target_name("DB1", "PUBLIC", "ORDERS", "BRONZE", "preserve-source"),
        "DB2.PUBLIC.ORDERS": target_name("DB2", "PUBLIC", "ORDERS", "BRONZE", "preserve-source"),
    }
    assert detect_target_collisions(mapping) == {}


def test_target_collision_check_is_case_insensitive():
    assert detect_target_collisions({"A": "bronze.S.T", "B": "BRONZE.S.T"})


def test_exported_constants():
    assert LAYERS == ("BRONZE", "SILVER", "GOLD")
    assert "layer-catalog" in STRATEGIES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_medallion.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'plan.medallion'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/plan/medallion.py
"""Medallion layer assignment and target namespace strategies. Pure.

"Generic unless it was provided": a generic name heuristic decides the layer by
default, and an explicitly-supplied mapping overrides it. An object that matches
nothing falls back to BRONZE and is REPORTED as a fallback -- never silently
assigned -- so the user can correct it before anything is created.

Strategy is a parameter, not a structural commitment, so the naming decision is
not a one-way door.
"""
from __future__ import annotations

import collections

__all__ = ["LAYERS", "STRATEGIES", "UnknownStrategy", "assign_layer",
           "detect_target_collisions", "target_name"]

LAYERS = ("BRONZE", "SILVER", "GOLD")
STRATEGIES = ("layer-catalog", "preserve-source", "layer-flattened")

# Order matters: GOLD and SILVER are checked before BRONZE so that a name like
# "gold_staging" resolves to GOLD rather than matching BRONZE's "stg".
_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("GOLD", ("gold", "mart", "dm_", "datamart", "reporting", "report")),
    ("SILVER", ("silver", "curated", "clean", "conformed")),
    ("BRONZE", ("bronze", "raw", "stg", "staging", "landing")),
)


class UnknownStrategy(ValueError):
    """Namespace strategy is not one of STRATEGIES."""


def _match(text: str) -> str | None:
    low = text.lower()
    for layer, needles in _RULES:
        if any(n in low for n in needles):
            return layer
    return None


def assign_layer(source_db: str, source_schema: str,
                 user_map: dict[str, str] | None = None, *,
                 source_identifier: str | None = None) -> tuple[str, str]:
    if user_map:
        candidates = []
        if source_identifier:
            candidates += [source_identifier, source_identifier.upper()]
        candidates += [f"{source_db}.{source_schema}", source_db]
        for cand in candidates:
            if cand in user_map:
                layer = user_map[cand].upper()
                if layer not in LAYERS:
                    raise ValueError(
                        f"invalid layer {user_map[cand]!r} for {cand!r}; "
                        f"expected one of {LAYERS}")
                return layer, "user_provided"

    for text in (source_db, source_schema):
        hit = _match(text)
        if hit:
            return hit, "matched_rule"
    return "BRONZE", "fallback"


def target_name(source_db: str, source_schema: str, object_name: str,
                layer: str, strategy: str) -> str:
    if strategy not in STRATEGIES:
        raise UnknownStrategy(
            f"unknown namespace strategy {strategy!r}; expected {STRATEGIES}")
    if strategy == "preserve-source":
        return f"{source_db}.{source_schema}.{object_name}"
    if strategy == "layer-catalog":
        return f"{layer.lower()}.{source_schema}.{object_name}"
    return f"{layer.lower()}.{source_db}_{source_schema}.{object_name}"


def detect_target_collisions(mapping: dict[str, str]) -> dict[str, list[str]]:
    """Two source objects mapping to one target name. Empty result means safe."""
    buckets: dict[str, list[str]] = collections.defaultdict(list)
    for source, target in mapping.items():
        buckets[target.upper()].append(source)
    return {mapping[v[0]]: sorted(v) for v in buckets.values() if len(v) > 1}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_medallion.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/plan/medallion.py engine/tests/test_medallion.py
git commit -m "feat(snowflake-migrator): medallion assignment with fallback reported and collisions detected"
```

---

## Task 9: Plan assembly

**Files:**
- Create: `engine/plan/build.py`
- Test: `engine/tests/test_plan_build.py`

**Interfaces:**
- Consumes: `compute_waves` (Task 7); `assign_layer`, `target_name`, `detect_target_collisions` (Task 8)
- Produces:
  - `build_plan(inventory: dict, dependencies: dict, *, user_map: dict[str, str] | None = None, strategy: str = "layer-catalog") -> dict` — the `plan.json` payload of spec §3.3
  - `class TargetCollision(RuntimeError)` carrying `.collisions: dict[str, list[str]]`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_plan_build.py
"""plan.json assembly. Pure."""
import pytest

from plan.build import TargetCollision, build_plan


def rec(ident, kind="TABLE", rows=10, status="supported", blocked=()):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "compatibility_status": status, "blocked_reasons": list(blocked),
            "row_count_exact": rows, "source_metadata": {"bytes": rows * 10}}


def test_single_table_gets_a_wave_and_a_target():
    plan = build_plan({"inventory": [rec("BRONZE_PROD.PUBLIC.ORDERS")]},
                      {"edges": []})
    assert plan["waves"] == [["BRONZE_PROD.PUBLIC.ORDERS"]]
    assert plan["target_names"]["BRONZE_PROD.PUBLIC.ORDERS"] == "bronze.PUBLIC.ORDERS"
    assert plan["medallion_assignment"]["BRONZE_PROD.PUBLIC.ORDERS"] == "BRONZE"
    assert plan["medallion_assignment_basis"]["BRONZE_PROD.PUBLIC.ORDERS"] == "matched_rule"


def test_strategy_recorded_in_the_plan():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []},
                      strategy="preserve-source")
    assert plan["namespace_strategy"] == "preserve-source"
    assert plan["target_names"]["D.S.T"] == "D.S.T"


def test_fallback_assignments_are_listed_for_review():
    plan = build_plan({"inventory": [rec("ANALYTICS.PUBLIC.T")]}, {"edges": []})
    assert plan["fallback_assignments"] == ["ANALYTICS.PUBLIC.T"]


def test_blocked_objects_excluded_from_waves_and_listed():
    inv = {"inventory": [rec("D.S.OK"),
                         rec("D.S.BAD", status="blocked", blocked=["PAYLOAD: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    assert plan["waves"] == [["D.S.OK"]]
    assert plan["blocked"] == [{"source_identifier": "D.S.BAD",
                                "reason": "PAYLOAD: VARIANT"}]


def test_views_are_planned_but_marked_unsupported_for_mvp1():
    inv = {"inventory": [rec("D.S.T"),
                         rec("D.S.V", kind="VIEW", status="requires_manual_design")]}
    plan = build_plan(inv, {"edges": [{"from": "D.S.V", "to": "D.S.T"}]})
    assert plan["waves"] == [["D.S.T"], ["D.S.V"]]
    assert any(u["source_identifier"] == "D.S.V" and u["feature"] == "VIEW"
               for u in plan["unsupported"])
    assert "D.S.V" not in plan["clone_targets"], "MVP-1 clones tables only"
    assert plan["clone_targets"] == ["D.S.T"]


def test_smaller_tables_ordered_first_within_a_wave():
    inv = {"inventory": [rec("D.S.BIG", rows=1000), rec("D.S.SMALL", rows=5)]}
    plan = build_plan(inv, {"edges": []})
    assert plan["waves"] == [["D.S.SMALL", "D.S.BIG"]]


def test_cycles_surfaced_from_waves():
    inv = {"inventory": [rec("D.S.A", kind="VIEW", status="requires_manual_design"),
                         rec("D.S.B", kind="VIEW", status="requires_manual_design")]}
    plan = build_plan(inv, {"edges": [{"from": "D.S.A", "to": "D.S.B"},
                                      {"from": "D.S.B", "to": "D.S.A"}]})
    assert sorted(plan["cycles"][0]) == ["D.S.A", "D.S.B"]


def test_target_collision_halts_rather_than_guessing():
    inv = {"inventory": [rec("DB1.PUBLIC.ORDERS"), rec("DB2.PUBLIC.ORDERS")]}
    with pytest.raises(TargetCollision) as exc:
        build_plan(inv, {"edges": []})
    assert "bronze.PUBLIC.ORDERS" in exc.value.collisions


def test_preserve_source_avoids_that_collision():
    inv = {"inventory": [rec("DB1.PUBLIC.ORDERS"), rec("DB2.PUBLIC.ORDERS")]}
    plan = build_plan(inv, {"edges": []}, strategy="preserve-source")
    assert len(plan["target_names"]) == 2


def test_dependency_source_carried_into_the_plan():
    plan = build_plan({"inventory": [rec("D.S.T")]},
                      {"edges": [], "source_used": "parsed_ddl",
                       "coverage_note": "view->object edges ONLY"})
    assert plan["dependency_source"] == "parsed_ddl"
    assert "ONLY" in plan["dependency_coverage_note"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_plan_build.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'plan.build'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/plan/build.py
"""Assemble plan.json from an inventory and a dependency graph. Pure.

Wave ordering inside a layer is dependency depth, then object size, then name.
Usage-frequency ranking is the documented extension point for when the deferred
compute-calculation work lands: pass a sort_key that consults a usage profile.

A target-name collision raises. Two source objects silently merging into one
target table is a data-loss defect, so it stops the run.
"""
from __future__ import annotations

import datetime

from .medallion import assign_layer, detect_target_collisions, target_name
from .waves import compute_waves

__all__ = ["build_plan", "TargetCollision"]


class TargetCollision(RuntimeError):
    """Two or more source objects map to the same target name."""

    def __init__(self, collisions: dict[str, list[str]]):
        self.collisions = collisions
        detail = "; ".join(f"{t} <- {sorted(s)}" for t, s in collisions.items())
        super().__init__(f"target name collision, refusing to guess: {detail}")


def build_plan(inventory: dict, dependencies: dict, *,
               user_map: dict[str, str] | None = None,
               strategy: str = "layer-catalog") -> dict:
    records = inventory.get("inventory", [])

    assignment: dict[str, str] = {}
    basis: dict[str, str] = {}
    targets: dict[str, str] = {}
    blocked: list[dict] = []
    unsupported: list[dict] = []

    for rec in records:
        ident = rec["source_identifier"]
        db, schema, name = ident.split(".", 2)
        layer, why = assign_layer(db, schema, user_map, source_identifier=ident)
        assignment[ident] = layer
        basis[ident] = why
        targets[ident] = target_name(db, schema, name, layer, strategy)

        if rec.get("compatibility_status") == "blocked":
            reason = "; ".join(rec.get("blocked_reasons") or ["unspecified"])
            blocked.append({"source_identifier": ident, "reason": reason})
        if rec.get("object_type") == "VIEW":
            unsupported.append({
                "source_identifier": ident, "feature": "VIEW",
                "resolution": "View translation is out of MVP-1 scope. Source SQL is "
                              "captured verbatim in the inventory; deploy views "
                              "topologically in a later phase."})

    collisions = detect_target_collisions(targets)
    if collisions:
        raise TargetCollision(collisions)

    blocked_ids = {b["source_identifier"] for b in blocked}
    planned = [r for r in records if r["source_identifier"] not in blocked_ids]
    sizes = {r["source_identifier"]: (r.get("row_count_exact") or 0) for r in planned}
    waved = compute_waves(
        [r["source_identifier"] for r in planned],
        dependencies.get("edges", []),
        sort_key=lambda n: (sizes.get(n, 0), n))

    clone_targets = [r["source_identifier"] for r in planned
                     if r.get("object_type") == "TABLE"]

    return {
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "namespace_strategy": strategy,
        "waves": waved["waves"],
        "cycles": waved["cycles"],
        "medallion_assignment": assignment,
        "medallion_assignment_basis": basis,
        "fallback_assignments": sorted(k for k, v in basis.items() if v == "fallback"),
        "target_names": targets,
        "clone_targets": clone_targets,
        "blocked": blocked,
        "unsupported": unsupported,
        "dependency_source": dependencies.get("source_used"),
        "dependency_coverage_note": dependencies.get("coverage_note"),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_plan_build.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/plan/build.py engine/tests/test_plan_build.py
git commit -m "feat(snowflake-migrator): plan assembly halting on target-name collision"
```

---

## Task 10: Runtime-only target resolution

**Files:**
- Create: `engine/target/coords.py`
- Test: `engine/tests/test_coords.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `@dataclass(frozen=True) Target(datalake_ocid: str, workspace: str, cluster_id: str, catalog: str)`
  - `resolve_target(*, datalake_ocid=None, workspace=None, cluster_id=None, catalog=None) -> Target`
  - `class MissingTarget(RuntimeError)`
  - `REGIONS: dict[str, str]` and `region_from_ocid(ocid: str) -> str`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_coords.py
"""AIDP target coordinates: runtime-supplied only, never discovered."""
import pathlib

import pytest

from target.coords import MissingTarget, Target, region_from_ocid, resolve_target

OK = dict(datalake_ocid="ocid1.aidataplatform.oc1.iad.aaaa",
          workspace="ws-1", cluster_id="cl-1", catalog="bronze")


def test_all_four_arguments_produce_a_target():
    t = resolve_target(**OK)
    assert isinstance(t, Target)
    assert t.catalog == "bronze"


@pytest.mark.parametrize("missing", list(OK))
def test_each_argument_is_mandatory(missing):
    args = {k: v for k, v in OK.items() if k != missing}
    with pytest.raises(MissingTarget, match=missing):
        resolve_target(**args)


def test_nothing_supplied_names_all_four():
    with pytest.raises(MissingTarget) as exc:
        resolve_target()
    for k in OK:
        assert k in str(exc.value)


def test_environment_variables_are_ignored(monkeypatch):
    # The safety requirement: coordinates cannot be DISCOVERED, only passed.
    for k, v in OK.items():
        monkeypatch.setenv(k.upper(), v)
        monkeypatch.setenv(f"AIDP_{k.upper()}", v)
    with pytest.raises(MissingTarget):
        resolve_target()


def test_module_contains_no_environment_or_file_reads():
    # Enforced by inspection so a future edit cannot quietly add a lookup.
    src = pathlib.Path(region_from_ocid.__module__.replace(".", "/") + ".py")
    text = (pathlib.Path(__file__).resolve().parents[1] / "target/coords.py").read_text()
    for forbidden in ("os.environ", "getenv", "open(", "read_text", "Path("):
        assert forbidden not in text, f"coords.py must not use {forbidden}"


def test_target_is_immutable():
    with pytest.raises(Exception):
        resolve_target(**OK).catalog = "gold"


def test_blank_strings_rejected_like_missing():
    with pytest.raises(MissingTarget):
        resolve_target(**{**OK, "workspace": "   "})


@pytest.mark.parametrize("short,region", [
    ("iad", "us-ashburn-1"), ("phx", "us-phoenix-1"), ("fra", "eu-frankfurt-1"),
])
def test_region_derived_from_ocid(short, region):
    assert region_from_ocid(f"ocid1.aidataplatform.oc1.{short}.aaaa") == region


def test_unmapped_region_code_errors_rather_than_defaulting():
    with pytest.raises(ValueError, match="zzz"):
        region_from_ocid("ocid1.aidataplatform.oc1.zzz.aaaa")


def test_malformed_ocid_errors():
    with pytest.raises(ValueError):
        region_from_ocid("not-an-ocid")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_coords.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'target.coords'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/target/coords.py
"""AIDP target coordinates.

DELIBERATELY INERT. This module cannot discover a target and cannot persist one.
It reads no environment variable, no config file, and no cache, and it writes
nothing to disk -- a test asserts that by inspecting this file's source.

Coordinates arrive as function arguments, supplied in the conversation that uses
them. Absent coordinates are a hard error instructing the caller to ask the user.

An unmapped region short-code errors rather than defaulting: guessing a region
would point a write at the wrong tenancy.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["MissingTarget", "REGIONS", "Target", "region_from_ocid", "resolve_target"]

REGIONS = {
    "iad": "us-ashburn-1", "phx": "us-phoenix-1", "fra": "eu-frankfurt-1",
    "lhr": "uk-london-1", "bom": "ap-mumbai-1", "hyd": "ap-hyderabad-1",
    "sin": "ap-singapore-1", "nrt": "ap-tokyo-1", "syd": "ap-sydney-1",
    "gru": "sa-saopaulo-1", "yyz": "ca-toronto-1", "icn": "ap-seoul-1",
}


class MissingTarget(RuntimeError):
    """One or more AIDP target coordinates were not supplied."""


@dataclass(frozen=True)
class Target:
    datalake_ocid: str
    workspace: str
    cluster_id: str
    catalog: str


def resolve_target(*, datalake_ocid: str | None = None, workspace: str | None = None,
                   cluster_id: str | None = None,
                   catalog: str | None = None) -> Target:
    supplied = {"datalake_ocid": datalake_ocid, "workspace": workspace,
                "cluster_id": cluster_id, "catalog": catalog}
    missing = [k for k, v in supplied.items() if not (v and str(v).strip())]
    if missing:
        raise MissingTarget(
            "AIDP target coordinates not supplied: " + ", ".join(sorted(missing)) +
            ". These are never read from the environment or a config file -- ask the "
            "user for them and pass them explicitly.")
    return Target(**{k: str(v).strip() for k, v in supplied.items()})


def region_from_ocid(ocid: str) -> str:
    parts = (ocid or "").split(".")
    if len(parts) < 5 or parts[0] != "ocid1":
        raise ValueError(f"malformed OCID: {ocid!r}")
    code = parts[3]
    if code not in REGIONS:
        raise ValueError(
            f"unmapped OCI region short-code {code!r}; add it to REGIONS rather than "
            "defaulting -- guessing a region would target the wrong tenancy")
    return REGIONS[code]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_coords.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/target/coords.py engine/tests/test_coords.py
git commit -m "feat(snowflake-migrator): inert target resolution that cannot discover or persist coordinates"
```

---

## Task 11: CREATE TABLE generation with rule audit

**Files:**
- Create: `engine/target/ddl.py`
- Test: `engine/tests/test_ddl.py`

**Interfaces:**
- Consumes: inventory records (Task 5); target names (Task 8)
- Produces:
  - `@dataclass(frozen=True) RuleApplication(rule_id: str, detail: str)`
  - `@dataclass RewriteResult(source_identifier, target_fqn, sql, rules_applied, warnings, omitted_properties, blocked, blocked_reason)`
  - `class UnsupportedDDL(Exception)` with `.rule_id`
  - `build_create_schema(catalog: str, schema: str) -> str`
  - `build_create_table(record: dict, target_fqn: str) -> RewriteResult`
  - `SCRUBBED_PROPERTIES: tuple[str, ...]`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_ddl.py
"""Spark/Delta DDL generation with a per-rule audit trail. Pure."""
import pytest

from target.ddl import (
    RewriteResult, SCRUBBED_PROPERTIES, build_create_schema, build_create_table,
)


def col(name, dt, target, *, nullable="YES", pos=1, comment=None,
        precision=None, scale=None):
    return {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
            "IS_NULLABLE": nullable, "ORDINAL_POSITION": pos, "COMMENT": comment,
            "NUMERIC_PRECISION": precision, "NUMERIC_SCALE": scale}


def record(columns, **over):
    r = {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "source_database": "D", "source_schema": "PUBLIC",
         "compatibility_status": "supported", "blocked_reasons": [],
         "columns": columns, "source_metadata": {}}
    r.update(over)
    return r


def test_minimal_table_sql():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO"),
                col("PAID", "NUMBER", "DECIMAL(18,2)", pos=2)]),
        "bronze.PUBLIC.ORDERS")
    assert isinstance(res, RewriteResult)
    assert res.sql == (
        "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (\n"
        "  `ORDER_ID` DECIMAL(38,0) NOT NULL,\n"
        "  `PAID` DECIMAL(18,2)\n"
        ")\nUSING DELTA")
    assert res.blocked is False


def test_never_emits_create_or_replace():
    res = build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.S.T")
    assert "OR REPLACE" not in res.sql
    assert "IF NOT EXISTS" in res.sql


def test_columns_ordered_by_ordinal_position():
    res = build_create_table(
        record([col("SECOND", "TEXT", "STRING", pos=2),
                col("FIRST", "TEXT", "STRING", pos=1)]), "bronze.S.T")
    assert res.sql.index("`FIRST`") < res.sql.index("`SECOND`")


def test_column_comment_emitted_and_escaped():
    res = build_create_table(
        record([col("A", "TEXT", "STRING", comment="it's fine")]), "bronze.S.T")
    assert "COMMENT 'it''s fine'" in res.sql


def test_rules_recorded_with_ids():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(5,2)", precision=5, scale=2)]),
        "bronze.S.T")
    ids = {r.rule_id for r in res.rules_applied}
    assert {"R01_TARGET_NAME", "R02_QUOTE_BACKTICK", "R30_USING_DELTA"} <= ids
    assert any(r.rule_id == "R03_TYPE_MAP" and "DECIMAL(5,2)" in r.detail
               for r in res.rules_applied)


@pytest.mark.parametrize("prop", ["cluster_by", "retention_time", "change_tracking"])
def test_snowflake_properties_scrubbed_and_recorded(prop):
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: "something"}),
        "bronze.S.T")
    assert any(prop in o for o in res.omitted_properties)
    assert prop not in res.sql
    assert prop in SCRUBBED_PROPERTIES


def test_blocked_record_produces_no_sql():
    res = build_create_table(
        record([col("P", "VARIANT", None)], compatibility_status="blocked",
               blocked_reasons=["P: VARIANT semi-structured"]),
        "bronze.S.T")
    assert res.blocked is True
    assert res.sql is None
    assert "VARIANT" in res.blocked_reason


def test_column_with_no_target_type_blocks_the_table():
    res = build_create_table(record([col("A", "TEXT", "STRING"),
                                     col("P", "GEOGRAPHY", None, pos=2)]),
                             "bronze.S.T")
    assert res.blocked is True
    assert "GEOGRAPHY" in res.blocked_reason


def test_table_with_no_columns_is_blocked():
    res = build_create_table(record([]), "bronze.S.T")
    assert res.blocked is True
    assert "no columns" in res.blocked_reason.lower()


def test_constraints_are_reported_not_emitted():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(38,0)", nullable="NO")],
               constraints=[{"type": "PRIMARY KEY", "columns": ["A"]}]),
        "bronze.S.T")
    assert "PRIMARY KEY" not in res.sql
    assert any(r.rule_id == "R20_CONSTRAINTS_NOT_EMITTED" for r in res.rules_applied)


def test_create_schema_never_emits_comment():
    # AIDP silently fails to persist CREATE SCHEMA ... COMMENT, and ISO-timestamp
    # colons in the comment are the specific trigger. Never emit it.
    sql = build_create_schema("bronze", "PUBLIC")
    assert sql == "CREATE SCHEMA IF NOT EXISTS `bronze`.`PUBLIC`"
    assert "COMMENT" not in sql


def test_target_fqn_must_be_three_part():
    with pytest.raises(ValueError, match="three-part"):
        build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.ORDERS")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_ddl.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'target.ddl'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/target/ddl.py
"""Spark/Delta DDL generation. Pure functions, zero I/O.

Every transformation is attributable to a named rule, and every dropped property
is recorded. This audit shape is carried over from the Databricks migrator's
catalog_ddl_rewriter.py, which is the one part of it worth keeping.

Two AIDP-specific behaviours are encoded here rather than rediscovered:
  * CREATE SCHEMA ... COMMENT silently fails to persist -- specifically when the
    comment contains ISO-timestamp colons -- so COMMENT is never emitted on a
    schema create.
  * USING DELTA is always explicit, so the managed-table format does not depend
    on a cluster default.
"""
from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["RuleApplication", "RewriteResult", "UnsupportedDDL",
           "SCRUBBED_PROPERTIES", "build_create_schema", "build_create_table"]

# Snowflake table properties with no Delta equivalent. Recorded, never emitted.
SCRUBBED_PROPERTIES = (
    "cluster_by", "retention_time", "change_tracking", "is_iceberg", "is_dynamic",
    "is_secure", "max_data_extension_time_in_days", "data_retention_time_in_days",
    "owner", "rows", "bytes", "created_on",
)


@dataclass(frozen=True)
class RuleApplication:
    rule_id: str
    detail: str


@dataclass
class RewriteResult:
    source_identifier: str
    target_fqn: str
    sql: str | None
    rules_applied: list[RuleApplication] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    omitted_properties: list[str] = field(default_factory=list)
    blocked: bool = False
    blocked_reason: str | None = None


class UnsupportedDDL(Exception):
    def __init__(self, rule_id: str, message: str):
        self.rule_id = rule_id
        super().__init__(f"{rule_id}: {message}")


def _q(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _qualify(fqn: str) -> str:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"target_fqn must be three-part catalog.schema.table: {fqn!r}")
    return ".".join(_q(p) for p in parts)


def build_create_schema(catalog: str, schema: str) -> str:
    # R14: no COMMENT here, ever. See module docstring.
    return f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}"


def build_create_table(record: dict, target_fqn: str) -> RewriteResult:
    qualified = _qualify(target_fqn)
    res = RewriteResult(record["source_identifier"], target_fqn, None)
    res.rules_applied.append(RuleApplication(
        "R01_TARGET_NAME", f'{record["source_identifier"]} -> {target_fqn}'))
    res.rules_applied.append(RuleApplication(
        "R02_QUOTE_BACKTICK", "Snowflake double-quote identifiers -> Spark backticks"))

    if record.get("compatibility_status") == "blocked":
        res.blocked = True
        res.blocked_reason = "; ".join(record.get("blocked_reasons") or ["unspecified"])
        return res

    columns = sorted(record.get("columns") or [],
                     key=lambda c: c.get("ORDINAL_POSITION") or 0)
    if not columns:
        res.blocked = True
        res.blocked_reason = ("table has no columns visible to this role "
                             "(Delta-shared or insufficient privilege)")
        return res

    unmapped = [c["COLUMN_NAME"] + ": " + str(c.get("DATA_TYPE"))
                for c in columns if not c.get("target_type")]
    if unmapped:
        res.blocked = True
        res.blocked_reason = "unmapped column types: " + "; ".join(unmapped)
        return res

    lines = []
    for c in columns:
        piece = f'  {_q(c["COLUMN_NAME"])} {c["target_type"]}'
        if str(c.get("IS_NULLABLE", "YES")).upper() == "NO":
            piece += " NOT NULL"
        if c.get("COMMENT"):
            piece += " COMMENT '" + str(c["COMMENT"]).replace("'", "''") + "'"
        lines.append(piece)
        res.rules_applied.append(RuleApplication(
            "R03_TYPE_MAP",
            f'{c["COLUMN_NAME"]}: {c.get("DATA_TYPE")} -> {c["target_type"]}'))

    for prop, value in (record.get("source_metadata") or {}).items():
        if prop in SCRUBBED_PROPERTIES and value not in (None, "", "false"):
            res.omitted_properties.append(f"{prop}={value}")
    if res.omitted_properties:
        res.rules_applied.append(RuleApplication(
            "R10_PROP_SCRUB",
            "dropped Snowflake properties with no Delta equivalent: "
            + ", ".join(res.omitted_properties)))

    if record.get("constraints"):
        res.rules_applied.append(RuleApplication(
            "R20_CONSTRAINTS_NOT_EMITTED",
            "Snowflake PK/FK/UNIQUE are unenforced metadata and Delta does not "
            "enforce them either; captured in the inventory, not emitted as DDL"))

    res.rules_applied.append(RuleApplication(
        "R30_USING_DELTA", "explicit USING DELTA so format is not cluster-default"))
    res.sql = (f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
               + ",\n".join(lines) + "\n)\nUSING DELTA")

    for c in columns:
        for w in record.get("warnings") or []:
            if w.startswith(c["COLUMN_NAME"] + ":") and w not in res.warnings:
                res.warnings.append(w)
    return res
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_ddl.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/target/ddl.py engine/tests/test_ddl.py
git commit -m "feat(snowflake-migrator): Delta DDL generation with per-rule audit trail"
```

---

## Task 12: Deployment, dry-run by default

**Files:**
- Create: `engine/target/deploy.py`
- Test: `engine/tests/test_deploy.py`

**Interfaces:**
- Consumes: `Target`, `MissingTarget` (Task 10); `ddl_plan` (Task 11 output assembled by the CLI)
- Produces:
  - `deploy(ddl_plan: dict, *, target=None, execute: bool = False, run_sql=None, chunk_size: int = 25) -> dict`
  - `class RefusedToExecute(RuntimeError)`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_deploy.py
"""Deployment gating, batching, and per-statement verification."""
import pytest

from target.coords import resolve_target
from target.deploy import RefusedToExecute, deploy

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="bronze")


def plan(n=1):
    return {"statements": [
        {"source_identifier": f"D.S.T{i}", "target_fqn": f"bronze.S.T{i}",
         "sql": f"CREATE TABLE IF NOT EXISTS `bronze`.`S`.`T{i}` (`A` STRING)\nUSING DELTA"}
        for i in range(n)], "blocked": []}


class Recorder:
    def __init__(self, existing=None, fail_on=None):
        self.calls = []
        self.existing = existing
        self.fail_on = fail_on or ()

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        if any(f in sql for f in self.fail_on):
            raise RuntimeError("boom")
        if sql.lstrip().upper().startswith("SHOW TABLES"):
            name = sql.split("LIKE")[1].strip().strip("'")
            if self.existing is None or name in self.existing:
                return [{"tableName": name}]
            return []
        return [{"status": "ok"}]


def test_dry_run_is_the_default_and_touches_nothing():
    rec = Recorder()
    out = deploy(plan(2), run_sql=rec)
    assert out["dry_run"] is True
    assert out["executed"] == 0
    assert rec.calls == [], "dry run must not issue a single statement"
    assert len(out["statements"]) == 2


def test_execute_without_a_target_is_refused():
    with pytest.raises(RefusedToExecute, match="target"):
        deploy(plan(), execute=True, run_sql=Recorder())


def test_execute_without_run_sql_is_refused():
    with pytest.raises(RefusedToExecute, match="run_sql"):
        deploy(plan(), execute=True, target=TARGET)


def test_execute_creates_the_schema_before_the_tables():
    rec = Recorder()
    deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    joined = " || ".join(rec.calls)
    assert "CREATE SCHEMA IF NOT EXISTS" in joined
    assert joined.index("CREATE SCHEMA") < joined.index("CREATE TABLE")


def test_statements_are_batched_into_one_execution_per_chunk():
    # AIDP discards per-statement DDL on session close, so DDL is batched.
    rec = Recorder()
    deploy(plan(5), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    batches = [c for c in rec.calls if c.count("CREATE TABLE") >= 1
               and not c.lstrip().upper().startswith("SHOW")]
    assert len(batches) == 3, "5 statements at chunk_size 2 -> 3 batches"


def test_every_statement_is_probed_individually_after_its_chunk():
    # A chunk can report success while individual statements inside it failed.
    rec = Recorder()
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=3)
    probes = [c for c in rec.calls if c.lstrip().upper().startswith("SHOW TABLES")]
    assert len(probes) == 3
    assert out["verified"] == 3
    assert out["failed"] == []


def test_missing_table_after_a_successful_chunk_is_reported_failed():
    rec = Recorder(existing={"T0"})
    out = deploy(plan(2), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    assert out["verified"] == 1
    assert [f["target_fqn"] for f in out["failed"]] == ["bronze.S.T1"]


def test_already_exists_is_not_a_failure():
    rec = Recorder(fail_on=("CREATE TABLE",))
    rec.fail_on = ()
    out = deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    assert out["failed"] == []


def test_chunk_error_does_not_abort_remaining_chunks():
    rec = Recorder(existing={"T2"}, fail_on=("`T0`",))
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=1)
    assert out["verified"] == 1
    assert len(out["failed"]) == 2
    assert out["chunk_errors"], "the failing chunk must be recorded, not swallowed"


def test_blocked_statements_are_never_executed():
    p = plan(1)
    p["blocked"] = [{"source_identifier": "D.S.BAD", "reason": "VARIANT"}]
    rec = Recorder()
    out = deploy(p, execute=True, target=TARGET, run_sql=rec)
    assert "BAD" not in " ".join(rec.calls)
    assert out["blocked_count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_deploy.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'target.deploy'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/target/deploy.py
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
"""
from __future__ import annotations

import datetime
from typing import Callable

from .ddl import build_create_schema

__all__ = ["deploy", "RefusedToExecute"]


class RefusedToExecute(RuntimeError):
    """Execution was requested without the arguments that make it safe."""


def _schema_of(target_fqn: str) -> tuple[str, str, str]:
    catalog, schema, table = target_fqn.split(".", 2)
    return catalog, schema, table


def deploy(ddl_plan: dict, *, target=None, execute: bool = False,
           run_sql: Callable[..., list[dict]] | None = None,
           chunk_size: int = 25) -> dict:
    statements = [s for s in ddl_plan.get("statements", []) if s.get("sql")]
    blocked_count = len(ddl_plan.get("blocked", []))

    out = {
        "ran_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "dry_run": not execute,
        "statements": statements,
        "statement_count": len(statements),
        "blocked_count": blocked_count,
        "executed": 0, "verified": 0, "failed": [], "chunk_errors": [],
    }
    if not execute:
        return out

    if target is None:
        raise RefusedToExecute(
            "execute=True requires a resolved target; ask the user for the AIDP "
            "datalake OCID, workspace, cluster and catalog and pass them explicitly")
    if run_sql is None:
        raise RefusedToExecute("execute=True requires a run_sql callable")

    schemas = {(_schema_of(s["target_fqn"])[0], _schema_of(s["target_fqn"])[1])
               for s in statements}
    for catalog, schema in sorted(schemas):
        try:
            run_sql(build_create_schema(catalog, schema))
        except Exception as exc:
            out["chunk_errors"].append(f"CREATE SCHEMA {catalog}.{schema}: {exc}")

    for start in range(0, len(statements), chunk_size):
        chunk = statements[start:start + chunk_size]
        batch = ";\n".join(s["sql"] for s in chunk)
        try:
            run_sql(batch)
            out["executed"] += len(chunk)
        except Exception as exc:
            out["chunk_errors"].append(
                f"chunk {start // chunk_size}: {str(exc)[:300]}")

        # Never trust the chunk's own result. Probe each object.
        for stmt in chunk:
            catalog, schema, table = _schema_of(stmt["target_fqn"])
            try:
                rows = run_sql(
                    f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'")
                if rows:
                    out["verified"] += 1
                else:
                    out["failed"].append({
                        "source_identifier": stmt.get("source_identifier"),
                        "target_fqn": stmt["target_fqn"],
                        "reason": "not present after its chunk reported completion"})
            except Exception as exc:
                out["failed"].append({
                    "source_identifier": stmt.get("source_identifier"),
                    "target_fqn": stmt["target_fqn"],
                    "reason": f"existence probe failed: {str(exc)[:200]}"})
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_deploy.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/target/deploy.py engine/tests/test_deploy.py
git commit -m "feat(snowflake-migrator): dry-run-default deploy with per-statement verification"
```

---

## Task 13: Report rendering

**Files:**
- Create: `engine/report/render.py`
- Test: `engine/tests/test_render.py`

**Interfaces:**
- Consumes: all four artifacts
- Produces: `render_inventory(inv) -> str` · `render_plan(plan) -> str` · `render_ddl_plan(ddl) -> str` · `render_deploy(result) -> str`

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_render.py
"""Artifact -> markdown. Pure."""
from report.render import (
    render_ddl_plan, render_deploy, render_inventory, render_plan,
)

INV = {
    "probed_at": "2026-09-09T00:00:00+00:00",
    "session": {"A": "DU58131", "R": "AWS_US_EAST_2", "ROLE": "ACCOUNTADMIN"},
    "databases_in_scope": ["MYDB"], "object_count": 2,
    "counts_by_type": {"TABLE": 1, "VIEW": 1},
    "identifier_case_collisions": {}, "extraction_notes": [],
    "inventory": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS", "object_type": "TABLE",
         "row_count_exact": 100, "source_metadata": {"bytes": 9728},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "supported", "columns": [1, 2, 3]},
        {"source_identifier": "MYDB.PUBLIC.ORDERS_VW", "object_type": "VIEW",
         "row_count_exact": 100, "source_metadata": {},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "requires_manual_design", "columns": [1]},
    ],
}


def test_inventory_lists_objects_with_exact_row_counts():
    md = render_inventory(INV)
    assert "MYDB.PUBLIC.ORDERS" in md
    assert "100" in md
    assert "exact" in md.lower(), "must label counts as exact, not estimated"


def test_inventory_shows_the_type_breakdown():
    md = render_inventory(INV)
    assert "TABLE" in md and "VIEW" in md


def test_collisions_rendered_as_a_halt_not_a_footnote():
    md = render_inventory({**INV, "identifier_case_collisions":
                           {"A.B.C": ["A.B.C", "A.B.c"]}})
    assert "HALT" in md.upper()
    assert "A.B.c" in md


def test_extraction_notes_surfaced_when_present():
    md = render_inventory({**INV, "extraction_notes": ["MYDB.X: denied"]})
    assert "denied" in md


def test_plan_shows_waves_layers_and_fallbacks():
    plan = {"namespace_strategy": "layer-catalog",
            "waves": [["MYDB.PUBLIC.ORDERS"], ["MYDB.PUBLIC.ORDERS_VW"]],
            "cycles": [],
            "medallion_assignment": {"MYDB.PUBLIC.ORDERS": "BRONZE",
                                     "MYDB.PUBLIC.ORDERS_VW": "BRONZE"},
            "medallion_assignment_basis": {"MYDB.PUBLIC.ORDERS": "fallback",
                                           "MYDB.PUBLIC.ORDERS_VW": "fallback"},
            "fallback_assignments": ["MYDB.PUBLIC.ORDERS"],
            "target_names": {"MYDB.PUBLIC.ORDERS": "bronze.PUBLIC.ORDERS",
                             "MYDB.PUBLIC.ORDERS_VW": "bronze.PUBLIC.ORDERS_VW"},
            "clone_targets": ["MYDB.PUBLIC.ORDERS"],
            "blocked": [], "unsupported": [
                {"source_identifier": "MYDB.PUBLIC.ORDERS_VW", "feature": "VIEW",
                 "resolution": "out of MVP-1 scope"}],
            "dependency_source": "parsed_ddl",
            "dependency_coverage_note": "view->object edges ONLY"}
    md = render_plan(plan)
    assert "Wave 1" in md and "Wave 2" in md
    assert "bronze.PUBLIC.ORDERS" in md
    assert "fallback" in md.lower(), "fallback assignments must be visible for review"
    assert "parsed_ddl" in md, "lineage provenance must be stated"
    assert "ONLY" in md


def test_plan_flags_cycles():
    md = render_plan({"namespace_strategy": "layer-catalog", "waves": [],
                      "cycles": [["A", "B"]], "medallion_assignment": {},
                      "medallion_assignment_basis": {}, "fallback_assignments": [],
                      "target_names": {}, "clone_targets": [], "blocked": [],
                      "unsupported": []})
    assert "cycle" in md.lower()


def test_ddl_plan_shows_sql_rules_and_omissions():
    md = render_ddl_plan({"statements": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS",
         "target_fqn": "bronze.PUBLIC.ORDERS",
         "sql": "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (`A` STRING)",
         "rules_applied": [{"rule_id": "R03_TYPE_MAP", "detail": "A: TEXT -> STRING"}],
         "warnings": [], "omitted_properties": ["cluster_by=X"]}],
        "blocked": [{"source_identifier": "MYDB.PUBLIC.J", "reason": "VARIANT"}]})
    assert "CREATE TABLE IF NOT EXISTS" in md
    assert "R03_TYPE_MAP" in md
    assert "cluster_by=X" in md
    assert "VARIANT" in md


def test_deploy_report_distinguishes_dry_run():
    md = render_deploy({"dry_run": True, "statement_count": 3, "executed": 0,
                        "verified": 0, "failed": [], "chunk_errors": [],
                        "blocked_count": 0})
    assert "DRY RUN" in md.upper()
    assert "nothing was created" in md.lower()


def test_deploy_report_lists_failures_prominently():
    md = render_deploy({"dry_run": False, "statement_count": 2, "executed": 2,
                        "verified": 1, "failed": [
                            {"target_fqn": "bronze.S.T1",
                             "reason": "not present after its chunk reported completion"}],
                        "chunk_errors": [], "blocked_count": 0})
    assert "bronze.S.T1" in md
    assert "1/2" in md or "1 of 2" in md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_render.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'report.render'`

- [ ] **Step 3: Write minimal implementation**

```python
# engine/report/render.py
"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

__all__ = ["render_inventory", "render_plan", "render_ddl_plan", "render_deploy"]


def _bytes(n) -> str:
    if not n:
        return "-"
    step = 1024.0
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < step:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= step
    return f"{n:.1f} EB"


def render_inventory(inv: dict) -> str:
    s = inv.get("session", {})
    out = ["# Snowflake estate inventory", "",
           f'Probed **{inv.get("probed_at")}** · account `{s.get("A")}` · '
           f'region `{s.get("R")}` · role `{s.get("ROLE")}`',
           f'Databases in scope: {", ".join(inv.get("databases_in_scope") or []) or "-"}',
           "",
           f'**{inv.get("object_count", 0)} objects** — '
           + " · ".join(f"{k} {v}" for k, v in (inv.get("counts_by_type") or {}).items()),
           ""]

    collisions = inv.get("identifier_case_collisions") or {}
    if collisions:
        out += ["## ⚠️ HALT — identifier-case collisions", "",
                "These differ only by case. Snowflake treats them as distinct objects; "
                "Spark folds to lower and would merge them. Resolve before migrating.",
                ""]
        out += [f"- `{k}` ← {', '.join('`'+x+'`' for x in v)}"
                for k, v in collisions.items()] + [""]

    out += ["## Objects", "",
            "Row counts are **exact** (in-session `count(*)`), not `SHOW` estimates. "
            "Sizes are Snowflake-reported compressed bytes.", "",
            "| Object | Type | Rows (exact) | Size | Cols | Case form | Compatibility |",
            "|---|---|---:|---:|---:|---|---|"]
    for r in inv.get("inventory", []):
        rows = r.get("row_count_exact")
        out.append(
            f'| `{r["source_identifier"]}` | {r["object_type"]} '
            f'| {rows if rows is not None else "ERROR"} '
            f'| {_bytes((r.get("source_metadata") or {}).get("bytes"))} '
            f'| {len(r.get("columns") or [])} | {r.get("identifier_case_form")} '
            f'| {r.get("compatibility_status")} |')

    notes = inv.get("extraction_notes") or []
    if notes:
        out += ["", "## Extraction notes", "",
                "Objects or scopes that could not be read. Absence below is not "
                "evidence the object does not exist.", ""]
        out += [f"- {n}" for n in notes]
    return "\n".join(out) + "\n"


def render_plan(plan: dict) -> str:
    out = ["# Migration plan", "",
           f'Namespace strategy: **{plan.get("namespace_strategy")}**"',
           ""]
    if plan.get("dependency_source"):
        out += [f'Lineage source: **{plan["dependency_source"]}** — '
                f'{plan.get("dependency_coverage_note") or ""}', ""]

    if plan.get("cycles"):
        out += ["## ⚠️ Dependency cycles", "",
                "These cannot be ordered and are excluded from the waves. They need a "
                "human decision, not an arbitrary broken edge.", ""]
        out += [f'- {", ".join(f"`{n}`" for n in c)}' for c in plan["cycles"]] + [""]

    out += ["## Waves", "",
            "Dependencies land before their dependents. Within a wave, smaller "
            "objects first.", ""]
    for i, wave in enumerate(plan.get("waves") or [], 1):
        out.append(f"### Wave {i} — {len(wave)} object(s)")
        out += [f'- `{n}` → `{plan.get("target_names", {}).get(n, "?")}` '
                f'[{plan.get("medallion_assignment", {}).get(n, "?")}]' for n in wave]
        out.append("")

    fallbacks = plan.get("fallback_assignments") or []
    if fallbacks:
        out += ["## Layer assigned by fallback — confirm before proceeding", "",
                "No naming rule matched these, so they defaulted to BRONZE. "
                "Override with an explicit mapping if that is wrong.", ""]
        out += [f"- `{n}`" for n in fallbacks] + [""]

    out += [f'## Clone targets — {len(plan.get("clone_targets") or [])} table(s)', "",
            "MVP-1 creates empty managed Delta tables. Views are inventoried and "
            "ordered but not translated.", ""]

    if plan.get("blocked"):
        out += ["## Blocked", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in plan["blocked"]]
        out.append("")
    if plan.get("unsupported"):
        out += ["## Unsupported features", ""]
        out += [f'- `{u["source_identifier"]}` — **{u["feature"]}**: {u["resolution"]}'
                for u in plan["unsupported"]]
    return "\n".join(out) + "\n"


def render_ddl_plan(ddl: dict) -> str:
    stmts = ddl.get("statements") or []
    out = ["# Target DDL plan", "",
           f"{len(stmts)} statement(s). Nothing has been executed.", ""]
    for st in stmts:
        out += [f'## `{st["source_identifier"]}` → `{st["target_fqn"]}`', "",
                "```sql", st["sql"], "```", ""]
        rules = st.get("rules_applied") or []
        if rules:
            out += ["Rules applied:", ""]
            out += [f'- `{r["rule_id"]}` — {r["detail"]}' for r in rules] + [""]
        if st.get("omitted_properties"):
            out += ["Properties dropped (no Delta equivalent): "
                    + ", ".join(f"`{p}`" for p in st["omitted_properties"]), ""]
        if st.get("warnings"):
            out += ["Warnings:", ""] + [f"- {w}" for w in st["warnings"]] + [""]
    if ddl.get("blocked"):
        out += ["## Blocked — no DDL generated", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in ddl["blocked"]]
    return "\n".join(out) + "\n"


def render_deploy(res: dict) -> str:
    if res.get("dry_run"):
        return ("# Deployment — DRY RUN\n\n"
                f'{res.get("statement_count", 0)} statement(s) would run; '
                "**nothing was created**.\n\nRe-run with `--execute` and the AIDP "
                "target coordinates to apply.\n")
    total = res.get("statement_count", 0)
    out = ["# Deployment result", "",
           f'Executed {res.get("executed", 0)}/{total} · '
           f'**verified {res.get("verified", 0)}/{total}**', "",
           "Verification probes each object individually: a batch can report success "
           "while statements inside it failed.", ""]
    if res.get("blocked_count"):
        out += [f'{res["blocked_count"]} object(s) were blocked and never attempted.',
                ""]
    if res.get("failed"):
        out += ["## Failed", ""]
        out += [f'- `{f["target_fqn"]}` — {f["reason"]}' for f in res["failed"]] + [""]
    if res.get("chunk_errors"):
        out += ["## Batch errors", ""] + [f"- {e}" for e in res["chunk_errors"]]
    return "\n".join(out) + "\n"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_render.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/report/render.py engine/tests/test_render.py
git commit -m "feat(snowflake-migrator): markdown reports labelling exact values and halts"
```

---

## Task 14: CLI

**Files:**
- Create: `engine/snowmig.py`
- Test: `engine/tests/test_cli.py`

**Interfaces:**
- Consumes: every module above
- Produces: `main(argv: list[str] | None = None) -> int` with subcommands `assess` · `deps` · `plan` · `ddl` · `deploy`. Exit codes: `0` success · `1` error · `3` halt (case or target collision).

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_cli.py
"""CLI wiring. The offline subcommands are tested with no connection."""
import json

import pytest

from snowmig import main

INV = {"probed_at": "t", "session": {}, "databases_in_scope": ["D"],
       "object_count": 1, "counts_by_type": {"TABLE": 1},
       "identifier_case_collisions": {}, "extraction_notes": [],
       "inventory": [{
           "source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
           "source_database": "D", "source_schema": "PUBLIC",
           "compatibility_status": "supported", "blocked_reasons": [],
           "row_count_exact": 100, "source_metadata": {"bytes": 10},
           "warnings": [],
           "columns": [{"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER",
                        "target_type": "DECIMAL(38,0)", "IS_NULLABLE": "NO",
                        "ORDINAL_POSITION": 1, "COMMENT": None}]}]}
DEPS = {"edges": [], "source_used": "parsed_ddl", "coverage_note": "views only"}


def write(tmp, name, payload):
    p = tmp / name
    p.write_text(json.dumps(payload))
    return p


def test_plan_subcommand_writes_both_artifacts(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    rc = main(["plan", "--out-dir", str(tmp_path)])
    assert rc == 0
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["clone_targets"] == ["D.PUBLIC.ORDERS"]
    assert "Wave 1" in (tmp_path / "MIGRATION_PLAN.md").read_text()


def test_plan_honours_the_strategy_flag(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path), "--namespace-strategy", "preserve-source"])
    plan = json.loads((tmp_path / "plan.json").read_text())
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "D.PUBLIC.ORDERS"


def test_plan_exits_3_on_target_collision(tmp_path):
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D2.PUBLIC.ORDERS"
    second["source_database"] = "D2"
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 3


def test_assess_exits_3_on_case_collision(tmp_path, monkeypatch):
    inv = json.loads(json.dumps(INV))
    inv["identifier_case_collisions"] = {"D.PUBLIC.ORDERS": ["D.PUBLIC.ORDERS",
                                                             "D.PUBLIC.orders"]}
    monkeypatch.setattr("snowmig._assess_inventory", lambda args: inv)
    assert main(["assess", "--out-dir", str(tmp_path), "--account", "a",
                 "--user", "u", "--auth", "keypair", "--key-path", "/k"]) == 3


def test_ddl_subcommand_generates_sql_offline(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    rc = main(["ddl", "--out-dir", str(tmp_path)])
    assert rc == 0
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text())
    assert len(ddl["statements"]) == 1
    assert "CREATE TABLE IF NOT EXISTS" in ddl["statements"][0]["sql"]
    assert "USING DELTA" in ddl["statements"][0]["sql"]


def test_ddl_only_emits_for_tables_not_views(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V", object_type="VIEW",
                compatibility_status="requires_manual_design")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text())
    assert [s["source_identifier"] for s in ddl["statements"]] == ["D.PUBLIC.ORDERS"]


def test_deploy_defaults_to_dry_run(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 0
    res = json.loads((tmp_path / "deploy_result.json").read_text())
    assert res["dry_run"] is True and res["executed"] == 0
    assert "DRY RUN" in (tmp_path / "DEPLOY.md").read_text().upper()


def test_deploy_execute_without_coordinates_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["deploy", "--out-dir", str(tmp_path), "--execute"])
    assert rc == 1
    assert "ask the user" in capsys.readouterr().err


def test_missing_input_artifact_is_a_clear_error(tmp_path, capsys):
    assert main(["plan", "--out-dir", str(tmp_path)]) == 1
    assert "inventory.json" in capsys.readouterr().err
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snowmig'`

- [ ] **Step 3: Write minimal implementation**

```python
#!/usr/bin/env python3
# engine/snowmig.py
"""snowmig -- Snowflake -> AIDP migrator CLI.

Five subcommands, one per pipeline stage. Each reads the previous stage's JSON
and writes its own plus a markdown report, so any stage can be re-run alone.

  assess  -> inventory.json      + INVENTORY.md        (needs Snowflake)
  deps    -> dependencies.json                          (needs Snowflake)
  plan    -> plan.json           + MIGRATION_PLAN.md    (offline)
  ddl     -> ddl_plan.json       + DDL_PLAN.md          (offline)
  deploy  -> deploy_result.json  + DEPLOY.md            (dry-run offline;
                                                         --execute needs AIDP)
Exit codes: 0 ok · 1 error · 3 HALT (identifier-case or target-name collision)
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import pathlib
import sys

from plan.build import TargetCollision, build_plan
from report.render import (
    render_ddl_plan, render_deploy, render_inventory, render_plan,
)
from snowflake_source.conn import AuthError, build_connect_kwargs, connect, make_run_sql
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.dependencies import extract_dependencies
from target.coords import MissingTarget, resolve_target
from target.ddl import build_create_table
from target.deploy import RefusedToExecute, deploy

HALT = 3


def _read(out_dir: pathlib.Path, name: str) -> dict:
    path = out_dir / name
    if not path.is_file():
        raise FileNotFoundError(
            f"{name} not found in {out_dir}. Run the earlier stage first.")
    return json.loads(path.read_text())


def _write(out_dir: pathlib.Path, name: str, payload) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        (out_dir / name).write_text(payload)
    else:
        (out_dir / name).write_text(json.dumps(payload, indent=2, default=str))
    print(f"  -> {out_dir / name}")


def _run_sql_from_args(args):
    kwargs = build_connect_kwargs(
        args.auth, account=args.account, user=args.user, role=args.role,
        warehouse=args.warehouse, key_path=args.key_path,
        key_passphrase=args.key_passphrase, pat_path=args.pat_path,
        password_path=args.password_path)
    return make_run_sql(connect(**kwargs))


def _assess_inventory(args) -> dict:
    """Seam for tests: patched to avoid a live connection."""
    return build_inventory(_run_sql_from_args(args), args.database or None)


def cmd_assess(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _assess_inventory(args)
    _write(out, "inventory.json", inv)
    _write(out, "INVENTORY.md", render_inventory(inv))
    if inv.get("identifier_case_collisions"):
        print("HALT: identifier-case collisions; see INVENTORY.md", file=sys.stderr)
        return HALT
    return 0


def cmd_deps(args) -> int:
    out = pathlib.Path(args.out_dir)
    deps = extract_dependencies(_run_sql_from_args(args), _read(out, "inventory.json"))
    _write(out, "dependencies.json", deps)
    print(f'  lineage source: {deps["source_used"]}')
    return 0


def cmd_plan(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    deps = _read(out, "dependencies.json")
    user_map = json.loads(pathlib.Path(args.layer_map).read_text()) \
        if args.layer_map else None
    try:
        built = build_plan(inv, deps, user_map=user_map,
                           strategy=args.namespace_strategy)
    except TargetCollision as exc:
        print(f"HALT: {exc}", file=sys.stderr)
        return HALT
    _write(out, "plan.json", built)
    _write(out, "MIGRATION_PLAN.md", render_plan(built))
    return 0


def cmd_ddl(args) -> int:
    out = pathlib.Path(args.out_dir)
    inv = _read(out, "inventory.json")
    built = _read(out, "plan.json")
    by_id = {r["source_identifier"]: r for r in inv["inventory"]}

    statements, blocked = [], list(built.get("blocked", []))
    for ident in built.get("clone_targets", []):
        res = build_create_table(by_id[ident], built["target_names"][ident])
        if res.blocked:
            blocked.append({"source_identifier": ident, "reason": res.blocked_reason})
            continue
        statements.append({
            "source_identifier": res.source_identifier,
            "target_fqn": res.target_fqn, "sql": res.sql,
            "rules_applied": [dataclasses.asdict(r) for r in res.rules_applied],
            "warnings": res.warnings, "omitted_properties": res.omitted_properties})

    payload = {"statements": statements, "blocked": blocked,
               "namespace_strategy": built.get("namespace_strategy")}
    _write(out, "ddl_plan.json", payload)
    _write(out, "DDL_PLAN.md", render_ddl_plan(payload))
    return 0


def cmd_deploy(args) -> int:
    out = pathlib.Path(args.out_dir)
    ddl_plan = _read(out, "ddl_plan.json")
    target, run_sql = None, None
    if args.execute:
        target = resolve_target(datalake_ocid=args.datalake_ocid,
                                workspace=args.workspace,
                                cluster_id=args.cluster_id, catalog=args.catalog)
        from target.cluster_session import get_run_sql  # provided by the fork module
        run_sql = get_run_sql(target)
    result = deploy(ddl_plan, target=target, execute=args.execute, run_sql=run_sql,
                    chunk_size=args.chunk_size)
    _write(out, "deploy_result.json", result)
    _write(out, "DEPLOY.md", render_deploy(result))
    return 1 if result.get("failed") or result.get("chunk_errors") else 0


def _add_snowflake_args(p) -> None:
    p.add_argument("--account")
    p.add_argument("--user")
    p.add_argument("--role")
    p.add_argument("--warehouse")
    p.add_argument("--auth", default="keypair",
                   choices=["keypair", "pat", "password", "externalbrowser"])
    p.add_argument("--key-path")
    p.add_argument("--key-passphrase")
    p.add_argument("--pat-path")
    p.add_argument("--password-path")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="snowmig", description=__doc__)
    ap.add_argument("--out-dir", default="snowmig_out")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("assess", help="read-only estate inventory")
    _add_snowflake_args(a)
    a.add_argument("--database", action="append",
                   help="repeatable; omit to scan all non-system databases")
    a.set_defaults(func=cmd_assess)

    d = sub.add_parser("deps", help="dependency edges")
    _add_snowflake_args(d)
    d.set_defaults(func=cmd_deps)

    p = sub.add_parser("plan", help="waves + medallion layout (offline)")
    p.add_argument("--namespace-strategy", default="layer-catalog",
                   choices=["layer-catalog", "preserve-source", "layer-flattened"])
    p.add_argument("--layer-map", help="JSON file of explicit source->layer overrides")
    p.set_defaults(func=cmd_plan)

    g = sub.add_parser("ddl", help="generate target DDL (offline)")
    g.set_defaults(func=cmd_ddl)

    dep = sub.add_parser("deploy", help="dry-run by default")
    dep.add_argument("--execute", action="store_true")
    dep.add_argument("--datalake-ocid")
    dep.add_argument("--workspace")
    dep.add_argument("--cluster-id")
    dep.add_argument("--catalog")
    dep.add_argument("--chunk-size", type=int, default=25)
    dep.set_defaults(func=cmd_deploy)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (AuthError, MissingTarget, RefusedToExecute, FileNotFoundError,
            ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
```

Also add the `get_run_sql` adapter the deploy path needs, appended to `engine/target/cluster_session.py`:

```python
# appended to engine/target/cluster_session.py
def get_run_sql(target):
    """Return run_sql(sql, params=None) -> list[dict] bound to an AIDP cluster.

    Wraps the existing kernel-session machinery from the Databricks migrator so
    target/deploy.py consumes the same injected-I/O shape as the Snowflake side.
    """
    session = get_session(target.datalake_ocid, target.workspace, target.cluster_id)

    def run_sql(sql, params=None):
        if params:
            raise ValueError("bound parameters are not supported on the AIDP kernel")
        result = session.execute_sql(sql)
        return result if isinstance(result, list) else []
    return run_sql
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_cli.py -v`
Expected: PASS. If `get_session`/`execute_sql` are named differently in the fork's `cluster_session.py`, read that file and adapt `get_run_sql` — the offline CLI tests never import it, so only the `deploy --execute` path depends on those names.

- [ ] **Step 5: Commit**

```bash
git add engine/snowmig.py engine/tests/test_cli.py engine/target/cluster_session.py
git commit -m "feat(snowflake-migrator): snowmig CLI with five pipeline stages"
```

---

## Task 15: Plugin surface — MVP item 1

**Files:**
- Rewrite: `.claude-plugin/plugin.json`
- Create: `skills/snowflake-migrator-overview/SKILL.md`, `skills/snowflake-migrator-bootstrap/SKILL.md`, `skills/snowflake-assess-estate/SKILL.md`, `skills/snowflake-migration-plan/SKILL.md`, `skills/snowflake-medallion-clone/SKILL.md`
- Create: `commands/snowflake-assess.md`, `commands/snowflake-plan.md`, `commands/snowflake-soft-clone.md`
- Create: `references/type-mapping.md`
- Rewrite: `README.md` (drop the SCAFFOLD banner)
- Test: `engine/tests/test_plugin_surface.py`

**Interfaces:**
- Consumes: `engine/snowmig.py` (Task 14)
- Produces: the installable plugin

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_plugin_surface.py
"""Plugin manifest and skill/command structure."""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS = ["snowflake-migrator-overview", "snowflake-migrator-bootstrap",
          "snowflake-assess-estate", "snowflake-migration-plan",
          "snowflake-medallion-clone"]
COMMANDS = ["snowflake-assess", "snowflake-plan", "snowflake-soft-clone"]


def frontmatter(path: pathlib.Path) -> dict:
    text = path.read_text()
    assert text.startswith("---\n"), f"{path} needs YAML frontmatter"
    block = text.split("---", 2)[1]
    out = {}
    for line in block.splitlines():
        if ":" in line and not line.startswith(" "):
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def test_manifest_is_valid_and_keeps_the_name():
    m = json.loads((ROOT / ".claude-plugin/plugin.json").read_text())
    assert m["name"] == "oracle-ai-data-platform-workbench-snowflake-migrator"
    assert "SCAFFOLD" not in m["description"]
    assert m["version"] and m["license"]


@pytest.mark.parametrize("name", SKILLS)
def test_skill_exists_with_matching_frontmatter(name):
    fm = frontmatter(ROOT / "skills" / name / "SKILL.md")
    assert fm["name"] == name
    assert len(fm["description"]) > 60, "description drives routing; make it specific"
    assert "SCAFFOLD" not in fm["description"]
    assert "Databricks" not in fm["description"]


@pytest.mark.parametrize("name", COMMANDS)
def test_command_exists(name):
    fm = frontmatter(ROOT / "commands" / f"{name}.md")
    assert fm["description"] and "SCAFFOLD" not in fm["description"]


def test_no_databricks_scaffold_survives():
    stale = [p for p in (ROOT / "skills").glob("*") if p.name.startswith("snowflake-")
             and p.name not in SKILLS]
    assert stale == [], f"stale skill dirs: {[p.name for p in stale]}"
    assert not (ROOT / "agents").exists() or not list((ROOT / "agents").glob("*.md"))


def test_skills_invoke_the_engine_by_plugin_root():
    for name in SKILLS:
        text = (ROOT / "skills" / name / "SKILL.md").read_text()
        if "snowmig" in text:
            assert "${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py" in text, name


def test_clone_skill_states_the_runtime_coordinate_rule():
    text = (ROOT / "skills/snowflake-medallion-clone/SKILL.md").read_text()
    low = text.lower()
    assert "ask the user" in low
    assert "--execute" in text
    assert "dry" in low


def test_type_mapping_reference_covers_the_blocked_types():
    text = (ROOT / "references/type-mapping.md").read_text()
    for t in ["NUMBER", "TIMESTAMP_NTZ", "VARIANT", "GEOGRAPHY"]:
        assert t in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_plugin_surface.py -v`
Expected: FAIL — skill directories do not exist (Task 1 deleted the Databricks ones).

- [ ] **Step 3: Write the plugin surface**

`.claude-plugin/plugin.json`:

```json
{
  "name": "oracle-ai-data-platform-workbench-snowflake-migrator",
  "version": "0.1.0",
  "description": "Investigate a Snowflake estate and migrate its structure onto Oracle AI Data Platform (AIDP). Lists every table and view with exact row counts, column types with precision and scale, and identifier case forms; builds a dependency graph and topological migration waves; proposes a medallion (bronze/silver/gold) layout; and creates empty managed Delta tables in an INTERNAL AIDP catalog. Schema only - moves no data. Read-only against Snowflake, dry-run by default against AIDP, and AIDP target coordinates are never stored: they are supplied per conversation and confirmed before any write.",
  "homepage": "https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-snowflake-migrator",
  "repository": "https://github.com/oracle-samples/oracle-aidp-samples",
  "license": "MIT",
  "keywords": ["oracle", "aidp", "snowflake", "migration", "medallion", "delta",
    "soft-clone", "schema-migration", "information-schema", "get-ddl",
    "object-dependencies", "type-mapping", "decimal-precision",
    "identifier-case-folding", "topological-waves", "dry-run", "lakehouse", "oci"]
}
```

`skills/snowflake-migrator-overview/SKILL.md`:

```markdown
---
name: snowflake-migrator-overview
description: Router and shared rules for migrating a Snowflake estate onto Oracle AI Data Platform (AIDP). Read this first whenever the user mentions moving, assessing, inventorying, or cloning Snowflake tables, views, schemas, or databases onto AIDP, or asks what a Snowflake migration would involve. Explains the four-stage pipeline and picks the right next skill; adds no API surface of its own.
---

# Snowflake → AIDP migrator — router

Four stages, each producing a reviewable artifact. Run them in order; each is
re-runnable on its own.

| Stage | Skill | Produces |
|---|---|---|
| 0 | `snowflake-migrator-bootstrap` | verified Snowflake auth |
| 1 | `snowflake-assess-estate` | `inventory.json` + `INVENTORY.md` |
| 2 | `snowflake-migration-plan` | `dependencies.json`, `plan.json` + `MIGRATION_PLAN.md` |
| 3 | `snowflake-medallion-clone` | `ddl_plan.json` + `DDL_PLAN.md`, then optional deploy |

## Routing

- "what's in this Snowflake account", "list the tables", "how big are they" → stage 1
- "what order would we migrate in", "what depends on what", "show me a plan" → stage 2
- "create the medallion structure", "clone the schema", "soft clone" → stage 3
- auth or connection errors from any stage → stage 0

## Rules that apply to every stage

1. **Read-only against Snowflake.** Only `SHOW`, `SELECT`, `DESCRIBE`, `GET_DDL`.
   Never DDL or DML against the source.
2. **MVP-1 moves no data and translates no views.** It creates *empty* Delta
   tables. If the user expects rows to arrive, say so plainly before running.
3. **AIDP target coordinates are never stored and never guessed.** There is no
   config file and no environment default. Ask the user for the DataLake OCID,
   workspace, cluster and catalog in the turn you need them.
4. **Dry-run is the default.** Nothing is created on AIDP without `--execute`
   plus all four coordinates in the same command, and an explicit confirmation
   in that turn. An approval from an earlier turn does not carry.
5. **Never present an approximation as a conversion.** An unmapped type or an
   unrecognised construct is reported as blocked, with the reason. Do not
   substitute a "close enough" type.
6. **A halt is a halt.** Exit code 3 means an identifier-case or target-name
   collision. Show the collisions and stop; do not pick a winner.

## Engine

All stages call one CLI:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py <stage> --out-dir <dir> [...]
```
```

`skills/snowflake-migrator-bootstrap/SKILL.md`:

```markdown
---
name: snowflake-migrator-bootstrap
description: First-run setup for the Snowflake to AIDP migrator. Verifies Python dependencies and Snowflake authentication by key-pair, programmatic access token, password, or SSO, then smoke-tests the connection and reports the account, region, role and warehouse. Use the first time the migrator runs on a machine, or when any other stage fails with an authentication or connection error.
---

# Bootstrap

## 1. Dependencies

```bash
python3 -m pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt
```

If the system Python is externally managed (PEP 668), create a venv and use its
interpreter for every later command. Do not pass `--break-system-packages`.

## 2. Ask which auth method the user has

**Never ask for a password or token in chat.** Have the user put the secret in a
file and give you the path.

| Method | What to ask for | Notes |
|---|---|---|
| Key-pair *(preferred)* | path to an unencrypted PKCS#8 key | Also what AIDP's native Snowflake connector uses, so it is not throwaway setup |
| PAT | path to a file holding the token | Scoped, expiring, revocable. Some accounts require a network policy first |
| Password | path to a file holding it | |
| SSO | nothing | Needs a SAML IdP on the account. A plain Snowflake account fails with `390190` |

Key-pair setup, if they need it:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out ~/.sf_key.p8
chmod 600 ~/.sf_key.p8
openssl rsa -in ~/.sf_key.p8 -pubout | grep -v '^-----' | tr -d '\n'
# then in Snowsight:  ALTER USER <user> SET RSA_PUBLIC_KEY='<that string>';
```

## 3. Smoke-test

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py assess \
  --account <org>-<account> --user <user> --auth keypair --key-path ~/.sf_key.p8 \
  --database <one small db> --out-dir ./snowmig_out
```

Report the account, region, role and warehouse back to the user. A suspended
warehouse auto-resumes on the first query — mention it if the first call is slow.

**Do not ask for AIDP coordinates here.** They belong to stage 3 only.
```

`skills/snowflake-assess-estate/SKILL.md`:

```markdown
---
name: snowflake-assess-estate
description: Read-only investigation of a Snowflake environment. Lists every table and view with exact row counts, compressed byte sizes, full column types including numeric precision and scale, and the identifier case form of each object, then halts if two objects differ only by case. Use when the user asks what is in a Snowflake account, wants an inventory or estate assessment, asks how big the tables are, or before planning any migration.
---

# Stage 1 — assess the estate

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py assess \
  --account <org>-<account> --user <user> --auth <method> [--key-path ...] \
  [--warehouse <wh>] [--database DB]... --out-dir ./snowmig_out
```

Omit `--database` to scan every non-system database. Repeat it to scope.
Ask the user which databases to scan if the account is large — `count(*)` per
object is exact but costs warehouse time.

## Reading the result

`INVENTORY.md` is for the user; `inventory.json` feeds stage 2. Present:

- object counts by type, and total rows and bytes
- the largest objects
- anything with `compatibility_status: blocked` and why
- views, noting they are inventoried but **not translated** in MVP-1

## Three things to say out loud

1. **Row counts are exact**, from in-session `count(*)` — not `SHOW TABLES`
   estimates. Sizes are Snowflake's compressed bytes, which are not the size the
   data will occupy as Delta.
2. **Exit code 3 means HALT** on an identifier-case collision. Show the colliding
   names and stop. Snowflake treats `ORDERS` and `"orders"` as different objects;
   Spark folds to lower and would merge them, losing data with no error.
3. **`extraction_notes` is not decoration.** If it is non-empty, some scope could
   not be read, and absence from the inventory is not evidence the object does not
   exist. Say which scopes failed.
```

`skills/snowflake-migration-plan/SKILL.md`:

```markdown
---
name: snowflake-migration-plan
description: Build a high-level Snowflake to AIDP migration plan and present it for approval. Derives a dependency graph from ACCOUNT_USAGE.OBJECT_DEPENDENCIES or, where that privilege is unavailable, by parsing view DDL; orders objects into topological waves so dependencies land first; proposes a medallion bronze/silver/gold assignment; and reports cycles, blocked objects and unsupported features. Use after an estate assessment, or when the user asks in what order to migrate, what depends on what, or wants to see the migration plan.
---

# Stage 2 — dependencies and plan

Two commands. The first needs Snowflake; the second is offline.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deps \
  --account <...> --user <...> --auth <...> [--key-path ...] --out-dir ./snowmig_out

python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py plan \
  --out-dir ./snowmig_out [--namespace-strategy layer-catalog] [--layer-map map.json]
```

## Lineage provenance matters — state it

`deps` prints its source. Report which one:

- `account_usage` — authoritative lineage across all object types
- `parsed_ddl` — the `ACCOUNT_USAGE` grant was unavailable, so edges come from
  parsing view DDL. **View→object edges only.** Tell the user the graph is
  partial and why; do not present it as complete lineage.

## Namespace strategy

Default `layer-catalog` → `bronze.<schema>.<table>`. It drops the source database,
so two databases sharing `schema.table` collide — the run halts with exit 3 if
that happens. Offer `preserve-source` (`<db>.<schema>.<table>`, never collides) or
`layer-flattened` (`bronze.<db>_<schema>.<table>`) when it does.

## Present for approval, do not just run

Walk the user through `MIGRATION_PLAN.md`:

1. the waves, and why the order is what it is
2. **`fallback_assignments`** — objects no naming rule matched, defaulted to
   BRONZE. These are the ones most likely wrong. Ask before accepting them; a
   `--layer-map` JSON of `{"SOURCE": "SILVER"}` overrides.
3. cycles, blocked objects, unsupported features
4. that `clone_targets` is tables only

Get explicit agreement on the layer assignment before stage 3.
```

`skills/snowflake-medallion-clone/SKILL.md`:

```markdown
---
name: snowflake-medallion-clone
description: Create a medallion architecture on Oracle AI Data Platform and start the soft clone - generating Spark/Delta CREATE TABLE DDL from an approved Snowflake migration plan and, only after explicit confirmation, creating the bronze/silver/gold schemas and empty managed Delta tables in an INTERNAL AIDP catalog. Schema only; moves no data and translates no views. Use when the user asks to create the medallion structure, soft clone, or deploy the target schema.
---

# Stage 3 — medallion structure and soft clone

Two phases, deliberately separate: generate, then deploy.

## Phase A — generate the DDL (offline, safe)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py ddl --out-dir ./snowmig_out
```

Show the user `DDL_PLAN.md`: the SQL, the rule applied to each transformation,
every dropped property, and everything blocked. Nothing has touched AIDP.

## Phase B — deploy (requires coordinates AND confirmation)

**You must ask the user for these in this turn. They are not stored anywhere, and
there is no default to fall back on:**

- DataLake OCID
- workspace
- cluster id (must be ACTIVE)
- target catalog — must be **INTERNAL**

Then confirm explicitly what will be created, and only then:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py deploy --out-dir ./snowmig_out \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>
```

Without `--execute` this is a dry run and creates nothing.

## Rules

1. **Never run `--execute` on the strength of an earlier approval.** Ask in the
   turn you run it. If the user approved a plan yesterday, ask again.
2. **INTERNAL catalogs only.** ADB/ADW/ALH appear in AIDP as EXTERNAL, read-only
   JDBC catalogs and cannot hold a managed Delta table. If the user names one,
   explain that rather than trying.
3. **Report `verified`, not `executed`.** A batch can report success while
   statements inside it failed, so every table is probed individually afterwards.
   The honest number is `verified/total`. Never say "created" on the strength of
   `executed`.
4. **Empty tables.** Say clearly that these have zero rows and that data movement
   is a later phase.
5. **No `CREATE OR REPLACE`, no `DROP`.** Existing tables are left alone; a 409
   means "already exists", not a failure.
```

`commands/snowflake-assess.md`:

```markdown
---
description: Investigate a Snowflake environment and list every table and view with exact row counts, sizes, and column types. Read-only.
---

# `/snowflake-assess`

Thin wrapper over [`snowflake-assess-estate`](../skills/snowflake-assess-estate/SKILL.md).

1. If auth has never been verified, run [`snowflake-migrator-bootstrap`](../skills/snowflake-migrator-bootstrap/SKILL.md) first.
2. Ask which databases to scope, warning that exact row counts cost warehouse time.
3. Run the assess stage; present `INVENTORY.md`.
4. On exit 3, show the identifier-case collisions and stop.
```

`commands/snowflake-plan.md`:

```markdown
---
description: Build and present a high-level Snowflake to AIDP migration plan - dependency waves plus a proposed medallion layout - for approval.
---

# `/snowflake-plan`

Thin wrapper over [`snowflake-migration-plan`](../skills/snowflake-migration-plan/SKILL.md).

1. Require `inventory.json`; run `/snowflake-assess` first if absent.
2. Run `deps`, then `plan`. State the lineage source and whether it is partial.
3. Walk through the waves and the medallion assignment, calling out every fallback assignment.
4. Get explicit agreement before any clone.
```

`commands/snowflake-soft-clone.md`:

```markdown
---
description: Create the medallion structure and empty Delta tables on AIDP from an approved plan. Generates DDL first; deploys only after you supply target coordinates and confirm.
---

# `/snowflake-soft-clone`

Thin wrapper over [`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Require an approved `plan.json`.
2. Generate DDL and show `DDL_PLAN.md`. Nothing has touched AIDP.
3. Ask for the DataLake OCID, workspace, cluster id and INTERNAL catalog **in this turn**.
4. Confirm what will be created, then deploy with `--execute`.
5. Report `verified/total`, never `executed`.
```

`references/type-mapping.md`:

```markdown
# Snowflake → Spark/Delta type mapping

Precision and scale always come from `INFORMATION_SCHEMA.COLUMNS`. They are never
inferred from sampled data: `NUMBER` is Snowflake's default numeric type, and
getting its scale wrong does not raise — it silently changes values.

| Snowflake | Spark / Delta | Note |
|---|---|---|
| `NUMBER(p,s)`, `DECIMAL`, `NUMERIC` | `DECIMAL(p,s)` | Highest-consequence mapping. `NUMBER(38,0)` stays `DECIMAL(38,0)` — 38 digits do not fit in a `BIGINT` |
| `NUMBER` with no precision | **blocked** | Refuses to guess |
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ` | **Not `TIMESTAMP`.** Spark's bare `TIMESTAMP` is session-timezone-dependent |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP` | `TIMESTAMP` | Timezone semantics differ; recorded as a warning |
| `TEXT`, `VARCHAR(n)`, `CHAR` | `STRING` | Declared length is not enforced by Delta; recorded |
| `BOOLEAN`, `DATE`, `BINARY` | `BOOLEAN`, `DATE`, `BINARY` | Direct |
| `FLOAT`, `DOUBLE`, `REAL` | `DOUBLE` | |
| `TIME` | `STRING` | No direct Spark equivalent |
| `VARIANT`, `OBJECT`, `ARRAY` | **blocked** | Semi-structured; needs an explicit struct/map/array design |
| `GEOGRAPHY`, `GEOMETRY` | **blocked** | No target type |
| anything else | **blocked** | Unmapped types are never approximated |

## Properties dropped

Recorded in `omitted_properties`, never emitted: `CLUSTER BY` ·
`DATA_RETENTION_TIME_IN_DAYS` · `CHANGE_TRACKING` ·
`MAX_DATA_EXTENSION_TIME_IN_DAYS` · tags · masking policies · row-access policies.

## Constraints

Snowflake `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` are unenforced metadata — only
`NOT NULL` is enforced. Delta does not enforce them either. They are captured in
the inventory and reported, not emitted as DDL.
```

Then rewrite `README.md`: drop the SCAFFOLD banner, describe the four stages, link
the spec and `references/type-mapping.md`, and keep the pointer to
`RAPPI-CONTEXT.md` only if that file is still present.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests/test_plugin_surface.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add .claude-plugin skills commands references README.md engine/tests/test_plugin_surface.py
git commit -m "feat(snowflake-migrator): plugin surface with five skills and three commands"
```

---

## Task 16: End-to-end run against the live test estate

**Files:**
- Create: `engine/tests/test_live_smoke.py`
- Create: `engine/tests/fixtures/README.md`
- Test: the file above, gated behind `SNOWMIG_LIVE=1`

**Interfaces:**
- Consumes: the whole pipeline
- Produces: recorded fixtures under `engine/tests/fixtures/`, and proof the offline suite matches live behaviour

- [ ] **Step 1: Write the failing test**

```python
# engine/tests/test_live_smoke.py
"""End-to-end against a real Snowflake estate. Skipped unless SNOWMIG_LIVE=1.

Read-only. Runs assess -> deps -> plan -> ddl and asserts the pipeline holds
together on real metadata. It does NOT deploy: that needs AIDP coordinates,
which are supplied per conversation and never stored.
"""
import json
import os
import pathlib

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SNOWMIG_LIVE") != "1",
    reason="set SNOWMIG_LIVE=1 plus SNOWFLAKE_* to run against a live estate")

from snowmig import main  # noqa: E402

DB = os.environ.get("SNOWMIG_LIVE_DB", "TEST_DB_20260908_1529")


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    return tmp_path_factory.mktemp("live")


def auth_args():
    return ["--account", os.environ["SNOWFLAKE_ACCOUNT"],
            "--user", os.environ["SNOWFLAKE_USER"],
            "--auth", "keypair",
            "--key-path", os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            "--warehouse", os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")]


def test_assess_finds_tables_and_views(out):
    rc = main(["assess", "--out-dir", str(out), "--database", DB] + auth_args())
    assert rc == 0, "exit 3 would mean an identifier-case collision"
    inv = json.loads((out / "inventory.json").read_text())
    assert inv["counts_by_type"].get("TABLE", 0) >= 1
    assert inv["counts_by_type"].get("VIEW", 0) >= 1
    assert inv["extraction_notes"] == []
    # Exact counts, not SHOW estimates.
    assert all(r["row_count_exact"] is not None for r in inv["inventory"])


def test_decimal_columns_map_with_precision(out):
    inv = json.loads((out / "inventory.json").read_text())
    decimals = [c for r in inv["inventory"] for c in r["columns"]
                if (c.get("DATA_TYPE") or "").upper() == "NUMBER"]
    assert decimals, "the estate should contain NUMBER columns"
    for c in decimals:
        assert c["target_type"].startswith("DECIMAL("), c
        assert c["NUMERIC_PRECISION"] is not None


def test_deps_and_plan(out):
    assert main(["deps", "--out-dir", str(out)] + auth_args()) == 0
    deps = json.loads((out / "dependencies.json").read_text())
    assert deps["source_used"] in ("account_usage", "parsed_ddl")
    assert main(["plan", "--out-dir", str(out)]) == 0
    plan = json.loads((out / "plan.json").read_text())
    assert plan["waves"], "at least one wave expected"
    assert plan["clone_targets"], "tables should be clone targets"
    assert all(u["feature"] == "VIEW" for u in plan["unsupported"])


def test_view_lands_after_its_base_tables(out):
    plan = json.loads((out / "plan.json").read_text())
    deps = json.loads((out / "dependencies.json").read_text())
    if not deps["edges"]:
        pytest.skip("no edges resolved; ordering assertion not meaningful")
    wave_of = {n: i for i, w in enumerate(plan["waves"]) for n in w}
    for edge in deps["edges"]:
        if edge["from"] in wave_of and edge["to"] in wave_of:
            assert wave_of[edge["to"]] < wave_of[edge["from"]], edge


def test_ddl_generates_delta_tables_and_no_replace(out):
    assert main(["ddl", "--out-dir", str(out)]) == 0
    ddl = json.loads((out / "ddl_plan.json").read_text())
    assert ddl["statements"]
    for st in ddl["statements"]:
        assert "USING DELTA" in st["sql"]
        assert "IF NOT EXISTS" in st["sql"]
        assert "OR REPLACE" not in st["sql"]


def test_deploy_dry_run_creates_nothing(out):
    assert main(["deploy", "--out-dir", str(out)]) == 0
    res = json.loads((out / "deploy_result.json").read_text())
    assert res["dry_run"] is True and res["executed"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd engine
SNOWMIG_LIVE=1 SNOWFLAKE_ACCOUNT=npxbexe-op03637 SNOWFLAKE_USER=nheydari \
SNOWFLAKE_PRIVATE_KEY_PATH="$HOME/.sf_key.p8" SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
/Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest \
  tests/test_live_smoke.py -v
```
Expected: FAIL initially if any stage has a defect the offline fixtures did not cover. Fix the module at fault, not the test.

- [ ] **Step 3: Record fixtures from the live run**

```python
# engine/tests/fixtures/README.md content is prose; the recording is a one-off:
# from the live inventory, save the raw SHOW / INFORMATION_SCHEMA payloads so the
# offline suite replays real shapes rather than hand-written ones.
```

Write `engine/tests/fixtures/README.md`:

```markdown
# Fixtures

Recorded `SHOW` / `INFORMATION_SCHEMA` / `GET_DDL` payloads from a real Snowflake
estate, replayed by the offline suite through `tests/fake_sql.py`.

Re-record after a Snowflake behaviour change:

    SNOWMIG_LIVE=1 ... pytest tests/test_live_smoke.py

then copy the relevant payloads from `inventory.json` into a new fixture file.

**Never commit a fixture containing customer data, credentials, or an account
identifier that is not the shared test account.**
```

- [ ] **Step 4: Run the full suite, offline and live**

Run: `cd engine && /Users/nheydari/Workspace/oracle/snowflake_migrator/.venv/bin/python -m pytest tests -v`
Expected: PASS — every offline suite green, live suite skipped without `SNOWMIG_LIVE=1`.

Then re-run with `SNOWMIG_LIVE=1` as in Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/tests/test_live_smoke.py engine/tests/fixtures
git commit -m "test(snowflake-migrator): live end-to-end smoke gated behind SNOWMIG_LIVE"
```

---

## Self-review

**Spec coverage.** Every spec section maps to a task: §2 plugin surface → Task 15 ·
§3.1 inventory → Task 5 · §3.2 dependencies → Task 6 · §3.3 plan → Tasks 7–9 ·
§3.4 ddl_plan → Tasks 11, 14 · §4 engine layout → Tasks 1, 14 · §5 namespace →
Task 8 · §6 types → Tasks 2, 11 · §7 safety → Tasks 10, 12, 15 · §8 error handling
→ Tasks 5, 7, 9, 12 · §9 testing → Task 16 · §10 fork reuse → Task 1 · §11 deferred
publish → **not implemented in MVP-1 by design**; `publish.py` is spec'd but only
needed once data moves, so it is deliberately absent from these tasks · §12
acceptance criteria → all covered except #9's "confirm each one individually",
which Task 12 implements and Task 16 exercises only in dry-run because no AIDP
environment exists yet · §14 relocation → Task 1.

**Known gap, stated rather than hidden:** acceptance criterion 9 (create tables in
an INTERNAL catalog and confirm each individually) cannot be *verified* until AIDP
coordinates exist. Tasks 12 and 14 implement it and unit-test it against a
recorder; the live proof is outstanding. Same for `deploy --execute` and
`target/cluster_session.py::get_run_sql`, whose function names must be checked
against the fork.

**Placeholder scan.** No TBD/TODO. Every code step carries runnable code. The one
approximation is `get_run_sql` in Task 14, which depends on the fork's existing
`cluster_session.py` API — flagged in that task's Step 4 with instructions to read
the file and adapt.

**Type consistency.** `run_sql(sql, params=None) -> list[dict]` is used identically
in Tasks 4, 5, 6, 12, 14. `RewriteResult` fields in Task 11 match the `ddl_plan.json`
keys the CLI writes in Task 14 and `render_ddl_plan` reads in Task 13. Edge shape
`{"from","to","kind","source"}` is consistent across Tasks 6, 7, 9. `Target` is
constructed only by `resolve_target` (Task 10) and consumed in Task 12.
