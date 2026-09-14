"""Build `connectionDetails` for an EXTERNAL/SNOWFLAKE AIDP catalog.

Read from a YAML or JSON config FILE, never taken as inline arguments or
environment variables -- the same rule `snowflake_source/conn.py` applies to
the source side. Secrets (private key, password, PAT) are themselves paths to
separate files inside that config, so a committed config file cannot leak one.

⚠️ UNVERIFIED SHAPE. The AIDP `aidp-table-management` skill gives the outer
`CreateCatalogDetails` envelope but explicitly declines to guess
`connectionDetails` for any source type -- it has to be confirmed against a
live DataLake with `aidp catalog test-connection`. The field names below
(`accountName`, `warehouseName`, ...) are this plugin's best-effort guess from
the AIDP Snowflake Spark connector's own option names, not a verified REST
contract. Test-connection before using this against production.
"""
from __future__ import annotations

import json
import pathlib

__all__ = ["ConnectionConfigError", "load_connection_config",
           "build_snowflake_connection_details"]

_AUTH_MODES = ("keypair", "password", "pat")


class ConnectionConfigError(ValueError):
    """The connection config file is missing, unreadable, or incomplete."""


def load_connection_config(path: str | pathlib.Path) -> dict:
    """Parse a YAML or JSON connection config file. Never guesses a location."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text()
    except OSError as exc:
        raise ConnectionConfigError(
            f"connection config not readable at {p}: {exc.strerror}") from exc

    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise ConnectionConfigError(
                "PyYAML is not installed; either `pip install pyyaml` or "
                "write the config as JSON instead") from exc
        data = yaml.safe_load(text) or {}
    else:
        try:
            data = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError as exc:
            raise ConnectionConfigError(f"{p}: not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ConnectionConfigError(f"{p}: expected a mapping at the top level")
    return data


def _read_secret_file(path: str, label: str) -> str:
    p = pathlib.Path(path).expanduser()
    try:
        return p.read_text().strip()
    except OSError as exc:
        raise ConnectionConfigError(
            f"{label} not readable at {path}: {exc.strerror}") from exc


def build_snowflake_connection_details(config: dict) -> dict:
    """`connectionDetails` for a SNOWFLAKE EXTERNAL catalog, from a loaded config.

    Required: account, warehouse, database, auth (one of "keypair", "password",
    "pat"), user. The credential itself is always a path to a separate file:

      keypair  -- key_path (+ optional key_passphrase_path)
      password -- password_path
      pat      -- pat_path
    """
    missing = [k for k in ("account", "warehouse", "database", "auth", "user")
               if not config.get(k)]
    if missing:
        raise ConnectionConfigError(
            "connection config is missing required field(s): "
            + ", ".join(sorted(missing)))

    auth = str(config["auth"]).strip().lower()
    if auth not in _AUTH_MODES:
        raise ConnectionConfigError(
            f"auth must be one of {_AUTH_MODES}, got {config['auth']!r}")

    details = {
        "accountName": str(config["account"]).strip(),
        "warehouseName": str(config["warehouse"]).strip(),
        "databaseName": str(config["database"]).strip(),
        "userName": str(config["user"]).strip(),
    }
    if config.get("role"):
        details["role"] = str(config["role"]).strip()
    if config.get("schema"):
        details["schemaName"] = str(config["schema"]).strip()

    if auth == "keypair":
        if not config.get("key_path"):
            raise ConnectionConfigError("auth: keypair requires key_path")
        details["authenticationType"] = "KEY_PAIR"
        details["privateKey"] = _read_secret_file(config["key_path"], "private key")
        if config.get("key_passphrase_path"):
            details["privateKeyPassphrase"] = _read_secret_file(
                config["key_passphrase_path"], "private key passphrase")
    elif auth == "password":
        if not config.get("password_path"):
            raise ConnectionConfigError("auth: password requires password_path")
        details["authenticationType"] = "PASSWORD"
        details["password"] = _read_secret_file(config["password_path"], "password")
    else:  # pat
        if not config.get("pat_path"):
            raise ConnectionConfigError("auth: pat requires pat_path")
        details["authenticationType"] = "OAUTH_PAT"
        details["token"] = _read_secret_file(config["pat_path"], "PAT")

    return details
