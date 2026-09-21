"""ONE config file for both ends of a migration. Pure parsing, no network.

The operating assumption, chosen deliberately: the person running a migration
is an engineer with full access to both environments, so making them juggle
several files and repeat coordinates on every command buys nothing. One file
holds the Snowflake connection AND the AIDP destination, and a secret may sit
inline rather than in a companion file.

That trades away one protection, so the ones that remain have to be explicit:

  * **The file is gitignored and must stay out of tickets, commits and chat.**
    A secret inline is a secret that leaks if the file travels.
  * **Secrets are never echoed.** `redact()` is the only way this plugin
    renders a config, and `preflight` uses it.
  * **A destination read from a file is announced, not assumed.** The CLI
    prints which AIDP coordinates it took from the config, and writing still
    needs `--execute`. `target/coords.py` itself still performs no I/O at
    all -- it cannot discover a destination, only be handed one -- so a
    stale config cannot silently redirect a write.

Two shapes are accepted. The documented one is nested:

    snowflake:
      account: ORG-ACCOUNT
      ...
    aidp:
      datalake_ocid: ocid1.aidataplatform...
      ...

A flat mapping (no `snowflake:` key) is read as the Snowflake block alone,
which is what older configs look like.
"""
from __future__ import annotations

import json
import os
import pathlib

__all__ = ["ConfigError", "CONFIG_NAMES", "SECRET_FIELDS", "TEMPLATE_NAME",
           "discover_config", "load_config", "redact", "resolve_secret",
           "snowflake_block", "aidp_block", "write_template"]

# The names looked for, in order, when no --config is given. One file, one
# expected name, so a second conversation finds what the first one made.
CONFIG_NAMES = ("snowmig-config.yaml", "snowmig-config.yml",
                "snowmig-config.json")

TEMPLATE_NAME = "snowmig-config.example.yaml"

# Fields whose VALUE is a credential, whether inline or as a `*_path`.
SECRET_FIELDS = ("password", "private_key", "key_content", "token",
                 "key_passphrase")

_REDACTED = "<redacted>"

# What `aidp:` may carry. An unknown key is reported rather than ignored, so a
# typo cannot silently leave a destination unset.
AIDP_FIELDS = ("datalake_ocid", "workspace", "cluster_id", "catalog",
               "external_catalog", "target_catalog", "oci_profile",
               "subnet_id")


class ConfigError(ValueError):
    """The config is missing, unreadable, or says something contradictory."""


def discover_config(explicit: str | pathlib.Path | None = None, *,
                    cwd: pathlib.Path | None = None,
                    plugin_root: pathlib.Path | None = None) -> pathlib.Path:
    """Where the migration config is, in a fixed order of preference.

    Search order, and why:

      1. `--config <path>`, when given. An explicit path always wins.
      2. `./snowmig-config.yaml` in the working directory. This is where a
         config belongs when the plugin is INSTALLED rather than cloned: the
         plugin directory may be read-only, and the config is the operator's
         file, not the plugin's.
      3. the same name beside the plugin itself, which is the convenient spot
         while working inside a checkout of this repo.

    Raises with the copy-paste fix when there is none, rather than running
    against coordinates nobody confirmed.
    """
    if explicit:
        p = pathlib.Path(explicit).expanduser()
        if not p.is_file():
            raise ConfigError(
                f"--config {p} does not exist. Create it from "
                f"{TEMPLATE_NAME}, or drop the flag to look for "
                f"{CONFIG_NAMES[0]} in the current directory.")
        return p

    roots = [cwd or pathlib.Path.cwd()]
    if plugin_root:
        roots.append(pathlib.Path(plugin_root))
    for root in roots:
        for name in CONFIG_NAMES:
            candidate = root / name
            if candidate.is_file():
                return candidate

    looked = ", ".join(str(r / CONFIG_NAMES[0]) for r in roots)
    raise ConfigError(
        f"no migration config found (looked for: {looked}). Create one with "
        f"`snowmig.py init-config`, or copy {TEMPLATE_NAME} to "
        f"./{CONFIG_NAMES[0]} and fill it in — it holds the Snowflake "
        f"connection and the AIDP destination, and nothing else needs "
        f"passing on the command line.")


def write_template(destination: pathlib.Path, *,
                   template: pathlib.Path,
                   overwrite: bool = False) -> pathlib.Path:
    """Put a fill-me-in config where the operator works, mode 0600.

    Two deliberate choices:

      * it refuses to overwrite -- the file it would clobber is the one
        carrying live credentials;
      * it is created 0600 before anything is written to it, because this
        file is about to hold a password in plain text and a default-umask
        644 would make it world-readable on a shared host.
    """
    destination = pathlib.Path(destination).expanduser()
    if destination.exists() and not overwrite:
        raise ConfigError(
            f"{destination} already exists and holds credentials; refusing to "
            f"overwrite it. Edit it, or pass --force if you really mean to "
            f"replace it.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Create it closed, THEN fill it: a chmod after the write leaves a window
    # where the secret-bearing file is readable by everyone.
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(pathlib.Path(template).read_text(encoding="utf-8"))
    os.chmod(destination, 0o600)
    return destination


def load_config(path: str | pathlib.Path) -> dict:
    """Parse the migration config. Never guesses a location."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"config not readable at {p}: {exc.strerror}") from exc

    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise ConfigError(
                f"{p} is YAML but PyYAML is not installed; `pip install "
                f"pyyaml` or write the config as JSON") from exc
        data = yaml.safe_load(text) or {}
    else:
        try:
            data = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{p}: not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"{p}: expected a mapping at the top level")
    return data


def snowflake_block(config: dict) -> dict:
    """The Snowflake half, from either shape."""
    if "snowflake" in config:
        block = config.get("snowflake") or {}
        if not isinstance(block, dict):
            raise ConfigError("`snowflake:` must be a mapping")
        return block
    # A flat config predates the `aidp:` half; everything is Snowflake's.
    return {k: v for k, v in config.items() if k != "aidp"}


def aidp_block(config: dict) -> dict:
    """The AIDP half, or {} when the config carries none."""
    block = config.get("aidp") or {}
    if not isinstance(block, dict):
        raise ConfigError("`aidp:` must be a mapping")
    unknown = sorted(k for k in block if k not in AIDP_FIELDS)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `aidp:`: {', '.join(unknown)}. Expected "
            f"any of: {', '.join(AIDP_FIELDS)}")
    return block


def resolve_secret(block: dict, inline: str, path_field: str) -> str | None:
    """A credential, from `inline` or from the file at `path_field`.

    Inline is the documented default now (one file, one place to look). A
    path still works, and is the better choice for a PEM key. Both set at
    once is a contradiction, not a precedence question: refuse rather than
    pick, because the wrong guess is an auth failure nobody can explain.
    """
    value, path = block.get(inline), block.get(path_field)
    if value and path:
        raise ConfigError(
            f"both `{inline}` and `{path_field}` are set; keep one. Inline "
            f"is simplest; a path keeps the secret out of this file.")
    if value:
        return str(value)
    if not path:
        return None
    p = pathlib.Path(str(path)).expanduser()
    try:
        return p.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConfigError(
            f"`{path_field}` points at {p}, which is not readable: "
            f"{exc.strerror}") from exc


def redact(value):
    """The config with every credential replaced. The ONLY way to render one.

    Recurses, so a nested block cannot smuggle a secret past it, and reports
    a `*_path` as the path itself -- the path is useful to a reader and is
    not the secret.
    """
    if isinstance(value, dict):
        out = {}
        for key, inner in value.items():
            if key in SECRET_FIELDS:
                out[key] = _REDACTED if inner else inner
            else:
                out[key] = redact(inner)
        return out
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value
