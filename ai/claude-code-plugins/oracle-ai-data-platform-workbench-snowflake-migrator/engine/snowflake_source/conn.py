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
import re
from typing import Any, Callable

__all__ = ["AuthError", "SourceWriteRefused", "READ_ONLY_VERBS",
           "build_connect_kwargs", "load_private_key_der", "connect",
           "make_run_sql"]

# The ONLY statements this plugin may send to Snowflake. Default deny: an
# unrecognised verb is refused rather than assumed safe.
#
# This is enforced at the transport, not by convention, so it holds even when the
# credential has write privileges and even if a future skill, agent or prompt
# asks for a write. Nothing is written to or dropped from the source, ever.
READ_ONLY_VERBS = ("SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")

_COMMENT_LINE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)

_MODES = {"keypair", "pat", "password", "externalbrowser"}


class AuthError(RuntimeError):
    """Auth arguments are missing, contradictory, or unreadable."""


class SourceWriteRefused(PermissionError):
    """A statement that is not a read was aimed at Snowflake. Refused."""


def _strip_comments(sql: str) -> str:
    return _COMMENT_LINE.sub(" ", _COMMENT_BLOCK.sub(" ", sql or ""))


def assert_read_only(sql: str) -> None:
    """Refuse anything that is not a read. Raises SourceWriteRefused.

    Checked per statement, so a write cannot be smuggled in after a read via
    statement stacking, and comments cannot disguise the leading verb.
    """
    cleaned = _strip_comments(sql)
    statements = [part.strip() for part in cleaned.split(";") if part.strip()]
    if not statements:
        raise SourceWriteRefused(
            f"empty statement refused; this plugin is read-only against "
            f"Snowflake (allowed: {', '.join(READ_ONLY_VERBS)})")
    for part in statements:
        verb = part.split(None, 1)[0].upper().lstrip("(")
        if verb not in READ_ONLY_VERBS:
            recognised = "not a recognised read verb"
            raise SourceWriteRefused(
                f"{verb}: {recognised}. This plugin is strictly read-only "
                f"against Snowflake and never writes to or drops from the "
                f"source, regardless of what the credential permits. "
                f"Allowed: {', '.join(READ_ONLY_VERBS)}.")


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

    Every statement passes assert_read_only first. A write never reaches
    Snowflake, whatever the credential allows.
    """
    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        assert_read_only(sql)
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return run_sql
