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

from .dialect import lexer

__all__ = ["AuthError", "SourceWriteRefused", "READ_ONLY_VERBS",
           "build_connect_kwargs", "load_private_key_der", "connect",
           "make_run_sql"]

# The ONLY statements this plugin may send to Snowflake. Default deny: an
# unrecognised verb is refused rather than assumed safe. WITH is on the list
# for the CTE-SELECT and only for it: assert_read_only looks past the CTE
# list, because `WITH x AS (...) INSERT ...` leads with WITH too.
#
# This is enforced at the transport, not by convention, so it holds even when the
# credential has write privileges and even if a future skill, agent or prompt
# asks for a write. Nothing is written to or dropped from the source, ever.
READ_ONLY_VERBS = ("SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")

_MODES = {"keypair", "pat", "password", "externalbrowser"}


class AuthError(RuntimeError):
    """Auth arguments are missing, contradictory, or unreadable."""


class SourceWriteRefused(PermissionError):
    """A statement that is not a read was aimed at Snowflake. Refused."""


def assert_read_only(sql: str) -> None:
    """Refuse anything that is not a read. Raises SourceWriteRefused.

    Statement boundaries and the leading keyword come from the scanner in
    `dialect.lexer`, not from a regex, because a regex cannot tell code from
    the inside of a string literal. Three consequences:

      * a `;` inside a literal does not fabricate a second statement, so
        `select 'a;drop table t'` is correctly one read and is allowed
      * a comment marker inside a literal cannot hide a real separator
      * a statement whose first content is a literal has no verb at all and is
        refused rather than having a word read out of the literal

    A leading WITH is a read only when the statement after the CTE list is a
    SELECT: `WITH x AS (...) INSERT ...` is refused, naming INSERT, and so is
    a CTE whose body the walker cannot identify (a parenthesised body).

    Fails closed: SQL the scanner cannot make sense of is refused.
    """
    try:
        statements = lexer.split_statements(sql or "")
    except lexer.UnterminatedLiteral as exc:
        raise SourceWriteRefused(
            f"statement could not be scanned ({exc}); refused. This plugin "
            f"only sends Snowflake statements it can positively identify as "
            f"reads.") from exc

    if not statements:
        raise SourceWriteRefused(
            f"empty statement refused; this plugin is read-only against "
            f"Snowflake (allowed: {', '.join(READ_ONLY_VERBS)})")

    for part in statements:
        verb = lexer.leading_verb(part)
        if verb is None:
            raise SourceWriteRefused(
                f"statement has no leading SQL keyword; refused. This plugin "
                f"only sends statements it can positively identify as reads. "
                f"Allowed: {', '.join(READ_ONLY_VERBS)}.")
        if verb not in READ_ONLY_VERBS:
            raise SourceWriteRefused(
                f"{verb}: not a recognised read verb. This plugin is strictly "
                f"read-only against Snowflake and never writes to or drops "
                f"from the source, regardless of what the credential permits. "
                f"Allowed: {', '.join(READ_ONLY_VERBS)}.")
        if verb == "WITH":
            body = lexer.cte_body_verb(part)
            if body != "SELECT":
                raise SourceWriteRefused(
                    f"WITH ... {body or '<no keyword>'}: a common table "
                    f"expression is only a read when the statement after the "
                    f"CTE list is a SELECT; refused. This plugin is strictly "
                    f"read-only against Snowflake and never writes to or drops "
                    f"from the source, regardless of what the credential "
                    f"permits. Allowed: {', '.join(READ_ONLY_VERBS)}.")


def _read_secret_file(path: str, label: str) -> str:
    p = pathlib.Path(path).expanduser()
    try:
        return p.read_text(encoding="utf-8").strip()
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
        # The connector builds its PAT authenticator from `token`. A PAT
        # handed over as `password` goes to the wire as TOKEN: null.
        kw["token"] = _read_secret_file(pat_path, "PAT file")
    elif auth == "password":
        if not password_path:
            raise AuthError("password auth requires password_path")
        kw["password"] = _read_secret_file(password_path, "password file")
    return kw


# A driver error code -> the config field that is actually wrong. The first
# thing anyone gets wrong is the account identifier, and the driver's own
# message for that is a 404 on a URL, which reads like a tool failure.
_CONNECT_HINTS = {
    "250001": "the account/host could not be reached at all",
    "290404": "the account/host could not be reached at all",
    "390100": "the user or the password/key was rejected",
    "390190": "this account has no SAML IdP, so `auth: externalbrowser` "
              "cannot work here",
    "390201": "the role or warehouse named does not exist, or the user has "
              "no grant on it",
    "002003": "the database, schema or warehouse named does not exist for "
              "this role",
}

_CONNECT_ADVICE = (
    "Check `account`/`host`, `user`, `role`, `warehouse` and `database` in the "
    "migration config, then re-run `preflight --test-source`. Do not paste the "
    "credential here — fix it in the file.")


def connect(**kwargs):
    """Open the source connection, or fail with something a human can act on.

    The driver reports a mistyped account as `404 Not Found: post
    <account>.snowflakecomputing.com/session/v1/login-request` and lets the
    traceback escape. That is the single most common first-run mistake, and a
    traceback sends people looking for a bug in this tool instead of at the
    one field they need to fix -- so every driver-level failure is re-raised
    as an AuthError naming the likely field.

    The secret is never in the message: only the error code, the driver's own
    text (which carries the host, not the credential) and what to check.
    """
    import snowflake.connector
    from snowflake.connector.errors import Error as SnowflakeError
    try:
        return snowflake.connector.connect(**kwargs)
    except SnowflakeError as exc:
        code = str(getattr(exc, "errno", "") or "")
        hint = _CONNECT_HINTS.get(code)
        account = kwargs.get("account") or "<unset>"
        head = (f"could not connect to Snowflake account {account}"
                + (f": {hint}" if hint else ""))
        raise AuthError(
            f"{head}.\n  driver said ({code or 'no code'}): {exc}\n  "
            f"{_CONNECT_ADVICE}") from exc


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
