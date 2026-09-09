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
