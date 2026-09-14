"""The EXTERNAL catalog's connectionDetails, from a YAML/JSON config file.

The rule the source side already follows: a secret is a PATH in the config,
read at call time, never an inline value. A config file that ends up in a
ticket or a commit then carries no credential.
"""
import json

import pytest

from target.snowflake_catalog_connection import (
    ConnectionConfigError, build_snowflake_connection_details,
    load_connection_config,
)


def _config(tmp_path, **overrides):
    key = tmp_path / "rsa.p8"
    key.write_text("-----BEGIN PRIVATE KEY-----\nabc\n")
    base = {"account": "ORG-ACC", "warehouse": "WH", "database": "SALES_DB",
            "user": "SVC", "auth": "keypair", "key_path": str(key)}
    base.update(overrides)
    return base


def test_json_config_loads_without_pyyaml(tmp_path):
    path = tmp_path / "conn.json"
    path.write_text(json.dumps({"account": "A"}))
    assert load_connection_config(path) == {"account": "A"}


def test_yaml_config_loads(tmp_path):
    pytest.importorskip("yaml")
    path = tmp_path / "conn.yaml"
    path.write_text("account: A\nwarehouse: WH\n")
    assert load_connection_config(path)["warehouse"] == "WH"


def test_a_missing_config_file_names_the_path(tmp_path):
    with pytest.raises(ConnectionConfigError, match="not readable"):
        load_connection_config(tmp_path / "absent.json")


def test_keypair_credential_is_read_from_the_file_not_the_config(tmp_path):
    config = _config(tmp_path)
    details = build_snowflake_connection_details(config)
    assert details["authenticationType"] == "KEY_PAIR"
    assert details["privateKey"].startswith("-----BEGIN PRIVATE KEY-----")
    # The path itself is not what gets sent.
    assert config["key_path"] not in json.dumps(details)


def test_account_warehouse_database_and_user_are_carried_over(tmp_path):
    details = build_snowflake_connection_details(_config(tmp_path))
    assert details["accountName"] == "ORG-ACC"
    assert details["warehouseName"] == "WH"
    assert details["databaseName"] == "SALES_DB"
    assert details["userName"] == "SVC"


def test_optional_role_and_schema_are_omitted_when_absent(tmp_path):
    details = build_snowflake_connection_details(_config(tmp_path))
    assert "role" not in details
    assert "schemaName" not in details


def test_password_auth_reads_the_password_file(tmp_path):
    secret = tmp_path / "pw"
    secret.write_text("hunter2\n")
    details = build_snowflake_connection_details(
        _config(tmp_path, auth="password", password_path=str(secret)))
    assert details["authenticationType"] == "PASSWORD"
    assert details["password"] == "hunter2"


def test_pat_auth_reads_the_token_file(tmp_path):
    secret = tmp_path / "pat"
    secret.write_text("tok\n")
    details = build_snowflake_connection_details(
        _config(tmp_path, auth="pat", pat_path=str(secret)))
    assert details["authenticationType"] == "OAUTH_PAT"
    assert details["token"] == "tok"


@pytest.mark.parametrize("field",
                         ["account", "warehouse", "database", "user", "auth"])
def test_a_missing_required_field_names_itself(tmp_path, field):
    config = _config(tmp_path)
    del config[field]
    with pytest.raises(ConnectionConfigError, match=field):
        build_snowflake_connection_details(config)


def test_an_unknown_auth_mode_is_refused(tmp_path):
    with pytest.raises(ConnectionConfigError, match="auth must be"):
        build_snowflake_connection_details(_config(tmp_path, auth="oauth2"))


def test_keypair_without_a_key_path_is_refused(tmp_path):
    config = _config(tmp_path)
    del config["key_path"]
    with pytest.raises(ConnectionConfigError, match="key_path"):
        build_snowflake_connection_details(config)


def test_an_unreadable_credential_file_names_the_path(tmp_path):
    with pytest.raises(ConnectionConfigError, match="private key not readable"):
        build_snowflake_connection_details(
            _config(tmp_path, key_path=str(tmp_path / "gone.p8")))
