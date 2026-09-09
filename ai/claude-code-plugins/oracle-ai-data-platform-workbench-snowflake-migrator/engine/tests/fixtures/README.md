# Fixtures

Recorded `SHOW` / `INFORMATION_SCHEMA` / `GET_DDL` payloads from a real Snowflake
estate, replayed by the offline suite through `tests/fake_sql.py`.

Re-record after a Snowflake behaviour change:

    SNOWMIG_LIVE=1 ... pytest tests/test_live_smoke.py

then copy the relevant payloads from `inventory.json` into a new fixture file.

**Never commit a fixture containing customer data, credentials, or an account
identifier that is not the shared test account.**
