"""How the migration scripts READ Snowflake from inside AIDP. Two modes.

LIVE-VERIFIED 2026-09-16 on a real cluster (Spark 3.5, AIDP 4.x):

  connector (default)  spark.read.format("aidataplatform") with
                       type=SNOWFLAKE. Talked to the account, read a table
                       (87 rows x 9 cols) and ran a pushdown query
                       (`current_user()` = the service user). Needs NO extra
                       cluster library — the format is built in — and needs
                       NO successful catalog crawl.

  external-catalog     three-part names `catalog.schema.table` against a
                       registered EXTERNAL catalog. Cheaper (no per-read
                       Snowflake login) but it can only see what the CRAWLER
                       has already discovered, and on the validated
                       deployment the crawler failed with
                       "CONNECTOR_0067 ... Login has timed out" while the
                       connector above worked with the same credentials.

So: connector mode is the default because it is the one proven end to end,
and external-catalog mode is kept for a deployment whose crawl succeeds.

The option NAMES are the live-verified raw ones, which differ from the
Python helper's keyword names — `user.name` (not `user`), `database.name`,
`authentication.method` ∈ {Basic, KeyPair}, `private.key.content`. A wrong
name fails loud (`DATA_ACCESS_LAYER_0001 - Required spark option ... was not
provided`), which is how these were established.

Credentials come from a CONFIG FILE, never from arguments: the same rule the
control plane follows. Point --source-config at a JSON file shaped like
snowmig-config.example.yaml (JSON, on the workspace mount).
"""
from __future__ import annotations

import json
import pathlib

__all__ = ["SOURCE_MODES", "SourceConfigError", "SnowflakeSource",
           "load_source_config"]

SOURCE_MODES = ("connector", "external-catalog")

AIDP_FORMAT = "aidataplatform"


class SourceConfigError(ValueError):
    """The source config is missing, unreadable or incomplete."""


def q(identifier: str) -> str:
    """Backtick-quote one Spark identifier."""
    return "`" + str(identifier).replace("`", "``") + "`"


def _sql_ident(identifier: str) -> str:
    """Double-quote one SNOWFLAKE identifier (the pushdown runs there)."""
    return '"' + str(identifier).replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    """Escape a string literal for Snowflake SQL."""
    return str(value).replace("'", "''")


def load_source_config(path: str | pathlib.Path) -> dict:
    """Read the Snowflake connection config (JSON) from the workspace."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourceConfigError(
            f"source config not readable at {p}: {exc.strerror}") from exc
    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise SourceConfigError(
                f"{p} is YAML but PyYAML is not on the cluster; write the "
                f"config as JSON instead") from exc
        data = yaml.safe_load(text) or {}
    else:
        data = json.loads(text) if text.strip() else {}
    if not isinstance(data, dict):
        raise SourceConfigError(f"{p}: expected a mapping at the top level")
    # THE MIGRATION CONFIG IS ONE FILE FOR BOTH ENDS: the Snowflake connection
    # nested under `snowflake:`, the AIDP coordinates under `aidp:`. That is
    # the file `provision --source-config` uploads, and it uploads it
    # verbatim -- so the shape that actually reaches the mount is the nested
    # one, and reading the top level for `account` found nothing but the two
    # envelope keys. The failure surfaced on the cluster, as every required
    # field missing at once, which reads like a broken credential rather than
    # a config one level too deep.
    nested = data.get("snowflake")
    if isinstance(nested, dict):
        return dict(nested)
    return data


class SnowflakeSource:
    """Read-only access to the Snowflake source, in either mode.

    Nothing here can write: every method issues a read, and the connector is
    read-only in AIDP 4.0 by Oracle's own statement.
    """

    def __init__(self, spark, *, mode: str = "connector",
                 config: dict | None = None,
                 external_catalog: str | None = None,
                 session_schema: str | None = None):
        if mode not in SOURCE_MODES:
            raise SourceConfigError(
                f"unknown source mode {mode!r}; expected one of "
                f"{list(SOURCE_MODES)}")
        self.spark = spark
        self.mode = mode
        self.external_catalog = external_catalog
        self._options: dict[str, str] = {}
        # A REAL schema, used only to scope a pushdown session. The connector
        # validates this option against the schemas it can see and rejects
        # INFORMATION_SCHEMA itself with DATA_ACCESS_LAYER_0031 -- but a
        # pushdown query scoped to a real schema may reference
        # INFORMATION_SCHEMA freely (both established live).
        self.session_schema = session_schema or (config or {}).get("schema")

        if mode == "external-catalog":
            if not external_catalog:
                raise SourceConfigError(
                    "external-catalog mode needs --source-catalog")
            return

        cfg = config or {}
        missing = [k for k in ("account", "warehouse", "database", "user",
                               "auth") if not cfg.get(k)]
        if missing:
            raise SourceConfigError(
                "source config is missing required field(s): "
                + ", ".join(sorted(missing)))
        account = str(cfg["account"]).strip()
        # The connector wants the account/server URL host, which is what the
        # Snowflake console calls "Account/Server URL".
        host = str(cfg.get("host")
                   or f"{account}.snowflakecomputing.com").strip()
        opts = {
            "type": "SNOWFLAKE",
            "host": host,
            "port": str(cfg.get("port") or 443),
            "database.name": str(cfg["database"]).strip(),
            "user.name": str(cfg["user"]).strip(),
            "warehouse": str(cfg["warehouse"]).strip(),
        }
        if cfg.get("role"):
            opts["role"] = str(cfg["role"]).strip()

        # A secret may be inline in the one config file, or in a file the
        # config points at. On a cluster the inline form is usually the only
        # one available, since the workspace mount carries the config but not
        # the operator's home directory.
        def secret(inline, path_field):
            if cfg.get(inline):
                return str(cfg[inline])
            if cfg.get(path_field):
                return pathlib.Path(
                    str(cfg[path_field])).expanduser().read_text(encoding="utf-8").strip()
            return None

        auth = str(cfg["auth"]).strip().lower()
        if auth == "keypair":
            key = secret("private_key", "key_path")
            if not key:
                raise SourceConfigError(
                    "auth: keypair needs `private_key` (inline) or `key_path`")
            opts["authentication.method"] = "KeyPair"
            opts["private.key.content"] = key
            passphrase = secret("key_passphrase", "key_passphrase_path")
            if passphrase:
                opts["private.key.pass.phrase"] = passphrase
        elif auth == "password":
            password = secret("password", "password_path")
            if not password:
                raise SourceConfigError(
                    "auth: password needs `password` (inline) or "
                    "`password_path`")
            opts["authentication.method"] = "Basic"
            opts["password"] = password
        else:
            raise SourceConfigError(
                f"auth {auth!r} is not supported by the AIDP Snowflake "
                f"connector; use keypair (preferred) or password")
        self._options = opts

    # -- describing the estate ------------------------------------------

    def database(self) -> str | None:
        return self._options.get("database.name")

    def pushdown(self, sql: str, *, schema: str | None = None):
        """Run `sql` IN SNOWFLAKE and return a DataFrame.

        Connector mode only. The `schema` option must name a REAL schema:
        the connector rejects INFORMATION_SCHEMA there with
        DATA_ACCESS_LAYER_0031, while a query scoped to a real schema may
        reference INFORMATION_SCHEMA freely. Both established live.
        """
        if self.mode != "connector":
            raise SourceConfigError(
                "pushdown is connector-mode only; external-catalog mode has "
                "no Snowflake session to push into")
        scope = schema or self.session_schema
        if not scope:
            raise SourceConfigError(
                "connector pushdown needs a REAL schema to scope the "
                "session: add `schema:` to the source config or pass "
                "--session-schema. INFORMATION_SCHEMA is not accepted there.")
        return (self.spark.read.format(AIDP_FORMAT)
                .options(**self._options)
                .option("schema", scope)
                .option("pushdown.sql", sql)
                .load())

    def source_counts(self, schema: str, tables: list[str], *,
                      chunk: int = 50) -> dict[str, int]:
        """`{table: COUNT(*)}` for many tables in ONE round trip per chunk.

        Connector mode opens a Snowflake session per read, and the copy needs
        a source count twice per table (before, and again after, to catch a
        source that moved during the copy). Per-table counting therefore cost
        more session setup than the copy itself on a live run. A single
        UNION ALL answers a whole schema instead; it is chunked because a
        statement with thousands of branches is its own problem.

        External-catalog mode has no session to amortise, so it falls back to
        a count per table -- correct either way, and the caller does not care.
        """
        out: dict[str, int] = {}
        if self.mode != "connector":
            for table in tables:
                out[table] = self.read_table(schema, table).count()
            return out
        for start in range(0, len(tables), chunk):
            batch = tables[start:start + chunk]
            sql = " union all ".join(
                # The literal is the table's own name, so one query can carry
                # many counts and still say which is which. Both the literal
                # and the identifier are escaped: one apostrophe in a table
                # name would otherwise break the whole chunk.
                f"select '{_sql_literal(t)}' as SNOWMIG_TABLE, "
                f"count(*) as SNOWMIG_N from {_sql_ident(t)}"
                for t in batch)
            for row in self.pushdown(sql, schema=schema).collect():
                data = row.asDict()
                out[str(data["SNOWMIG_TABLE"])] = int(data["SNOWMIG_N"])
        return out

    def read_table(self, schema: str, table: str):
        """A DataFrame over one source table."""
        if self.mode == "external-catalog":
            return self.spark.table(
                f"{q(self.external_catalog)}.{q(schema)}.{q(table)}")
        return (self.spark.read.format(AIDP_FORMAT)
                .options(**self._options)
                .option("schema", schema)
                .option("table", table)
                .load())

    def register_temp_view(self, schema: str, table: str, view: str) -> str:
        """Expose a source table to SQL as a temp view, and return its name.

        INSERT ... SELECT needs the source addressable in SQL. In
        external-catalog mode the three-part name already is; in connector
        mode the DataFrame is registered as a session-local temp view, which
        is dropped by the caller.
        """
        if self.mode == "external-catalog":
            return f"{q(self.external_catalog)}.{q(schema)}.{q(table)}"
        self.read_table(schema, table).createOrReplaceTempView(view)
        return q(view)

    def drop_temp_view(self, view: str) -> None:
        if self.mode == "connector":
            self.spark.catalog.dropTempView(view)

    def describe(self) -> dict:
        """What this source is, for the report header. Never the credential."""
        out = {"mode": self.mode}
        if self.mode == "external-catalog":
            out["external_catalog"] = self.external_catalog
        else:
            out.update(session_schema=self.session_schema,
                       host=self._options.get("host"),
                       database=self._options.get("database.name"),
                       user=self._options.get("user.name"),
                       warehouse=self._options.get("warehouse"),
                       role=self._options.get("role"),
                       auth=self._options.get("authentication.method"))
        return out
