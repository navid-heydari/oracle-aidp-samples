"""The in-AIDP data-migration scripts, exercised offline.

They run on a cluster, so they were the one part of this plugin with no test
coverage — and the first thing a review found there was a `fail()` that
recursed into itself, which would have turned every refusal into a
RecursionError instead of one message and exit 1.

Spark is injected as a fake here (the scripts take `spark` from the session
they are run in, and everything else through `SnowflakeSource`), so the
decisions are testable without a cluster: which statements are issued, what
gets refused, and what the reports record.
"""
import importlib.util
import json
import pathlib
import sys

import pytest

# The canonical stage sources. They live under engine/ because the shipped
# artifact is now a generated notebook (see target/stage_notebooks.py) and
# `data-migration-scripts/` holds only `.ipynb`.
SCRIPTS = (pathlib.Path(__file__).resolve().parents[1] / "dataplane")


def _load(name: str):
    """Import one script by path. They are not a package on purpose: each is
    uploaded to the workspace as a single file."""
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        f"snowmig_script_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def discover():
    return _load("00_discover_snowflake")


@pytest.fixture(scope="module")
def structure():
    return _load("01_create_structure")


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


@pytest.fixture(scope="module")
def reconcile():
    return _load("03_reconcile")


# --- fail(): the helper every refusal path goes through -------------------

@pytest.mark.parametrize("name", ["00_discover_snowflake",
                                  "01_create_structure",
                                  "02_copy_schema", "03_reconcile"])
def test_fail_returns_one_and_prints_on_both_streams(name, capsys):
    module = _load(name)
    assert module.fail("a refusal") == 1
    captured = capsys.readouterr()
    # stdout is what a notebook task captures; stderr is what a shell run
    # shows. A refusal that reaches only one of them is invisible in the
    # other, which is how a live job failed with no explanation anywhere.
    assert "a refusal" in captured.out
    assert "a refusal" in captured.err


# --- discovery ------------------------------------------------------------

def test_decimal_from_snowflake_survives_the_json_write(discover):
    import decimal
    # `json.dumps` refuses Decimal, and it did so AFTER a successful
    # 1065-relation read, losing the whole discovery.
    assert discover._plain(decimal.Decimal("38")) == 38
    assert isinstance(discover._plain(decimal.Decimal("38")), int)
    assert discover._plain(decimal.Decimal("1.5")) == "1.5"
    json.dumps({"n": discover._plain(decimal.Decimal("1250000"))})


def test_source_types_are_recorded_with_their_precision(discover):
    import decimal
    D = decimal.Decimal
    assert discover._snowflake_type(
        {"DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": D(38),
         "NUMERIC_SCALE": D(0)}) == "NUMBER(38,0)"
    assert discover._snowflake_type(
        {"DATA_TYPE": "TEXT", "CHARACTER_MAXIMUM_LENGTH": D(200)}) == "TEXT(200)"
    # Unqualified types pass through rather than being invented.
    assert discover._snowflake_type({"DATA_TYPE": "BOOLEAN"}) == "BOOLEAN"


class _FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]

    def count(self):
        return len(self._rows)


class _FakeSource:
    """A SnowflakeSource stand-in that records what was asked of it."""

    def __init__(self, *, tables=(), columns=(), mode="connector"):
        self.mode = mode
        self.external_catalog = "ext"
        self.spark = _FakeSpark()
        self.queries: list[str] = []
        self._tables, self._columns = list(tables), list(columns)
        self.session_schema = "PUBLIC"

    def pushdown(self, sql, schema=None):
        self.queries.append(sql)
        return _FakeDF(self._columns if "COLUMNS" in sql else self._tables)

    def describe(self):
        return {"mode": self.mode, "database": "DB", "host": "h",
                "user": "u", "warehouse": "w", "role": "r",
                "session_schema": self.session_schema, "auth": "KeyPair"}

    def database(self):
        return "DB"


class _FakeSpark:
    def __init__(self):
        self.statements: list[str] = []
        self.counts: dict[str, int] = {}

    def sql(self, statement):
        self.statements.append(" ".join(statement.split()))
        low = statement.lower()
        if "count(*)" in low:
            for fqn, n in self.counts.items():
                if fqn in statement:
                    return _FakeDF([{"n": n}])
            return _FakeDF([{"n": 0}])
        if low.startswith("describe"):
            return _FakeDF([{"col_name": "A", "data_type": "string"}])
        return _FakeDF([])


def test_discovery_reads_the_whole_estate_in_two_queries(discover):
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "SALES", "TABLE_NAME": "ORDERS",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 10, "BYTES": 99},
                {"TABLE_SCHEMA": "SALES", "TABLE_NAME": "V_ORDERS",
                 "TABLE_TYPE": "VIEW", "ROW_COUNT": None, "BYTES": None},
                {"TABLE_SCHEMA": "INFORMATION_SCHEMA", "TABLE_NAME": "TABLES",
                 "TABLE_TYPE": "VIEW", "ROW_COUNT": None, "BYTES": None}],
        columns=[{"TABLE_SCHEMA": "SALES", "TABLE_NAME": "ORDERS",
                  "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
                  "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
                  "CHARACTER_MAXIMUM_LENGTH": None},
                 {"TABLE_SCHEMA": "SALES", "TABLE_NAME": "V_ORDERS",
                  "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES",
                  "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
                  "CHARACTER_MAXIMUM_LENGTH": None}])
    schemas = discover.discover_via_connector(
        source, wanted=None, exclude={"information_schema"})
    assert len(source.queries) == 2, "the whole estate, in two queries"
    assert [s["name"] for s in schemas] == ["SALES"], \
        "INFORMATION_SCHEMA is never a migration target"
    sales = schemas[0]
    assert [t["name"] for t in sales["tables"]] == ["ORDERS"]
    assert [v["name"] for v in sales["views"]] == ["V_ORDERS"]
    assert sales["tables"][0]["columns"][0]["type"] == "NUMBER(38,0)"
    assert sales["errors"] == []


def test_a_relation_with_no_columns_is_incomplete_not_empty(discover):
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 1}],
        columns=[])
    schemas = discover.discover_via_connector(source, wanted=None, exclude=set())
    assert schemas[0]["errors"], "a column-less relation must be flagged"
    assert "INCOMPLETE" in schemas[0]["errors"][0]["error"]


# --- structure ------------------------------------------------------------

def test_the_approved_plan_is_the_authority(structure):
    plan = {"statements": [
        {"source_identifier": "DB.SALES.ORDERS",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]},
        {"source_identifier": "DB.SALES.BLOCKED"},          # no columns
        {"source_identifier": "SHORT.NAME"}]}               # malformed
    columns = structure.columns_from_ddl_plan(plan)
    assert list(columns) == [("SALES", "ORDERS")]
    assert columns[("SALES", "ORDERS")][0]["type"] == "DECIMAL(38,0)"


def test_snowflake_types_in_a_manifest_are_refused_not_translated(structure):
    # A connector-mode manifest records SOURCE types on purpose; feeding them
    # to Delta would be a silent mistranslation.
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "NUMBER(38,0)"}]) is True
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "VARIANT"}]) is True
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "decimal(38,0)"}]) is False


def test_create_table_from_columns_is_if_not_exists_and_delta(structure):
    spark = _FakeSpark()
    structure.create_table_from_columns(
        spark, [{"name": "ID", "type": "DECIMAL(38,0)"}], "lake", "sales", "t")
    statement = spark.statements[0]
    assert "CREATE TABLE IF NOT EXISTS" in statement
    assert "USING DELTA" in statement
    assert "`lake`.`sales`.`t`" in statement
    assert "DROP" not in statement.upper()


def test_an_empty_column_list_raises_rather_than_creating_nothing(structure):
    with pytest.raises(ValueError, match="no column list"):
        structure.create_table_from_columns(_FakeSpark(), [], "c", "s", "t")


def test_a_report_from_a_different_target_is_not_reused(structure, tmp_path):
    # Resumability is keyed by SOURCE schema, so a prior record against
    # another destination must not let this run skip every create -- observed
    # live, against an empty target schema.
    path = tmp_path / "structure_report_sales.json"
    path.write_text(json.dumps({"schema": "SALES", "target": "lake.old",
                                "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    fresh = structure._load_report(path, "SALES", "lake.new")
    assert fresh["objects"] == {}
    assert fresh["target"] == "lake.new"
    # The old record is kept, not destroyed.
    assert list(tmp_path.glob("structure_report_sales.lake_old.json"))


def test_a_report_for_the_same_target_is_resumed(structure, tmp_path):
    path = tmp_path / "structure_report_sales.json"
    path.write_text(json.dumps({"schema": "SALES", "target": "lake.new",
                                "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    prior = structure._load_report(path, "SALES", "lake.new")
    assert prior["objects"]["T"]["status"] == "created"


# --- copy -----------------------------------------------------------------

def test_the_copy_never_drops_and_overwrite_rewrites_rows(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`src`": 3, "`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`",
                            mode="overwrite", verify="counts",
                            retries=0, retry_wait=0, started="now")
    blob = " ".join(spark.statements).upper()
    assert "INSERT OVERWRITE" in blob
    assert "DROP" not in blob and "TRUNCATE" not in blob
    assert out["status"] in ("verified", "count_mismatch")


def test_skip_existing_leaves_a_nonempty_target_alone(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`src`": 3, "`lake`.`s`.`t`": 3}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`",
                            mode="skip-existing", verify="counts",
                            retries=0, retry_wait=0, started="now")
    assert out["status"] == "skipped_nonempty"
    assert not any("INSERT" in s.upper() for s in spark.statements)


def test_a_count_mismatch_is_not_verified(copy_schema):
    class Mismatch(_FakeSpark):
        def sql(self, statement):
            out = super().sql(statement)
            if "count(*)" in statement.lower() and "src" in statement:
                return _FakeDF([{"n": 9}])
            return out

    spark = Mismatch()
    spark.counts = {"`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`", mode="append",
                            verify="counts", retries=0, retry_wait=0,
                            started="now")
    assert out["status"] == "count_mismatch"
    assert "NOT verified" in out["reason"]


def test_a_batched_source_count_is_used_instead_of_a_fresh_one(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`", mode="append",
                            verify="counts", retries=0, retry_wait=0,
                            started="now", source_count=0)
    # The pre-copy source count came from the caller's batch, so only the
    # post-copy verification counts the source again.
    assert sum(1 for s in spark.statements
               if "count(*)" in s.lower() and "`src`" in s) == 1
    assert out["status"] == "verified"


# --- reconcile ------------------------------------------------------------

def test_an_unreadable_target_schema_is_not_reported_as_empty(reconcile):
    class NoList(_FakeSpark):
        def sql(self, statement):
            if statement.lower().startswith("show tables"):
                raise RuntimeError("denied")
            return super().sql(statement)

    manifest = {"schemas": [{"name": "SALES",
                             "tables": [{"name": "ORDERS", "columns": []}],
                             "views": [], "errors": []}]}
    rec = reconcile.reconcile(NoList(), manifest=manifest,
                              target_catalog="lake",
                              reports=pathlib.Path("/nonexistent"),
                              counts=False)
    schema = rec["schemas"][0]
    assert schema["target_readable"] is False
    assert schema["tables"][0]["verdict"] == "TARGET_UNREADABLE"
    assert "UNREADABLE" in reconcile.render(rec)


def test_a_table_a_report_claims_but_the_catalog_lacks_is_flagged(reconcile,
                                                                  tmp_path):
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.sales",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    manifest = {"schemas": [{"name": "SALES",
                             "tables": [{"name": "ORDERS", "columns": []}],
                             "views": [], "errors": []}]}
    rec = reconcile.reconcile(_FakeSpark(), manifest=manifest,
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    row = rec["schemas"][0]["tables"][0]
    assert row["verdict"] == "MISSING_DESPITE_REPORT"
    assert rec["totals"]["MISSING_DESPITE_REPORT"] == 1


def test_a_table_with_no_target_is_a_finding_not_a_crash(copy_schema):
    """Live, the copy died on the sixth table of a schema: the approved plan
    covered five, the manifest listed a thousand, and the missing target took
    the whole run with it. A missing target is now recorded and skipped."""
    class NoTarget(_FakeSpark):
        def sql(self, statement):
            if statement.lower().startswith("describe"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return super().sql(statement)

    source = _FakeSource()
    source.spark = NoTarget()
    out = copy_schema.copy_table(source, "SALES", "ORDERS",
                                 "`lake`.`s`.`orders`", mode="append",
                                 verify="counts", retries=0, retry_wait=0)
    assert out["status"] == "target_missing"
    assert "not in the approved plan" in out["reason"]
    assert not any("INSERT" in s.upper() for s in source.spark.statements)


def test_not_migrated_is_pending_not_a_problem(reconcile):
    """A migration runs schema by schema, so most of the estate is "not
    attempted yet" for most of the project. Exiting non-zero on that would
    make every partial run look broken — and that is how a real signal gets
    ignored."""
    assert "NOT_MIGRATED" not in reconcile.PROBLEM_VERDICTS
    assert "STRUCTURE_ONLY" not in reconcile.PROBLEM_VERDICTS
    for verdict in ("MISSING_DESPITE_REPORT", "STRUCTURE_ONLY_COPY_FAILED",
                    "TARGET_UNREADABLE"):
        assert verdict in reconcile.PROBLEM_VERDICTS


def test_the_report_says_plainly_when_nothing_is_wrong(reconcile):
    md = reconcile.render({"target_catalog": "lake", "generated_at": "now",
                           "totals": {"MIGRATED_VERIFIED": 5,
                                      "NOT_MIGRATED": 995},
                           "schemas": []})
    assert "No table is in a problem state" in md
    assert "not a failure" in md


def test_the_report_leads_with_the_count_that_needs_attention(reconcile):
    md = reconcile.render({"target_catalog": "lake", "generated_at": "now",
                           "totals": {"MISSING_DESPITE_REPORT": 2,
                                      "MIGRATED_VERIFIED": 1},
                           "schemas": []})
    assert "2 table(s) need attention" in md


# --- the source config as it actually arrives on the mount -----------------

@pytest.fixture(scope="module")
def source_helpers():
    return _load("snowmig_source")


def test_the_one_migration_config_is_read_as_uploaded(source_helpers, tmp_path):
    """`provision --source-config` uploads the migration config VERBATIM, and
    that file nests the connection under `snowflake:`. Reading the top level
    for `account` found only the envelope keys, so the cluster reported every
    required field missing at once — which reads like a dead credential, not a
    config one level too deep."""
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(json.dumps({
        "snowflake": {"account": "ACC", "warehouse": "WH", "database": "DB",
                      "user": "u", "auth": "password", "password": "p"},
        "aidp": {"datalake_ocid": "ocid1.aidataplatform.oc1..x"},
    }), encoding="utf-8")
    loaded = source_helpers.load_source_config(cfg)
    assert loaded["account"] == "ACC"
    assert loaded["auth"] == "password"
    assert "aidp" not in loaded


def test_a_flat_source_config_still_loads(source_helpers, tmp_path):
    """The loader predates the envelope and JSON is the documented fallback
    for a cluster with no PyYAML, so the flat shape stays supported."""
    cfg = tmp_path / "source.json"
    cfg.write_text(json.dumps({"account": "ACC", "warehouse": "WH",
                               "database": "DB", "user": "u",
                               "auth": "password"}), encoding="utf-8")
    assert source_helpers.load_source_config(cfg)["account"] == "ACC"


def test_the_structure_stage_does_not_ship_a_default_that_cannot_work():
    """`manifest` mode reads types from the discovery manifest, but a manifest
    built in `connector` mode carries SNOWFLAKE types and Delta rejects them
    verbatim. Shipping `source-mode: connector` beside `mode: manifest` meant
    the default pair refused every table on a first run."""
    import sys
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES

    stage = next(s for s in STAGES if s.key == "structure")
    assert stage.params["mode"] == "ddl-plan", stage.params
    if stage.params.get("source-mode") == "connector":
        assert stage.params["mode"] != "manifest", (
            "connector-built manifests carry Snowflake types; this pair "
            "cannot create a Delta table")


# --- discovery: --schemas is a predicate, and a scoped run merges -------------
#
# Two defects with one root: `--schemas` was applied AFTER an unfiltered
# INFORMATION_SCHEMA fetch. So it could not narrow a query that hits
# Snowflake's result cap, and in connector mode the filtered result was
# ASSIGNED over the loaded manifest -- following DISCOVERY.md's own advice
# ("re-run with --force --schemas <name>") deleted every other schema from
# discovery_manifest.json, in both modes.

def _rel(schema, name, rows=1):
    return {"TABLE_SCHEMA": schema, "TABLE_NAME": name, "TABLE_TYPE": "BASE TABLE",
            "ROW_COUNT": rows, "BYTES": 10 * rows}


def _colrow(schema, name):
    return {"TABLE_SCHEMA": schema, "TABLE_NAME": name, "COLUMN_NAME": "ID",
            "ORDINAL_POSITION": 1, "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
            "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
            "CHARACTER_MAXIMUM_LENGTH": None}


def test_wanted_schemas_are_pushed_down_not_filtered_client_side(discover):
    source = _FakeSource(tables=[_rel("SALES", "ORDERS"), _rel("HR", "EMP")],
                         columns=[_colrow("SALES", "ORDERS"), _colrow("HR", "EMP")])
    discover.discover_via_connector(source, wanted=["SALES"], exclude=set())
    assert len(source.queries) == 2
    for sql in source.queries:
        assert "where TABLE_SCHEMA in ('SALES')" in sql, sql
    # And an unscoped run stays unfiltered: the two-query fast path.
    source = _FakeSource(tables=[_rel("SALES", "ORDERS")],
                         columns=[_colrow("SALES", "ORDERS")])
    discover.discover_via_connector(source, wanted=None, exclude=set())
    assert not any("where TABLE_SCHEMA" in sql for sql in source.queries)


def test_schema_name_with_apostrophe_is_escaped_in_the_predicate(discover):
    source = _FakeSource(tables=[], columns=[])
    discover.discover_via_connector(source, wanted=["O'BRIEN", "HR"], exclude=set())
    assert "where TABLE_SCHEMA in ('O''BRIEN', 'HR')" in source.queries[0]


def test_discovery_summary_says_other_schemas_are_kept(discover):
    md = discover.render_summary({"source": {"mode": "connector"},
                                  "schemas": [], "generated_at": "now"})
    assert "kept" in md
    assert "--force --schemas" not in md, \
        "the old advice, followed literally, truncated the manifest"


# main() end to end, with pyspark and SnowflakeSource stubbed.

class _Estate:
    """Mutable estate the fakes answer from, so a test can change a row count
    or drop a schema between runs."""
    tables = [_rel("HR", "EMP", rows=3), _rel("SALES", "ORDERS", rows=5),
              _rel("FIN", "LEDGER", rows=7)]

    @classmethod
    def columns(cls):
        return [_colrow(t["TABLE_SCHEMA"], t["TABLE_NAME"]) for t in cls.tables]

    @classmethod
    def schemas(cls):
        return sorted({t["TABLE_SCHEMA"] for t in cls.tables})


class _MainSource:
    """Stands in for SnowflakeSource inside main(): honours the schema
    predicate the way Snowflake would."""

    def __init__(self, spark, *, mode, config=None, external_catalog=None,
                 session_schema=None):
        self.spark, self.mode = spark, mode
        self.external_catalog = external_catalog or "ext"
        self.session_schema = session_schema or "PUBLIC"
        self.queries: list[str] = []

    def pushdown(self, sql, schema=None):
        import re
        self.queries.append(sql)
        rows = _Estate.columns() if "COLUMNS" in sql else list(_Estate.tables)
        m = re.search(r"where TABLE_SCHEMA in \((.*?)\)", sql)
        if m:
            wanted = {s.strip()[1:-1].replace("''", "'")
                      for s in m.group(1).split(",")}
            rows = [r for r in rows if r["TABLE_SCHEMA"] in wanted]
        return _FakeDF(rows)

    def describe(self):
        return {"mode": self.mode, "database": "DB", "host": "h", "user": "u",
                "warehouse": "w", "role": "r", "auth": "KeyPair",
                "external_catalog": self.external_catalog,
                "session_schema": self.session_schema}

    def database(self):
        return "DB"


class _CatalogSpark(_FakeSpark):
    """Answers SHOW SCHEMAS / SHOW TABLES / SHOW VIEWS / DESCRIBE against the
    same estate, for external-catalog mode."""

    def sql(self, statement):
        low = statement.lower()
        if low.startswith("show schemas"):
            return _FakeDF([{"namespace": s} for s in _Estate.schemas()])
        if low.startswith("show tables"):
            schema = statement.rsplit("`", 2)[-2]
            return _FakeDF([{"tableName": t["TABLE_NAME"]} for t in _Estate.tables
                            if t["TABLE_SCHEMA"] == schema])
        if low.startswith("show views"):
            return _FakeDF([])
        return super().sql(statement)


def _stub_pyspark(monkeypatch, spark):
    import types
    pyspark, sql = types.ModuleType("pyspark"), types.ModuleType("pyspark.sql")

    class _Builder:
        @staticmethod
        def getOrCreate():
            return spark

    class SparkSession:
        builder = _Builder()

    sql.SparkSession = SparkSession
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)


def _manifest_schemas(reports):
    data = json.loads((reports / "discovery_manifest.json").read_text(encoding="utf-8"))
    return {s["name"]: s for s in data["schemas"]}


@pytest.fixture
def estate(monkeypatch, discover):
    monkeypatch.setattr(_Estate, "tables", [_rel("HR", "EMP", rows=3),
                                            _rel("SALES", "ORDERS", rows=5),
                                            _rel("FIN", "LEDGER", rows=7)])
    monkeypatch.setattr(discover, "SnowflakeSource", _MainSource)
    _stub_pyspark(monkeypatch, _CatalogSpark())
    return _Estate


def test_a_scoped_connector_rerun_keeps_the_other_schemas(discover, estate, tmp_path):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]

    estate.tables[1] = _rel("SALES", "ORDERS", rows=500)       # SALES changed
    assert discover.main(base + ["--force", "--schemas", "SALES"]) == 0
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"], "the other schemas are kept"
    assert got["SALES"]["tables"][0]["source_rows"] == 500, "SALES was refreshed"
    assert got["HR"]["tables"][0]["source_rows"] == 3

    estate.tables[1] = _rel("SALES", "ORDERS", rows=501)
    assert discover.main(base + ["--schemas", "SALES"]) == 0   # no --force
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"]
    assert got["SALES"]["tables"][0]["source_rows"] == 501


def test_force_without_schemas_rediscovers_the_whole_estate(discover, estate, tmp_path):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    del estate.tables[2]                                         # FIN is gone
    assert discover.main(base + ["--force"]) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["HR", "SALES"], \
        "a full re-discovery is authoritative and drops what no longer exists"


def test_a_scoped_external_catalog_force_keeps_the_other_schemas(discover, estate,
                                                                 tmp_path):
    base = ["--source-mode", "external-catalog", "--source-catalog", "ext",
            "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]
    assert discover.main(base + ["--force", "--schemas", "SALES"]) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]


def test_a_manifest_for_a_different_source_is_refused_even_with_force(discover, estate,
                                                                       tmp_path):
    # The identity guard used to be skipped under --force, which let a --force
    # run silently overwrite another database's manifest.
    (tmp_path / "discovery_manifest.json").write_text(json.dumps(
        {"schemas": [], "source_identity": "OTHER_DB"}), encoding="utf-8")
    rc = discover.main(["--source-mode", "connector", "--reports-dir",
                        str(tmp_path), "--force"])
    assert rc == 1
    data = json.loads((tmp_path / "discovery_manifest.json").read_text(encoding="utf-8"))
    assert data["source_identity"] == "OTHER_DB", "left untouched"


def test_zero_schemas_message_points_at_the_traceback_too(discover, estate, tmp_path,
                                                          monkeypatch, capsys):
    class Broken(_MainSource):
        def pushdown(self, sql, schema=None):
            raise RuntimeError("SQL compilation error: Information schema query "
                               "returned too much data. Please repeat query with "
                               "more selective predicates.")

    monkeypatch.setattr(discover, "SnowflakeSource", Broken)
    rc = discover.main(["--source-mode", "connector", "--reports-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "DISCOVERY FAILED" in out and "too much data" in out
    assert "DISCOVERY FAILED above" in out, \
        "the hint must not point only at the credentials"


def test_the_committed_discovery_notebook_matches_its_source():
    """The AIDP job runs the .ipynb, not the .py. A fix to the source that is
    not regenerated into the notebook ships the old behaviour to the cluster
    while every test here passes against the new one."""
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES, build_stage_notebook
    stage = next(s for s in STAGES if s.source == "00_discover_snowflake.py")
    committed = SCRIPTS.parents[1] / "data-migration-scripts" / stage.notebook_name
    generated = build_stage_notebook(stage)
    assert json.loads(committed.read_text(encoding="utf-8")) == generated, \
        "regenerate with `snowmig.py build-notebooks`"
