"""The helpers every generated notebook carries inline.

Inline rather than a wheel: a notebook that needs a library installed on the
cluster before it can run is one more thing to get wrong at cutover, and these
are small. Each helper is a pure function of its arguments plus ``spark``.

``_aidp_parameter`` follows the shape the Fabric migrator verified live
(2026-09): AIDP injects ``oidlUtils`` with no import, ``getParameter`` takes
``(name, default)`` and returns strings; off AIDP the name is absent and the
default is used, so the same notebook also runs on a local Spark.
"""

RUNTIME = r'''# ocidi2aidp runtime helpers -- generated; nothing to install on the cluster.
import datetime as _dt
import decimal as _dec
import re as _re
import uuid as _uuid

from pyspark.sql import functions as F
from pyspark.sql import Window


def _aidp_parameter(name, default):
    """This task's AIDP parameter ``name``, or ``default`` where AIDP is not running."""
    try:
        return oidlUtils.parameters.getParameter(name, default)  # noqa: F821
    except (NameError, AttributeError):
        return default


_INT_TYPES = {"INTEGER", "INT", "TINYINT", "SMALLINT", "LONG", "BIGINT"}
_NUM_TYPES = {"NUMERIC", "NUMBER", "DECIMAL"}
_FLOAT_TYPES = {"DOUBLE", "FLOAT"}


def _coerce(value, di_type):
    """AIDP passes parameters as strings; give them their declared OCI-DI type."""
    if value is None or not isinstance(value, str):
        return value
    t = (di_type or "STRING").upper().split("(")[0]
    if t in _INT_TYPES:
        return int(value)
    if t in _NUM_TYPES:
        return _dec.Decimal(value)
    if t in _FLOAT_TYPES:
        return float(value)
    if t == "BOOLEAN":
        return value.strip().lower() in ("true", "1", "yes", "y")
    if t == "DATE":
        return _dt.date.fromisoformat(value[:10])
    if t in ("DATETIME", "TIMESTAMP"):
        return _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _sql_lit(value):
    """A Python value as a Spark SQL literal -- the only way a parameter enters SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, _dec.Decimal):
        return format(value, "f")
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, _dt.datetime):
        return "TIMESTAMP'" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    if isinstance(value, _dt.date):
        return "DATE'" + value.isoformat() + "'"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql(template, params):
    """Substitute ``${NAME}`` placeholders with quoted literals from ``params``."""
    return _re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: _sql_lit(params[m.group(1)]),
                   template)


def _tbl(*parts):
    return ".".join("`" + str(p).replace("`", "``") + "`" for p in parts)


def _os_path(root, bucket, obj):
    """``oci://{bucket}@namespace/`` + object name or glob."""
    return root.format(bucket=bucket) + obj.lstrip("/")


def _rename_by_pattern(df, pattern, replacement):
    """OCI-DI map-by-pattern: rename every column matching ``pattern``."""
    rx = _re.compile(pattern)
    for c in df.columns:
        if rx.fullmatch(c):
            df = df.withColumnRenamed(c, rx.sub(replacement, c))
    return df


def _merge_into(df, table, cond):
    """SQL MERGE: unlike DeltaTable.forName it accepts a catalog.schema.table name."""
    view = "_ocidi_src_" + _uuid.uuid4().hex[:12]
    df.createOrReplaceTempView(view)
    try:
        spark.sql("MERGE INTO %s t USING %s s ON %s WHEN MATCHED THEN UPDATE SET * "  # noqa: F821
                  "WHEN NOT MATCHED THEN INSERT *" % (table, view, cond))
    finally:
        spark.catalog.dropTempView(view)  # noqa: F821


def _align(df, table):
    """Cast ``df`` to an existing table's column types, as a database INSERT would.

    Spark's default (non-ANSI) cast turns a value that does not fit into NULL
    without a word; the database OCI-DI wrote to would have raised instead
    (e.g. ORA-01438). So any cast that loses a non-NULL value stops the write.
    """
    if not spark.catalog.tableExists(table):  # noqa: F821
        return df
    target = {f.name.lower(): f.dataType for f in spark.table(table).schema.fields}  # noqa: F821
    cols, lossy = [], []
    for f in df.schema.fields:
        src = F.col("`" + f.name.replace("`", "``") + "`")
        t = target.get(f.name.lower())
        if t is None or t == f.dataType:
            cols.append(src)
            continue
        cast = src.cast(t)
        cols.append(cast.alias(f.name))
        lossy.append(cast.isNull() & src.isNotNull())
    if lossy:
        cond = lossy[0]
        for c in lossy[1:]:
            cond = cond | c
        if df.filter(cond).limit(1).count():
            raise ValueError("%s: a value does not fit the target column type (it would be "
                             "written as NULL)" % table)
    return df.select(*cols)


def _write_delta(df, table, mode, keys=None):
    """Write ``df`` to a managed Delta table with OCI-DI write-mode semantics."""
    df = _align(df, table)
    if mode == "merge":
        if not spark.catalog.tableExists(table):  # noqa: F821
            df.write.format("delta").mode("append").saveAsTable(table)
            return
        _merge_into(df, table, " AND ".join("t.`%s` = s.`%s`" % (k, k) for k in keys))
        return
    df.write.format("delta").mode(mode).saveAsTable(table)


_WM_PENDING = []


def _wm_get(task, source):
    """Last committed watermark for ``(task, source)``; None means full load."""
    table = _tbl(TARGET_CATALOG, CONTROL_SCHEMA, "watermarks")  # noqa: F821
    if not spark.catalog.tableExists(table):  # noqa: F821
        return None
    row = (spark.table(table)  # noqa: F821
           .where((F.col("task") == task) & (F.col("source") == source))
           .agg(F.max("value")).first())
    return row[0] if row else None


def _wm_commit(task):
    """Record this run's watermarks -- called only after every target wrote."""
    if not _WM_PENDING:
        return
    table = _tbl(TARGET_CATALOG, CONTROL_SCHEMA, "watermarks")  # noqa: F821
    rows = [(task, s, c, v, _dt.datetime.utcnow()) for (s, c, v) in _WM_PENDING if v is not None]
    if not rows:
        return
    upd = spark.createDataFrame(  # noqa: F821
        rows, "task STRING, source STRING, column_name STRING, value TIMESTAMP, updated_at TIMESTAMP")
    if not spark.catalog.tableExists(table):  # noqa: F821
        upd.write.format("delta").mode("append").saveAsTable(table)
        return
    _merge_into(upd, table, "t.task = s.task AND t.source = s.source")


_RUN_STARTED = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
_WRITTEN = []
'''
