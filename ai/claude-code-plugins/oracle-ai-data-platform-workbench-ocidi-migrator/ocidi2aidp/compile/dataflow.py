"""Compile one OCI-DI data flow into a PySpark notebook.

One DataFrame variable per operator output port, emitted in topological
order, one notebook cell per operator. Targets are emitted last, in
``loadOrder`` -- they have no outputs, so deferring them changes nothing but
the order the writes happen in, which is what ``loadOrder`` controls.

An operator the compiler cannot translate deterministically does not stop the
notebook: it becomes a stub function that raises ``NotImplementedError`` and,
unless it is a ``manual`` case, an LLM work order (``fallback/<id>.json``).
The notebook is therefore always structurally complete, and an unconverted
operator always fails loudly rather than producing wrong rows.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Optional

from ..di import (entity_ref, expr_text, has_pattern_rules, mt, param_value, port_fields,
                  rule_names, type_name)
from ..naming import ident, py_ident, unique
from ..notebook import Notebook
from ..report import Finding
from . import sources as srcmod
from .expressions import ParamSpec, Scope, Untranslatable, spark_type, translate
from .graph import FlowGraph, GNode, GraphError
from .runtime import RUNTIME


class NeedsFallback(Exception):
    """No deterministic translation. ``kind`` names the work-order type;
    ``manual`` means no work order (an LLM cannot supply what is missing)."""

    def __init__(self, reason: str, kind: str = "operator"):
        super().__init__(reason)
        self.reason = reason
        self.kind = kind


def pylit(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def pyval(value) -> str:
    """Any JSON scalar as Python source (``True``, not ``true``)."""
    return pylit(value) if isinstance(value, str) else repr(value)


@dataclass
class Frame:
    var: str
    columns: Optional[list]     # known output column names, or None
    node: str                   # producing node key


@dataclass
class FlowResult:
    notebook: Notebook
    findings: list = field(default_factory=list)
    fallbacks: list = field(default_factory=list)     # work-order dicts
    targets: list = field(default_factory=list)
    sources: list = field(default_factory=list)       # SourceAccess
    params: dict = field(default_factory=dict)        # UPPER name -> ParamSpec
    ddl: list = field(default_factory=list)           # {"schema", "table", "columns": [(n, t)]}
    watermarks: list = field(default_factory=list)    # {"source", "column"}
    failed: bool = False


_WRITE_MODES = {"APPEND": "append", "INSERT": "append", "OVERWRITE": "overwrite",
                "TRUNCATE": "overwrite", "MERGE": "merge", "UPSERT": "merge", "IGNORE": "ignore",
                "BACKUP": "overwrite"}
_JOIN_HOW = {"INNER": "inner", "LEFT": "left", "RIGHT": "right", "FULL": "full"}


class FlowCompiler:
    def __init__(self, flow: dict, *, snapshot, config, name: str, key: str, kind: str,
                 overrides: Optional[dict] = None, udfs: Optional[dict] = None,
                 wm_task: str = "", notebook_path: str = "", strict: bool = False):
        self.flow = flow or {}
        self.snapshot = snapshot
        self.config = config
        self.name = name
        self.key = key
        self.kind = kind
        self.overrides = overrides or {}
        self.udfs = udfs or {}
        self.wm_task = wm_task or name
        self.notebook_path = notebook_path
        self.strict = strict
        self.res = FlowResult(Notebook(name))
        self.frames: dict = {}
        self.frames_by_node: dict = {}
        self.entities: dict = {}        # node key -> entity name
        self.vars: set = set()
        self.cells: list = []           # (node, lines)
        self.used_params: set = set()
        self.source_params: dict = {}   # parameter -> SourceAccess
        self.key_names: dict = {}       # field key -> field name
        self.incremental: set = set()   # node keys of incremental sources

    # ------------------------------------------------------------ findings
    def note(self, code, message, severity="review", node=""):
        f = Finding(code, message, severity, node)
        if f not in self.res.findings:
            self.res.findings.append(f)

    # ---------------------------------------------------------------- main
    def compile(self) -> FlowResult:
        self._params()
        try:
            self.graph = FlowGraph(self.flow)
            order = self.graph.order()
        except GraphError as exc:
            self.note("DF01_GRAPH", str(exc), "manual")
            self.res.failed = True
            self.res.notebook.md(f"# {self.name}\n\nNot migrated: {exc}")
            return self.res
        if not order:
            self.note("DF02_EMPTY", "the data flow has no operators", "manual")
        self._index_keys()
        targets = [n for n in order if n.op_type == "TARGET_OPERATOR"]
        others = [n for n in order if n.op_type != "TARGET_OPERATOR"]
        position = {n.key: i for i, n in enumerate(order)}
        targets.sort(key=lambda n: (n.operator.get("loadOrder") or 0, position[n.key]))
        for node in others + targets:
            self._node(node)
        self._assemble()
        return self.res

    def _index_keys(self) -> None:
        for node in self.graph.nodes.values():
            for port in node.in_ports + node.out_ports:
                for f in port_fields(port):
                    if f.key and f.name:
                        self.key_names[f.key] = f.name
            ent = (node.operator.get("entity") or {})
            for e in (((ent.get("shape") or {}).get("type") or {}).get("elements") or []):
                if isinstance(e, dict) and e.get("key") and e.get("name"):
                    self.key_names[e["key"]] = e["name"]
        for k, wrapper in (self.flow.get("typedObjectMap") or {}).items():
            obj = (wrapper or {}).get("typedObject") or wrapper or {}
            if isinstance(obj, dict) and obj.get("name"):
                self.key_names[k] = obj["name"]
                if obj.get("key"):
                    self.key_names[obj["key"]] = obj["name"]

    def _params(self) -> None:
        for p in self.flow.get("parameters") or []:
            name = p.get("name") or p.get("identifier")
            if not name:
                continue
            typ = p.get("typeName") or type_name(p.get("type")) or "STRING"
            default = p.get("defaultValue")
            if isinstance(default, dict):
                default = param_value(default) if "stringValue" in default else default
            if name in self.overrides:
                default = self.overrides[name]
            self.res.params[name.upper()] = ParamSpec(name, typ, default)

    # ---------------------------------------------------------------- nodes
    def _frame_for(self, edge) -> Frame:
        frame = self.frames.get((edge.src, edge.src_port))
        if frame is None:
            produced = self.frames_by_node.get(edge.src) or []
            if not produced:
                raise NeedsFallback(f"input from {self.graph.nodes[edge.src].label} has no "
                                    f"DataFrame (its operator was not compiled)")
            frame = produced[0]
        return frame

    def _var(self, label: str, suffix: str = "") -> str:
        base = "df_" + py_ident(label) + (("_" + py_ident(suffix)) if suffix else "")
        return unique(base, self.vars)

    def _out(self, node: GNode, var: str, columns, port: Optional[dict] = None) -> Frame:
        frame = Frame(var, list(columns) if columns is not None else None, node.key)
        ports = [port] if port is not None else node.data_out_ports()
        for p in ports or [{}]:
            self.frames[(node.key, (p or {}).get("key") or "")] = frame
        self.frames.setdefault((node.key, ""), frame)
        self.frames_by_node.setdefault(node.key, []).append(frame)
        return frame

    def _rollback(self, node: GNode, vars_before: set) -> None:
        """Forget what a failed handler reserved, so the stub owns clean names."""
        self.vars.intersection_update(vars_before)
        for k in [k for k in self.frames if k[0] == node.key]:
            del self.frames[k]
        self.frames_by_node.pop(node.key, None)

    def _node(self, node: GNode) -> None:
        handler = getattr(self, "_op_" + (node.op_type or "UNKNOWN"), None)
        vars_before = set(self.vars)
        try:
            ins = [self._frame_for(e) for e in self.graph.inputs(node.key)]
            if handler is None:
                kind = "table_function" if "TABLE_FUNCTION" in node.op_type else "operator"
                raise NeedsFallback(f"operator type {node.op_type or '(none)'} has no "
                                    f"deterministic translation", kind)
            lines = handler(node, ins)
        except (NeedsFallback, Untranslatable) as exc:
            kind = exc.kind if isinstance(exc, NeedsFallback) else "expression"
            reason = exc.reason if isinstance(exc, NeedsFallback) else str(exc)
            self._rollback(node, vars_before)
            lines = self._fallback(node, reason, kind)
        except Exception as exc:  # a compiler defect: surface it, never hide it
            if self.strict:
                raise
            self._rollback(node, vars_before)
            lines = self._fallback(node, f"compiler error: {type(exc).__name__}: {exc}",
                                   "compiler_error")
        self.cells.append((node, lines))

    # ------------------------------------------------------------- scopes
    def _scope_single(self, node: GNode, ins: list) -> Scope:
        aliases = {n: "" for n in node.names}
        entities = {}
        for f in ins:
            for n in self.graph.names_through(f.node):
                aliases[n] = ""
            for k in self.graph.ancestors(f.node) | {f.node}:
                if k in self.entities:
                    entities[self.entities[k].upper()] = ""
        return Scope(aliases=aliases, entities=entities, params=self.res.params, udfs=self.udfs,
                     node=node.label)

    def _scope_pair(self, node: GNode, left: Frame, right: Frame) -> Scope:
        ln, rn = self.graph.names_through(left.node), self.graph.names_through(right.node)
        aliases = {n: "?" for n in node.names}
        for n in ln | rn:
            aliases[n] = "?" if (n in ln and n in rn) else ("l" if n in ln else "r")
        # The immediate inputs are never ambiguous, even in a self-join.
        for n in self.graph.nodes[left.node].names:
            aliases[n] = "l"
        for n in self.graph.nodes[right.node].names:
            aliases[n] = "r"
        entities = {}
        for side, f in (("l", left), ("r", right)):
            for k in self.graph.ancestors(f.node) | {f.node}:
                if k in self.entities:
                    e = self.entities[k].upper()
                    entities[e] = "?" if entities.get(e, side) != side else side
        return Scope(aliases=aliases, entities=entities, params=self.res.params, udfs=self.udfs,
                     node=node.label)

    def _expr(self, text: Optional[str], scope: Scope) -> str:
        if text is None:
            raise Untranslatable("expression missing from the operator JSON")
        t = translate(text, scope)
        for f in t.findings:
            if f not in self.res.findings:
                self.res.findings.append(f)
        self.used_params |= t.params
        if t.params:
            return f"F.expr(_sql({pylit(t.sql)}, PARAMS))"
        return f"F.expr({pylit(t.sql)})"

    def _single(self, node: GNode, ins: list) -> Frame:
        if len(ins) != 1:
            raise NeedsFallback(f"{node.op_type} expects one input, has {len(ins)}")
        return ins[0]

    # ------------------------------------------------------------ sources
    def _source_access(self, access: srcmod.SourceAccess, node: GNode) -> None:
        if access.parameter not in self.source_params:
            self.source_params[access.parameter] = access
            self.res.sources.append(access)
        if access.severity != "info":
            self.note("SR01_SOURCE_ACCESS", f"{access.data_asset}: {access.action}",
                      access.severity, node.label)

    def _name_code(self, name: str, node: GNode) -> str:
        """An entity name, with ``${PARAM}`` interpolated from PARAMS at run time."""
        parts = re.split(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", name)
        if len(parts) == 1:
            return pylit(name)
        code = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                if part:
                    code.append(pylit(part))
            else:
                spec = self.res.params.get(part.upper())
                if spec is None:
                    raise Untranslatable(f"entity name {name!r} uses undeclared parameter {part}")
                self.used_params.add(spec.name)
                code.append(f"str(PARAMS[{pylit(spec.name)}])")
        self.note("SR05_PARAM_ENTITY", f"entity name {name!r} is resolved from parameters at "
                  f"run time", "info", node.label)
        return " + ".join(code)

    def _op_SOURCE_OPERATOR(self, node: GNode, ins: list) -> list:
        op = node.operator
        ent = entity_ref(op)
        entity_name = ent.name or (ent.resource_name or "").split(".")[-1]
        self.entities[node.key] = entity_name or node.label
        asset = srcmod.asset_from_ref(self.snapshot, ent.data_asset)
        if asset is None:
            bound = ", ".join(f"{k}={v}" for k, v in ent.parameterized.items())
            raise NeedsFallback("the source's data asset could not be resolved from the "
                                "operator JSON" + (f" (bound to parameters: {bound})" if bound
                                                   else ""), "manual")
        access = srcmod.resolve(asset, self.config.sources)
        self._source_access(access, node)
        if access.kind == "none":
            raise NeedsFallback(f"{access.data_asset} ({access.asset_type}): {access.action}",
                                "manual")
        if ent.model_type == "SQL_ENTITY" or ent.sql:
            raise NeedsFallback("custom SQL source: the query is written in the source "
                                "database's dialect against its own table names", "custom_sql")
        schema = ent.schema.name if ent.schema else ""
        if not schema and "." in (ent.resource_name or ""):
            schema = ent.resource_name.split(".")[-2]
        name_code = self._name_code(entity_name, node)
        var = self._var(node.label)
        p = access.parameter
        if access.kind == "catalog":
            if not schema:
                self.note("SR02_NO_SCHEMA", f"no schema found for {entity_name}; reading "
                          f"`{p}.{entity_name}`", "assume", node.label)
                read = f"spark.table(_tbl({p}, {name_code}))"
            else:
                read = f"spark.table(_tbl({p}, {pylit(schema)}, {name_code}))"
        elif access.kind == "path":
            read = self._file_read(ent, schema, name_code, p, node)
        else:  # jdbc
            dbtable = f"{schema}.{entity_name}" if schema else entity_name
            driver = f'\n    .option("driver", {pylit(access.driver)})' if access.driver else ""
            read = (f'(spark.read.format("jdbc")\n'
                    f'    .option("url", aidputils.secrets.get(name={p}, key="url"))  # noqa: F821\n'
                    f'    .option("user", aidputils.secrets.get(name={p}, key="user"))  # noqa: F821\n'
                    f'    .option("password", aidputils.secrets.get(name={p}, key="password"))  # noqa: F821'
                    f'{driver}\n    .option("dbtable", {pylit(dbtable)})\n    .load())')
        names = [f.name for p_ in node.data_out_ports() for f in port_fields(p_) if f.name]
        lines = []
        wm = self._incremental(op)
        if wm:
            lines.append(f"_wm_prev = _wm_get(WM_TASK, {pylit(node.label)})")
        lines.append(f"{var} = {read}")
        if names:
            lines.append(f"{var} = {var}.select({', '.join(pylit(n) for n in names)})")
        if wm:
            col, cmp = wm
            lines += [
                f"_wm_next = {var}.agg(F.max(F.col({pylit(col)}))).first()[0]",
                "if _wm_prev is not None:",
                f"    {var} = {var}.filter(F.col({pylit(col)}) {cmp} F.lit(_wm_prev))",
                "if _wm_next is not None:",
                f"    {var} = {var}.filter(F.col({pylit(col)}) <= F.lit(_wm_next))",
                f"_WM_PENDING.append(({pylit(node.label)}, {pylit(col)}, _wm_next))",
            ]
            self.res.watermarks.append({"source": node.label, "column": col})
            self.incremental.add(node.key)
            self.note("SR10_INCREMENTAL", f"incremental load on {col}: watermark kept in "
                      f"<catalog>.{self.config.target.control_schema}.watermarks; seed it from "
                      f"watermarks/seed.sql before the first run or it reloads everything",
                      "review", node.label)
        columns = names or [f.name for f in ent.shape] or None
        self._out(node, var, columns)
        return lines

    def _incremental(self, op: dict):
        roc = op.get("readOperationConfig") or {}
        inc = roc.get("incrementalReadConfig") or {}
        clauses = inc.get("lastExtractedFieldDate") or []
        if not clauses:
            return None
        clause = clauses[0]
        col = clause.get("incrementalFieldName")
        if not col:
            return None
        cmp = {"GREATERTHANOREQUALTO": ">=", "GREATER_THAN_OR_EQUAL_TO": ">="}.get(
            (clause.get("incrementalComparator") or "").upper(), ">")
        return col, cmp

    def _file_read(self, ent, bucket: str, name_code: str, p: str, node: GNode) -> str:
        fmt = (ent.data_format or "").lower()
        if not fmt:
            raise NeedsFallback("file source with no data format in the operator JSON")
        if fmt == "xml":
            self.note("SR03_XML", "XML files need the spark-xml package on the cluster", "review",
                      node.label)
        if fmt == "avro":
            self.note("SR04_AVRO", "Avro needs the spark-avro package on the cluster", "review",
                      node.label)
        if not bucket:
            raise NeedsFallback("file source with no bucket (schema) in the operator JSON")
        options = dict(ent.format_options)
        if fmt == "csv":
            options.setdefault("header", True)
        if fmt == "json":
            options.setdefault("multiLine", False)
        opts = "".join(f".option({pylit(k)}, {pyval(v)})" for k, v in options.items())
        typed = [(f.name, spark_type(f.type)[0]) for f in ent.shape if f.name and f.type]
        if fmt in ("csv", "json", "xml") and typed and len(typed) == len(ent.shape):
            # OCI-DI reads text files with the entity's declared shape; without it every
            # CSV column would arrive as STRING.
            ddl = ", ".join(f"`{n}` {t}" for n, t in typed)
            opts = f".schema({pylit(ddl)})" + opts
        raw_name = ent.name or ""
        if re.search(r"(\.\*|\\[dws.]|\[|\(|\||\^|\$)", raw_name.replace("${", "")):
            self.note("SR06_FILE_PATTERN", f"file name {raw_name!r} looks like a regular "
                      f"expression; Spark reads Hadoop globs -- check the converted path",
                      "review", node.label)
            name_code = pylit(re.sub(r"\.\*", "*", raw_name))
        return (f"spark.read.format({pylit(fmt)}){opts}"
                f".load(_os_path({p}, {pylit(bucket)}, {name_code}))")

    # ------------------------------------------------------------ targets
    def _op_TARGET_OPERATOR(self, node: GNode, ins: list) -> list:
        src = self._single(node, ins)
        op = node.operator
        ent = entity_ref(op)
        asset = srcmod.asset_from_ref(self.snapshot, ent.data_asset)
        legacy_schema = ent.schema.name if ent.schema else ""
        asset_name = (asset or {}).get("name") or (ent.data_asset.name if ent.data_asset else "")
        schema_key = legacy_schema or asset_name or "default"
        t_schema = self.config.target.schema_map.get(schema_key) or ident(schema_key)
        entity_name = ent.name or (ent.resource_name or "").split(".")[-1] or node.label
        if "${" in entity_name:
            raise NeedsFallback(f"target entity name {entity_name!r} is parameterized; a Delta "
                                f"table name must be fixed for DDL and reconciliation", "manual")
        prefix = op.get("targetEntityNamePrefix") or ""
        suffix = op.get("targetEntityNameSuffix") or ""
        if op.get("isUseSameSourceName"):
            self.note("TG07_SAME_SOURCE_NAME", "target uses 'same name as source'; the name "
                      f"{entity_name!r} from the JSON was used", "review", node.label)
        t_table = ident(prefix + entity_name + suffix)
        table_code = f"_tbl(TARGET_CATALOG, {pylit(t_schema)}, {pylit(t_table)})"

        woc = op.get("writeOperationConfig") or {}
        raw_mode = (woc.get("writeMode") or op.get("dataProperty") or "APPEND").upper()
        mode = _WRITE_MODES.get(raw_mode)
        if mode is None:
            raise NeedsFallback(f"write mode {raw_mode} is not known", "manual")
        if raw_mode == "IGNORE":
            self.note("TG02_IGNORE", "write mode IGNORE read as 'skip if the table exists' "
                      "(Spark mode('ignore'))", "assume", node.label)
        if raw_mode == "BACKUP":
            self.note("TG03_BACKUP", "write mode BACKUP has no Delta equivalent; written as "
                      "OVERWRITE -- Delta time travel keeps the previous version", "review",
                      node.label)
        keys = []
        if mode == "merge":
            keys = rule_names(woc.get("mergeKey")) or self._key_attr_names(woc.get("mergeKey"))
            if not keys and ent.unique_keys:
                keys = ent.unique_keys[0]
                self.note("TG04_MERGE_KEY", f"MERGE key taken from the entity's unique key "
                          f"{keys}", "assume", node.label)
            if not keys:
                raise NeedsFallback("MERGE target with no merge key in the JSON", "manual")
        if (ent.data_format or "") and asset and asset.get("modelType", "").endswith(
                "OBJECT_STORAGE_DATA_ASSET"):
            self.note("TG05_FILES_TO_DELTA", f"OCI-DI wrote {ent.data_format} files to bucket "
                      f"{legacy_schema!r}; AIDP writes the managed Delta table "
                      f"{t_schema}.{t_table} instead", "info", node.label)
        if mode == "overwrite" and self.graph.ancestors(node.key) & self.incremental:
            self.note("TG08_OVERWRITE_INCREMENTAL", "OVERWRITE target fed by an incremental "
                      "source: every run replaces the table with only that run's new rows (as "
                      "OCI-DI did) -- confirm that is intended", "review", node.label)
        if woc.get("partitionConfig"):
            self.note("TG06_PARTITION", "the target's partition config was not carried over",
                      "info", node.label)

        target_fields = ent.shape or [f for p in node.in_ports for f in port_fields(p)]
        edge = self.graph.inputs(node.key)[0]
        lines, var, columns = self._field_map(edge.field_map, src, target_fields, node)
        key_arg = f", keys={json.dumps(keys)}" if keys else ""
        lines += [f"_write_delta({var}, {table_code}, {pylit(mode)}{key_arg})",
                  f"_WRITTEN.append(({table_code}, {pylit(mode)}))"]
        typed = [(f.name, spark_type(f.type)[0]) for f in target_fields if f.name and f.type]
        if typed and len(typed) == len([f for f in target_fields if f.name]):
            self.res.ddl.append({"schema": t_schema, "table": t_table, "columns": typed,
                                 "keys": keys})
        else:
            self.res.ddl.append({"schema": t_schema, "table": t_table, "columns": [],
                                 "keys": keys})
        self.res.targets.append({
            "table": f"{t_schema}.{t_table}", "schema": t_schema, "name": t_table,
            "mode": mode, "keys": keys, "node": node.label, "columns": columns,
            "legacy": {"data_asset": asset_name, "schema": legacy_schema, "entity": entity_name,
                       "asset_type": (asset or {}).get("modelType", ""),
                       "parameter": srcmod.param_name(asset_name) if asset_name else ""},
        })
        return lines

    def _key_attr_names(self, key: Optional[dict]) -> list:
        out = []
        for attr in sorted((key or {}).get("attributeRefs") or [],
                           key=lambda a: a.get("position") or 0):
            sf = attr.get("attribute") or attr.get("shapeField") or {}
            name = sf.get("name") if isinstance(sf, dict) else self.key_names.get(sf)
            if name:
                out.append(name)
        return out

    def _ref_name(self, ref) -> Optional[str]:
        if isinstance(ref, dict):
            return ref.get("name") or self.key_names.get(ref.get("key") or "")
        if isinstance(ref, str):
            return self.key_names.get(ref) or (ref if "." not in ref and len(ref) < 64
                                               and not re.fullmatch(r"[0-9a-f-]{20,}", ref)
                                               else None)
        return None

    def _field_map(self, fm, src: Frame, target_fields: list, node: GNode):
        """Input DataFrame -> the DataFrame to write, per the link's field map."""
        var = self._var(node.label, "out")
        kind = mt(fm)
        if not fm:
            return self._map_by_name(src, target_fields, node, var, implicit=True)
        if kind == "RULE_BASED_FIELD_MAP":
            return self._rule_map(fm, src, target_fields, node, var)
        if kind in ("DIRECT_FIELD_MAP", "COMPOSITE_FIELD_MAP"):
            maps = [fm] if kind == "DIRECT_FIELD_MAP" else (fm.get("fieldMaps") or [])
            if all(mt(m) == "DIRECT_FIELD_MAP" for m in maps) and maps:
                pairs = []
                for m in maps:
                    s = self._ref_name(m.get("sourceTypedObject"))
                    t = self._ref_name(m.get("targetTypedObject"))
                    if not s or not t:
                        raise NeedsFallback("a direct field map names a field key that is not in "
                                            "the data flow JSON")
                    pairs.append((s, t))
                cols = ", ".join(f"F.col({pylit(s)}).alias({pylit(t)})" for s, t in pairs)
                return [f"{var} = {src.var}.select({cols})"], var, [t for _, t in pairs]
            if len(maps) == 1:
                return self._field_map(maps[0], src, target_fields, node)
            raise NeedsFallback("a composite field map mixing rule-based and direct maps")
        raise NeedsFallback(f"field map type {kind} is not known")

    def _map_by_name(self, src: Frame, target_fields: list, node: GNode, var: str,
                     implicit: bool = False):
        names = [f for f in target_fields if f.name]
        if not names:
            return [f"{var} = {src.var}"], var, src.columns
        if src.columns is None:
            cols = ", ".join(pylit(f.name) for f in names)
            if implicit:
                self.note("TG10_IMPLICIT_MAP", "no field map on the target link; columns are "
                          "selected by target name", "info", node.label)
            return [f"{var} = {src.var}.select({cols})"], var, [f.name for f in names]
        have = {c.upper(): c for c in src.columns}
        exprs, missing = [], []
        for f in names:
            if f.name.upper() in have:
                exprs.append(f"F.col({pylit(have[f.name.upper()])}).alias({pylit(f.name)})")
            else:
                missing.append(f.name)
                exprs.append(f"F.lit(None).cast({pylit(spark_type(f.type)[0])})"
                             f".alias({pylit(f.name)})")
        if missing:
            self.note("TG11_UNMAPPED", f"target columns with no matching input column are "
                      f"written as NULL: {', '.join(missing)}", "review", node.label)
        dropped = [c for c in src.columns if c.upper() not in {f.name.upper() for f in names}]
        if dropped:
            self.note("TG12_DROPPED", f"input columns not in the target: {', '.join(dropped)}",
                      "info", node.label)
        return [f"{var} = {src.var}.select({', '.join(exprs)})"], var, [f.name for f in names]

    def _rule_map(self, fm: dict, src: Frame, target_fields: list, node: GNode, var: str):
        map_type = (fm.get("mapType") or "MAPBYNAME").upper()
        if map_type == "MAPBYNAME":
            return self._map_by_name(src, target_fields, node, var)
        if map_type == "MAPBYPOSITION":
            names = [f.name for f in target_fields if f.name]
            if src.columns is None or not names:
                raise NeedsFallback("map-by-position needs both the input and target columns, "
                                    "and the JSON does not list them")
            if len(src.columns) < len(names):
                self.note("TG13_POSITION", f"map-by-position: {len(src.columns)} input columns "
                          f"for {len(names)} target columns", "review", node.label)
            pairs = list(zip(src.columns, names))
            cols = ", ".join(f"F.col({pylit(s)}).alias({pylit(t)})" for s, t in pairs)
            return [f"{var} = {src.var}.select({cols})"], var, [t for _, t in pairs]
        if map_type == "MAPBYPATTERN":
            pattern, repl = _di_pattern(fm.get("fromPattern") or "", fm.get("toPattern") or "",
                                        bool(fm.get("isJavaRegexSyntax")))
            lines = [f"{var} = _rename_by_pattern({src.var}, {pylit(pattern)}, {pylit(repl)})"]
            if target_fields:
                sub_lines, var2, cols = self._map_by_name(Frame(var, None, src.node),
                                                          target_fields, node,
                                                          self._var(node.label, "mapped"))
                return lines + sub_lines, var2, cols
            return lines, var, None
        raise NeedsFallback(f"rule-based map type {map_type} is not known")

    # ---------------------------------------------------------- operators
    def _op_FILTER_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        cond = self._expr(expr_text(node.operator.get("filterCondition")),
                          self._scope_single(node, ins))
        var = self._var(node.label)
        self._out(node, var, src.columns)
        return [f"{var} = {src.var}.filter({cond})"]

    def _op_EXPRESSION_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        scope = self._scope_single(node, ins)
        fields = [f for p in node.out_ports for f in port_fields(p)]
        derived = [f for f in fields if f.model_type == "DERIVED_FIELD" or
                   (f.expr and f.model_type != "MACRO_FIELD")]
        macros = [f for f in fields if f.model_type == "MACRO_FIELD"]
        if not derived and not macros:
            self.note("EX40_NO_DERIVED", "expression operator with no derived fields; passed "
                      "through", "info", node.label)
        var = self._var(node.label)
        parts, columns = [], list(src.columns) if src.columns is not None else None
        for f in derived:
            parts.append(f".withColumn({pylit(f.name)}, {self._expr(f.expr, scope)})")
            if columns is not None and f.name.upper() not in {c.upper() for c in columns}:
                columns.append(f.name)
        for m in macros:
            matched = rule_names(m.raw.get("type")) or rule_names(m.raw.get("useType")) or \
                rule_names(m.raw.get("configValues"))
            if not matched:
                raise NeedsFallback(f"bulk expression {m.name!r}: the attributes it applies to "
                                    f"are selected by a pattern the JSON does not resolve")
            for col in matched:
                out_name = m.name.replace("%MACRO_INPUT%", col) if "%MACRO_INPUT%" in m.name \
                    else col
                mscope = Scope(**{**scope.__dict__, "macro_input": f"`{col}`"})
                parts.append(f".withColumn({pylit(out_name)}, {self._expr(m.expr, mscope)})")
                if columns is not None and out_name.upper() not in {c.upper() for c in columns}:
                    columns.append(out_name)
        self._out(node, var, columns)
        if not parts:
            return [f"{var} = {src.var}"]
        return [f"{var} = ({src.var}"] + ["    " + p for p in parts] + [")"]

    def _op_AGGREGATOR_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        op = node.operator
        keys = rule_names(op.get("groupByColumns")) or rule_names(op.get("materializedGroupByColumns"))
        if not keys and has_pattern_rules(op.get("groupByColumns")):
            raise NeedsFallback("group-by columns selected by pattern")
        scope = self._scope_single(node, ins)
        aggs = [f for p in node.out_ports for f in port_fields(p) if f.expr]
        var = self._var(node.label)
        key_code = ", ".join(pylit(k) for k in keys)
        if not aggs:
            self._out(node, var, keys)
            return [f"{var} = {src.var}.select({key_code}).distinct()"]
        agg_code = ",\n    ".join(f"{self._expr(f.expr, scope)}.alias({pylit(f.name)})"
                                  for f in aggs)
        self._out(node, var, keys + [f.name for f in aggs])
        group = f".groupBy({key_code})" if keys else ".groupBy()"
        return [f"{var} = {src.var}{group}.agg(\n    {agg_code}\n)"]

    def _op_DISTINCT_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        var = self._var(node.label)
        self._out(node, var, src.columns)
        return [f"{var} = {src.var}.distinct()"]

    def _op_SORT_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        key = node.operator.get("sortKey") or {}
        order = []
        rules = key.get("sortRules") if isinstance(key, dict) else None
        if rules:
            for r in rules:
                names = rule_names(r.get("wrappedRule"))
                if not names:
                    raise NeedsFallback("sort rule selects columns by pattern")
                asc = r.get("isAscending", True) is not False
                order += [(n, asc) for n in names]
        elif isinstance(key, list):          # list[SortClause]
            for clause in key:
                name = (clause.get("field") or {}).get("name")
                if name:
                    order.append((name, (clause.get("order") or "ASC").upper() != "DESC"))
        if not order:
            raise NeedsFallback("sort operator with no sort key in the JSON")
        var = self._var(node.label)
        code = ", ".join(f"F.col({pylit(n)}).{'asc' if a else 'desc'}()" for n, a in order)
        self._out(node, var, src.columns)
        downstream = [self.graph.nodes[e.dst].op_type for e in self.graph.outputs(node.key)]
        if downstream and all(t == "TARGET_OPERATOR" for t in downstream):
            self.note("SO01_ORDER_NOT_KEPT", "a Delta table does not keep row order; this sort "
                      "only changes file layout", "info", node.label)
        return [f"{var} = {src.var}.orderBy({code})"]

    def _setop(self, node, ins, what: str):
        if len(ins) < 2:
            raise NeedsFallback(f"{what} with {len(ins)} input(s)")
        op = node.operator
        by_name = (op.get("unionType") or op.get("minusType") or op.get("intersectType")
                   or "NAME").upper() == "NAME"
        is_all = bool(op.get("isAll"))
        var = self._var(node.label)
        first = ins[0]
        lines = []
        acc = first.var
        for other in ins[1:]:
            rhs = f"{other.var}.select(*{acc}.columns)" if by_name and what != "union" else other.var
            if what == "union":
                expr = f"{acc}.unionByName({other.var})" if by_name else f"{acc}.union({other.var})"
            elif what == "minus":
                expr = f"{acc}.exceptAll({rhs})" if is_all else f"{acc}.subtract({rhs})"
            else:
                expr = f"{acc}.intersectAll({rhs})" if is_all else f"{acc}.intersect({rhs})"
            lines.append(f"{var} = {expr}")
            acc = var
        if what == "union" and not is_all:
            lines.append(f"{var} = {var}.distinct()")
        self._out(node, var, first.columns)
        return lines

    def _op_UNION_OPERATOR(self, node, ins):
        return self._setop(node, ins, "union")

    def _op_MINUS_OPERATOR(self, node, ins):
        if len(ins) != 2:
            raise NeedsFallback(f"minus with {len(ins)} inputs (OCI-DI allows exactly 2)")
        return self._setop(node, ins, "minus")

    def _op_INTERSECT_OPERATOR(self, node, ins):
        return self._setop(node, ins, "intersect")

    def _op_SPLIT_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        scope = self._scope_single(node, ins)
        strategy = (node.operator.get("dataRoutingStrategy") or "FIRST").upper()
        ports = node.data_out_ports()
        cond_ports = [p for p in ports if p.get("splitCondition")]
        other = [p for p in ports if not p.get("splitCondition")]
        if not cond_ports:
            raise NeedsFallback("split operator with no split conditions in the JSON")
        lines, conds = [], []
        base = py_ident(node.label)
        for i, port in enumerate(cond_ports):
            c = f"_c_{base}_{i}"
            lines.append(f"{c} = F.coalesce({self._expr(expr_text(port['splitCondition']), scope)}, "
                         f"F.lit(False))")
            conds.append(c)
        for i, port in enumerate(cond_ports):
            var = self._var(node.label, port.get("name") or f"out{i}")
            if strategy == "FIRST" and i > 0:
                prior = " | ".join(conds[:i])
                lines.append(f"{var} = {src.var}.filter({conds[i]} & ~({prior}))")
            else:
                lines.append(f"{var} = {src.var}.filter({conds[i]})")
            self._out(node, var, src.columns, port)
        for port in other:
            var = self._var(node.label, port.get("name") or "unmatched")
            lines.append(f"{var} = {src.var}.filter(~({' | '.join(conds)}))")
            self._out(node, var, src.columns, port)
        return lines

    def _pair_projection(self, node, left: Frame, right: Frame, joined: str, var: str,
                         extra_right: tuple = ()):
        """Columns after a join: l's, then r's that l does not have."""
        out_names = [f.name for p in node.data_out_ports() for f in port_fields(p) if f.name]
        if left.columns is None or right.columns is None:
            self.note("JN02_COLUMNS_UNKNOWN", "input columns are not listed in the JSON; both "
                      "sides are kept, and a column present on both sides is ambiguous "
                      "downstream", "review", node.label)
            keep = ['"l.*"', '"r.*"'] + [f'F.col("r.{c}")' for c in extra_right]
            return [f"{var} = {joined}.select({', '.join(keep)})"], None
        lcols = {c.upper(): c for c in left.columns}
        rcols = {c.upper(): c for c in right.columns}
        if out_names:
            exprs, used = [], set()
            for n in out_names:
                u = n.upper()
                if u in lcols and ("l", u) not in used:
                    exprs.append(f"F.col({pylit('l.`' + lcols[u] + '`')}).alias({pylit(n)})")
                    used.add(("l", u))
                elif u in rcols and ("r", u) not in used:
                    exprs.append(f"F.col({pylit('r.`' + rcols[u] + '`')}).alias({pylit(n)})")
                    used.add(("r", u))
                else:
                    m = re.fullmatch(r"(.+?)_?(\d+)", n)
                    if m and m.group(1).upper() in rcols and ("r", m.group(1).upper()) not in used:
                        exprs.append(f"F.col({pylit('r.`' + rcols[m.group(1).upper()] + '`')})"
                                     f".alias({pylit(n)})")
                        used.add(("r", m.group(1).upper()))
                    else:
                        raise NeedsFallback(f"output field {n!r} does not come from either "
                                            f"join input")
            exprs += [f'F.col("r.{c}")' for c in extra_right]
            return [f"{var} = {joined}.select({', '.join(exprs)})"], out_names
        dup = [c for u, c in rcols.items() if u in lcols]
        if dup:
            self.note("JN03_DUPLICATE_COLUMNS", f"columns on both join inputs keep the left "
                      f"side's value: {', '.join(dup)}", "review", node.label)
        exprs = [f"F.col({pylit('l.`' + c + '`')})" for c in left.columns]
        exprs += [f"F.col({pylit('r.`' + c + '`')})" for u, c in rcols.items() if u not in lcols]
        exprs += [f'F.col("r.{c}")' for c in extra_right]
        cols = list(left.columns) + [c for u, c in rcols.items() if u not in lcols]
        return [f"{var} = {joined}.select({', '.join(exprs)})"], cols

    def _op_JOINER_OPERATOR(self, node, ins):
        if len(ins) != 2:
            raise NeedsFallback(f"join with {len(ins)} inputs")
        left, right = ins
        how = _JOIN_HOW.get((node.operator.get("joinType") or "INNER").upper())
        if how is None:
            raise NeedsFallback(f"join type {node.operator.get('joinType')} is not known")
        cond = self._expr(expr_text(node.operator.get("joinCondition")),
                          self._scope_pair(node, left, right))
        var = self._var(node.label)
        joined = f'{left.var}.alias("l").join({right.var}.alias("r"), {cond}, {pylit(how)})'
        lines, cols = self._pair_projection(node, left, right, joined, var)
        self._out(node, var, cols)
        return lines

    def _op_LOOKUP_OPERATOR(self, node, ins):
        if len(ins) != 2:
            raise NeedsFallback(f"lookup with {len(ins)} inputs")
        primary, lookup = ins
        op = node.operator
        cond = self._expr(expr_text(op.get("lookupCondition")),
                          self._scope_pair(node, primary, lookup))
        strategy = (op.get("multiMatchStrategy") or "RETURN_ANY").upper()
        how = "inner" if op.get("isSkipNoMatch") else "left"
        base = py_ident(node.label)
        var = self._var(node.label)
        lines = [f'_lk_{base} = {lookup.var}.withColumn("_ocidi_lk", F.lit(True))']
        left_var = primary.var
        dedupe = strategy in ("RETURN_ANY", "RETURN_FIRST", "RETURN_LAST", "RETURN_ERROR")
        if dedupe:
            lines.append(f'_pk_{base} = {primary.var}.withColumn("_ocidi_row", '
                         f'F.monotonically_increasing_id())')
            left_var = f"_pk_{base}"
        joined = f'{left_var}.alias("l").join(_lk_{base}.alias("r"), {cond}, {pylit(how)})'
        lframe = Frame(left_var, (primary.columns + ["_ocidi_row"]) if (dedupe and primary.columns
                                                                         is not None) else
                       primary.columns, primary.node)
        proj, cols = self._pair_projection(node, lframe, lookup, joined, f"_j_{base}",
                                           extra_right=("_ocidi_lk",))
        lines += proj
        if dedupe and lframe.columns is None:
            lines.append(f'_j_{base} = _j_{base}.withColumn("_ocidi_row", F.col("_ocidi_row"))')
        if strategy == "RETURN_ERROR":
            lines += [f'if _j_{base}.groupBy("_ocidi_row").count().where("count > 1").limit(1).count():',
                      f'    raise ValueError("lookup {node.label}: more than one lookup row '
                      f'matched (OCI-DI multi-match = RETURN_ERROR)")']
        if dedupe:
            if strategy in ("RETURN_FIRST", "RETURN_LAST"):
                self.note("LK02_FIRST_LAST", f"{strategy}: Spark has no lookup-source row order; "
                          f"one matching row is kept, not necessarily OCI-DI's first/last",
                          "review", node.label)
            lines += [f'_w_{base} = Window.partitionBy("_ocidi_row").orderBy(F.lit(1))',
                      f'_j_{base} = (_j_{base}.withColumn("_ocidi_rn", F.row_number().over(_w_{base}))'
                      f'.where("_ocidi_rn = 1").drop("_ocidi_rn", "_ocidi_row"))']
        fills = op.get("nullFillValues") or {}
        for col, value in fills.items():
            lines.append(f'_j_{base} = _j_{base}.withColumn({pylit(col)}, F.when(F.col("_ocidi_lk")'
                         f'.isNull(), F.lit({pyval(param_value(value))})).otherwise('
                         f'F.col({pylit(col)})))')
        lines.append(f'{var} = _j_{base}.drop("_ocidi_lk")')
        out_cols = [c for c in (cols or []) if c not in ("_ocidi_row",)] if cols else None
        self._out(node, var, out_cols)
        return lines

    def _op_PIVOT_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        op = node.operator
        keys = rule_names(op.get("groupByColumns")) or rule_names(op.get("materializedGroupByColumns"))
        pk = op.get("pivotKeys") or {}
        axis = pk.get("pivotAxis") or []
        kv = pk.get("pivotKeyValueMap") or {}
        if len(axis) != 1 or not kv:
            raise NeedsFallback("pivot needs exactly one pivot column and an explicit value list")
        values = []
        for v in kv.values():
            values += v if isinstance(v, list) else [v]
        scope = self._scope_single(node, ins)
        aggs = [f for p in node.out_ports for f in port_fields(p) if f.expr]
        if not aggs:
            raise NeedsFallback("pivot with no aggregate expression in the JSON")
        var = self._var(node.label)
        agg_code = ", ".join(f"{self._expr(f.expr, scope)}.alias({pylit(f.name)})" for f in aggs)
        self.note("PV01_NAMES", "pivoted column names follow Spark's <value>[_<aggregate>] "
                  "convention, not OCI-DI's column-name pattern", "review", node.label)
        self._out(node, var, None)
        return [f"{var} = {src.var}.groupBy({', '.join(pylit(k) for k in keys)})"
                f".pivot({pylit(axis[0])}, {json.dumps(values)}).agg({agg_code})"]

    def _op_FLATTEN_OPERATOR(self, node, ins):
        src = self._single(node, ins)
        fd = node.operator.get("flattenDetails") or {}
        path = fd.get("flattenAttributePath") or next(iter(rule_names(node.operator.get(
            "flattenField"))), None)
        if not path:
            raise NeedsFallback("flatten operator with no attribute path in the JSON")
        prefs = fd.get("flattenProjectionPreferences") or {}
        if any(str(v).upper() in ("ALLOW", "TRUE") for v in prefs.values()):
            self.note("FL01_PREFERENCES", f"flatten projection preferences {prefs} are not "
                      f"applied", "review", node.label)
        var = self._var(node.label)
        base = py_ident(node.label)
        self._out(node, var, None)
        leaf = path.split(".")[-1]
        return [
            "from pyspark.sql.types import ArrayType, StructType",
            f'_f_{base} = {src.var}.withColumn("_ocidi_flat", F.explode_outer(F.col({pylit(path)})))',
            f'if isinstance(_f_{base}.schema["_ocidi_flat"].dataType, StructType):',
            f'    {var} = _f_{base}.select(*[F.col("`" + c + "`") for c in _f_{base}.columns '
            f'if c != "_ocidi_flat"], "_ocidi_flat.*")',
            "else:",
            f'    {var} = _f_{base}.withColumnRenamed("_ocidi_flat", {pylit(leaf + "_value")})',
        ]

    def _op_FUNCTION_OPERATOR(self, node, ins):
        fn = node.operator.get("ociFunction") or {}
        raise NeedsFallback(f"OCI Function {fn.get('functionId') or '(unknown id)'} is called per "
                            f"row batch; its code is not part of the OCI-DI export", "oci_function")

    # ------------------------------------------------------------ fallback
    def _fallback(self, node: GNode, reason: str, kind: str) -> list:
        fid = "FB_" + hashlib.sha1(f"{self.key}:{node.key}".encode()).hexdigest()[:10]
        manual = kind == "manual"
        self.note("FB01_" + ("MANUAL" if manual else "LLM"), reason,
                  "manual" if manual else "fallback", node.label)
        ins = []
        try:
            ins = [self._frame_for(e) for e in self.graph.inputs(node.key)]
        except NeedsFallback:
            pass
        ports = node.data_out_ports() if node.op_type != "TARGET_OPERATOR" else []
        inputs_code = ", ".join(f'"in{i}": {f.var}' for i, f in enumerate(ins))
        fn = f"_fb_{fid}"
        srcs = [p for p, a in self.source_params.items() if a.kind != "none"]
        params_code = ("{**PARAMS, \"__sources__\": {" + ", ".join(f"{pylit(p)}: {p}" for p in srcs)
                       + "}}") if srcs else "PARAMS"
        doc = (f"Work order fallback/{fid}.json ({kind}). Inputs: "
               + (", ".join(f"in{i} <- {self.graph.nodes[f.node].label}" for i, f in enumerate(ins))
                  or "none")
               + ". Returns " + ("a dict of port name -> DataFrame" if len(ports) > 1 else
                                 "a DataFrame" if ports else "None (writes its own target)"))
        message = (f"{fid}: {node.label} ({node.op_type}) was not converted -- {reason}. "
                   + ("Rebuild it by hand; see REVIEW.md." if manual else
                      "Fill it with the ocidi-fallback skill or `ocidi2aidp fallback`."))
        lines = [f"# REVIEW REQUIRED [{fid}]: {reason}",
                 f"# >>> {fid}",
                 f"def {fn}(spark, inputs, params):",
                 f"    {pylit(doc)}",
                 f"    raise NotImplementedError({pylit(message)})",
                 f"# <<< {fid}",
                 f"_r_{fid} = {fn}(spark, {{{inputs_code}}}, {params_code})"]
        expected = []
        for i, port in enumerate(ports):
            pname = port.get("name") or f"out{i}"
            var = self._var(node.label, pname if len(ports) > 1 else "")
            # A port's field list is not reliably the whole output (an expression or
            # function operator lists only what it adds), so downstream treats the
            # stub's columns as unknown -- except an expression operator over a known
            # input, whose output is that input plus the listed fields.
            cols = None
            if node.op_type == "EXPRESSION_OPERATOR" and len(ins) == 1 and ins[0].columns:
                added = [f.name for f in port_fields(port) if f.name]
                cols = ins[0].columns + [c for c in added if c.upper() not in
                                         {x.upper() for x in ins[0].columns}]
            lines.append(f"{var} = _r_{fid}[{pylit(pname)}]" if len(ports) > 1 else
                         f"{var} = _r_{fid}")
            self._out(node, var, cols, port if len(ports) > 1 else None)
            expected.append({"port": pname, "variable": var,
                             "columns": [{"name": f.name, "type": f.type}
                                         for f in port_fields(port) if f.name]})
        if not manual:
            self.res.fallbacks.append({
                "id": fid, "kind": kind, "reason": reason, "function": fn,
                "object": {"name": self.name, "key": self.key, "kind": self.kind},
                "notebook": self.notebook_path,
                "node": {"label": node.label, "key": node.key, "op_type": node.op_type},
                "inputs": [{"name": f"in{i}", "from": self.graph.nodes[f.node].label,
                            "variable": f.var, "columns": f.columns} for i, f in enumerate(ins)],
                "outputs": expected,
                "parameters": {p.name: {"type": p.type, "default": p.default}
                               for p in self.res.params.values()},
                "di_json": node.raw,
            })
        return lines

    # ------------------------------------------------------------ assemble
    def _assemble(self) -> None:
        nb = self.res.notebook
        nb.meta.update({"source_kind": self.kind, "source_key": self.key, "source_name": self.name})
        n_fb = len([f for f in self.res.findings if f.severity == "fallback"])
        n_manual = len([f for f in self.res.findings if f.severity == "manual"])
        status = ("**Contains unconverted operators** -- it raises at the first one until they "
                  "are filled in." if (n_fb or n_manual) else "All operators converted.")
        nb.md(f"# {self.name}\n\nMigrated from OCI Data Integration {self.kind} `{self.name}` by "
              f"ocidi2aidp. {status}\n\nTargets are managed Delta tables in the catalog named by "
              f"`TARGET_CATALOG`. Read `REVIEW.md` in the migration output before running this "
              f"against production data.")
        nb.code(RUNTIME, tags=["ocidi2aidp-runtime"])
        nb.code(self._param_cell(), tags=["parameters"])
        for node, lines in self.cells:
            header = f"# [{node.label}] {node.op_type or 'UNKNOWN'}"
            meta = {"ocidi2aidp": {"node": node.label, "op_type": node.op_type}}
            fid = next((ln.split("[")[1].split("]")[0] for ln in lines
                        if ln.startswith("# REVIEW REQUIRED [")), None)
            if fid:
                meta["ocidi2aidp"]["fallback"] = fid
            nb.code("\n".join([header] + lines), **meta)
        if self.res.watermarks or "SYS_LAST_LOAD_DATE" in self.used_params:
            nb.code("# Commit watermarks -- only reached when every target above wrote.\n"
                    + ("_WM_PENDING.append((\"SYS.LAST_LOAD_DATE\", \"SYS.LAST_LOAD_DATE\", "
                       "_RUN_STARTED))\n" if "SYS_LAST_LOAD_DATE" in self.used_params else "")
                    + "_wm_commit(WM_TASK)")
        nb.code("for _t, _m in _WRITTEN:\n    print(f\"wrote {_t} ({_m})\")")

    def _param_cell(self) -> str:
        cfg = self.config
        lines = ["# Parameters: an AIDP job task parameter with the same name overrides each default.",
                 f'TARGET_CATALOG = _aidp_parameter("TARGET_CATALOG", {pylit(cfg.target.catalog)})',
                 f'CONTROL_SCHEMA = _aidp_parameter("CONTROL_SCHEMA", '
                 f'{pylit(cfg.target.control_schema)})',
                 f"WM_TASK = {pylit(self.wm_task)}"]
        for p, access in sorted(self.source_params.items()):
            if access.kind == "none":
                continue
            lines.append(f"{p} = _aidp_parameter({pylit(p)}, {pylit(access.default)})"
                         f"  # {access.data_asset} ({access.kind})")
        lines.append("PARAMS = {")
        for spec in self.res.params.values():
            default = spec.default
            if isinstance(default, bool):
                default = str(default).lower()
            dcode = "None" if default is None or isinstance(default, (dict, list)) else \
                pylit(str(default))
            lines.append(f"    {pylit(spec.name)}: _coerce(_aidp_parameter({pylit(spec.name)}, "
                         f"{dcode}), {pylit(spec.type)}),")
        lines.append("}")
        sys_lines = {
            "SYS_TASK_START_TIME": "_RUN_STARTED",
            "SYS_TASK_RUN_KEY": 'str(_aidp_parameter("SYS_TASK_RUN_KEY", str(_uuid.uuid4())))',
            "SYS_TASK_RUN_NAME": f'str(_aidp_parameter("SYS_TASK_RUN_NAME", {pylit(self.name)}))',
            "SYS_TASK_SCHEDULE_TRIGGER_TIME": "_RUN_STARTED",
            "SYS_TASK_SCHEDULE_TIMEZONE": '_aidp_parameter("SYS_TASK_SCHEDULE_TIMEZONE", "UTC")',
            "SYS_RETRY_ATTEMPT": "0",
            "SYS_LAST_LOAD_DATE": '(_wm_get(WM_TASK, "SYS.LAST_LOAD_DATE") or '
                                  '_dt.datetime(1970, 1, 1))',
        }
        for name in sorted(n for n in self.used_params if n in sys_lines):
            lines.append(f"PARAMS[{pylit(name)}] = {sys_lines[name]}")
        return "\n".join(lines)


def _di_pattern(from_pattern: str, to_pattern: str, is_regex: bool) -> tuple:
    """OCI-DI map-by-pattern -> (Python regex, replacement).

    ``*NAME`` -> ``TGT_$1``: ``*`` and ``?`` are wildcards whose matches are
    numbered groups. Matching is case-insensitive unless the pattern starts
    with ``(?c)``.
    """
    case_sensitive = from_pattern.startswith("(?c)")
    body = from_pattern[4:] if case_sensitive else from_pattern
    if not is_regex:
        body = "".join("(.*)" if ch == "*" else "(.)" if ch == "?" else re.escape(ch)
                       for ch in body)
    repl = re.sub(r"\$(\d+)", r"\\\1", to_pattern)
    return ("" if case_sensitive else "(?i)") + body, repl
