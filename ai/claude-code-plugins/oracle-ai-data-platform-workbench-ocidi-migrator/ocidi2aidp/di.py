"""Read OCI-DI JSON without the typed SDK.

The OCI Python SDK deserialises by ``modelType`` and silently drops fields of
any model it does not know (the Table Function operator has no model class in
2.165.1), so everything here reads raw ``dict``s as the REST API returns them
-- camelCase keys, as in the SDK ``attribute_map``s.

Where the SDK models leave a location ambiguous (which config parameter carries
a source's data asset, for instance) the readers below look in every place the
models allow and say so in a docstring. Tighten them once a real export has
been seen; see references/known-limitations.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional


def mt(obj: Any) -> str:
    return (obj or {}).get("modelType", "") if isinstance(obj, dict) else ""


def expr_text(expr: Any) -> Optional[str]:
    """An ``Expression`` ({exprString, configValues}) or a bare string -> text."""
    if expr is None:
        return None
    if isinstance(expr, str):
        return expr
    if isinstance(expr, dict):
        for key in ("exprString", "expression", "expr"):
            value = expr.get(key)
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                return expr_text(value)
    return None


def config_values(obj: Any) -> dict:
    """``opConfigValues.configParamValues`` / ``configValues.configParamValues``."""
    if not isinstance(obj, dict):
        return {}
    for key in ("opConfigValues", "configValues"):
        cv = obj.get(key)
        if isinstance(cv, dict) and isinstance(cv.get("configParamValues"), dict):
            return cv["configParamValues"]
    if isinstance(obj.get("configParamValues"), dict):
        return obj["configParamValues"]
    return {}


def param_value(cpv: Any) -> Any:
    """A ``ConfigParameterValue`` -> its single populated value."""
    if not isinstance(cpv, dict):
        return cpv
    for key in ("stringValue", "intValue", "objectValue", "refValue", "parameterValue",
                "rootObjectValue"):
        if cpv.get(key) is not None:
            return cpv[key]
    return None


def type_name(t: Any) -> str:
    """A field type -> its DI type name ("VARCHAR", "NUMBER", ...), best effort.

    ``DerivedField.type`` is a string; ``ShapeField.type`` is an object that may
    be a ``DataType`` ({dtType, name}), a ``ConfiguredType`` ({wrappedType,
    configValues: length/precision/scale}), a ``JavaType`` or a reference.
    """
    if t is None:
        return ""
    if isinstance(t, str):
        return t.split(":")[-1].split("/")[-1].upper()
    if isinstance(t, dict):
        if mt(t) == "CONFIGURED_TYPE":
            inner = type_name(t.get("wrappedType"))
            cv = config_values(t)
            args = [param_value(cv[k]) for k in ("length", "precision", "scale") if k in cv]
            args = [str(a) for a in args if a not in (None, "")]
            return f"{inner}({','.join(args)})" if args and inner else inner
        for key in ("dtType", "name", "javaTypeName", "typeSystemName"):
            if isinstance(t.get(key), str) and t[key]:
                return t[key].split(":")[-1].split("/")[-1].upper()
        if isinstance(t.get("key"), str):
            return t["key"].split(":")[-1].split("/")[-1].upper()
    return ""


@dataclass
class Field:
    name: str
    type: str = ""
    model_type: str = ""
    key: str = ""
    expr: Optional[str] = None          # DERIVED_FIELD / MACRO_FIELD expression
    raw: dict = field(default_factory=dict)


def port_fields(port: Any) -> list[Field]:
    out = []
    for f in (port or {}).get("fields") or []:
        if not isinstance(f, dict):
            continue
        out.append(Field(name=f.get("name") or "", type=type_name(f.get("type")),
                         model_type=mt(f), key=f.get("key") or "",
                         expr=expr_text(f.get("expr")), raw=f))
    return out


def rule_names(obj: Any) -> list[str]:
    """Field names named by a projection-rule tree.

    Used for ``Aggregator.groupByColumns`` (a DynamicProxyField whose type's
    handler is a RuleTypeConfig of projection rules), ``materializedGroupByColumns``
    (a MaterializedDynamicField whose type lists elements), ``SortKeyRule.
    wrappedRule`` (a NameListRule), and similar. Returns ``[]`` when the tree
    selects by pattern only -- a caller must treat that as unresolved, not empty.
    """
    names: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        kind = mt(node)
        if kind in ("NAME_LIST_RULE", "TYPED_NAME_PATTERN_RULE") and node.get("names"):
            if (node.get("ruleType") or "INCLUDE").upper() != "EXCLUDE":
                names.extend(n for n in node["names"] if isinstance(n, str))
            return
        if kind in ("SHAPE_FIELD", "PROXY_FIELD", "INPUT_FIELD", "OUTPUT_FIELD") and node.get("name"):
            names.append(node["name"])
            return
        for key in ("type", "typeHandler", "projectionRules", "wrappedRule", "elements",
                    "rules", "fields"):
            if key in node:
                walk(node[key])

    walk(obj)
    seen, ordered = set(), []
    for n in names:
        if n.upper() not in seen:
            seen.add(n.upper())
            ordered.append(n)
    return ordered


def has_pattern_rules(obj: Any) -> bool:
    """True if a projection-rule tree selects anything by pattern."""
    found = False

    def walk(node: Any) -> None:
        nonlocal found
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            if node.get("pattern") and mt(node) in ("TYPED_NAME_PATTERN_RULE",
                                                    "GROUPED_NAME_PATTERN_RULE"):
                found = True
            for value in node.values():
                if isinstance(value, (dict, list)):
                    walk(value)

    walk(obj)
    return found


@dataclass
class Ref:
    model_type: str
    key: str
    name: str
    raw: dict


def _refs(obj: Any) -> Iterable[Ref]:
    """Every object-reference in a config-values map, with its modelType."""
    for value in config_values(obj).values():
        v = param_value(value)
        if isinstance(v, dict) and mt(v):
            yield Ref(mt(v), v.get("key") or "", v.get("name") or v.get("identifier") or "", v)


def parameter_bindings(obj: Any) -> dict:
    """Config parameters that are bound to a DI parameter (``parameterValue``)."""
    out = {}
    for name, value in config_values(obj).items():
        if isinstance(value, dict) and isinstance(value.get("parameterValue"), str):
            out[name] = value["parameterValue"]
    return out


@dataclass
class EntityRef:
    """Where a source/target operator reads or writes, as far as the JSON says."""
    model_type: str = ""                 # TABLE_ENTITY | VIEW_ENTITY | FILE_ENTITY | SQL_ENTITY | ...
    name: str = ""
    resource_name: str = ""
    sql: Optional[str] = None
    data_format: str = ""
    format_options: dict = field(default_factory=dict)
    shape: list = field(default_factory=list)          # list[Field]
    unique_keys: list = field(default_factory=list)    # list[list[str]]
    data_asset: Optional[Ref] = None
    connection: Optional[Ref] = None
    schema: Optional[Ref] = None
    parameterized: dict = field(default_factory=dict)  # config name -> DI parameter name


def _shape_fields(entity: dict) -> list[Field]:
    shape = entity.get("shape") or {}
    t = shape.get("type") if isinstance(shape, dict) else None
    elements = []
    if isinstance(t, dict):
        elements = t.get("elements") or t.get("attributes") or []
    out = []
    for e in elements:
        if isinstance(e, dict) and e.get("name"):
            out.append(Field(name=e["name"], type=type_name(e.get("type")), model_type=mt(e),
                             key=e.get("key") or "", raw=e))
    return out


def _unique_keys(entity: dict) -> list[list[str]]:
    keys = []
    for uk in entity.get("uniqueKeys") or []:
        cols = []
        for attr in sorted(uk.get("attributeRefs") or [], key=lambda a: a.get("position") or 0):
            sf = attr.get("attribute") or attr.get("shapeField") or {}
            if sf.get("name"):
                cols.append(sf["name"])
        if cols:
            keys.append(cols)
    return keys


def _format(fmt: Any) -> tuple[str, dict]:
    if not isinstance(fmt, dict):
        return "", {}
    kind = (fmt.get("type") or "").upper()
    attr = fmt.get("formatAttribute") or {}
    options = {}
    if kind == "CSV":
        for src, dst in (("delimiter", "sep"), ("quoteCharacter", "quote"),
                         ("escapeCharacter", "escape"), ("encoding", "encoding"),
                         ("timestampFormat", "timestampFormat")):
            if attr.get(src) not in (None, ""):
                options[dst] = attr[src]
        if attr.get("hasHeader") is not None:
            options["header"] = bool(attr["hasHeader"])
    if kind == "JSON" and attr.get("encoding"):
        options["encoding"] = attr["encoding"]
    if (fmt.get("compressionConfig") or {}).get("codec"):
        options["compression"] = fmt["compressionConfig"]["codec"]
    return kind, options


def entity_ref(operator: dict) -> EntityRef:
    """Describe a SOURCE_OPERATOR / TARGET_OPERATOR's entity.

    The data asset, connection and schema are references inside
    ``opConfigValues.configParamValues``; the SDK does not fix the parameter
    names, so every ``refValue`` is classified by its ``modelType`` instead
    (``*_DATA_ASSET``, ``*_CONNECTION``, ``SCHEMA``). A config parameter bound to
    a DI parameter (``parameterValue``) is recorded in ``parameterized``.
    """
    entity = operator.get("entity") or {}
    ref = EntityRef(model_type=mt(entity), name=entity.get("name") or "",
                    resource_name=entity.get("resourceName") or "",
                    sql=entity.get("sqlQuery"),
                    shape=_shape_fields(entity), unique_keys=_unique_keys(entity),
                    parameterized=parameter_bindings(operator))
    fmt = entity.get("dataFormat")
    roc = operator.get("readOperationConfig") or operator.get("writeOperationConfig") or {}
    if not fmt:
        fmt = roc.get("dataFormat")
    ref.data_format, ref.format_options = _format(fmt)
    for r in _refs(operator):
        if r.model_type.endswith("_DATA_ASSET") and ref.data_asset is None:
            ref.data_asset = r
        elif r.model_type.endswith("_CONNECTION") and ref.connection is None:
            ref.connection = r
        elif r.model_type == "SCHEMA" and ref.schema is None:
            ref.schema = r
    return ref


def node_label(node: dict) -> str:
    op = node.get("operator") or {}
    return op.get("identifier") or node.get("name") or op.get("name") or node.get("key") or "?"
