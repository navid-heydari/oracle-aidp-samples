"""Static checks on an LLM-written fallback function. Nothing here executes it.

The answer is accepted only if it is exactly one function with the contracted
name and signature, imports only from an allow-list, and reaches none of the
capabilities its work-order kind does not grant (network, credentials, the
JVM, the filesystem, process control, dynamic code). An answer whose body
only raises ``NotImplementedError`` is *declined*, not invalid: the author
said the construct cannot be reproduced, which is a legitimate answer.
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

from .workorder import CAPABILITIES

_BASE_IMPORTS = {"pyspark", "datetime", "decimal", "re", "json", "math", "functools",
                 "itertools", "collections", "typing"}
_BANNED_NAMES = {"exec", "eval", "compile", "__import__", "open", "globals", "locals", "vars",
                 "breakpoint", "input", "dbutils", "oidlUtils", "getattr", "setattr", "delattr"}
_BANNED_MODULES = {"os", "sys", "subprocess", "socket", "shutil", "pathlib", "requests",
                   "http", "ftplib", "pickle", "marshal", "ctypes", "importlib", "builtins",
                   "multiprocessing", "threading", "tempfile", "glob", "io"}
_SECRET_PATTERNS = [
    (re.compile(r"jdbc:[a-z0-9]+:", re.I), "a JDBC URL literal"),
    (re.compile(r"(password|passwd|pwd|secret|token|api[_-]?key)\s*[=:]\s*['\"][^'\"]+['\"]",
                re.I), "a credential literal"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "a private key"),
    (re.compile(r"\bsk-ant-[A-Za-z0-9_-]+"), "an API key"),
]
_FENCE = re.compile(r"```(?:python|py)?\s*\n(.*?)```", re.S)


@dataclass
class Verdict:
    ok: bool
    declined: bool = False
    errors: list = field(default_factory=list)
    assumptions: list = field(default_factory=list)
    code: str = ""


def extract_code(text: str) -> str:
    """The function from a reply: the first fenced block, else the whole text."""
    m = _FENCE.search(text or "")
    return (m.group(1) if m else (text or "")).strip() + "\n"


def _attr_chain(node: ast.AST) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def validate(code: str, order: dict) -> Verdict:
    caps = CAPABILITIES.get(order.get("kind", ""), set())
    fn = order["function"]
    v = Verdict(ok=False, code=code)
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        v.errors.append(f"does not parse: {exc.msg} (line {exc.lineno})")
        return v
    defs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    others = [n for n in tree.body if not isinstance(n, (ast.FunctionDef, ast.Import,
                                                         ast.ImportFrom))
              and not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant))]
    if len(defs) != 1 or defs[0].name != fn:
        v.errors.append(f"must define exactly one top-level function named {fn}; found "
                        f"{[d.name for d in defs] or 'none'}")
    if others:
        v.errors.append("only the function (and imports) may be at the top level")
    if defs:
        args = [a.arg for a in defs[0].args.args]
        if args != ["spark", "inputs", "params"]:
            v.errors.append(f"signature must be ({fn})(spark, inputs, params); got {args}")

    allowed_modules = set(_BASE_IMPORTS)
    if "urllib" in caps:
        allowed_modules.add("urllib")
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                [node.module or ""]
            for name in names:
                root = name.split(".")[0]
                if root not in allowed_modules:
                    v.errors.append(f"import of {name!r} is not allowed")
        elif isinstance(node, ast.Name):
            if node.id in _BANNED_NAMES:
                v.errors.append(f"use of {node.id!r} is not allowed")
            if node.id in _BANNED_MODULES:
                v.errors.append(f"use of module {node.id!r} is not allowed")
            if node.id == "aidputils" and "secrets" not in caps:
                v.errors.append("aidputils (credential store) is not available to this kind")
        elif isinstance(node, ast.Attribute):
            chain = _attr_chain(node)
            if chain.startswith("aidputils.") and not chain.startswith("aidputils.secrets.get") \
                    and chain != "aidputils.secrets":
                v.errors.append(f"{chain} is not allowed (only aidputils.secrets.get)")
            if re.match(r"spark\._(jvm|sc|jsparkSession)", chain) and "jvm" not in caps:
                v.errors.append(f"{chain}: JVM access is not available to this kind")
            if re.match(r"spark\.conf\.(set|unset)", chain):
                v.errors.append(f"{chain}: changing session configuration is not allowed")
            if node.attr in ("__globals__", "__builtins__", "__subclasses__", "__code__"):
                v.errors.append(f"dunder access {node.attr!r} is not allowed")
    for pattern, what in _SECRET_PATTERNS:
        if pattern.search(code):
            v.errors.append(f"contains {what}")
    v.assumptions = [ln.strip()[len("# ASSUMPTION:"):].strip() for ln in code.splitlines()
                     if ln.strip().upper().startswith("# ASSUMPTION:")]
    if defs and not v.errors:
        body = [n for n in defs[0].body if not (isinstance(n, ast.Expr) and
                                                 isinstance(n.value, ast.Constant))]
        if len(body) == 1 and isinstance(body[0], ast.Raise):
            exc = body[0].exc
            name = exc.func.id if isinstance(exc, ast.Call) and isinstance(exc.func, ast.Name) \
                else (exc.id if isinstance(exc, ast.Name) else "")
            if name == "NotImplementedError":
                v.declined = True
    v.ok = not v.errors
    return v
