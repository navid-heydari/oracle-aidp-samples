"""Notebooks for the OCI-DI task types that are not data flows.

* ``REST_TASK`` -- compiled when it is a plain call (no auth, or basic auth
  from a credential): ``urllib`` from the standard library, so the cluster
  needs nothing installed. Polling, cancel endpoints and OCI resource-principal
  signing become an LLM work order: their semantics are in config the
  compiler can quote but not safely re-implement.
* ``SQL_TASK`` -- runs a stored procedure or SQL script *in the source
  database*. That database stays where it is; the notebook must call it over
  JDBC with an AIDP credential. Always a work order: the call shape depends
  on the driver the cluster has.
* ``OCI_DATAFLOW_TASK`` -- runs an OCI Data Flow application whose code lives
  in Object Storage, outside OCI-DI. ``manual``: the code has to be fetched
  and re-hosted, and nothing in the DI export contains it.
"""
from __future__ import annotations

import hashlib
import json

from ..di import config_values, expr_text, param_value
from ..notebook import Notebook
from ..report import Finding
from . import sources as srcmod
from .dataflow import pylit
from .runtime import RUNTIME


class TaskNotebook:
    def __init__(self, task: dict, *, snapshot, config, notebook_path: str):
        self.task = task
        self.snapshot = snapshot
        self.config = config
        self.notebook_path = notebook_path
        self.name = task.get("name") or task.get("identifier") or task.get("key")
        self.kind = task.get("modelType", "")
        self.findings: list = []
        self.fallbacks: list = []
        self.nb = Notebook(self.name)

    def note(self, code, message, severity="review"):
        self.findings.append(Finding(code, message, severity, self.name))

    def _params(self) -> dict:
        out = {}
        for p in self.task.get("parameters") or []:
            if p.get("name"):
                out[p["name"]] = p.get("defaultValue")
        for name, value in ((self.task.get("configProviderDelegate") or {}).get("bindings")
                            or {}).items():
            out[name] = (value or {}).get("simpleValue")
        return out

    def _header(self, what: str) -> None:
        self.nb.meta.update({"source_kind": self.kind, "source_key": self.task.get("key"),
                             "source_name": self.name})
        self.nb.md(f"# {self.name}\n\nMigrated from OCI Data Integration {self.kind} "
                   f"`{self.name}` by ocidi2aidp. {what}")
        self.nb.code(RUNTIME, tags=["ocidi2aidp-runtime"])
        lines = ["# Parameters: an AIDP job task parameter with the same name overrides each default.",
                 f'TARGET_CATALOG = _aidp_parameter("TARGET_CATALOG", {pylit(self.config.target.catalog)})',
                 f'CONTROL_SCHEMA = _aidp_parameter("CONTROL_SCHEMA", '
                 f'{pylit(self.config.target.control_schema)})',
                 "PARAMS = {"]
        for name, default in self._params().items():
            d = "None" if default is None else pylit(str(default))
            lines.append(f"    {pylit(name)}: _aidp_parameter({pylit(name)}, {d}),")
        lines.append("}")
        self.nb.code("\n".join(lines), tags=["parameters"])

    def _stub(self, reason: str, kind: str, context: dict) -> None:
        fid = "FB_" + hashlib.sha1(f"{self.task.get('key')}:task".encode()).hexdigest()[:10]
        manual = kind == "manual"
        self.note("FB01_" + ("MANUAL" if manual else "LLM"), reason,
                  "manual" if manual else "fallback")
        message = (f"{fid}: {self.name} ({self.kind}) was not converted -- {reason}. "
                   + ("Rebuild it by hand; see REVIEW.md." if manual else
                      "Fill it with the ocidi-fallback skill or `ocidi2aidp fallback`."))
        self.nb.code("\n".join([
            f"# REVIEW REQUIRED [{fid}]: {reason}",
            f"# >>> {fid}",
            f"def _fb_{fid}(spark, inputs, params):",
            f"    {pylit('Work order fallback/' + fid + '.json (' + kind + '). Returns None.')}",
            f"    raise NotImplementedError({pylit(message)})",
            f"# <<< {fid}",
            f"_fb_{fid}(spark, {{}}, PARAMS)"]),
            ocidi2aidp={"node": self.name, "op_type": self.kind, "fallback": fid})
        if not manual:
            self.fallbacks.append({
                "id": fid, "kind": kind, "reason": reason, "function": f"_fb_{fid}",
                "object": {"name": self.name, "key": self.task.get("key"), "kind": self.kind},
                "notebook": self.notebook_path,
                "node": {"label": self.name, "key": self.task.get("key"), "op_type": self.kind},
                "inputs": [], "outputs": [],
                "parameters": {k: {"type": "STRING", "default": v} for k, v in self._params().items()},
                "context": context, "di_json": self.task,
            })

    # ------------------------------------------------------------------
    def build(self) -> Notebook:
        handler = {"REST_TASK": self._rest, "SQL_TASK": self._sql,
                   "OCI_DATAFLOW_TASK": self._ocidf}.get(self.kind)
        if handler is None:
            self._header("")
            self._stub(f"task type {self.kind} is not supported", "manual", {})
        else:
            handler()
        return self.nb

    def _rest(self) -> None:
        t = self.task
        auth = ((t.get("authDetails") or {}).get("modelType") or "NO_AUTH").upper()
        method = (t.get("methodType") or "GET").upper()
        endpoint = expr_text(t.get("endpoint")) or ""
        body = t.get("jsonData") or ""
        headers = t.get("headers") if isinstance(t.get("headers"), dict) else {}
        self._header("Calls the REST endpoint the OCI-DI task called.")
        complex_bits = [k for k in ("pollRestCallConfig", "cancelEndpoint", "cancelRestCallConfig")
                        if t.get(k)]
        if (t.get("apiCallMode") or "SYNCHRONOUS").upper() != "SYNCHRONOUS":
            complex_bits.append("apiCallMode=" + str(t.get("apiCallMode")))
        if "NO_AUTH" not in auth or complex_bits or not endpoint:
            why = []
            if "NO_AUTH" not in auth:
                why.append(f"authentication {auth}")
            if complex_bits:
                why.append("asynchronous polling/cancel configuration: " + ", ".join(complex_bits))
            if not endpoint:
                why.append("no endpoint in the JSON")
            self._stub("REST task with " + "; ".join(why), "rest_task",
                       {"method": method, "endpoint": endpoint, "headers": headers,
                        "json_body": body, "auth": auth})
            return
        if (t.get("executeRestCallConfig") or {}).get("configValues"):
            self.note("RT02_SUCCESS_CONDITION", "the task's execute-call config (e.g. a success "
                      "condition) is not applied; any HTTP status below 400 counts as success")
        self.nb.code("\n".join([
            f"# [{self.name}] REST_TASK",
            "import urllib.request as _ur",
            "",
            "def _subst(text):",
            "    return _re.sub(r\"\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}\", "
            "lambda m: str(PARAMS[m.group(1)]), text)",
            "",
            f"_url = _subst({pylit(endpoint)})",
            f"_body = _subst({pylit(body)}).encode('utf-8') if {pylit(body)} else None",
            f"_req = _ur.Request(_url, data=_body, method={pylit(method)}, "
            f"headers={json.dumps(headers)})",
            "with _ur.urlopen(_req, timeout=300) as _resp:",
            "    _status = _resp.status",
            "    _text = _resp.read(512 * 1024).decode('utf-8', 'replace')  # OCI-DI's 512K cap",
            "if _status >= 400:",
            f"    raise RuntimeError(f\"{self.name}: HTTP {{_status}}: {{_text[:500]}}\")",
            "print(f\"HTTP {_status}\")",
        ]), ocidi2aidp={"node": self.name, "op_type": self.kind})
        self.note("RT01_EGRESS", f"the cluster must be able to reach {endpoint.split('/')[2] if '//' in endpoint else endpoint}", "review")

    def _sql(self) -> None:
        t = self.task
        script = t.get("script") or {}
        kind = (t.get("sqlScriptType") or "STORED_PROCEDURE").upper()
        target = script.get("name") or script.get("key") or "(unnamed)"
        asset = None
        for value in config_values(t).values():
            v = param_value(value)
            if isinstance(v, dict) and str(v.get("modelType", "")).endswith("_DATA_ASSET"):
                asset = self.snapshot.data_asset_for(v.get("key")) or self.snapshot.data_asset_for(
                    v.get("name")) or v
        access = srcmod.resolve(asset, self.config.sources) if asset else None
        self._header("Runs the stored procedure / SQL script in the source database, where it "
                     "still lives.")
        self._stub(f"SQL task: {kind.lower().replace('_', ' ')} {target} runs inside "
                   f"{(asset or {}).get('name', 'the source database')}; it must be called over "
                   f"JDBC with an AIDP credential", "sql_task",
                   {"script_type": kind, "script": target, "script_text": script.get("text"),
                    "data_asset": (asset or {}).get("name"),
                    "data_asset_type": (asset or {}).get("modelType"),
                    "credential_hint": access.default if access else "",
                    "operation": t.get("operation")})

    def _ocidf(self) -> None:
        app = (self.task.get("dataflowApplication") or {}).get("applicationId") or "(unknown)"
        self._header("Placeholder for an OCI Data Flow application.")
        self._stub(f"OCI Data Flow application {app}: its Spark code lives in Object Storage, "
                   f"not in OCI-DI. Fetch it (`oci data-flow application get --application-id "
                   f"{app}`), then run it here or as an AIDP python task", "manual", {})
