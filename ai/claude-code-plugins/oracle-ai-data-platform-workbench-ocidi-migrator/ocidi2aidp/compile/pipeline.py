"""OCI-DI pipeline -> the ``tasks`` of one AIDP job.

A pipeline is a graph of operators (OCI SDK ``Pipeline.nodes``):

* ``TASK_OPERATOR`` -- runs a task; becomes one ``NOTEBOOK_TASK``. Its
  ``triggerRule`` (ALL_SUCCESS / ALL_FAILED / ALL_COMPLETE) becomes ``runIf``
  (ALL_SUCCESS / ALL_FAILED / ALL_DONE -- the three values the sibling
  migrators created jobs with live).
* ``START_OPERATOR`` / ``END_OPERATOR`` -- structure only.
* ``MERGE_OPERATOR`` -- a join point. Tasks after it depend on every task
  before it, with ``runIf`` from the merge's ``triggerRule``. ONE_SUCCESS /
  ONE_FAILED map to AT_LEAST_ONE_SUCCESS / AT_LEAST_ONE_FAILED, which no
  sibling has created live -- reported for review.
* ``DECISION_OPERATOR`` -- a branch on a runtime condition. AIDP job ``runIf``
  cannot evaluate a condition, so the tasks behind a decision are **left out
  of the job** and reported ``manual``. Running both branches, or neither
  silently, would each be wrong.
* ``EXPRESSION_OPERATOR`` (pipeline variables) -- passed through; the
  assignment is reported.
* A ``PIPELINE_TASK`` inside a pipeline is expanded in place (its tasks are
  inlined with a key prefix), so the result is one flat job.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from ..naming import task_key, unique
from ..report import Finding
from .graph import FlowGraph, GraphError

_RUN_IF = {"ALL_SUCCESS": "ALL_SUCCESS", "ALL_FAILED": "ALL_FAILED", "ALL_COMPLETE": "ALL_DONE",
           "ONE_SUCCESS": "AT_LEAST_ONE_SUCCESS", "ONE_FAILED": "AT_LEAST_ONE_FAILED"}
_UNVERIFIED_RUN_IF = {"AT_LEAST_ONE_SUCCESS", "AT_LEAST_ONE_FAILED"}
_DELAY_MS = {"SECONDS": 1000, "MINUTES": 60_000, "HOURS": 3_600_000, "DAYS": 86_400_000}
_STRUCTURAL = {"START_OPERATOR", "END_OPERATOR"}


@dataclass
class TaskArtifact:
    """What a pipeline task operator runs, as resolved by the caller."""
    notebook_path: str = ""
    parameters: list = field(default_factory=list)      # [{"name", "value"}]
    pipeline: Optional[dict] = None                     # set for a nested PIPELINE_TASK
    name: str = ""
    error: str = ""                                      # set when it cannot be resolved


@dataclass
class PipelineResult:
    tasks: list = field(default_factory=list)
    findings: list = field(default_factory=list)
    manual: bool = False


class PipelineCompiler:
    def __init__(self, pipeline: dict, resolve: Callable[[dict], TaskArtifact], *,
                 name: str = "", depth: int = 0):
        self.pipeline = pipeline
        self.resolve = resolve
        self.name = name or pipeline.get("name") or "pipeline"
        self.depth = depth
        self.res = PipelineResult()
        self.keys: set = set()

    def note(self, code, message, severity="review", node=""):
        f = Finding(code, message, severity, node)
        if f not in self.res.findings:
            self.res.findings.append(f)

    def compile(self) -> PipelineResult:
        self._expand(self.pipeline, prefix="", depth=self.depth)
        return self.res

    # Returns (root task keys, leaf task keys) of the expanded pipeline.
    def _expand(self, pipeline: dict, prefix: str, depth: int):
        if depth > 5:
            self.note("PL09_NESTING", f"pipeline {pipeline.get('name')} nests more than 5 deep",
                      "manual")
            self.res.manual = True
            return [], []
        try:
            graph = FlowGraph(pipeline)
            order = graph.order()
        except GraphError as exc:
            self.note("PL01_GRAPH", f"pipeline {pipeline.get('name')}: {exc}", "manual")
            self.res.manual = True
            return [], []
        for v in pipeline.get("variables") or []:
            self.note("PL07_VARIABLE", f"pipeline variable {v.get('name')!r} has no AIDP job "
                      f"equivalent; tasks that read it get their notebook default", "review")
        if pipeline.get("parameters"):
            names = ", ".join(p.get("name", "?") for p in pipeline["parameters"])
            self.note("PL08_PIPELINE_PARAMS", f"pipeline parameters ({names}) are not bound to "
                      f"task parameters automatically; set them on the job tasks", "review")

        # Per operator node: the AIDP task keys that "complete" it (what a
        # downstream node depends on), and the runIf a downstream task inherits.
        done: dict = {}          # node key -> list of task keys
        blocked: set = set()     # nodes behind a decision (or an unresolvable task)
        roots, leaves = [], []

        def upstream(node_key):
            """(task keys, inherited runIf, blocked?) for one node's inputs."""
            keys, run_ifs, is_blocked = [], [], False
            for e in graph.inputs(node_key):
                src = graph.nodes[e.src]
                if e.src in blocked:
                    is_blocked = True
                    continue
                if src.op_type == "MERGE_OPERATOR":
                    keys += done.get(e.src, [])
                    run_ifs.append(_RUN_IF.get((src.operator.get("triggerRule") or
                                                "ALL_SUCCESS").upper(), "ALL_SUCCESS"))
                else:
                    keys += done.get(e.src, [])
            return list(dict.fromkeys(keys)), run_ifs, is_blocked

        for node in order:
            op = node.operator
            keys, inherited, is_blocked = upstream(node.key)
            if is_blocked:
                blocked.add(node.key)
                if node.op_type == "TASK_OPERATOR":
                    self.note("PL03_BEHIND_DECISION", f"task {node.label} runs behind a decision "
                              f"or an unmigrated task; it is not in the AIDP job -- rebuild the "
                              f"branch (e.g. split into separate jobs)", "manual", node.label)
                    self.res.manual = True
                continue
            if node.op_type in _STRUCTURAL:
                done[node.key] = keys
                if node.op_type == "END_OPERATOR" and (op.get("triggerRule") or "ALL_SUCCESS") \
                        != "ALL_SUCCESS":
                    self.note("PL06_END_RULE", f"END node rule {op.get('triggerRule')}: AIDP "
                              f"derives the job status from its tasks", "info", node.label)
                continue
            if node.op_type == "MERGE_OPERATOR":
                done[node.key] = keys
                rule = (op.get("triggerRule") or "ALL_SUCCESS").upper()
                if _RUN_IF.get(rule) in _UNVERIFIED_RUN_IF:
                    self.note("PL05_RUN_IF", f"merge {node.label} rule {rule} -> runIf "
                              f"{_RUN_IF[rule]}, a value not yet created live on AIDP", "review",
                              node.label)
                continue
            if node.op_type == "DECISION_OPERATOR":
                blocked.add(node.key)
                self.note("PL02_DECISION", f"decision {node.label}: AIDP job runIf cannot "
                          f"evaluate a condition; every task behind it is left out of the job",
                          "manual", node.label)
                self.res.manual = True
                continue
            if node.op_type == "EXPRESSION_OPERATOR":
                done[node.key] = keys
                self.note("PL04_EXPRESSION", f"pipeline expression {node.label} (variable "
                          f"assignment) is not carried over", "review", node.label)
                continue
            if node.op_type != "TASK_OPERATOR":
                blocked.add(node.key)
                self.note("PL10_OPERATOR", f"pipeline operator {node.op_type} is not supported",
                          "manual", node.label)
                self.res.manual = True
                continue

            artifact = self.resolve(op.get("task") or {})
            if artifact.error:
                blocked.add(node.key)
                self.note("PL11_TASK", f"task {node.label}: {artifact.error}", "manual",
                          node.label)
                self.res.manual = True
                continue
            own = _RUN_IF.get((op.get("triggerRule") or "ALL_SUCCESS").upper(), "ALL_SUCCESS")
            run_if = own
            if own == "ALL_SUCCESS" and inherited:
                if len(set(inherited)) > 1:
                    self.note("PL05_RUN_IF", f"task {node.label} follows merges with different "
                              f"rules {sorted(set(inherited))}; ALL_SUCCESS used", "review",
                              node.label)
                else:
                    run_if = inherited[0]
            if run_if in _UNVERIFIED_RUN_IF:
                self.note("PL05_RUN_IF", f"task {node.label}: runIf {run_if} has not been "
                          f"created live on AIDP", "review", node.label)

            if artifact.pipeline is not None:
                child_prefix = f"{prefix}{task_key(node.label)}__"
                c_roots, c_leaves = self._expand(artifact.pipeline, child_prefix, depth + 1)
                for t in self.res.tasks:
                    if t["taskKey"] in c_roots and keys:
                        t["dependsOn"] = [{"taskKey": k} for k in keys]
                        t["runIf"] = run_if
                done[node.key] = c_leaves
                if not keys:
                    roots += c_roots
                self.note("PL12_NESTED", f"nested pipeline {node.label} inlined "
                          f"({len(c_roots)} entry task(s))", "info", node.label)
                continue

            k = unique(prefix + task_key(node.label), self.keys)
            task = {"taskKey": k, "type": "NOTEBOOK_TASK", "runIf": run_if,
                    "notebookPath": artifact.notebook_path,
                    "dependsOn": [{"taskKey": u} for u in keys]}
            if artifact.parameters:
                task["parameters"] = artifact.parameters
            retries = int(op.get("retryAttempts") or 0)
            if retries > 0:
                task["maxRetries"] = retries
                delay = float(op.get("retryDelay") or 0)
                unit = (op.get("retryDelayUnit") or "SECONDS").upper()
                if delay:
                    task["minRetryIntervalMillis"] = int(delay * _DELAY_MS.get(unit, 1000))
            if op.get("expectedDuration"):
                self.note("PL13_EXPECTED_DURATION", f"task {node.label}: expected-duration alert "
                          f"is not carried over", "info", node.label)
            self.res.tasks.append(task)
            done[node.key] = [k]
            if not keys:
                roots.append(k)

        has_downstream = {d["taskKey"] for t in self.res.tasks for d in t.get("dependsOn", [])}
        own_keys = [t["taskKey"] for t in self.res.tasks if t["taskKey"].startswith(prefix)]
        leaves = [k for k in own_keys if k not in has_downstream]
        return roots, leaves
