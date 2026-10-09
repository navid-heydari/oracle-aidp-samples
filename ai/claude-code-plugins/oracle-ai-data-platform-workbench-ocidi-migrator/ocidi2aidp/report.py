"""Findings and the migration report.

Every decision the compiler makes that a person might disagree with is a
``Finding``. Findings travel beside an artifact, never inside the payload
sent to AIDP, so a job body only ever carries keys the jobs API knows.

Severity:
  * ``info``    -- a faithful rewrite worth knowing about (e.g. a renamed function).
  * ``assume``  -- the compiler had to pick a meaning; confirm it.
  * ``review``  -- converted, but a human must check the semantics before cutover.
  * ``fallback``-- not converted; an LLM work order was written.
  * ``manual``  -- no faithful AIDP equivalent; a stub and a rebuild note.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

SEVERITIES = ("info", "assume", "review", "fallback", "manual")
# Ordered worst-last: an object's status is driven by its worst finding.
STATUSES = ("ok", "needs_review", "llm_assisted", "fallback_pending", "manual", "failed")


@dataclass(frozen=True)
class Finding:
    code: str
    message: str
    severity: str = "review"
    node: str = ""

    def __post_init__(self):
        if self.severity not in SEVERITIES:
            raise ValueError(f"unknown severity {self.severity!r}")

    def render(self) -> str:
        where = f" [{self.node}]" if self.node else ""
        return f"{self.code}{where}: {self.message}"


def status_for(findings, *, failed: bool = False) -> str:
    if failed:
        return "failed"
    severities = {f.severity for f in findings}
    if "manual" in severities:
        return "manual"
    if "fallback" in severities:
        return "fallback_pending"
    if severities & {"review", "assume"}:
        return "needs_review"
    return "ok"


@dataclass
class ObjectResult:
    """One migrated OCI-DI object and what became of it."""

    kind: str                 # notebook | job | ddl | reconcile
    name: str
    source_kind: str          # INTEGRATION_TASK | PIPELINE_TASK | DATA_FLOW | ...
    source_key: str
    status: str = "ok"
    artifacts: list = field(default_factory=list)   # paths relative to out/
    findings: list = field(default_factory=list)    # list[Finding]
    fallbacks: list = field(default_factory=list)   # work-order ids
    extra: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        data = asdict(self)
        data["findings"] = [asdict(f) for f in self.findings]
        return data

    @classmethod
    def from_json(cls, data: dict) -> "ObjectResult":
        data = dict(data)
        data["findings"] = [Finding(**f) for f in data.get("findings", [])]
        return cls(**data)


@dataclass
class Report:
    tool_version: str
    snapshot: str
    config: dict
    results: list = field(default_factory=list)     # list[ObjectResult]
    sources: list = field(default_factory=list)     # source access requirements
    targets: list = field(default_factory=list)     # Delta tables the migration writes

    def add(self, result: ObjectResult) -> ObjectResult:
        self.results.append(result)
        return result

    def counts(self) -> dict:
        out = {s: 0 for s in STATUSES}
        for r in self.results:
            out[r.status] = out.get(r.status, 0) + 1
        return out

    def to_json(self) -> dict:
        return {
            "tool_version": self.tool_version,
            "snapshot": self.snapshot,
            "config": self.config,
            "counts": self.counts(),
            "results": [r.to_json() for r in self.results],
            "sources": self.sources,
            "targets": self.targets,
        }

    def write(self, out_dir: Path) -> None:
        out_dir = Path(out_dir)
        (out_dir / "report.json").write_text(
            json.dumps(self.to_json(), indent=2, sort_keys=False) + "\n", encoding="utf-8")
        (out_dir / "REVIEW.md").write_text(render_review(self), encoding="utf-8")

    @classmethod
    def load(cls, out_dir: Path) -> "Report":
        data = json.loads((Path(out_dir) / "report.json").read_text(encoding="utf-8"))
        rep = cls(tool_version=data["tool_version"], snapshot=data["snapshot"],
                  config=data.get("config", {}), sources=data.get("sources", []),
                  targets=data.get("targets", []))
        rep.results = [ObjectResult.from_json(r) for r in data.get("results", [])]
        return rep


def render_review(report: Report) -> str:
    lines = ["# Migration review", "",
             f"Tool version {report.tool_version}; snapshot `{report.snapshot}`.", ""]
    counts = report.counts()
    lines += ["| Status | Objects |", "|---|---|"]
    lines += [f"| {s} | {n} |" for s, n in counts.items() if n]
    lines.append("")
    lines += [
        "Statuses: `ok` converted with nothing to check; `needs_review` converted, "
        "read the findings; `fallback_pending` contains a stub that raises until an "
        "LLM work order is filled (`ocidi2aidp fallback`); `llm_assisted` contains "
        "LLM-written code that passed validation -- read it; `manual` has no faithful "
        "AIDP equivalent.", ""]
    order = {s: i for i, s in enumerate(reversed(STATUSES))}
    for r in sorted(report.results, key=lambda r: (order.get(r.status, 0), r.kind, r.name)):
        if r.status == "ok" and not r.findings:
            continue
        lines.append(f"## {r.kind}: `{r.name}` -- {r.status}")
        lines.append("")
        lines.append(f"From OCI-DI {r.source_kind} `{r.source_key}`. "
                     f"Artifacts: {', '.join(f'`{a}`' for a in r.artifacts) or 'none'}.")
        lines.append("")
        for sev in reversed(SEVERITIES):
            for f in r.findings:
                if f.severity == sev:
                    lines.append(f"- **{sev}** {f.render()}")
        lines.append("")
    if report.sources:
        lines += ["## Sources AIDP must be able to read", "",
                  "| DI data asset | Type | AIDP access | Parameter | Action |", "|---|---|---|---|---|"]
        for s in report.sources:
            lines.append(f"| {s['data_asset']} | {s['asset_type']} | {s['access']} | "
                         f"`{s['parameter']}` | {s['action']} |")
        lines.append("")
    return "\n".join(lines) + "\n"
