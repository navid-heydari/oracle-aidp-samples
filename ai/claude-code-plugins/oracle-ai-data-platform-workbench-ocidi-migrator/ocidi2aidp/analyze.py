"""``analyze``: what a migration of this snapshot would involve -- before doing it.

Runs the compiler into a scratch directory and summarises: inventory by
object type, operator coverage, work orders and manual items per task, the
sources AIDP must reach, schedules, and an effort band per object. Writes
``analysis.json`` and ``analysis.md``; nothing else.
"""
from __future__ import annotations

import json
import tempfile
from collections import Counter
from pathlib import Path

from .migrate import Migration
from .snapshot import Snapshot


def _band(result) -> str:
    sev = Counter(f.severity for f in result.findings)
    if sev["manual"] or result.status == "failed":
        return "HIGH"
    if sev["fallback"] or sev["review"] > 3:
        return "MEDIUM"
    return "LOW"


def analyze(snapshot_path, config, out_dir) -> dict:
    snap = Snapshot.load(snapshot_path)
    with tempfile.TemporaryDirectory() as scratch:
        report = Migration(snap, Path(scratch), config).run()
    ops = Counter()
    for t in snap.all("tasks").values():
        for node in (t.get("dataFlow") or {}).get("nodes") or []:
            ops[(node.get("operator") or {}).get("modelType", "?")] += 1
    rows = []
    for r in report.results:
        if r.kind not in ("notebook", "job"):
            continue
        sev = Counter(f.severity for f in r.findings)
        rows.append({"kind": r.kind, "name": r.name, "source_kind": r.source_kind,
                     "status": r.status, "effort": _band(r), "work_orders": len(r.fallbacks),
                     "manual": sev["manual"], "review": sev["review"] + sev["assume"],
                     "published": r.extra.get("published"),
                     "scheduled": r.extra.get("scheduled")})
    data = {"snapshot": str(snapshot_path), "source_format": snap.source_format,
            "inventory": snap.counts(), "operators": dict(ops.most_common()),
            "objects": rows, "sources": report.sources,
            "targets": sorted({t["table"] for t in report.targets}),
            "totals": {"work_orders": sum(r["work_orders"] for r in rows),
                       "manual_items": sum(r["manual"] for r in rows),
                       "effort": dict(Counter(r["effort"] for r in rows))}}
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "analysis.json").write_text(json.dumps(data, indent=2, default=str) + "\n")
    (out / "analysis.md").write_text(render(data), encoding="utf-8")
    return data


def render(data: dict) -> str:
    lines = ["# OCI-DI migration analysis", "",
             f"Snapshot `{data['snapshot']}` ({data['source_format']}).", "",
             "## Inventory", "", "| Object type | Count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in data["inventory"].items()]
    lines += ["", "## Data flow operators", "", "| Operator | Count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in data["operators"].items()]
    t = data["totals"]
    lines += ["", "## Effort", "",
              f"{t['work_orders']} LLM work order(s), {t['manual_items']} manual item(s). "
              f"Bands: {', '.join(f'{k} {v}' for k, v in sorted(t['effort'].items()))}.", "",
              "| Kind | Object | From | Status | Effort | Work orders | Manual | Review |",
              "|---|---|---|---|---|---|---|---|"]
    for r in sorted(data["objects"], key=lambda r: ({"HIGH": 0, "MEDIUM": 1}.get(r["effort"], 2),
                                                    r["kind"], r["name"])):
        lines.append(f"| {r['kind']} | {r['name']} | {r['source_kind']} | {r['status']} | "
                     f"{r['effort']} | {r['work_orders']} | {r['manual']} | {r['review']} |")
    lines += ["", "## Sources AIDP must reach", "",
              "| DI data asset | Type | Access | What must exist |", "|---|---|---|---|"]
    lines += [f"| {s['data_asset']} | {s['asset_type']} | {s['access']} | {s['action']} |"
              for s in data["sources"]]
    lines += ["", "## Delta tables the migration writes", ""]
    lines += [f"- `{tb}`" for tb in data["targets"]]
    return "\n".join(lines) + "\n"
