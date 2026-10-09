"""ocidi2aidp command line.

    python -m ocidi2aidp doctor
    python -m ocidi2aidp extract   --workspace-id <ocid> --region <r> -o snapshot/
    python -m ocidi2aidp analyze   snapshot/ -o analysis/
    python -m ocidi2aidp migrate   snapshot/ -o out/
    python -m ocidi2aidp fallback  list|prompt|apply|run out/
    python -m ocidi2aidp verify    out/
    python -m ocidi2aidp provision [--apply]
    python -m ocidi2aidp publish   out/ [--apply]
    python -m ocidi2aidp status    out/

Only extract (reads OCI-DI), provision --apply and publish --apply (write to
AIDP) touch a network. Everything else is offline.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from . import __version__
from .config import load_config


def _cfg(args):
    cfg = load_config(getattr(args, "config", None))
    for flag, (section, key) in {
        "instance_id": ("aidp", "instance_id"), "instance_name": ("aidp", "instance_name"),
        "region": ("aidp", "region"), "profile": ("aidp", "profile"), "auth": ("aidp", "auth"),
        "workspace_name": ("aidp", "workspace_name"), "workspace_key": ("aidp", "workspace_key"),
        "cluster_key": ("aidp", "cluster_key"), "catalog": ("target", "catalog"),
        "notebook_root": ("aidp", "notebook_root"),
    }.items():
        value = getattr(args, flag, None)
        if value:
            setattr(getattr(cfg, section), key, value)
    if getattr(args, "job_prefix", None):
        cfg.job_prefix = args.job_prefix
    if getattr(args, "timezone", None):
        cfg.default_timezone = args.timezone
    return cfg


def _aidp_flags(p):
    p.add_argument("--instance-id", help="AIDP DataLake OCID")
    p.add_argument("--instance-name", help="AIDP instance (DataLake) display name")
    p.add_argument("--region")
    p.add_argument("--profile", help="OCI config profile (default DEFAULT)")
    p.add_argument("--auth", help="api_key | security_token (default api_key)")
    p.add_argument("--workspace-name", help="default ocidi_migrated")
    p.add_argument("--workspace-key")
    p.add_argument("--catalog", help="target catalog (default ocidi_migrated)")


def cmd_doctor(args) -> int:
    ok = True
    print(f"ocidi2aidp {__version__}, Python {sys.version.split()[0]}")
    for name, why in (("oci", "extract, provision by instance name"),
                      ("anthropic", "fallback run --provider anthropic"),
                      ("pyspark", "local execution tests"), ("yaml", "YAML config")):
        try:
            __import__(name)
            print(f"  [ok]   {name:10} ({why})")
        except ImportError:
            print(f"  [--]   {name:10} not installed ({why})")
    if shutil.which("aidp"):
        print("  [ok]   aidp CLI   (provision, publish)")
    else:
        ok = False
        print("  [!!]   aidp CLI   not on PATH -- provision (even a dry run) and publish --apply will fail")
    try:
        cfg = _cfg(args)
        print(f"  config: workspace={cfg.aidp.workspace_name} catalog={cfg.target.catalog} "
              f"instance={'set' if cfg.aidp.instance_id else cfg.aidp.instance_name or 'NOT SET'}")
    except Exception as exc:
        ok = False
        print(f"  [!!]   config: {exc}")
    return 0 if ok else 1


def cmd_extract(args) -> int:
    from .extract.extractor import DiClient, extract
    cfg = _cfg(args)
    ws = args.workspace_id or cfg.di.workspace_id
    region = args.di_region or cfg.di.region
    if not ws or not region:
        print("extract needs --workspace-id and --di-region (or di.workspace_id / di.region)",
              file=sys.stderr)
        return 2
    client = DiClient(region, profile=args.di_profile or cfg.di.profile)
    manifest = extract(client, ws, args.out)
    print(json.dumps(manifest["counts"], indent=2))
    print(f"snapshot written to {args.out}")
    return 0


def cmd_analyze(args) -> int:
    from .analyze import analyze
    data = analyze(args.snapshot, _cfg(args), args.out)
    t = data["totals"]
    print(f"{len(data['objects'])} objects; {t['work_orders']} work orders; "
          f"{t['manual_items']} manual items; effort {t['effort']}")
    print(f"wrote {Path(args.out) / 'analysis.md'}")
    return 0


def cmd_migrate(args) -> int:
    from .migrate import migrate
    cfg = _cfg(args)
    only = set(args.only.split(",")) if args.only else None
    report = migrate(args.snapshot, args.out, cfg, strict=args.strict, only=only)
    counts = {k: v for k, v in report.counts().items() if v}
    print(f"migrated {len(report.results)} objects: {counts}")
    pending = sum(len(r.fallbacks) for r in report.results)
    if pending:
        print(f"{pending} LLM work order(s) in {Path(args.out) / 'fallback'} -- run the "
              f"ocidi-fallback skill or `ocidi2aidp fallback run`")
    print(f"read {Path(args.out) / 'REVIEW.md'}")
    return 0


def cmd_fallback(args) -> int:
    from .fallback.apply import apply
    from .fallback.workorder import load_orders, render_prompt, response_path
    out = Path(args.out)
    if args.action == "list":
        for o in load_orders(out):
            state = "answered" if response_path(out, o["id"]).exists() else "pending"
            print(f"{o['id']}  {state:9} {o['kind']:15} {o['object']['name']} / "
                  f"{o['node']['label']}: {o['reason'][:90]}")
        return 0
    if args.action == "prompt":
        orders = {o["id"]: o for o in load_orders(out)}
        if args.id not in orders:
            print(f"no work order {args.id}", file=sys.stderr)
            return 2
        print(render_prompt(orders[args.id]))
        print(f"\n# Write the answer to: {response_path(out, args.id)}")
        return 0
    if args.action == "run":
        if args.provider != "anthropic":
            print("in a Claude Code session, fill work orders with the ocidi-fallback skill; "
                  "for headless runs use --provider anthropic", file=sys.stderr)
            return 2
        from .fallback.providers import run_anthropic
        for fid, state, detail in run_anthropic(out, model=args.model, overwrite=args.overwrite):
            print(f"{fid}  {state:8} {detail}")
    outcomes = apply(out, author=args.author or args.provider or "session")
    bad = 0
    for o in outcomes:
        extra = "; ".join(o.errors or o.assumptions)
        print(f"{o.id}  {o.state:9} {extra}")
        bad += o.state == "rejected"
    return 1 if bad else 0


def cmd_verify(args) -> int:
    from .verify import verify
    checks = verify(args.out)
    for c in checks:
        print(f"{c.grade:6} {c.artifact}")
        for p in c.problems:
            print(f"         FAIL: {p}")
        for n in c.notes:
            print(f"         {n}")
    grades = [c.grade for c in checks]
    print(f"\nPASS {grades.count('PASS')}  REVIEW {grades.count('REVIEW')}  "
          f"FAIL {grades.count('FAIL')}")
    return 1 if "FAIL" in grades else 0


def _client(cfg):
    from .aidp.client import AidpClient
    from .aidp.provision import resolve_instance
    return AidpClient(instance_id=resolve_instance(cfg), region=cfg.aidp.region,
                      profile=cfg.aidp.profile, auth=cfg.aidp.auth)


def cmd_provision(args) -> int:
    from .aidp.provision import provision
    cfg = _cfg(args)
    res = provision(cfg, apply=args.apply, client=_client(cfg), skip_catalog=args.skip_catalog)
    for s in res.steps:
        print(f"{s.what:10} {s.state:13} {s.detail}")
    if args.save:
        Path(args.save).write_text(json.dumps(res.to_json(), indent=2) + "\n")
        print(f"wrote {args.save} (holds identifiers -- do not commit it)")
    if not args.apply:
        print("\ndry run: nothing was created. Re-run with --apply to create what is missing.")
    return 0 if all(s.state != "failed" for s in res.steps) else 1


def cmd_publish(args) -> int:
    from .aidp.publish import publish
    cfg = _cfg(args)
    client, ws = None, cfg.aidp.workspace_key
    if args.apply:
        client = _client(cfg)
        if not ws:
            match = next((w for w in client.workspaces()
                          if w.get("displayName") == cfg.aidp.workspace_name), None)
            if match is None:
                print(f"workspace {cfg.aidp.workspace_name!r} does not exist; run provision "
                      f"--apply first", file=sys.stderr)
                return 2
            ws = match["key"]
    plan, log = publish(args.out, cfg, client=client, workspace_key=ws,
                        cluster_key=args.cluster_key or cfg.aidp.cluster_key,
                        prefix=args.prefix or "", apply=args.apply)
    if not args.apply:
        print(f"dry run -- would upload {len(plan.notebooks)} notebook(s) and create "
              f"{len(plan.jobs)} job(s) in workspace {cfg.aidp.workspace_name!r}:")
        for nb in plan.notebooks:
            print(f"  notebook {nb['remote']}  [{nb['status']}]")
        for j in plan.jobs:
            sched = j["body"].get("schedule")
            when = f" schedule {sched['quartzCronExpression']} {sched['timezoneId']} PAUSED" \
                if sched else ""
            print(f"  job      {j['name']} ({len(j['notebooks'])} task(s)){when}")
        for w in plan.warnings:
            print(f"  warning: {w}")
        if plan.jobs and not (args.cluster_key or cfg.aidp.cluster_key):
            print("  warning: no --cluster-key -- notebooks would upload, but AIDP refuses jobs "
                  "whose tasks have no cluster, so no job would be created")
        print("\nNothing was sent. Re-run with --apply to publish.")
        return 0
    print(f"uploaded {len(log.uploaded)}, skipped {len(log.skipped)}, created "
          f"{len(log.created_jobs)} job(s), refused {len(log.refused_jobs)}, "
          f"errors {len(log.errors)}")
    for e in log.errors + log.refused_jobs:
        print(f"  {e}")
    return 1 if log.errors else 0


def cmd_status(args) -> int:
    from .report import Report
    rep = Report.load(args.out)
    print(json.dumps(rep.counts(), indent=2))
    for r in rep.results:
        if r.status != "ok":
            print(f"{r.kind:9} {r.status:17} {r.name}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ocidi2aidp", description=__doc__.split("\n")[0])
    ap.add_argument("--version", action="version", version=__version__)
    ap.add_argument("--config", help="ocidi-config.yaml / .json")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("doctor", help="check dependencies and configuration")
    _aidp_flags(p)
    p.set_defaults(fn=cmd_doctor)

    p = sub.add_parser("extract", help="read an OCI-DI workspace into a snapshot (read-only)")
    p.add_argument("--workspace-id", help="OCI-DI workspace OCID")
    p.add_argument("--di-region")
    p.add_argument("--di-profile")
    p.add_argument("-o", "--out", required=True)
    p.set_defaults(fn=cmd_extract)

    for name, fn, helptext in (("analyze", cmd_analyze, "inventory and effort, before migrating"),
                               ("migrate", cmd_migrate, "compile notebooks, jobs, DDL, work orders")):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("snapshot", help="snapshot directory, or an OCI-DI export (.zip or dir)")
        p.add_argument("-o", "--out", required=True)
        p.add_argument("--job-prefix")
        p.add_argument("--timezone", help="IANA zone for schedules that carry none")
        p.add_argument("--catalog", help="target catalog (default ocidi_migrated)")
        p.add_argument("--notebook-root", help="default /Workspace/ocidi")
        p.add_argument("--cluster-key")
        if name == "migrate":
            p.add_argument("--only", help="comma-separated task names/identifiers")
            p.add_argument("--strict", action="store_true", help="raise on compiler errors")
        p.set_defaults(fn=fn)

    p = sub.add_parser("fallback", help="LLM work orders: list, prompt, apply, run")
    p.add_argument("action", choices=("list", "prompt", "apply", "run"))
    p.add_argument("out")
    p.add_argument("--id", help="work order id (prompt)")
    p.add_argument("--provider", default="session", choices=("session", "anthropic"))
    p.add_argument("--model", default="claude-opus-5-5")
    p.add_argument("--author", help="recorded in the banner (default: provider)")
    p.add_argument("--overwrite", action="store_true")
    p.set_defaults(fn=cmd_fallback)

    p = sub.add_parser("verify", help="static PASS/REVIEW/FAIL checks on the output")
    p.add_argument("out")
    p.set_defaults(fn=cmd_verify)

    p = sub.add_parser("provision", help="ensure the AIDP workspace and catalog (dry run)")
    _aidp_flags(p)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--skip-catalog", action="store_true")
    p.add_argument("--save", help="write resolved identifiers to this JSON file")
    p.set_defaults(fn=cmd_provision)

    p = sub.add_parser("publish", help="upload notebooks and create jobs (dry run)")
    p.add_argument("out")
    _aidp_flags(p)
    p.add_argument("--cluster-key")
    p.add_argument("--prefix", help="per-person folder and job-name prefix")
    p.add_argument("--apply", action="store_true")
    p.set_defaults(fn=cmd_publish)

    p = sub.add_parser("status", help="summarise a migration's report")
    p.add_argument("out")
    p.set_defaults(fn=cmd_status)

    args = ap.parse_args(argv)
    from .aidp.client import AidpError, AidpUnavailable
    from .aidp.publish import PublishError
    from .snapshot import SnapshotError
    try:
        return args.fn(args)
    except (AidpError, AidpUnavailable, PublishError, SnapshotError, FileNotFoundError,
            ValueError) as exc:
        print(f"ocidi2aidp {args.cmd}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
