"""Build the data-plane stages as self-contained AIDP notebooks.

WHY NOTEBOOKS, AND WHY .ipynb SPECIFICALLY. AIDP types a workspace object by
its EXTENSION, not by the `--type` you upload it with: a `.py` is stored as
`FILE` even when created with `--type NOTEBOOK`, and only `.ipynb` becomes a
`NOTEBOOK`. A job task is a `NOTEBOOK_TASK` pointing at a NOTEBOOK, so the
data plane has to be `.ipynb` or it cannot be run as a job at all. Verified
live 2026-09-19 by uploading both shapes and reading the listing back.

WHY SELF-CONTAINED. The previous design uploaded four `.py` scripts plus a
shared `snowmig_source.py`, then generated a one-cell driver notebook per job
that `runpy`-ed the script off the `/Workspace` mount. That indirection cost
three things: the code a user opens in the console is not the code that runs,
a mount-path assumption sits between the job and its logic, and an editable
parameter lives in a generated wrapper rather than beside the work. Each
stage is now ONE notebook carrying its own parameters, its own copy of the
shared helpers, and its own stage logic.

WHY GENERATED RATHER THAN HAND-WRITTEN. The stage logic is long and tested;
hand-maintaining five copies of the shared helpers inside four notebooks is
how they drift. The canonical Python lives once in `engine/dataplane/`, and
this module assembles it into notebooks. The generated `.ipynb` ARE committed
-- they are the artifact that ships and the thing a reviewer reads -- but
they are regenerated, never edited by hand.
"""
from __future__ import annotations

import json
import pathlib
import re

__all__ = ["STAGES", "StageSpec", "build_stage_notebook", "dataplane_dir",
           "write_stage_notebooks"]

# The shared helper module every stage needs, inlined into each notebook.
SHARED_SOURCE_NAME = "snowmig_source.py"

# `from snowmig_source import (...)` plus the sys.path line that made it
# resolvable off the mount. Both are meaningless once the helpers are inlined
# in the notebook itself, so they are stripped -- and the strip is ASSERTED,
# because a silent miss would leave a notebook importing a module that is no
# longer uploaded.
_IMPORT_BLOCK = re.compile(
    r"^sys\.path\.insert\(0, str\(pathlib\.Path\(__file__\)[^\n]*\n"
    r"from snowmig_source import \([^)]*\)\n",
    re.MULTILINE)

# `if __name__ == "__main__": sys.exit(main())`. A notebook cell that raises
# SystemExit is reported as a FAILED task even when the work succeeded -- a
# full discovery once came back failed for exactly that reason -- so the run
# cell calls main() and inspects the returned code instead.
# An actual import of the helper module, as opposed to prose mentioning it.
_IMPORTS_SHARED = re.compile(
    r"^\s*(?:from\s+snowmig_source\s+import|import\s+snowmig_source)\b",
    re.MULTILINE)

_MAIN_GUARD = re.compile(
    r"\n\nif __name__ == [\"']__main__[\"']:\n    sys\.exit\(main\(\)\)\n?$")


class StageSpec:
    """One data-plane stage: its source, its job, its editable parameters."""

    def __init__(self, *, key: str, source: str, job: str, title: str,
                 blurb: str, params: dict[str, object],
                 required: tuple[str, ...] = ()):
        self.key = key
        self.source = source
        self.job = job
        self.title = title
        self.blurb = blurb
        self.params = params
        self.required = required

    @property
    def notebook_name(self) -> str:
        return self.source.replace(".py", ".ipynb")


# Parameter DEFAULTS only -- never an estate's real names. A value that
# identifies one customer's environment does not belong in a plugin that
# ships to everyone, so anything site-specific defaults to None and the
# provisioner fills it in from the run's own coordinates.
STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        key="discover", source="00_discover_snowflake.py",
        job="snowmig_00_discover",
        title="S6 · Discover the Snowflake estate",
        blurb=(
            "Reads the source through the AIDP connector and writes "
            "`discovery_manifest.json`. Costs TWO `INFORMATION_SCHEMA` "
            "queries for the whole database, which is what makes a "
            "hundred-thousand-table estate feasible; a `DESCRIBE` per object "
            "does not scale and is not the path.\n\n"
            "**Read-only against Snowflake.** The transport refuses any verb "
            "that is not SELECT/SHOW/DESCRIBE/DESC/WITH/EXPLAIN before it "
            "reaches the source."),
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "session-schema": None,
                "reports-dir": None},
    ),
    StageSpec(
        key="structure", source="01_create_structure.py",
        job="snowmig_01_structure",
        title="S10 · Create the target structure",
        blurb=(
            "Creates the schemas and the empty Delta tables in the INTERNAL "
            "target catalog, one schema per run. **Structure only — no rows "
            "move.** Every table arrives empty.\n\n"
            "Runs on compute rather than through the catalog CRUD API "
            "because that API returns `202 Accepted` and can create nothing, "
            "so a table 'created' that way cannot be verified."),
        # `mode` is `ddl-plan`, matching the stage's own argparse default and
        # runbook S10 ("reading the approved plan"). It used to ship
        # `manifest`, which CANNOT work alongside the `connector` source-mode
        # directly above it: a connector-built manifest records SNOWFLAKE
        # types, and Delta rejects them verbatim. The shipped default pair was
        # therefore guaranteed to fail -- every table refused with "use
        # --mode ddl-plan" -- on a first, unmodified run.
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "target-catalog": None,
                "schema": None, "mode": "ddl-plan", "reports-dir": None},
        required=("target-catalog",),
    ),
    StageSpec(
        key="copy_schema", source="02_copy_schema.py",
        job="snowmig_02_copy_schema",
        title="Copy one schema's data",
        blurb=(
            "Moves rows for a single schema. **Not part of S1–S12** — the "
            "migration registers this notebook and never runs it. Moving "
            "data is a later decision the customer makes, with the notebook "
            "already sitting here.\n\n"
            "One notebook per SCHEMA, never per table: a job run costs five "
            "to six minutes of startup, so per-table runs are the wrong "
            "shape."),
        params={"source-mode": "connector", "source-config": None,
                "source-catalog": None, "target-catalog": None,
                "schema": None, "mode": "skip-existing",
                "verify": "counts", "reports-dir": None},
        required=("target-catalog", "schema"),
    ),
    StageSpec(
        key="reconcile", source="03_reconcile.py",
        job="snowmig_03_reconcile",
        title="Reconcile source against target",
        blurb=(
            "Compares what the target holds against what discovery recorded "
            "and reports the verdict per table. Read-only on both ends."),
        params={"target-catalog": None, "reports-dir": None, "counts": False},
        required=("target-catalog",),
    ),
)


def dataplane_dir() -> pathlib.Path:
    """Where the canonical stage sources live, relative to this file."""
    return pathlib.Path(__file__).resolve().parent.parent / "dataplane"


def _md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [text]}


def _code(text: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": [], "source": [text]}


def _strip_shared_import(source: str, stage: str) -> tuple[str, bool]:
    """Remove the shared-module import. Returns (source, needs_helpers).

    Not every stage uses the helpers -- `03_reconcile` reads only the target
    -- so an absent import is fine. What is NOT fine is a stage that still
    mentions `snowmig_source` after the strip: that notebook would import a
    module it does not ship, and it would fail at run time on the cluster
    rather than here.
    """
    stripped, count = _IMPORT_BLOCK.subn("", source)
    # Only an IMPORT matters. Prose that merely names the helper file -- a
    # docstring, an argparse help string -- is fine and must not trip this.
    if not count and not _IMPORTS_SHARED.search(source):
        return stripped, False
    if _IMPORTS_SHARED.search(stripped):
        raise ValueError(
            f"{stage}: `snowmig_source` is still referenced after the import "
            f"block was stripped, so this notebook would import a module it "
            f"does not ship. Fix the pattern rather than emitting a notebook "
            f"that cannot run.")
    if not count:
        raise ValueError(
            f"{stage}: imports `snowmig_source` but its import block did "
            f"not match the expected shape, so nothing was stripped.")
    return stripped, True


def _strip_main_guard(source: str, stage: str) -> str:
    stripped, count = _MAIN_GUARD.subn("\n", source)
    if not count:
        raise ValueError(
            f"{stage}: no `if __name__ == '__main__': sys.exit(main())` "
            f"guard found. Left in place it raises SystemExit, which AIDP "
            f"reports as a FAILED task even when the stage succeeded.")
    return stripped


def _params_cell(stage: StageSpec,
                 overrides: dict[str, object] | None = None) -> str:
    lines = [
        "# ── PARAMETERS ─────────────────────────────────────────────────",
        "# Edit these, then run the notebook top to bottom. Every value is a",
        "# plain Python literal: `None` means the flag is not passed at all,",
        "# and `True` means a bare switch is passed.",
        "#",
        "# Scope and mode are INPUTS. To migrate less, change a value here --",
        "# never edit the stage logic below to make it cover less.",
        "PARAMS = {",
    ]
    merged = dict(stage.params)
    for key, value in (overrides or {}).items():
        # Only parameters the stage actually declares: a flag it does not
        # accept would make argparse reject the whole run.
        if key in merged and value is not None:
            merged[key] = value
    for key, value in merged.items():
        note = "  # REQUIRED" if key in stage.required else ""
        lines.append(f"    {key!r}: {value!r},{note}")
    lines += [
        "}",
        "",
        "",
        "def _argv(params):",
        '    """PARAMS -> argv. None is omitted; True is a bare switch."""',
        "    argv = []",
        "    for key, value in params.items():",
        "        if value is None or value is False:",
        "            continue",
        "        argv.append(f'--{key}')",
        "        if value is not True:",
        "            argv.extend(str(v) for v in (",
        "                value if isinstance(value, (list, tuple)) else [value]))",
        "    return argv",
        "",
        "",
        "ARGV = _argv(PARAMS)",
        "print('arguments:', ARGV)",
    ]
    missing = [k for k in stage.required]
    if missing:
        lines += [
            "",
            "_missing = [k for k in {!r} if not PARAMS.get(k)]".format(
                list(stage.required)),
            "if _missing:",
            "    raise ValueError(",
            "        f'set these PARAMS before running: {_missing}')",
        ]
    return "\n".join(lines)


_RUN_CELL = """\
# ── RUN ────────────────────────────────────────────────────────────
# main() RETURNS an exit code; it is not allowed to raise SystemExit here.
# A notebook cell that raises SystemExit is reported as a FAILED task even
# when the work succeeded, so the code is inspected and only a real failure
# is re-raised -- which keeps a genuinely failed stage failing.
code = main(ARGV)
print('exit code:', code, flush=True)
if code:
    raise RuntimeError(f'stage exited {code}')
"""


def build_stage_notebook(stage: StageSpec,
                         dataplane: pathlib.Path | None = None,
                         overrides: dict[str, object] | None = None) -> dict:
    """One self-contained `.ipynb` for `stage`: params, helpers, logic, run.

    `overrides` writes this run's coordinates into the PARAMS cell, so the
    notebook a user opens in the console already carries the right catalog
    and reports directory. Keys the stage does not declare are ignored
    rather than passed through -- argparse would reject an unknown flag and
    fail the whole run.
    """
    root = dataplane or dataplane_dir()
    body = (root / stage.source).read_text(encoding="utf-8")
    body, needs_helpers = _strip_shared_import(body, stage.source)
    body = _strip_main_guard(body, stage.source)

    header = (f"# {stage.title}\n\n{stage.blurb}\n\n"
              f"---\n\n"
              f"*Generated from `engine/dataplane/{stage.source}` by "
              f"`engine/target/stage_notebooks.py`. Regenerate with "
              f"`snowmig.py build-notebooks`; do not hand-edit — an edit here "
              f"is overwritten on the next build. Change the source instead.*")

    cells = [_md(header), _code(_params_cell(stage, overrides))]
    if needs_helpers:
        cells += [
            _md("## Shared source helpers\n\nInlined from "
                "`engine/dataplane/snowmig_source.py` so this notebook runs "
                "with nothing else uploaded beside it."),
            _code((root / SHARED_SOURCE_NAME).read_text(encoding="utf-8")),
        ]
    cells += [_md("## Stage logic"), _code(body), _code(_RUN_CELL)]
    return {"cells": cells,
            "metadata": {"snowmig": {"generated": True, "stage": stage.key,
                                     "source": stage.source,
                                     "job": stage.job},
                         "kernelspec": {"display_name": "Python 3",
                                        "language": "python",
                                        "name": "python3"},
                         "language_info": {"name": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


def write_stage_notebooks(out_dir: str | pathlib.Path,
                          dataplane: pathlib.Path | None = None,
                          overrides: dict[str, object] | None = None
                          ) -> list[pathlib.Path]:
    """Write every stage notebook into `out_dir`. Returns the paths written."""
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for stage in STAGES:
        nb = build_stage_notebook(stage, dataplane, overrides)
        path = out / stage.notebook_name
        path.write_text(json.dumps(nb, indent=1) + "\n", encoding="utf-8")
        written.append(path)
    return written
