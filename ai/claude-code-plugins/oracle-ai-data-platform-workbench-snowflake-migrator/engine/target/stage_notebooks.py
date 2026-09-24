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

import ast
import json
import pathlib
import re

__all__ = ["DIAGNOSE_NOTEBOOK_NAME", "DIAGNOSE_SOURCE_NAME", "STAGES",
           "StageSpec", "build_diagnose_notebook", "build_stage_notebook",
           "dataplane_dir", "write_stage_notebooks"]

# The shared helper module every stage needs, inlined into each notebook.
SHARED_SOURCE_NAME = "snowmig_source.py"

# The environment diagnosis is generated like the stages but is NOT a stage:
# no job runs it, it has no argparse `main()`, and a human reads its verdicts
# cell by cell. Its source is split on `# %%` markers -- one cell per check,
# `# %% [markdown]` for prose -- its module docstring is the notebook header,
# and its parameters are edited in place with this run's coordinates. It used
# to be a hand-maintained `.ipynb` that imported `snowmig_source` off the
# mount (never uploaded there any more) and echoed the config with a
# top-level-only redaction, which printed a nested `snowflake:` block whole.
DIAGNOSE_SOURCE_NAME = "diagnose_environment.py"
DIAGNOSE_NOTEBOOK_NAME = "diagnose_environment.ipynb"
# provision's override keys -> the parameter each one sets. The rest of what
# provision knows (target catalog, reports dir) is not a diagnosis input.
DIAGNOSE_PARAMS = {"source-config": "CONFIG_PATH",
                   "source-catalog": "EXTERNAL_CATALOG",
                   "session-schema": "SESSION_SCHEMA"}
_CELL_MARK = re.compile(r"^# %% ?(.*)$", re.MULTILINE)

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


def _split_cells(body: str) -> list[tuple[str, str]]:
    """`(title, source)` per `# %%` marker; index 0 is the untitled preamble."""
    cells = []
    pos, title = 0, ""
    for match in _CELL_MARK.finditer(body):
        cells.append((title, body[pos:match.start()]))
        title, pos = match.group(1).strip(), match.end() + 1
    cells.append((title, body[pos:]))
    return cells


def _set_parameter(source: str, name: str, value: str) -> str:
    """Rewrite `NAME = ...` in the parameters cell, keeping its comment."""
    pattern = re.compile(rf"^{re.escape(name)} = [^#\n]*?(\s*#[^\n]*)?$",
                         re.MULTILINE)
    new, count = pattern.subn(
        lambda m: f"{name} = {value!r}{m.group(1) or ''}", source, count=1)
    if count != 1:
        raise ValueError(
            f"{DIAGNOSE_SOURCE_NAME}: no `{name} = ...` line in the "
            f"parameters cell to fill in")
    return new


def build_diagnose_notebook(dataplane: pathlib.Path | None = None,
                            overrides: dict[str, object] | None = None
                            ) -> dict:
    """The environment diagnosis: header, parameters, inlined helpers, one
    cell per check, reading guide. Same inlining as the stages; no job.

    `overrides` is the same dict provision builds for the stages, so
    `CONFIG_PATH` becomes exactly the mount path the config was uploaded to.
    """
    root = dataplane or dataplane_dir()
    body = (root / DIAGNOSE_SOURCE_NAME).read_text(encoding="utf-8")
    body, needs_helpers = _strip_shared_import(body, DIAGNOSE_SOURCE_NAME)
    if not needs_helpers:
        raise ValueError(
            f"{DIAGNOSE_SOURCE_NAME}: expected an import of snowmig_source; "
            f"the connector check is meaningless without the helpers")
    module = ast.parse(body)
    header = ast.get_docstring(module) or ""
    if header and isinstance(module.body[0], ast.Expr):
        # The docstring is the notebook's markdown header, not a code cell.
        body = "".join(body.splitlines(keepends=True)[module.body[0].end_lineno:])
    sections = _split_cells(body)
    params = next((src for title, src in sections if title == "parameters"),
                  None)
    if params is None:
        raise ValueError(f"{DIAGNOSE_SOURCE_NAME}: no `# %% parameters` cell")
    for key, value in (overrides or {}).items():
        name = DIAGNOSE_PARAMS.get(key)
        if name and value is not None:
            params = _set_parameter(params, name, str(value))

    header += (f"\n\n---\n\n"
               f"*Generated from `engine/dataplane/{DIAGNOSE_SOURCE_NAME}` by "
               f"`engine/target/stage_notebooks.py`. Regenerate with "
               f"`snowmig.py build-notebooks`; do not hand-edit — an edit here "
               f"is overwritten on the next build. Change the source instead.*")
    cells = [
        _md(header), _code(params.strip("\n")),
        _md("## Shared source helpers\n\nInlined from "
            "`engine/dataplane/snowmig_source.py` so this notebook runs "
            "with nothing else uploaded beside it."),
        _code((root / SHARED_SOURCE_NAME).read_text(encoding="utf-8")),
    ]
    lead = sections[0][1].strip("\n")  # the imports, ahead of the first check
    for title, src in sections[1:]:
        if title == "parameters":
            continue
        if title.startswith("[markdown]"):
            heading = title[len("[markdown]"):].strip()
            text = "\n".join(line[2:] if line.startswith("# ") else line.lstrip("#")
                             for line in src.strip("\n").splitlines())
            cells.append(_md((f"## {heading}\n\n" if heading else "") + text))
            continue
        rule = "-" * max(3, 74 - len(title))
        code = f"# --- {title} {rule}\n{src.strip(chr(10))}"
        if lead:
            code, lead = f"{lead}\n\n{code}", ""
        cells.append(_code(code))
    return {"cells": cells,
            "metadata": {"snowmig": {"generated": True, "stage": "diagnose",
                                     "source": DIAGNOSE_SOURCE_NAME,
                                     "job": None},
                         "kernelspec": {"display_name": "Python 3",
                                        "language": "python",
                                        "name": "python3"},
                         "language_info": {"name": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


def _write_notebook(path: pathlib.Path, nb: dict) -> pathlib.Path:
    # LF on every platform. `write_text` translates to CRLF on Windows, and
    # the committed notebooks are LF, so a rebuild there showed every line
    # changed.
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(nb, indent=1) + "\n")
    return path


def write_stage_notebooks(out_dir: str | pathlib.Path,
                          dataplane: pathlib.Path | None = None,
                          overrides: dict[str, object] | None = None
                          ) -> list[pathlib.Path]:
    """Write every stage notebook, plus the environment diagnosis, into
    `out_dir`. Returns the paths written."""
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for stage in STAGES:
        nb = build_stage_notebook(stage, dataplane, overrides)
        written.append(_write_notebook(out / stage.notebook_name, nb))
    written.append(_write_notebook(
        out / DIAGNOSE_NOTEBOOK_NAME,
        build_diagnose_notebook(dataplane, overrides)))
    return written
