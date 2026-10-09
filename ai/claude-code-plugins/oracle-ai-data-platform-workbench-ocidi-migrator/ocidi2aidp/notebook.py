"""A minimal nbformat-4 writer. No nbformat dependency: the format is small and
stable, and the shipped package must import with nothing installed."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from . import __version__


@dataclass
class Cell:
    kind: str                    # code | markdown
    source: str
    tags: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)


@dataclass
class Notebook:
    title: str
    cells: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    def md(self, text: str, **meta) -> Cell:
        cell = Cell("markdown", text.rstrip() + "\n", meta=meta)
        self.cells.append(cell)
        return cell

    def code(self, text: str, tags=None, **meta) -> Cell:
        cell = Cell("code", text.rstrip() + "\n", list(tags or []), meta)
        self.cells.append(cell)
        return cell

    def to_ipynb(self) -> dict:
        cells = []
        for i, c in enumerate(self.cells):
            lines = c.source.splitlines(keepends=True)
            if lines and lines[-1].endswith("\n"):
                lines[-1] = lines[-1][:-1]
            meta = dict(c.meta)
            if c.tags:
                meta["tags"] = c.tags
            cid = hashlib.sha1(f"{i}:{c.source}".encode()).hexdigest()[:12]
            cell = {"cell_type": c.kind, "id": cid, "metadata": meta, "source": lines}
            if c.kind == "code":
                cell["execution_count"] = None
                cell["outputs"] = []
            cells.append(cell)
        return {
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {"name": "python"},
                "ocidi2aidp": {"version": __version__, **self.meta},
            },
            "cells": cells,
        }

    def write(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_ipynb(), indent=1, ensure_ascii=False) + "\n",
                        encoding="utf-8")


def read_ipynb(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cell_text(cell: dict) -> str:
    src = cell.get("source", "")
    return "".join(src) if isinstance(src, list) else str(src)


def write_ipynb(path: Path, nb: dict) -> None:
    Path(path).write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
