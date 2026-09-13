#!/usr/bin/env python3
"""CI guard: fail the build if the installed littledevil-shared doesn't match
this repo's pinned version. Run before tests in every CI job.

docs/repo-structure.md §3: "CI in each service repo pins its
littledevil-shared version explicitly and fails the build on a mismatch,
rather than silently running against whatever happened to be installed."
"""

from __future__ import annotations

import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def pinned_version() -> str:
    text = PYPROJECT.read_text()
    match = re.search(r"littledevil-shared\s*@[^#\n]*?@v([\d.]+)", text)
    if not match:
        match = re.search(r'"littledevil-shared==([\d.]+)"', text)
    if not match:
        print("could not find a littledevil-shared version pin in pyproject.toml", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def main() -> None:
    pinned = pinned_version()
    try:
        installed = version("littledevil-shared")
    except PackageNotFoundError:
        print("littledevil-shared is not installed", file=sys.stderr)
        sys.exit(1)

    if installed != pinned:
        print(
            f"version mismatch: pyproject.toml pins littledevil-shared=={pinned}, "
            f"but {installed} is installed",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"littledevil-shared=={installed} matches pin")


if __name__ == "__main__":
    main()
