"""Classify changed paths for proportional GitHub Actions validation."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from dataclasses import dataclass


_LIGHTWEIGHT_DOC_FILES = frozenset({"docs/README.md"})
_LIGHTWEIGHT_DOC_PREFIXES = (
    "docs/archive/",
    "docs/benchmarks/",
    "docs/how-to-guides/",
    "docs/implementation-plan/",
    "docs/tutorials/",
)
_PACKAGE_INPUT_FILES = frozenset({"LICENSE", "README.md", "pdm.lock", "pyproject.toml"})
_PACKAGE_INPUT_PREFIXES = ("benchmarks/", "src/")
_TEST_ONLY_FILES = frozenset({"AGENTS.md"})


@dataclass(frozen=True)
class ChangeScope:
    tests: bool
    distribution: bool
    dbcan: bool

    @property
    def name(self) -> str:
        if not self.tests:
            return "documentation"
        selected = [
            name
            for name, enabled in (
                ("tests", self.tests),
                ("distribution", self.distribution),
                ("dbcan", self.dbcan),
            )
            if enabled
        ]
        return "+".join(selected)


def classify_changes(paths: Iterable[str]) -> ChangeScope:
    """Return the CI jobs required to cover the changed repository paths."""
    changed = tuple(path for path in paths if path)
    if not changed:
        return ChangeScope(tests=True, distribution=True, dbcan=True)

    tests = False
    distribution = False
    dbcan = False
    for path in changed:
        if path in _LIGHTWEIGHT_DOC_FILES or path.startswith(_LIGHTWEIGHT_DOC_PREFIXES):
            continue

        tests = True
        if path in _PACKAGE_INPUT_FILES or path.startswith(_PACKAGE_INPUT_PREFIXES):
            distribution = True
            dbcan = True
        elif path in _TEST_ONLY_FILES:
            pass
        elif path in {".dockerignore", "Dockerfile"}:
            distribution = True
        elif path.startswith("containers/dbcan/"):
            dbcan = True
        elif path in {"scripts/ci_change_scope.py", ".github/workflows/ci.yml"}:
            distribution = True
            dbcan = True
        elif not path.startswith(("docs/", "scripts/", "tests/")):
            distribution = True
            dbcan = True

    return ChangeScope(tests=tests, distribution=distribution, dbcan=dbcan)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nul",
        action="store_true",
        help="read NUL-delimited paths from standard input",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    raw = sys.stdin.buffer.read()
    separator = b"\0" if args.nul else b"\n"
    paths = [part.decode() for part in raw.split(separator) if part]
    scope = classify_changes(paths)
    print(f"tests={str(scope.tests).lower()}")
    print(f"distribution={str(scope.distribution).lower()}")
    print(f"dbcan={str(scope.dbcan).lower()}")
    print(f"scope={scope.name}")


if __name__ == "__main__":
    main()
