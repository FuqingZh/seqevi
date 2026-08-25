"""Check repository Markdown relative links without external dependencies."""

from __future__ import annotations

import re
import sys
from pathlib import Path


_MARKDOWN_LINK = re.compile(r"(?<!!)\[[^]]*\]\(([^)]+)\)")
_REMOTE_PREFIXES = ("http://", "https://", "mailto:", "#")


def find_broken_links(root: Path) -> tuple[int, list[tuple[Path, str]]]:
    """Return the checked-link count and missing relative targets."""
    paths = [root / "README.md", *(root / "docs").rglob("*.md")]
    checked = 0
    broken: list[tuple[Path, str]] = []
    for path in paths:
        for target in _MARKDOWN_LINK.findall(path.read_text()):
            if not target or target.startswith(_REMOTE_PREFIXES):
                continue
            checked += 1
            relative_target = target.split("#", 1)[0]
            if (
                relative_target
                and not (path.parent / relative_target).resolve().exists()
            ):
                broken.append((path.relative_to(root), target))
    return checked, broken


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    checked, broken = find_broken_links(root)
    print(f"markdown_relative_links={checked} broken={len(broken)}")
    for path, target in broken:
        print(f"{path}: {target}")
    sys.exit(bool(broken))


if __name__ == "__main__":
    main()
