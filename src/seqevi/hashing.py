"""Incremental file hashing shared by runtime and output boundaries."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: Path) -> str:
    """Return SHA-256 for a file without materializing its bytes."""

    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()
