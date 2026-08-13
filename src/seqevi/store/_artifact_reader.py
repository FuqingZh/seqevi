"""Private framed artifact reader process for cancellable HTTP uploads."""

from __future__ import annotations

import os
import struct
import sys
from pathlib import Path

_CHUNK_SIZE = 1024 * 1024
_HEADER = struct.Struct("!q")
_EOF = -1
_ERROR = -2


def _write(content: bytes | memoryview) -> None:
    view = memoryview(content)
    while view:
        written = os.write(sys.stdout.fileno(), view)
        view = view[written:]


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    try:
        with Path(sys.argv[1]).open("rb") as handle:
            while chunk := handle.read(_CHUNK_SIZE):
                _write(_HEADER.pack(len(chunk)))
                _write(chunk)
        _write(_HEADER.pack(_EOF))
        return 0
    except BaseException as error:
        detail = f"{type(error).__name__}: {error}".encode("utf-8", errors="replace")[
            :4096
        ]
        try:
            _write(_HEADER.pack(_ERROR))
            _write(_HEADER.pack(len(detail)))
            _write(detail)
        except OSError:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
