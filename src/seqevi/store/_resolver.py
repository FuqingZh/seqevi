"""Private subprocess-backed resolver for ClaimSession HTTP connections."""

from __future__ import annotations

import json
import socket
import sys


def main() -> int:
    if len(sys.argv) != 7:
        return 2
    host, raw_port, raw_family, raw_type, raw_proto, raw_flags = sys.argv[1:]
    try:
        resolved = socket.getaddrinfo(
            host,
            int(raw_port),
            family=int(raw_family),
            type=int(raw_type),
            proto=int(raw_proto),
            flags=int(raw_flags),
        )
        payload = [
            [family, socktype, proto, canonname, sockaddr]
            for family, socktype, proto, canonname, sockaddr in resolved
        ]
        sys.stdout.write(json.dumps(payload, separators=(",", ":")))
        return 0
    except BaseException as error:
        sys.stderr.write(f"{type(error).__name__}: {error}"[:4096])
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
