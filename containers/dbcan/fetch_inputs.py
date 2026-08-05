from __future__ import annotations

import hashlib
import json
import sys
import urllib.request
from pathlib import Path


def main() -> None:
    manifest_path, destination_text = sys.argv[1:]
    destination = Path(destination_text)
    destination.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    for artifact in manifest["artifacts"]:
        target = destination / artifact["name"]
        with urllib.request.urlopen(artifact["url"], timeout=120) as response:
            content = response.read()
        digest = hashlib.sha256(content).hexdigest()
        if digest != artifact["sha256"]:
            raise RuntimeError(f"{artifact['name']} SHA-256 mismatch: {digest}")
        target.write_bytes(content)


if __name__ == "__main__":
    main()
