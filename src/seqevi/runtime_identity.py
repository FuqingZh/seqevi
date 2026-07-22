"""Path-independent identity for result-producing external tool runtimes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .errors import AdapterError
from .evidence import sha256_digest


@dataclass(frozen=True, slots=True)
class RuntimeComponent:
    """One result-producing runtime file under a stable logical name."""

    name: str
    path: Path


def calculate_runtime_digest(
    *,
    runtime_name: str,
    versions: Mapping[str, str],
    components: tuple[RuntimeComponent, ...],
) -> str:
    """Hash normalized versions and component contents without installation paths."""

    if not runtime_name.strip() or runtime_name != runtime_name.strip():
        raise AdapterError(f"invalid runtime name: {runtime_name!r}")
    if not versions:
        raise AdapterError("runtime identity requires at least one version")
    normalized_versions = {}
    for name, value in versions.items():
        _validate_logical_name(name, label="runtime version")
        if not value.strip() or value != value.strip():
            raise AdapterError(f"invalid runtime version {name!r}: {value!r}")
        normalized_versions[name] = value
    if not components:
        raise AdapterError("runtime identity requires at least one component")

    seen_names: set[str] = set()
    component_digests = []
    for component in components:
        _validate_logical_name(component.name, label="runtime component")
        if component.name in seen_names:
            raise AdapterError(f"duplicate runtime component name: {component.name}")
        seen_names.add(component.name)
        path = component.path.resolve()
        if not path.is_file():
            raise AdapterError(f"runtime component is not a file: {path}")
        component_digests.append((component.name, _stable_file_sha256(path)))

    identity = {
        "components": sorted(component_digests),
        "runtime": runtime_name,
        "versions": dict(sorted(normalized_versions.items())),
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256_digest(encoded)


def _validate_logical_name(value: str, *, label: str) -> None:
    if not value or "\\" in value:
        raise AdapterError(f"invalid {label} name: {value!r}")
    path = PurePosixPath(value)
    if (
        value == "."
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
    ):
        raise AdapterError(f"invalid {label} name: {value!r}")


def _stable_file_sha256(path: Path) -> str:
    before = path.stat()
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    after = path.stat()
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if before_identity != after_identity:
        raise AdapterError(f"runtime component changed while hashing: {path}")
    return digest
