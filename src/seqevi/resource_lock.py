"""Immutable database resource inventory used to avoid repeated large-file hashes."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import tempfile
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath

from .errors import ResourceLockError

LOCK_FILENAME = "seqevi.lock"
LOCK_VERSION = 1

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_READ_ONLY_ERRNOS = {errno.EACCES, errno.EPERM, errno.EROFS}


@dataclass(frozen=True, slots=True)
class ResourceComponent:
    """One named file declared by an adapter as part of a database resource."""

    name: str
    relative_path: str


@dataclass(frozen=True, slots=True)
class LockedResourceComponent:
    """One immutable component captured in a resource lock."""

    name: str
    relative_path: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ResourceLock:
    """Validated resource identity and component hashes for one database root."""

    resource_name: str
    resource_version: str
    components: tuple[LockedResourceComponent, ...]
    persisted: bool
    verified: bool

    def hash_for(self, name: str) -> str:
        """Return the locked digest for one adapter-declared component."""

        for component in self.components:
            if component.name == name:
                return component.sha256
        raise ResourceLockError(f"seqevi.lock has no component named {name!r}")


def resource_lock_path(database: Path) -> Path:
    """Return the SeqEvi lock path for an exact database root."""

    return database / LOCK_FILENAME


def resolve_resource_lock(
    *,
    database: Path,
    resource_name: str,
    resource_version: str,
    components: tuple[ResourceComponent, ...],
    verify: bool = False,
) -> ResourceLock:
    """Resolve component hashes from a lock or create one from native files.

    Existing locks are immutable. Normal resolution validates metadata without
    rereading file contents; ``verify=True`` performs a full content audit.
    """

    database = database.resolve()
    if not database.is_dir():
        raise ResourceLockError(f"resource database is not a directory: {database}")
    resource_name = _require_nonempty_string(resource_name, "resource name")
    resource_version = _require_nonempty_string(resource_version, "resource version")
    declarations = _validate_declarations(components)
    lock_path = resource_lock_path(database)
    if lock_path.is_symlink():
        raise ResourceLockError(f"resource lock must not be a symlink: {lock_path}")

    if lock_path.exists():
        locked = _read_lock(lock_path)
        _validate_lock_identity(
            locked,
            resource_name=resource_name,
            resource_version=resource_version,
        )
        _validate_declared_components(locked, declarations)
        _validate_component_files(database, locked.components, verify=verify)
        return replace(locked, persisted=True, verified=verify)

    locked = ResourceLock(
        resource_name=resource_name,
        resource_version=resource_version,
        components=tuple(
            _hash_component(database, declaration) for declaration in declarations
        ),
        persisted=False,
        verified=True,
    )
    published = _publish_lock(lock_path, locked)
    if not published:
        return locked

    persisted = _read_lock(lock_path)
    _validate_lock_equivalence(persisted, locked)
    return replace(persisted, persisted=True, verified=True)


def _validate_declarations(
    components: tuple[ResourceComponent, ...],
) -> tuple[ResourceComponent, ...]:
    if not components:
        raise ResourceLockError("a resource lock requires at least one component")
    names: set[str] = set()
    paths: set[str] = set()
    validated = []
    for component in components:
        name = _require_nonempty_string(component.name, "component name")
        relative_path = _validate_relative_path(component.relative_path)
        if name in names:
            raise ResourceLockError(f"duplicate resource component name: {name}")
        if relative_path in paths:
            raise ResourceLockError(
                f"duplicate resource component path: {relative_path}"
            )
        names.add(name)
        paths.add(relative_path)
        validated.append(ResourceComponent(name=name, relative_path=relative_path))
    return tuple(validated)


def _read_lock(path: Path) -> ResourceLock:
    if path.is_symlink():
        raise ResourceLockError(f"resource lock must not be a symlink: {path}")
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ResourceLockError(f"cannot read resource lock {path}: {error}") from error
    if set(document) != {"lock_version", "resource", "component"}:
        raise ResourceLockError(
            "seqevi.lock must contain only lock_version, resource and component"
        )
    lock_version = document["lock_version"]
    if (
        isinstance(lock_version, bool)
        or not isinstance(lock_version, int)
        or lock_version != LOCK_VERSION
    ):
        raise ResourceLockError(f"unsupported seqevi.lock version: {lock_version!r}")
    resource = document["resource"]
    if not isinstance(resource, dict) or set(resource) != {"name", "version"}:
        raise ResourceLockError("seqevi.lock resource must contain name and version")
    raw_components = document["component"]
    if not isinstance(raw_components, list) or not raw_components:
        raise ResourceLockError("seqevi.lock must declare at least one component")

    components = []
    declarations = []
    for index, raw in enumerate(raw_components, start=1):
        if not isinstance(raw, dict) or set(raw) != {
            "name",
            "path",
            "size",
            "sha256",
        }:
            raise ResourceLockError(
                f"seqevi.lock component {index} has an invalid schema"
            )
        name = _require_nonempty_string(raw["name"], f"component {index} name")
        relative_path = _validate_relative_path(raw["path"])
        size = raw["size"]
        digest = raw["sha256"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ResourceLockError(
                f"seqevi.lock component {name!r} has an invalid size"
            )
        if not isinstance(digest, str) or not _SHA256_PATTERN.fullmatch(digest):
            raise ResourceLockError(
                f"seqevi.lock component {name!r} has an invalid SHA-256"
            )
        declarations.append(ResourceComponent(name, relative_path))
        components.append(LockedResourceComponent(name, relative_path, size, digest))
    _validate_declarations(tuple(declarations))
    return ResourceLock(
        resource_name=_require_nonempty_string(resource["name"], "resource name"),
        resource_version=_require_nonempty_string(
            resource["version"], "resource version"
        ),
        components=tuple(components),
        persisted=True,
        verified=False,
    )


def _validate_lock_identity(
    lock: ResourceLock, *, resource_name: str, resource_version: str
) -> None:
    if (lock.resource_name, lock.resource_version) != (
        resource_name,
        resource_version,
    ):
        raise ResourceLockError(
            "seqevi.lock resource identity conflicts with the selected database: "
            f"locked {lock.resource_name}/{lock.resource_version}, requested "
            f"{resource_name}/{resource_version}"
        )


def _validate_declared_components(
    lock: ResourceLock, declarations: tuple[ResourceComponent, ...]
) -> None:
    locked_by_name = {component.name: component for component in lock.components}
    for declaration in declarations:
        locked = locked_by_name.get(declaration.name)
        if locked is None:
            raise ResourceLockError(
                f"seqevi.lock is missing required component {declaration.name!r}"
            )
        if locked.relative_path != declaration.relative_path:
            raise ResourceLockError(
                f"seqevi.lock component {declaration.name!r} path conflicts with "
                f"the adapter declaration"
            )


def _validate_component_files(
    database: Path,
    components: tuple[LockedResourceComponent, ...],
    *,
    verify: bool,
) -> None:
    for component in components:
        path = _resolve_component_path(database, component.relative_path)
        try:
            size = path.stat().st_size
        except OSError as error:
            raise ResourceLockError(
                f"resource component does not exist: {component.relative_path}"
            ) from error
        if not path.is_file():
            raise ResourceLockError(
                f"resource component is not a file: {component.relative_path}"
            )
        if size != component.size:
            raise ResourceLockError(
                f"resource component size does not match seqevi.lock: "
                f"{component.relative_path}"
            )
        if verify:
            _, digest = _stable_file_sha256(path)
            if digest != component.sha256:
                raise ResourceLockError(
                    f"resource component SHA-256 does not match seqevi.lock: "
                    f"{component.relative_path}"
                )


def _hash_component(
    database: Path, declaration: ResourceComponent
) -> LockedResourceComponent:
    path = _resolve_component_path(database, declaration.relative_path)
    if not path.is_file():
        raise ResourceLockError(
            f"resource component does not exist: {declaration.relative_path}"
        )
    stat, digest = _stable_file_sha256(path)
    return LockedResourceComponent(
        name=declaration.name,
        relative_path=declaration.relative_path,
        size=stat.st_size,
        sha256=digest,
    )


def _resolve_component_path(database: Path, relative_path: str) -> Path:
    candidate = (database / relative_path).resolve()
    if not candidate.is_relative_to(database):
        raise ResourceLockError(
            f"resource component escapes the database root: {relative_path}"
        )
    return candidate


def _validate_relative_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        raise ResourceLockError(f"invalid resource component path: {raw!r}")
    path = PurePosixPath(raw)
    if (
        raw == "."
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ResourceLockError(f"invalid resource component path: {raw!r}")
    canonical = path.as_posix()
    if canonical != raw:
        raise ResourceLockError(f"resource component path is not canonical: {raw!r}")
    return canonical


def _require_nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ResourceLockError(f"invalid {label}: {value!r}")
    return value


def _publish_lock(path: Path, lock: ResourceLock) -> bool:
    encoded = _render_lock(lock).encode("utf-8")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{LOCK_FILENAME}.",
            dir=path.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = _read_lock(path)
            _validate_lock_equivalence(existing, lock)
        _fsync_directory(path.parent)
        return True
    except OSError as error:
        if error.errno in _READ_ONLY_ERRNOS:
            return False
        raise ResourceLockError(
            f"cannot publish resource lock {path}: {error}"
        ) from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _validate_lock_equivalence(actual: ResourceLock, expected: ResourceLock) -> None:
    if (
        actual.resource_name,
        actual.resource_version,
        actual.components,
    ) != (
        expected.resource_name,
        expected.resource_version,
        expected.components,
    ):
        raise ResourceLockError("concurrent seqevi.lock creation produced a conflict")


def _render_lock(lock: ResourceLock) -> str:
    lines = [
        f"lock_version = {LOCK_VERSION}",
        "",
        "[resource]",
        f"name = {_toml_string(lock.resource_name)}",
        f"version = {_toml_string(lock.resource_version)}",
    ]
    for component in lock.components:
        lines.extend(
            (
                "",
                "[[component]]",
                f"name = {_toml_string(component.name)}",
                f"path = {_toml_string(component.relative_path)}",
                f"size = {component.size}",
                f"sha256 = {_toml_string(component.sha256)}",
            )
        )
    return "\n".join(lines) + "\n"


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _stable_file_sha256(path: Path) -> tuple[os.stat_result, str]:
    before = path.stat()
    digest = _file_sha256(path)
    after = path.stat()
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise ResourceLockError(f"resource component changed while hashing: {path}")
    return after, digest


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
