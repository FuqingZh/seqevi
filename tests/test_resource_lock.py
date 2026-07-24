from __future__ import annotations

import hashlib
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import seqevi.resource_lock
from seqevi.errors import ResourceLockError
from seqevi.resource_lock import (
    LOCK_FILENAME,
    ResourceComponent,
    resolve_resource_lock,
)


def _components() -> tuple[ResourceComponent, ...]:
    return (
        ResourceComponent("annotations", "annotations.db"),
        ResourceComponent("search", "search.dmnd"),
    )


def _write_database(root: Path) -> Path:
    root.mkdir()
    (root / "annotations.db").write_bytes(b"annotations-v1")
    (root / "search.dmnd").write_bytes(b"search-v1")
    return root


def _resolve(database: Path, *, verify: bool = False):
    return resolve_resource_lock(
        database=database,
        resource_name="fixture",
        resource_version="1.0",
        components=_components(),
        verify=verify,
    )


def test_resource_lock_is_created_and_reused_without_content_hashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = _write_database(tmp_path / "database")
    created = _resolve(database)

    lock_path = database / LOCK_FILENAME
    assert created.persisted is True
    assert created.verified is True
    assert lock_path.is_file()
    with lock_path.open("rb") as handle:
        document = tomllib.load(handle)
    assert document["resource"] == {"name": "fixture", "version": "1.0"}
    assert [item["path"] for item in document["component"]] == [
        "annotations.db",
        "search.dmnd",
    ]

    def fail_hash(path: Path) -> str:
        raise AssertionError(f"unexpected content hash: {path}")

    monkeypatch.setattr(seqevi.resource_lock, "_file_sha256", fail_hash)
    reused = _resolve(database)
    assert reused.components == created.components
    assert reused.verified is False


def test_full_verification_detects_same_size_content_change(tmp_path: Path) -> None:
    database = _write_database(tmp_path / "database")
    locked = _resolve(database)
    (database / "search.dmnd").write_bytes(b"SEARCH-v1")

    cached = _resolve(database)
    assert cached.components == locked.components
    with pytest.raises(ResourceLockError, match=r"SHA-256.*search\.dmnd"):
        _resolve(database, verify=True)


def test_lock_rejects_size_change_without_rereading_content(tmp_path: Path) -> None:
    database = _write_database(tmp_path / "database")
    _resolve(database)
    (database / "annotations.db").write_bytes(b"changed-size")

    with pytest.raises(ResourceLockError, match=r"size.*annotations\.db"):
        _resolve(database)


@pytest.mark.parametrize(
    "replacement",
    [
        'path = "../outside.db"',
        'path = "/tmp/outside.db"',
        'sha256 = "invalid"',
    ],
)
def test_lock_rejects_invalid_schema_values(tmp_path: Path, replacement: str) -> None:
    database = _write_database(tmp_path / "database")
    _resolve(database)
    lock_path = database / LOCK_FILENAME
    text = lock_path.read_text(encoding="utf-8")
    if replacement.startswith("path"):
        text = text.replace('path = "annotations.db"', replacement, 1)
    else:
        original = hashlib.sha256(b"annotations-v1").hexdigest()
        text = text.replace(f'sha256 = "{original}"', replacement, 1)
    lock_path.write_text(text, encoding="utf-8")

    with pytest.raises(ResourceLockError):
        _resolve(database)


def test_lock_rejects_resource_or_component_conflicts(tmp_path: Path) -> None:
    database = _write_database(tmp_path / "database")
    _resolve(database)

    with pytest.raises(ResourceLockError, match="resource identity conflicts"):
        resolve_resource_lock(
            database=database,
            resource_name="fixture",
            resource_version="2.0",
            components=_components(),
        )
    with pytest.raises(ResourceLockError, match="path conflicts"):
        resolve_resource_lock(
            database=database,
            resource_name="fixture",
            resource_version="1.0",
            components=(
                ResourceComponent("annotations", "search.dmnd"),
                ResourceComponent("search", "annotations.db"),
            ),
        )


def test_first_use_creation_is_atomic_under_same_content_concurrency(
    tmp_path: Path,
) -> None:
    database = _write_database(tmp_path / "database")

    with ThreadPoolExecutor(max_workers=4) as executor:
        locks = tuple(executor.map(lambda _: _resolve(database), range(4)))

    assert len({lock.components for lock in locks}) == 1
    assert all(lock.persisted for lock in locks)
    assert (database / LOCK_FILENAME).is_file()


def test_read_only_publication_falls_back_to_current_run_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = _write_database(tmp_path / "database")
    monkeypatch.setattr(seqevi.resource_lock, "_publish_lock", lambda path, lock: False)

    resolved = _resolve(database)

    assert resolved.persisted is False
    assert resolved.verified is True
    assert not (database / LOCK_FILENAME).exists()


def test_component_symlink_must_remain_inside_database(tmp_path: Path) -> None:
    outside = tmp_path / "outside.db"
    outside.write_bytes(b"outside")
    database = tmp_path / "database"
    database.mkdir()
    (database / "annotations.db").symlink_to(outside)
    (database / "search.dmnd").write_bytes(b"search-v1")

    with pytest.raises(ResourceLockError, match="escapes the database root"):
        _resolve(database)
