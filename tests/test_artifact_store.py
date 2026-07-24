from __future__ import annotations

from pathlib import Path

import pytest

from seqevi.errors import StoreIntegrityError
from seqevi.store.artifact import PosixArtifactStore

from .support import write_artifact_file


def test_artifact_store_round_trip_is_content_addressed(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = write_artifact_file(
        tmp_path / "source.parquet",
        b"normalized evidence",
        "application/x-parquet",
    )

    first = store.put(payload)
    second = store.put(payload)

    assert first == second
    assert first.relative_path == (
        f"sha256/{first.digest[:2]}/{first.digest[2:4]}/{first.digest}"
    )
    assert store.reference(first).path.read_bytes() == payload.path.read_bytes()


def test_artifact_store_detects_corrupt_existing_bytes(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = write_artifact_file(tmp_path / "source.txt", b"expected", "text/plain")
    artifact = store.put(payload)
    artifact_path = store.root / artifact.relative_path
    artifact_path.write_bytes(b"corrupt")

    with pytest.raises(StoreIntegrityError, match="corrupt"):
        store.reference(artifact)
    with pytest.raises(StoreIntegrityError, match="corrupt"):
        store.put(payload)


def test_artifact_store_rejects_non_digest_paths(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="SHA-256"):
        list(store.iter_bytes("../../outside"))


def test_artifact_store_rejects_source_changed_after_reference(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = write_artifact_file(tmp_path / "source.txt", b"before", "text/plain")
    payload.path.write_bytes(b"after!")

    with pytest.raises(StoreIntegrityError, match="digest changed during Store copy"):
        store.put(payload)


def test_store_reference_outlives_caller_source(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = write_artifact_file(tmp_path / "source.txt", b"persistent", "text/plain")
    stored = store.put(payload)
    payload.path.unlink()

    referenced = store.reference(stored)

    assert referenced.path.read_bytes() == b"persistent"
