from __future__ import annotations

from pathlib import Path

import pytest

from seqevi.errors import StoreIntegrityError
from seqevi.evidence import ArtifactPayload
from seqevi.store.artifact import PosixArtifactStore


def test_artifact_store_round_trip_is_content_addressed(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = ArtifactPayload(b"normalized evidence", "application/x-parquet")

    first = store.put(payload)
    second = store.put(payload)

    assert first == second
    assert first.relative_path == (
        f"sha256/{first.digest[:2]}/{first.digest[2:4]}/{first.digest}"
    )
    assert store.read(first.digest) == payload.data


def test_artifact_store_detects_corrupt_existing_bytes(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")
    payload = ArtifactPayload(b"expected", "text/plain")
    artifact = store.put(payload)
    artifact_path = store.root / artifact.relative_path
    artifact_path.write_bytes(b"corrupt")

    with pytest.raises(StoreIntegrityError, match="digest mismatch"):
        store.read(artifact.digest)
    with pytest.raises(StoreIntegrityError, match="corrupt"):
        store.put(payload)


def test_artifact_store_rejects_non_digest_paths(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="SHA-256"):
        store.read("../../outside")
