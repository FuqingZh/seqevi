"""Immutable POSIX content-addressed artifact storage."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import AsyncIterable, Iterator
from pathlib import Path

from seqevi.errors import StoreIntegrityError
from seqevi.evidence import ArtifactPayload, StoredArtifact

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class PosixArtifactStore:
    """Store exact bytes under deterministic SHA-256 paths."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, payload: ArtifactPayload) -> StoredArtifact:
        digest = payload.digest
        target = self._path_for_digest(digest)
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            self._verify_existing(target, digest)
        else:
            self._link_new_artifact(target, payload.data)

        return StoredArtifact(
            digest=digest,
            media_type=payload.media_type,
            byte_size=len(payload.data),
            relative_path=target.relative_to(self.root).as_posix(),
        )

    def read(self, digest: str) -> bytes:
        target = self._path_for_digest(digest)
        if target.is_symlink() or not target.is_file():
            raise StoreIntegrityError(f"artifact is missing: {digest}")
        data = target.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise StoreIntegrityError(f"artifact digest mismatch: {digest}")
        return data

    def describe_existing(
        self, *, digest: str, media_type: str, byte_size: int
    ) -> StoredArtifact:
        """Verify and describe an artifact already uploaded to this CAS."""

        target = self._path_for_digest(digest)
        self._verify_existing(target, digest)
        if target.stat().st_size != byte_size:
            raise StoreIntegrityError(f"artifact byte size mismatch: {digest}")
        return StoredArtifact(
            digest=digest,
            media_type=media_type,
            byte_size=byte_size,
            relative_path=target.relative_to(self.root).as_posix(),
        )

    def iter_bytes(
        self, digest: str, *, chunk_size: int = 1024 * 1024
    ) -> Iterator[bytes]:
        """Yield one verified artifact without loading it into memory."""

        target = self._path_for_digest(digest)
        if target.is_symlink() or not target.is_file():
            raise StoreIntegrityError(f"artifact is missing: {digest}")
        hasher = hashlib.sha256()
        with target.open("rb") as handle:
            while chunk := handle.read(chunk_size):
                hasher.update(chunk)
                yield chunk
        if hasher.hexdigest() != digest:
            raise StoreIntegrityError(f"artifact digest mismatch: {digest}")

    async def put_async(
        self,
        chunks: AsyncIterable[bytes],
        *,
        expected_digest: str,
        expected_size: int,
        media_type: str,
        maximum_size: int,
    ) -> tuple[StoredArtifact, bool]:
        """Stream a digest-verified artifact into the POSIX CAS."""

        target = self._path_for_digest(expected_digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            self._verify_existing(target, expected_digest)
            if target.stat().st_size != expected_size:
                raise StoreIntegrityError(
                    f"existing artifact byte size mismatch: {expected_digest}"
                )
            return (
                StoredArtifact(
                    expected_digest,
                    media_type,
                    expected_size,
                    target.relative_to(self.root).as_posix(),
                ),
                False,
            )

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".seqevi-upload-", dir=target.parent
        )
        temporary = Path(temporary_name)
        hasher = hashlib.sha256()
        byte_size = 0
        try:
            with os.fdopen(descriptor, "wb") as handle:
                async for chunk in chunks:
                    byte_size += len(chunk)
                    if byte_size > maximum_size:
                        raise ValueError("artifact exceeds configured upload limit")
                    hasher.update(chunk)
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            if byte_size != expected_size:
                raise StoreIntegrityError(
                    f"artifact byte size mismatch: expected {expected_size}, got {byte_size}"
                )
            if hasher.hexdigest() != expected_digest:
                raise StoreIntegrityError(
                    f"artifact digest mismatch: {expected_digest}"
                )
            try:
                os.link(temporary, target)
                created = True
                self._fsync_directory(target.parent)
            except FileExistsError:
                self._verify_existing(target, expected_digest)
                created = False
        finally:
            temporary.unlink(missing_ok=True)

        return (
            StoredArtifact(
                expected_digest,
                media_type,
                byte_size,
                target.relative_to(self.root).as_posix(),
            ),
            created,
        )

    def _path_for_digest(self, digest: str) -> Path:
        if not _SHA256_PATTERN.fullmatch(digest):
            raise ValueError("artifact digest must be a lowercase SHA-256 digest")
        return self.root / "sha256" / digest[:2] / digest[2:4] / digest

    def _verify_existing(self, target: Path, digest: str) -> None:
        if target.is_symlink() or not target.is_file():
            raise StoreIntegrityError(f"artifact path is not a regular file: {digest}")
        hasher = hashlib.sha256()
        with target.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        if hasher.hexdigest() != digest:
            raise StoreIntegrityError(f"existing artifact is corrupt: {digest}")

    def _link_new_artifact(self, target: Path, data: bytes) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".seqevi-artifact-", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
                self._fsync_directory(target.parent)
            except FileExistsError:
                self._verify_existing(target, hashlib.sha256(data).hexdigest())
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
