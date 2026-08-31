"""Immutable POSIX content-addressed artifact storage."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from collections.abc import AsyncIterable, Callable, Iterator
from pathlib import Path
from typing import BinaryIO, TypeVar

from seqevi.errors import StoreIntegrityError
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
    StoredArtifact,
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_T = TypeVar("_T")


async def _upload_io(operation: Callable[[], _T]) -> _T:
    """Drain admitted I/O even under repeated cancellation; never abandon a syscall."""
    work = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(work)
    except asyncio.CancelledError as cancelled:
        while not work.done():
            try:
                await asyncio.shield(work)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            work.result()
        except BaseException as error:
            cancelled.add_note(
                f"artifact I/O failed during cancellation: {type(error).__name__}"
            )
        raise


class PosixArtifactStore:
    """Store exact bytes under deterministic SHA-256 paths."""

    def __init__(self, root: Path, *, maximum_concurrent_uploads: int = 8) -> None:
        """Bound admitted async uploads, not other users of the loop's executor.

        Examples:
            ``PosixArtifactStore(Path("cache"), maximum_concurrent_uploads=2)``
            admits at most two concurrent streaming uploads. Synchronous local
            Store operations are unchanged.
        """
        if maximum_concurrent_uploads < 1:
            raise ValueError("maximum_concurrent_uploads must be positive")
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._upload_slots = asyncio.Semaphore(maximum_concurrent_uploads)

    def put(self, artifact: ArtifactFile) -> StoredArtifact:
        """Stream one caller-owned file into the content-addressed Store."""

        digest = artifact.digest
        target = self._path_for_digest(digest)
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            self._verify_existing(target, digest)
            if target.stat().st_size != artifact.byte_size:
                raise StoreIntegrityError(f"artifact byte size mismatch: {digest}")
        else:
            self._link_new_artifact(target, artifact)

        return StoredArtifact(
            digest=digest,
            media_type=artifact.media_type,
            byte_size=artifact.byte_size,
            relative_path=target.relative_to(self.root).as_posix(),
        )

    def reference(self, artifact: StoredArtifact) -> ArtifactFile:
        """Return an integrity-checked Store-owned file reference."""

        if artifact.storage_kind != "posix":
            raise StoreIntegrityError("OCI artifact is not a local POSIX reference")
        target = self._path_for_digest(artifact.digest)
        self._verify_existing(target, artifact.digest)
        if target.stat().st_size != artifact.byte_size:
            raise StoreIntegrityError(f"artifact byte size mismatch: {artifact.digest}")
        return ArtifactFile(
            path=target,
            media_type=artifact.media_type,
            byte_size=artifact.byte_size,
            digest=artifact.digest,
            lifetime=ArtifactLifetime.STORE,
        )

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

    def describe_registered(self, artifact: StoredArtifact) -> StoredArtifact:
        """Check known POSIX metadata without rehashing bytes during OCI finalize.

        Notes:
            Only previously registered, ingestion-verified artifacts may use
            this path. New direct uploads still require ``describe_existing``;
            downloads independently verify the full content digest.
        """
        if artifact.storage_kind != "posix":
            raise StoreIntegrityError("registered artifact is not POSIX")
        target = self._path_for_digest(artifact.digest)
        if target.is_symlink() or not target.is_file():
            raise StoreIntegrityError("registered POSIX artifact is missing")
        if (
            target.relative_to(self.root).as_posix() != artifact.relative_path
            or target.stat().st_size != artifact.byte_size
        ):
            raise StoreIntegrityError("registered POSIX artifact metadata differs")
        return artifact

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
        """Stream verified bytes without filesystem work on the event loop.

        Examples:
            ``stored, created = await store.put_async(chunks,
            expected_digest=digest, expected_size=size, media_type="text/plain",
            maximum_size=512 * 1024 * 1024)`` publishes only exact bytes.

        Notes:
            Each admitted upload submits at most one blocking operation at a
            time. Cancellation drains that operation and closes/unlinks the
            temporary file before releasing its slot. Uninterruptible I/O can
            delay cancellation; the default executor is not Store-exclusive.
        """
        if expected_size < 0 or expected_size > maximum_size:
            raise ValueError("artifact exceeds configured upload limit")
        writer = _ArtifactUpload(self, expected_digest, expected_size, maximum_size)
        async with self._upload_slots:
            primary: BaseException | None = None
            try:
                exists = await _upload_io(writer.start)
                if exists:
                    created = False
                else:
                    async for chunk in chunks:
                        await _upload_io(lambda: writer.write(chunk))
                    created = await _upload_io(writer.finish)
            except BaseException as error:
                primary = error
                raise
            finally:
                try:
                    await _upload_io(writer.close)
                except BaseException as cleanup_error:
                    if primary is None:
                        raise
                    primary.add_note(
                        f"artifact cleanup failed: {type(cleanup_error).__name__}"
                    )
        return (
            StoredArtifact(
                expected_digest,
                media_type,
                expected_size,
                writer.target.relative_to(self.root).as_posix(),
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

    def _link_new_artifact(self, target: Path, artifact: ArtifactFile) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".seqevi-artifact-", dir=target.parent
        )
        temporary = Path(temporary_name)
        hasher = hashlib.sha256()
        byte_size = 0
        try:
            with (
                artifact.path.open("rb") as source,
                os.fdopen(descriptor, "wb") as target_handle,
            ):
                while chunk := source.read(1024 * 1024):
                    byte_size += len(chunk)
                    hasher.update(chunk)
                    target_handle.write(chunk)
                target_handle.flush()
                os.fsync(target_handle.fileno())
            if byte_size != artifact.byte_size:
                raise StoreIntegrityError(
                    f"artifact byte size changed during Store copy: {artifact.digest}"
                )
            if hasher.hexdigest() != artifact.digest:
                raise StoreIntegrityError(
                    f"artifact digest changed during Store copy: {artifact.digest}"
                )
            try:
                os.link(temporary, target)
                self._fsync_directory(target.parent)
            except FileExistsError:
                self._verify_existing(target, artifact.digest)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class _ArtifactUpload:
    """Own temporary bytes across off-loop calls, including cancelled setup."""

    def __init__(
        self, store: PosixArtifactStore, digest: str, size: int, maximum_size: int
    ) -> None:
        self.store = store
        self.digest = digest
        self.size = size
        self.maximum_size = maximum_size
        self.target = store._path_for_digest(digest)
        self.temporary: Path | None = None
        self.descriptor: int | None = None
        self.handle: BinaryIO | None = None
        self.hasher = hashlib.sha256()
        self.byte_size = 0

    def start(self) -> bool:
        self.target.parent.mkdir(parents=True, exist_ok=True)
        if self.target.exists():
            self.store._verify_existing(self.target, self.digest)
            if self.target.stat().st_size != self.size:
                raise StoreIntegrityError(
                    f"existing artifact byte size mismatch: {self.digest}"
                )
            self.store._fsync_directory(self.target.parent)
            return True
        self.descriptor, name = tempfile.mkstemp(
            prefix=".seqevi-upload-", dir=self.target.parent
        )
        self.temporary = Path(name)
        self.handle = os.fdopen(self.descriptor, "wb")
        self.descriptor = None
        return False

    def write(self, chunk: bytes) -> None:
        assert self.handle is not None
        self.byte_size += len(chunk)
        if self.byte_size > self.maximum_size or self.byte_size > self.size:
            raise ValueError(
                "artifact exceeds declared size or configured upload limit"
            )
        self.hasher.update(chunk)
        self.handle.write(chunk)

    def finish(self) -> bool:
        assert self.handle is not None and self.temporary is not None
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.close()
        if self.byte_size != self.size:
            raise StoreIntegrityError(
                f"artifact byte size mismatch: expected {self.size}, got {self.byte_size}"
            )
        if self.hasher.hexdigest() != self.digest:
            raise StoreIntegrityError(f"artifact digest mismatch: {self.digest}")
        try:
            os.link(self.temporary, self.target)
        except FileExistsError:
            self.store._verify_existing(self.target, self.digest)
            self.store._fsync_directory(self.target.parent)
            return False
        # A failed directory sync may leave a visible target. Never delete it:
        # the next PUT must verify that target, not assume publication failed.
        self.store._fsync_directory(self.target.parent)
        return True

    def close(self) -> None:
        try:
            if self.handle is not None:
                self.handle.close()
            elif self.descriptor is not None:
                os.close(self.descriptor)
        finally:
            if self.temporary is not None:
                self.temporary.unlink(missing_ok=True)
