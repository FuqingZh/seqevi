from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from seqevi.errors import StoreIntegrityError
from seqevi.store import artifact as artifact_module
from seqevi.store.artifact import PosixArtifactStore


async def _chunks(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _put(store: PosixArtifactStore, data: bytes = b"payload") -> None:
    await store.put_async(
        _chunks(data),
        expected_digest=hashlib.sha256(data).hexdigest(),
        expected_size=len(data),
        media_type="text/plain",
        maximum_size=1024,
    )


async def _wait_for(event: threading.Event) -> None:
    async with asyncio.timeout(2):
        while not event.is_set():
            await asyncio.sleep(0.005)


@pytest.mark.parametrize("operation", ["start", "write", "finish", "close"])
def test_upload_repeated_cancellation_drains_io_and_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    store = PosixArtifactStore(tmp_path, maximum_concurrent_uploads=1)
    entered = threading.Event()
    release = threading.Event()
    original = getattr(artifact_module._ArtifactUpload, operation)

    def block(writer: object, *args: object) -> object:
        entered.set()
        assert release.wait(3), "test must release its I/O worker"
        return original(writer, *args)

    monkeypatch.setattr(artifact_module._ArtifactUpload, operation, block)

    async def exercise() -> None:
        task = asyncio.create_task(_put(store))
        try:
            await _wait_for(entered)
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.01)
            assert not task.done(), "cancellation must retain ownership of blocked I/O"
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not list(tmp_path.rglob(".seqevi-upload-*"))
        await _put(store)

    asyncio.run(exercise())


def test_upload_admission_bounds_active_writers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PosixArtifactStore(tmp_path, maximum_concurrent_uploads=2)
    two_entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    starts = 0
    original = artifact_module._ArtifactUpload.start

    def block(writer: artifact_module._ArtifactUpload) -> bool:
        nonlocal starts
        with lock:
            starts += 1
            if starts == 2:
                two_entered.set()
        assert release.wait(3)
        return original(writer)

    monkeypatch.setattr(artifact_module._ArtifactUpload, "start", block)

    async def exercise() -> None:
        tasks = [asyncio.create_task(_put(store, bytes([i]))) for i in range(3)]
        try:
            await _wait_for(two_entered)
            await asyncio.sleep(0.02)
            assert starts == 2
            tasks[2].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[2]
        finally:
            release.set()
            await asyncio.gather(*tasks[:2])
        assert starts == 2
        assert not list(tmp_path.rglob(".seqevi-upload-*"))

    asyncio.run(exercise())


def test_upload_round_trip_rejects_changed_existing_bytes(tmp_path: Path) -> None:
    store = PosixArtifactStore(tmp_path)
    asyncio.run(_put(store))
    digest = hashlib.sha256(b"payload").hexdigest()
    target = tmp_path / "sha256" / digest[:2] / digest[2:4] / digest
    target.write_bytes(b"corrupt")
    with pytest.raises(StoreIntegrityError, match="corrupt"):
        asyncio.run(_put(store))


@pytest.mark.parametrize("existing", [True, False])
def test_existing_or_concurrently_published_upload_syncs_its_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: bool
) -> None:
    store = PosixArtifactStore(tmp_path)
    digest = hashlib.sha256(b"payload").hexdigest()
    target = tmp_path / "sha256" / digest[:2] / digest[2:4] / digest
    if existing:
        asyncio.run(_put(store))
    else:

        def competing_publish(_source: Path, destination: Path) -> None:
            destination.write_bytes(b"payload")
            raise FileExistsError

        monkeypatch.setattr(artifact_module.os, "link", competing_publish)
    synced: list[Path] = []
    monkeypatch.setattr(store, "_fsync_directory", synced.append)
    asyncio.run(_put(store))
    assert synced == [target.parent]


def test_cancel_during_primary_io_then_cancel_again_during_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PosixArtifactStore(tmp_path, maximum_concurrent_uploads=1)
    writing = threading.Event()
    release_write = threading.Event()
    cleaning = threading.Event()
    release_cleanup = threading.Event()
    original_write = artifact_module._ArtifactUpload.write
    original_close = artifact_module._ArtifactUpload.close

    def block_write(writer: artifact_module._ArtifactUpload, data: bytes) -> None:
        writing.set()
        assert release_write.wait(3)
        original_write(writer, data)

    def block_close(writer: artifact_module._ArtifactUpload) -> None:
        cleaning.set()
        assert release_cleanup.wait(3)
        original_close(writer)

    monkeypatch.setattr(artifact_module._ArtifactUpload, "write", block_write)
    monkeypatch.setattr(artifact_module._ArtifactUpload, "close", block_close)

    async def exercise() -> None:
        task = asyncio.create_task(_put(store))
        try:
            await _wait_for(writing)
            task.cancel()
            release_write.set()
            await _wait_for(cleaning)
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.01)
            assert not task.done()
            assert store._upload_slots.locked()
        finally:
            release_write.set()
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not store._upload_slots.locked()
        assert not list(tmp_path.rglob(".seqevi-upload-*"))

    asyncio.run(exercise())


@pytest.mark.parametrize("data,size", [(b"wrong", 7), (b"payload-extra", 7)])
def test_upload_invalid_bytes_never_publish(
    tmp_path: Path, data: bytes, size: int
) -> None:
    store = PosixArtifactStore(tmp_path)

    async def exercise() -> None:
        with pytest.raises((ValueError, StoreIntegrityError)):
            await store.put_async(
                _chunks(data),
                expected_digest=hashlib.sha256(b"payload").hexdigest(),
                expected_size=size,
                media_type="text/plain",
                maximum_size=1024,
            )

    asyncio.run(exercise())
    assert not [path for path in tmp_path.rglob("*") if path.is_file()]
