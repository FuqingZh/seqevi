"""Regressions for cancellation of real ORAS-owned child processes.

The fake Store is only the HTTP bootstrap.  The Registry adapter and ToolRunner
are real: each staged ORAS command is replaced with an isolated Python child
that records its PID then sleeps.  The assertions therefore cover process-group
termination/reap rather than only a pre-set cancellation Event.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import threading
import time
from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from seqevi.evidence import ArtifactFile, ArtifactLifetime, SessionEvidenceClaim
from seqevi.errors import StoreError
from seqevi.runner import ToolRunner
from seqevi.store.client import HttpEvidenceStore
from seqevi.store.oci import OciRegistry, OciRegistryError
from tests.test_oci_client import _commit


_HEALTH = {
    "status": "ok",
    "api_version": "v1",
    "maximum_batch_size": 100,
    "maximum_artifact_bytes": 1024 * 1024,
}
_DISCOVERY = {
    "protocol": "storage-discovery-v1",
    "artifact_backend": "oci-registry",
    "minimum_client_capability": "oci-artifacts-v1",
    "registry": {
        "id": "primary",
        "endpoint": "https://registry.example.test",
        "repository": "seqevi/artifacts",
    },
}


def _capabilities() -> dict[str, object]:
    return {
        "protocol": "claim-session-v1",
        "maximum_batch_size": 100,
        "retention_seconds": 60,
        "maximum_session_receipt_headers": 1000,
        "maximum_session_receipt_items": 32000,
        "server_time": datetime.now(UTC).isoformat(),
    }


def _open() -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "session_id": "session",
        "owner_token": "owner",
        "generation": 1,
        "expires_at": (now + timedelta(seconds=60)).isoformat(),
        "remaining_lease_seconds": 60.0,
        # Long enough for the staged child to become observable, short enough
        # to make the heartbeat-loss case bounded.
        "heartbeat_after_seconds": 0.5,
        "renew_deadline_seconds": 30.0,
    }


def _transport(*, lose_heartbeat: bool, seen: list[str]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json=_HEALTH, request=request)
        if path == "/v1/storage/discovery":
            return httpx.Response(200, json=_DISCOVERY, request=request)
        if path == "/v1/internal/claim-sessions/capabilities":
            return httpx.Response(200, json=_capabilities(), request=request)
        if path == "/v1/internal/claim-sessions/open":
            return httpx.Response(200, json=_open(), request=request)
        if path == "/v1/internal/claim-sessions/renew":
            if lose_heartbeat:
                return httpx.Response(
                    500, json={"detail": "renew failed"}, request=request
                )
            return httpx.Response(200, json=_open(), request=request)
        if path == "/v1/internal/claim-sessions/close":
            return httpx.Response(
                200,
                json={"session_id": "session", "generation": 1, "closed": True},
                request=request,
            )
        if path == "/v1/artifacts/resolve":
            return httpx.Response(200, json={"artifact": None}, request=request)
        if path == "/v1/internal/claim-sessions/finalize":
            raise AssertionError("staging cancellation must not metadata-finalize")
        raise AssertionError(f"unexpected request {request.method} {path}")

    return httpx.MockTransport(handler)


def _artifact(tmp_path: Path) -> ArtifactFile:
    data = b"child-cancellation"
    path = tmp_path / "artifact.bin"
    path.write_bytes(data)
    return ArtifactFile(
        path=path,
        media_type="application/octet-stream",
        byte_size=len(data),
        digest=hashlib.sha256(data).hexdigest(),
        lifetime=ArtifactLifetime.CALLER,
    )


def _wait_for_pid(path: Path, timeout: float = 2.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return int(path.read_text(encoding="ascii"))
        time.sleep(0.01)
    raise AssertionError("child did not publish a pid within bounded wait")


def _assert_reaped(pid: int) -> None:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if not Path(f"/proc/{pid}").exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"ORAS-owned child {pid} remains live")


@pytest.fixture
def real_registry_sleeping_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Keep real `OciRegistry._run`; only replace external ORAS argv/preflight."""

    marker = tmp_path / "oras-child.pid"
    code = (
        "import os,pathlib,sys,time;"
        "marker=pathlib.Path(sys.argv[1]);"
        "temporary=marker.with_suffix('.tmp');"
        "temporary.write_text(str(os.getpid()), encoding='ascii');"
        "temporary.replace(marker);"
        "time.sleep(60)"
    )
    monkeypatch.setattr(OciRegistry, "preflight", lambda self, **_kwargs: None)
    monkeypatch.setattr(
        OciRegistry,
        "_oras_args",
        lambda self, *_args: (sys.executable, "-I", "-c", code, str(marker)),
    )
    return marker


def _store(
    transport: httpx.MockTransport,
    *,
    async_transport: httpx.MockTransport | None = None,
) -> HttpEvidenceStore:
    store = HttpEvidenceStore(
        "https://store.example.test",
        _client=httpx.Client(
            transport=transport, base_url="https://store.example.test"
        ),
        _async_transport=async_transport or transport,
    )
    # The production runner keeps a five-second TERM grace; retain real process
    # containment but shorten only this sleeping-child regression's grace.
    assert store._registry is not None
    store._registry.runner = ToolRunner(termination_grace_seconds=0.1)
    return store


def _start_upload(
    store: HttpEvidenceStore,
    artifact: ArtifactFile,
    errors: list[BaseException],
    completed: threading.Event,
):
    def run() -> None:
        try:
            # Match the R2 call path: an ORAS stage owns an active Claim transport
            # operation while it is subject to Store/session cancellation.
            with store._claim_transport.operation():
                store._upload(artifact)
        except BaseException as error:
            errors.append(error)
        finally:
            completed.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker


def _assert_cancelled(
    worker: threading.Thread,
    errors: list[BaseException],
    pid: int,
    completed: threading.Event,
) -> None:
    assert completed.wait(3.0), "upload worker did not synchronously return"
    worker.join(timeout=0.1)
    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], OciRegistryError)
    assert errors[0].category == "cancelled"
    _assert_reaped(pid)


def test_store_close_cancels_real_oras_child_and_never_finalizes(
    tmp_path: Path, real_registry_sleeping_child: Path
) -> None:
    seen: list[str] = []
    with _store(_transport(lose_heartbeat=False, seen=seen)) as store:
        errors: list[BaseException] = []
        completed = threading.Event()
        worker = _start_upload(store, _artifact(tmp_path), errors, completed)
        pid = _wait_for_pid(real_registry_sleeping_child)
        store.close()
        _assert_cancelled(worker, errors, pid, completed)
    assert "/v1/internal/claim-sessions/finalize" not in seen


@pytest.mark.parametrize("close_store", [False, True])
def test_session_close_waits_for_resolve_cleanup_and_rejects_new_work(
    tmp_path: Path, close_store: bool, real_registry_sleeping_child: Path
) -> None:
    entered = threading.Event()
    cleaning = threading.Event()
    release = threading.Event()
    cleaned = threading.Event()
    closed = threading.Event()
    second_closed = threading.Event()
    seen: list[str] = []
    transport = _transport(lose_heartbeat=False, seen=seen)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/artifacts/resolve" and not entered.is_set():
            entered.set()
            try:
                await asyncio.sleep(60)
            finally:
                cleaning.set()
                while not release.is_set():
                    await asyncio.sleep(0.01)
                cleaned.set()
        return transport.handle_request(request)

    with _store(transport, async_transport=httpx.MockTransport(handler)) as store:
        session = store.claim_session()
        other = store.claim_session()
        artifact = _artifact(tmp_path)
        upload_errors: list[BaseException] = []
        close_errors: list[BaseException] = []

        def upload() -> None:
            try:
                session._upload_artifact(artifact, deadline=time.monotonic() + 10)
            except BaseException as error:
                upload_errors.append(error)

        def close(*, second: bool = False) -> None:
            try:
                if second and close_store:
                    store.close()
                else:
                    session.close()
            except BaseException as error:
                close_errors.append(error)
            finally:
                (second_closed if second else closed).set()

        worker = threading.Thread(target=upload, daemon=True)
        closer = threading.Thread(target=close, daemon=True)
        second_closer = threading.Thread(
            target=close, kwargs={"second": True}, daemon=True
        )
        worker.start()
        try:
            assert entered.wait(3)
            closer.start()
            assert cleaning.wait(3), "resolve was not actively cancelled"
            assert not closed.is_set(), "close returned before HTTP cleanup"
            with pytest.raises(StoreError, match="closed"):
                session._upload_artifact(artifact, deadline=time.monotonic() + 10)
            # The sibling's transport remains usable while this close is waiting.
            response = other._request_until(
                "POST", "/v1/artifacts/resolve", deadline=time.monotonic() + 3
            )
            assert response.status_code == 200
            second_closer.start()
            assert not second_closed.wait(0.1)
        finally:
            release.set()
            for thread in (worker, closer, second_closer):
                if thread.ident is not None:
                    thread.join(timeout=5)
                    assert not thread.is_alive()
            other.close()
        assert cleaned.is_set()
        assert closed.is_set() and second_closed.is_set()
        assert not close_errors
        assert len(upload_errors) == 1
        assert isinstance(upload_errors[0], StoreError)
        assert "/v1/internal/claim-sessions/finalize" not in seen


def test_session_cannot_close_from_its_own_operation(
    real_registry_sleeping_child: Path,
) -> None:
    with _store(_transport(lose_heartbeat=False, seen=[])) as store:
        session = store.claim_session()
        with session._operation():
            with pytest.raises(RuntimeError, match="own operation"):
                session.close()
        assert not session._stop.is_set()
        session.close()
        session.close()


def test_interrupted_request_wait_drains_async_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    real_registry_sleeping_child: Path,
) -> None:
    entered = threading.Event()
    cleaned = threading.Event()
    interrupted = threading.Event()
    transport = _transport(lose_heartbeat=False, seen=[])

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/artifacts/resolve":
            entered.set()
            try:
                await asyncio.sleep(60)
            finally:
                await asyncio.sleep(0.05)
                cleaned.set()
        return transport.handle_request(request)

    with _store(transport, async_transport=httpx.MockTransport(handler)) as store:
        session = store.claim_session()
        original_result = Future.result
        caller = threading.get_ident()

        def interrupt_once(future, timeout=None):
            if threading.get_ident() == caller and not interrupted.is_set():
                assert entered.wait(3)
                interrupted.set()
                raise KeyboardInterrupt
            return original_result(future, timeout=timeout)

        with monkeypatch.context() as patch:
            patch.setattr(Future, "result", interrupt_once)
            with pytest.raises(KeyboardInterrupt):
                session._upload_artifact(
                    _artifact(tmp_path), deadline=time.monotonic() + 10
                )
        assert cleaned.is_set(), "caller interrupt returned before async cleanup"
        assert not session._active_operations
        session.close()


def test_session_close_cancels_real_oras_child_and_never_finalizes(
    tmp_path: Path, real_registry_sleeping_child: Path
) -> None:
    seen: list[str] = []
    with _store(_transport(lose_heartbeat=False, seen=seen)) as store:
        session = store.claim_session()
        commit = _commit(tmp_path)
        session._claims[commit.key] = SessionEvidenceClaim(commit.key, 1)
        errors: list[BaseException] = []
        completed = threading.Event()

        def run() -> None:
            try:
                session.finalize_many((commit,))
            except BaseException as error:
                errors.append(error)
            finally:
                completed.set()

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        pid = _wait_for_pid(real_registry_sleeping_child)
        session.close()
        assert not Path(f"/proc/{pid}").exists(), "close returned before child reap"
        _assert_cancelled(worker, errors, pid, completed)
    assert "/v1/internal/claim-sessions/finalize" not in seen


def test_heartbeat_loss_cancels_real_oras_child_and_never_finalizes(
    tmp_path: Path, real_registry_sleeping_child: Path
) -> None:
    seen: list[str] = []
    with _store(_transport(lose_heartbeat=True, seen=seen)) as store:
        session = store.claim_session()
        errors: list[BaseException] = []
        completed = threading.Event()

        def run() -> None:
            try:
                with store._claim_transport.operation():
                    session._upload_artifact(
                        _artifact(tmp_path), deadline=time.monotonic() + 10.0
                    )
            except BaseException as error:
                errors.append(error)
            finally:
                completed.set()

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        pid = _wait_for_pid(real_registry_sleeping_child)
        _assert_cancelled(worker, errors, pid, completed)
        session.close()
    assert "/v1/internal/claim-sessions/finalize" not in seen
