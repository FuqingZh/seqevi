"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import os
import random
import struct
import sys
import tempfile
import threading
import time
from collections.abc import Coroutine, Iterable, Iterator
from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from seqevi.errors import (
    ClaimReceiptCapacityError,
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
    ClaimSessionAuthority,
    SessionClaimAcquireResult,
    SessionEvidenceClaim,
)

from .transport import (
    ArtifactReferenceModel,
    ArtifactUploadResponse,
    CommitModel,
    CommitRequest,
    CommitResponse,
    ClaimSessionAcquireRequest,
    ClaimSessionAcquireResponse,
    ClaimSessionAuthorityCheckRequest,
    ClaimSessionAuthorityCheckResponse,
    ClaimSessionCapabilitiesResponse,
    ClaimSessionCloseRequest,
    ClaimSessionFinalizeItem,
    ClaimSessionFinalizeRequest,
    ClaimSessionFinalizeResponse,
    ClaimSessionOpenRequest,
    ClaimSessionOpenResponse,
    ClaimSessionRenewRequest,
    ClaimSessionRenewResponse,
    EvidenceKeyModel,
    EvidenceQueryModel,
    FetchManyRequest,
    FetchManyResponse,
    HealthResponse,
    LookupRequest,
    LookupResponse,
    SessionEvidenceClaimModel,
    canonical_query_digest,
)


class _StoreResponseError(StoreError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"shared Store returned HTTP {status_code}: {detail}")
        self.status_code = status_code


_CLAIM_RUNWAY_SECONDS = 5.0
_HANDOFF_SCHEDULING_SECONDS = 1.0
_CLAIM_MUTATION_CHUNK_SIZE = 250
_MAX_CONCURRENT_CLAIM_REQUESTS = 4
_RESERVED_CLAIM_RENEWAL_SLOTS = 2
_MAX_CLAIM_BACKPRESSURE_RETRIES = 3
_DEFAULT_CLAIM_RETRY_SECONDS = 0.25
_CLAIM_RETRY_JITTER_RATIO = 0.1
_TRANSFER_CHUNK_SIZE = 1024 * 1024
_FINALIZE_RECONCILIATION_RUNWAY_SECONDS = 1.0
_READER_STOP_GRACE_SECONDS = 0.2
_READER_HEADER = struct.Struct("!q")
_READER_EOF = -1
_READER_ERROR = -2


def _artifact_reader_command(path: Path) -> tuple[str, ...]:
    return (sys.executable, "-m", "seqevi.store._artifact_reader", str(path))


def _resolver_command(
    host: str | bytes,
    port: int,
    family: int,
    socktype: int,
    proto: int,
    flags: int,
) -> tuple[str, ...]:
    resolver_host = host.decode("ascii") if isinstance(host, bytes) else host
    return (
        sys.executable,
        "-m",
        "seqevi.store._resolver",
        resolver_host,
        str(port),
        str(family),
        str(socktype),
        str(proto),
        str(flags),
    )


class _ClaimTransportRuntime:
    """Store-owned async client whose loop makes request cancellation joinable."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._loop = asyncio.new_event_loop()
        self._started = threading.Event()
        self._closing = threading.Event()
        self._closed = threading.Event()
        self._admission_lock = threading.Lock()
        self._operation_condition = threading.Condition(self._admission_lock)
        self._operation_local = threading.local()
        self._active_operations = 0
        self._close_error: BaseException | None = None
        self._requests: set[asyncio.Task[object]] = set()
        self._thread = threading.Thread(
            target=self._run,
            name="seqevi-claim-http-runtime",
            daemon=True,
        )
        self._thread.start()
        self._started.wait()
        self._client = self.run(
            self._create_client(base_url, timeout_seconds, transport)
        )

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.getaddrinfo = self._getaddrinfo  # type: ignore[method-assign]
        self._started.set()
        self._loop.run_forever()
        self._loop.close()

    async def _getaddrinfo(
        self, host: str | bytes, port: int, *args: int, **kwargs: int
    ) -> list[tuple[int, int, int, str, tuple[object, ...]]]:
        family = int(kwargs.get("family", args[0] if len(args) > 0 else 0))
        socktype = int(kwargs.get("type", args[1] if len(args) > 1 else 0))
        proto = int(kwargs.get("proto", args[2] if len(args) > 2 else 0))
        flags = int(kwargs.get("flags", args[3] if len(args) > 3 else 0))
        process = await asyncio.create_subprocess_exec(
            *_resolver_command(host, port, family, socktype, proto, flags),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await process.communicate()
        except asyncio.CancelledError:
            await _stop_subprocess(process, cancel=True)
            raise
        if process.returncode != 0:
            raise OSError(stderr[:4096].decode("utf-8", errors="replace"))
        import json

        payload = json.loads(stdout)
        return [
            (family, socktype, proto, canonname, tuple(sockaddr))
            for family, socktype, proto, canonname, sockaddr in payload
        ]

    @contextlib.contextmanager
    def operation(self) -> Iterator[None]:
        depth = getattr(self._operation_local, "depth", 0)
        if depth:
            self._operation_local.depth = depth + 1
            try:
                yield
            finally:
                self._operation_local.depth -= 1
            return
        with self._operation_condition:
            if self._closing.is_set():
                raise RuntimeError("ClaimSession transport is closing")
            self._active_operations += 1
            self._operation_local.depth = 1
        try:
            yield
        finally:
            self._operation_local.depth = 0
            with self._operation_condition:
                self._active_operations -= 1
                self._operation_condition.notify_all()

    @staticmethod
    async def _create_client(
        base_url: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None,
    ) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(timeout_seconds),
            transport=transport,
        )

    def run(self, operation: Coroutine[object, object, Any]) -> Any:
        future: Future[Any] = asyncio.run_coroutine_threadsafe(operation, self._loop)
        return future.result()

    def request(
        self, method: str, path: str, *, deadline: float, **kwargs: Any
    ) -> httpx.Response:
        with self.operation():
            future: Future[httpx.Response] = asyncio.run_coroutine_threadsafe(
                self._request(method, path, deadline=deadline, **kwargs), self._loop
            )
            return future.result()

    async def _request(
        self, method: str, path: str, *, deadline: float, **kwargs: Any
    ) -> httpx.Response:
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        content = kwargs.get("content")
        close_content = getattr(content, "aclose", None)
        remaining = deadline - time.monotonic()
        try:
            if remaining <= 0:
                raise TimeoutError
            kwargs.setdefault("timeout", httpx.Timeout(remaining))
            # asyncio.timeout covers connection, headers, and complete body
            # consumption. Leaving it cancels and awaits httpx/httpcore before
            # the sync caller resumes.
            async with asyncio.timeout(remaining):
                return await self._client.request(method, path, **kwargs)
        finally:
            try:
                if close_content is not None:
                    cleanup = asyncio.create_task(close_content())
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await cleanup
                        raise
            finally:
                self._requests.discard(task)

    async def _shutdown(self) -> None:
        await self._client.aclose()

    def close(self) -> None:
        if getattr(self._operation_local, "depth", 0):
            raise RuntimeError("cannot close ClaimSession transport from an operation")
        owner = False
        with self._operation_condition:
            if self._closed.is_set():
                if self._close_error is not None:
                    raise RuntimeError(
                        "ClaimSession transport close failed"
                    ) from self._close_error
                return
            if not self._closing.is_set():
                owner = True
                self._closing.set()
            if not owner:
                while not self._closed.is_set():
                    self._operation_condition.wait()
                if self._close_error is not None:
                    raise RuntimeError(
                        "ClaimSession transport close failed"
                    ) from self._close_error
                return
            while self._active_operations:
                self._operation_condition.wait()
        error: BaseException | None = None
        try:
            self._closing.set()
            shutdown = asyncio.run_coroutine_threadsafe(self._shutdown(), self._loop)
            shutdown.result()
        except BaseException as caught:
            error = caught
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            with self._operation_condition:
                self._close_error = error
                self._closed.set()
                self._operation_condition.notify_all()
        if error is not None:
            raise error


class HttpEvidenceStore:
    """Remote Store client with exact artifact integrity verification."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 120.0,
        maximum_artifact_bytes: int | None = None,
        maximum_batch_size: int | None = None,
        _client: httpx.Client | None = None,
        _async_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        self._timeout_seconds = timeout_seconds
        self._closing = threading.Event()
        self._close_condition = threading.Condition()
        self._close_started = False
        self._closed = False
        self._close_error: BaseException | None = None
        self._uploaded_artifact_digests: set[str] = set()
        self._download_directory = tempfile.TemporaryDirectory(
            prefix="seqevi-http-artifacts-"
        )
        self._download_root = Path(self._download_directory.name)
        self._downloaded_artifacts: dict[str, ArtifactFile] = {}
        self._owns_client = _client is None
        self.client = _client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )
        self._claim_transport = _ClaimTransportRuntime(
            base_url.rstrip("/"), timeout_seconds, _async_transport
        )
        try:
            self._initialize(maximum_artifact_bytes, maximum_batch_size)
        except BaseException:
            self._claim_transport.close()
            if self._owns_client:
                self.client.close()
            self._download_directory.cleanup()
            raise

    def _initialize(
        self,
        maximum_artifact_bytes: int | None,
        maximum_batch_size: int | None,
    ) -> None:
        if maximum_artifact_bytes is None or maximum_batch_size is None:
            health = HealthResponse.model_validate(
                self._request("GET", "/health").json()
            )
            if maximum_artifact_bytes is None:
                maximum_artifact_bytes = health.maximum_artifact_bytes
            if maximum_batch_size is None:
                maximum_batch_size = health.maximum_batch_size
        if maximum_artifact_bytes < 1:
            raise ValueError("maximum_artifact_bytes must be positive")
        if maximum_batch_size < 1:
            raise ValueError("maximum_batch_size must be positive")
        self.maximum_artifact_bytes = maximum_artifact_bytes
        self.maximum_batch_size = maximum_batch_size
        try:
            capability_response = self._claim_transport.request(
                "GET",
                "/v1/internal/claim-sessions/capabilities",
                deadline=time.monotonic() + min(self._timeout_seconds, 30.0),
            )
        except (httpx.HTTPError, TimeoutError) as error:
            raise StoreError(
                "shared Store request failed during GET "
                "/v1/internal/claim-sessions/capabilities: "
                f"{error}"
            ) from error
        if capability_response.status_code == 404:
            self._claim_session_capabilities = None
        else:
            _raise_for_store_status(capability_response)
            self._claim_session_capabilities = (
                ClaimSessionCapabilitiesResponse.model_validate(
                    capability_response.json()
                )
            )

    @property
    def supports_claim_sessions(self) -> bool:
        return self._claim_session_capabilities is not None

    def claim_session(self) -> _HttpClaimSession:
        with self._claim_transport.operation():
            return self._claim_session()

    def _claim_session(self) -> _HttpClaimSession:
        if self._claim_session_capabilities is None:
            raise StoreError("shared Store does not support ClaimSession")
        deadline = time.monotonic() + min(self._timeout_seconds, 30.0)
        try:
            response = self._claim_transport.request(
                "GET",
                "/v1/internal/claim-sessions/capabilities",
                deadline=deadline,
            )
        except (httpx.HTTPError, TimeoutError) as error:
            raise StoreError("ClaimSession capability discovery failed") from error
        _raise_for_store_status(response)
        capabilities = ClaimSessionCapabilitiesResponse.model_validate(response.json())
        self._claim_session_capabilities = capabilities
        return _HttpClaimSession(self, capabilities)

    def close(self) -> None:
        with self._close_condition:
            if self._closed:
                if self._close_error is not None:
                    raise self._close_error
                return
            if self._close_started:
                while not self._closed:
                    self._close_condition.wait()
                if self._close_error is not None:
                    raise self._close_error
                return
            self._close_started = True
            self._closing.set()
        error: BaseException | None = None
        try:
            self._claim_transport.close()
        except BaseException as caught:
            error = caught
        try:
            if self._owns_client:
                self.client.close()
        except BaseException as caught:
            if error is None:
                error = caught
        try:
            self._download_directory.cleanup()
        except BaseException as caught:
            if error is None:
                error = caught
        finally:
            with self._close_condition:
                self._close_error = error
                self._closed = True
                self._close_condition.notify_all()
        if error is not None:
            raise error

    def __enter__(self) -> HttpEvidenceStore:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        requested = tuple(dict.fromkeys(requested_queries))
        expected = {query.key for query in requested}
        records: dict[EvidenceKey, EvidenceRecord] = {}
        for offset in range(0, len(requested), self.maximum_batch_size):
            chunk = requested[offset : offset + self.maximum_batch_size]
            request = LookupRequest(
                queries=[EvidenceQueryModel.from_domain(query) for query in chunk]
            )
            response = self._request(
                "POST", "/v1/evidence/lookup", json=request.model_dump(mode="json")
            )
            payload = LookupResponse.model_validate(response.json())
            for model in payload.records:
                record = model.to_domain()
                if record.key not in expected or record.key in records:
                    raise StoreIntegrityError(
                        "shared Store returned unexpected lookup records"
                    )
                records[record.key] = record
        return records

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        commits = tuple(proposed_commits)
        if len({commit.key for commit in commits}) != len(commits):
            raise ValueError("commit batch contains a duplicate evidence key")
        payloads: dict[str, ArtifactFile] = {}
        for commit in commits:
            for payload in (commit.normalized_artifact, commit.raw_artifact):
                if payload is None:
                    continue
                existing = payloads.setdefault(payload.digest, payload)
                if _artifact_identity(existing) != _artifact_identity(payload):
                    raise StoreIntegrityError(
                        f"artifact digest has conflicting payloads: {payload.digest}"
                    )
        for payload in payloads.values():
            if payload.digest not in self._uploaded_artifact_digests:
                self._upload(payload)
                self._uploaded_artifact_digests.add(payload.digest)
        outcomes: list[CommitOutcome] = []
        for offset in range(0, len(commits), self.maximum_batch_size):
            chunk = commits[offset : offset + self.maximum_batch_size]
            request = CommitRequest(
                commits=[CommitModel.from_domain(item) for item in chunk]
            )
            response = self._request(
                "POST", "/v1/evidence/commit", json=request.model_dump(mode="json")
            )
            chunk_outcomes = tuple(
                CommitResponse.model_validate(response.json()).outcomes
            )
            if len(chunk_outcomes) != len(chunk):
                raise StoreIntegrityError(
                    "shared Store returned incomplete commit outcomes"
                )
            outcomes.extend(chunk_outcomes)
        return tuple(outcomes)

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None:
        return self.fetch_many((key,)).get(key)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, FetchedEvidence]:
        """Fetch exact records and download each unique artifact once."""

        requested = tuple(dict.fromkeys(keys))
        records: dict[EvidenceKey, EvidenceRecord] = {}
        expected = set(requested)
        for offset in range(0, len(requested), self.maximum_batch_size):
            chunk = requested[offset : offset + self.maximum_batch_size]
            request = FetchManyRequest(
                keys=[EvidenceKeyModel.from_domain(key) for key in chunk]
            )
            response = self._request(
                "POST",
                "/v1/evidence/fetch-many",
                json=request.model_dump(mode="json"),
            )
            payload = FetchManyResponse.model_validate(response.json())
            for model in payload.records:
                record = model.to_domain()
                if record.key not in expected or record.key in records:
                    raise StoreIntegrityError(
                        "shared Store returned unexpected fetch records"
                    )
                records[record.key] = record
        digests = {
            digest
            for record in records.values()
            for digest in (
                record.normalized_artifact_digest,
                record.raw_artifact_digest,
            )
            if digest is not None
        }
        artifact_by_digest = {
            digest: self._download(digest) for digest in sorted(digests)
        }
        return {
            key: FetchedEvidence(
                record=record,
                normalized_artifact=(
                    artifact_by_digest[record.normalized_artifact_digest]
                    if record.normalized_artifact_digest is not None
                    else None
                ),
                raw_artifact=(
                    artifact_by_digest[record.raw_artifact_digest]
                    if record.raw_artifact_digest is not None
                    else None
                ),
            )
            for key, record in records.items()
        }

    def _upload(self, payload: ArtifactFile) -> None:
        headers = {
            "X-Artifact-Media-Type": payload.media_type,
            "X-Artifact-Byte-Size": str(payload.byte_size),
        }
        response = self._request(
            "PUT",
            f"/v1/artifacts/{payload.digest}",
            headers=headers,
            content=_file_chunks(payload.path),
        )
        uploaded = ArtifactUploadResponse.model_validate(response.json()).artifact
        expected = ArtifactReferenceModel(
            digest=payload.digest,
            media_type=payload.media_type,
            byte_size=payload.byte_size,
        )
        if uploaded != expected:
            raise StoreIntegrityError("shared Store returned wrong artifact metadata")

    def _download(self, digest: str) -> ArtifactFile:
        cached = self._downloaded_artifacts.get(digest)
        if cached is not None:
            return cached
        hasher = hashlib.sha256()
        byte_size = 0
        target = self._download_root / digest
        temporary = self._download_root / f".{digest}.partial"
        try:
            try:
                with (
                    self.client.stream("GET", f"/v1/artifacts/{digest}") as response,
                    temporary.open("wb") as handle,
                ):
                    _raise_for_store_status(response, include_body=False)
                    for chunk in response.iter_bytes(_TRANSFER_CHUNK_SIZE):
                        byte_size += len(chunk)
                        if byte_size > self.maximum_artifact_bytes:
                            raise StoreIntegrityError(
                                "artifact exceeds configured client download limit"
                            )
                        hasher.update(chunk)
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
                    content_length = response.headers.get("content-length")
                    media_type = response.headers.get(
                        "content-type", "application/octet-stream"
                    ).split(";", maxsplit=1)[0]
            except httpx.HTTPError as error:
                raise StoreError(f"shared Store request failed: {error}") from error
            if hasher.hexdigest() != digest:
                raise StoreIntegrityError(f"artifact digest mismatch: {digest}")
            if content_length is not None and int(content_length) != byte_size:
                raise StoreIntegrityError(f"artifact byte size mismatch: {digest}")
            os.replace(temporary, target)
            artifact = ArtifactFile(
                path=target,
                media_type=media_type,
                byte_size=byte_size,
                digest=digest,
                lifetime=ArtifactLifetime.STORE,
            )
            self._downloaded_artifacts[digest] = artifact
            return artifact
        finally:
            temporary.unlink(missing_ok=True)

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            raise StoreError(f"shared Store request failed: {error}") from error
        _raise_for_store_status(response)
        return response


class _HttpClaimSession:
    """One remote session, one heartbeat, and exact-key generation fences."""

    def __init__(
        self, store: HttpEvidenceStore, capabilities: ClaimSessionCapabilitiesResponse
    ) -> None:
        self.store = store
        self.capabilities = capabilities
        self.cancellation_signal = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._lost: BaseException | None = None
        self._claims: dict[EvidenceKey, SessionEvidenceClaim] = {}
        self._heartbeat_jitter = random.uniform(0.8, 1.2)
        request_started = time.monotonic()
        open_deadline = request_started + 30.0
        request = ClaimSessionOpenRequest(
            open_request_id=uuid4().hex,
            server_time=capabilities.server_time,
            open_not_after=capabilities.server_time + timedelta(seconds=30),
        )
        while True:
            response = self._request_until(
                "POST",
                "/v1/internal/claim-sessions/open",
                deadline=open_deadline,
                json=request.model_dump(mode="json"),
            )
            try:
                opened = ClaimSessionOpenResponse.model_validate(
                    response.json()
                ).to_domain()
                break
            except (ValueError, StoreIntegrityError):
                remaining = open_deadline - time.monotonic()
                if remaining <= 0:
                    raise
                if self._stop.wait(min(0.25, remaining)):
                    raise StoreError("ClaimSession closed during open recovery")
        self._authority = opened
        self._renew_deadline = self._authority_deadline(opened, request_started)
        self._thread = threading.Thread(
            target=self._heartbeat,
            name="seqevi-claim-session-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def __enter__(self) -> _HttpClaimSession:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def raise_if_lost(self) -> None:
        if self._lost is not None:
            raise EvidenceClaimLostError(
                "ClaimSession authority was lost"
            ) from self._lost

    @staticmethod
    def _authority_deadline(
        authority: ClaimSessionAuthority, request_started: float
    ) -> float:
        return min(
            request_started + authority.renew_deadline_seconds,
            request_started + authority.remaining_lease_seconds - _CLAIM_RUNWAY_SECONDS,
        )

    def _authority_request(self) -> dict[str, object]:
        with self._lock:
            return {
                "session_id": self._authority.session_id,
                "owner_token": self._authority.owner_token,
                "generation": self._authority.generation,
            }

    def _authority_request_and_deadline(self) -> tuple[dict[str, object], float]:
        with self._lock:
            return (
                {
                    "session_id": self._authority.session_id,
                    "owner_token": self._authority.owner_token,
                    "generation": self._authority.generation,
                },
                self._renew_deadline,
            )

    def _heartbeat(self) -> None:
        while True:
            with self._lock:
                delay = min(
                    self._authority.heartbeat_after_seconds * self._heartbeat_jitter,
                    max(self._renew_deadline - time.monotonic() - 1.0, 0.0),
                )
            if self._stop.wait(delay):
                return
            started = time.monotonic()
            try:
                with self.store._claim_transport.operation():
                    self._heartbeat_once(started)
            except BaseException as error:
                self._lost = error
                self.cancellation_signal.set()
                return

    def _heartbeat_once(self, started: float) -> None:
        authority_request, renew_deadline = self._authority_request_and_deadline()
        renew_json = ClaimSessionRenewRequest.model_validate(
            authority_request
        ).model_dump(mode="json")
        response = self._request_until(
            "POST",
            "/v1/internal/claim-sessions/renew",
            deadline=renew_deadline,
            json=renew_json,
        )
        renewed = ClaimSessionRenewResponse.model_validate(response.json()).to_domain()
        if (
            renewed.session_id != authority_request["session_id"]
            or renewed.owner_token != authority_request["owner_token"]
            or renewed.generation != authority_request["generation"]
        ):
            raise StoreIntegrityError(
                "renewal response switched ClaimSession authority"
            )
        with self._lock:
            self._authority = renewed
            self._renew_deadline = self._authority_deadline(renewed, started)

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> tuple[SessionClaimAcquireResult, ...]:
        with self.store._claim_transport.operation():
            return self._acquire_many(requested_queries)

    def _acquire_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> tuple[SessionClaimAcquireResult, ...]:
        self.raise_if_lost()
        requested = tuple(requested_queries)
        if len({query.key for query in requested}) != len(requested):
            raise ValueError("acquire batch contains a duplicate evidence key")
        results: list[SessionClaimAcquireResult] = []
        maximum = min(
            self.capabilities.maximum_batch_size, self.store.maximum_batch_size
        )
        for offset in range(0, len(requested), maximum):
            chunk = requested[offset : offset + maximum]
            models = [EvidenceQueryModel.from_domain(query) for query in chunk]
            while True:
                started = time.monotonic()
                authority_request, renew_deadline = (
                    self._authority_request_and_deadline()
                )
                deadline = min(started + 30.0, renew_deadline)
                request = ClaimSessionAcquireRequest.model_validate(
                    {
                        **authority_request,
                        "acquire_request_id": uuid4().hex,
                        "query_digest": canonical_query_digest(models),
                        "queries": [model.model_dump(mode="json") for model in models],
                    }
                )
                payload: ClaimSessionAcquireResponse | None = None
                staged: list[SessionClaimAcquireResult] | None = None
                while True:
                    try:
                        response = self._request_until(
                            "POST",
                            "/v1/internal/claim-sessions/acquire",
                            deadline=deadline,
                            deadline_loses_authority=False,
                            json=request.model_dump(mode="json"),
                        )
                        payload = ClaimSessionAcquireResponse.model_validate(
                            response.json()
                        )
                        if len(payload.results) != len(chunk):
                            raise StoreIntegrityError(
                                "shared Store returned incomplete acquire results"
                            )
                        staged = []
                        for query, model in zip(chunk, payload.results, strict=True):
                            record = (
                                None
                                if model.record is None
                                else model.record.to_domain()
                            )
                            claim = (
                                None if model.claim is None else model.claim.to_domain()
                            )
                            busy = (
                                None if model.busy is None else model.busy.to_domain()
                            )
                            result = SessionClaimAcquireResult(
                                model.disposition,
                                record=record,
                                claim=claim,
                                busy=busy,
                            )
                            result_key = (
                                record.key
                                if record is not None
                                else claim.key
                                if claim is not None
                                else busy.key  # type: ignore[union-attr]
                            )
                            if result_key != query.key:
                                raise StoreIntegrityError(
                                    "shared Store returned a mismatched acquire result"
                                )
                            staged.append(result)
                        break
                    except ClaimReceiptCapacityError:
                        delay = min(1.0, self._renew_deadline - time.monotonic())
                        if delay <= 0 or self._stop.wait(delay):
                            raise EvidenceClaimLostError(
                                "ClaimSession authority runway expired during receipt admission"
                            )
                        break
                    except (ValueError, StoreIntegrityError):
                        if time.monotonic() >= deadline:
                            raise
                        if self._stop.wait(min(0.25, deadline - time.monotonic())):
                            raise StoreError(
                                "ClaimSession closed during acquire recovery"
                            )
                        continue
                if staged is not None:
                    break
            for result in staged:
                if result.claim is not None:
                    self._claims[result.claim.key] = result.claim
                results.append(result)
        return tuple(results)

    def finalize_many(
        self, proposed: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        with self.store._claim_transport.operation():
            return self._finalize_many(proposed)

    def _finalize_many(
        self, proposed: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        self.raise_if_lost()
        commits = tuple(proposed)
        if len({commit.key for commit in commits}) != len(commits):
            raise ValueError("finalize batch contains a duplicate evidence key")
        if not commits:
            return ()
        maximum = min(
            self.capabilities.maximum_batch_size, self.store.maximum_batch_size
        )
        if len(commits) > maximum:
            return tuple(
                outcome
                for offset in range(0, len(commits), maximum)
                for outcome in self.finalize_many(commits[offset : offset + maximum])
            )
        for commit in commits:
            for payload in (commit.normalized_artifact, commit.raw_artifact):
                if (
                    payload is not None
                    and payload.digest not in self.store._uploaded_artifact_digests
                ):
                    self._upload_artifact(payload)
                    self.store._uploaded_artifact_digests.add(payload.digest)
        items = []
        for commit in commits:
            claim = self._claims.get(commit.key)
            if claim is None:
                raise EvidenceClaimLostError("no exact claim for finalization")
            items.append(
                ClaimSessionFinalizeItem(
                    commit=CommitModel.from_domain(commit),
                    claim_generation=claim.generation,
                )
            )
        started = time.monotonic()
        authority_request, renew_deadline = self._authority_request_and_deadline()
        deadline = min(started + 30.0, renew_deadline)
        transport_deadline = deadline - _FINALIZE_RECONCILIATION_RUNWAY_SECONDS
        if transport_deadline <= started:
            raise EvidenceClaimLostError(
                "ClaimSession finalization has no reconciliation runway"
            )
        request_id = uuid4().hex
        pending = list(zip(commits, items, strict=True))
        resolved: dict[EvidenceKey, CommitOutcome] = {}
        unknown_outcome = False
        while pending:
            request = ClaimSessionFinalizeRequest.model_validate(
                {
                    **authority_request,
                    "finalize_request_id": request_id,
                    "commits": [item.model_dump(mode="json") for _, item in pending],
                }
            )
            try:
                attempt_error: BaseException | None = None
                response: httpx.Response | None = None
                remaining_time = deadline - time.monotonic()
                if remaining_time <= 0:
                    raise EvidenceClaimLostError(
                        "ClaimSession finalization deadline expired"
                    )
                try:
                    response = self.store._claim_transport.request(
                        "POST",
                        "/v1/internal/claim-sessions/finalize",
                        deadline=transport_deadline,
                        json=request.model_dump(mode="json"),
                    )
                except (httpx.HTTPError, TimeoutError) as error:
                    unknown_outcome = True
                    attempt_error = StoreError(
                        "ClaimSession finalize transport outcome is unknown"
                    )
                    attempt_error.__cause__ = error
                else:
                    if (
                        response.status_code in {412, 503}
                        or response.status_code >= 500
                    ):
                        unknown_outcome = True
                        attempt_error = _StoreResponseError(
                            response.status_code, response.text
                        )
                    else:
                        _raise_for_store_status(response)
                if attempt_error is not None:
                    delay = min(0.25, deadline - time.monotonic())
                    if delay <= 0:
                        raise attempt_error
                    if self._stop.wait(delay):
                        raise StoreError("ClaimSession closed during finalize recovery")
                    # Reconcile this observed attempt before any same-ID retry.
                    raise attempt_error
                assert response is not None
                returned = tuple(
                    ClaimSessionFinalizeResponse.model_validate(
                        response.json()
                    ).outcomes
                )
                if len(returned) != len(pending):
                    raise StoreIntegrityError(
                        "shared Store returned incomplete finalize outcomes"
                    )
                if not unknown_outcome:
                    resolved.update(
                        (commit.key, outcome)
                        for (commit, _item), outcome in zip(
                            pending, returned, strict=True
                        )
                    )
                    break
                recovery_error: BaseException = StoreError(
                    "finalize response requires terminal evidence reconciliation"
                )
            except (StoreError, ValueError) as error:
                if (
                    isinstance(error, _StoreResponseError)
                    and error.status_code < 500
                    and error.status_code != 412
                    and not unknown_outcome
                ):
                    raise
                unknown_outcome = True
                recovery_error = error
            while True:
                try:
                    remaining_time = deadline - time.monotonic()
                    if remaining_time <= 0:
                        raise EvidenceClaimLostError(
                            "ClaimSession finalize readback deadline expired"
                        )
                    queries = tuple(
                        EvidenceQuery(commit.identity, commit.key)
                        for commit, _ in pending
                    )
                    lookup_request = LookupRequest(
                        queries=[
                            EvidenceQueryModel.from_domain(query) for query in queries
                        ]
                    )
                    try:
                        lookup_response = self.store._claim_transport.request(
                            "POST",
                            "/v1/evidence/lookup",
                            deadline=deadline,
                            json=lookup_request.model_dump(mode="json"),
                        )
                    except (httpx.HTTPError, TimeoutError) as error:
                        raise StoreError(
                            "finalize evidence readback transport failed"
                        ) from error
                    _raise_for_store_status(lookup_response)
                    lookup_payload = LookupResponse.model_validate(
                        lookup_response.json()
                    )
                    expected = {query.key for query in queries}
                    found = {}
                    for model in lookup_payload.records:
                        record = model.to_domain()
                        if record.key not in expected or record.key in found:
                            raise StoreIntegrityError(
                                "shared Store returned unexpected lookup records"
                            )
                        found[record.key] = record
                    break
                except (StoreError, ValueError):
                    remaining_time = deadline - time.monotonic()
                    if remaining_time <= 0:
                        raise
                    if self._stop.wait(min(0.25, remaining_time)):
                        raise StoreError("ClaimSession closed during finalize readback")
            remaining = []
            for commit, item in pending:
                record = found.get(commit.key)
                if record is None:
                    remaining.append((commit, item))
                    continue
                if not _record_matches_commit(record, commit):
                    raise EvidenceConflictError(
                        "finalize readback found conflicting immutable evidence"
                    ) from recovery_error
                resolved[commit.key] = CommitOutcome.EXISTING
                self._claims.pop(commit.key, None)
            if not remaining:
                break
            authority_check = ClaimSessionAuthorityCheckRequest.model_validate(
                {
                    **self._authority_request(),
                    "claims": [
                        SessionEvidenceClaimModel.from_domain(
                            self._claims[commit.key]
                        ).model_dump(mode="json")
                        for commit, _item in remaining
                    ],
                }
            )
            while True:
                authority_response = self._request_until(
                    "POST",
                    "/v1/internal/claim-sessions/authority",
                    deadline=deadline,
                    json=authority_check.model_dump(mode="json"),
                )
                try:
                    authority_is_live = (
                        ClaimSessionAuthorityCheckResponse.model_validate(
                            authority_response.json()
                        ).live
                    )
                    break
                except ValueError:
                    remaining_time = deadline - time.monotonic()
                    if remaining_time <= 0:
                        raise
                    if self._stop.wait(min(0.25, remaining_time)):
                        raise StoreError(
                            "ClaimSession closed during authority recovery"
                        )
            if not authority_is_live:
                raise EvidenceClaimLostError(
                    "exact ClaimSession finalization authority was lost"
                ) from recovery_error
            if time.monotonic() >= deadline:
                raise recovery_error
            if len(remaining) == len(pending):
                if self._stop.wait(min(0.25, deadline - time.monotonic())):
                    raise StoreError("ClaimSession closed during finalize recovery")
            pending = remaining
        for commit in commits:
            self._claims.pop(commit.key, None)
        return tuple(resolved[commit.key] for commit in commits)

    def _request_until(
        self,
        method: str,
        path: str,
        *,
        deadline: float,
        deadline_loses_authority: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if deadline_loses_authority:
                    raise EvidenceClaimLostError(
                        "ClaimSession operation deadline expired"
                    )
                raise StoreError("ClaimSession operation deadline expired")
            try:
                response = self.store._claim_transport.request(
                    method, path, deadline=deadline, **kwargs
                )
            except (httpx.HTTPError, TimeoutError):
                response = None
            if time.monotonic() >= deadline:
                if deadline_loses_authority:
                    raise EvidenceClaimLostError(
                        "ClaimSession operation response exceeded its deadline"
                    )
                raise StoreError(
                    "ClaimSession operation response exceeded its deadline"
                )
            if response is not None and response.status_code == 412:
                raise EvidenceClaimLostError(response.text)
            if response is not None and _is_receipt_capacity(response):
                raise ClaimReceiptCapacityError("claim_receipt_capacity")
            if response is not None and response.status_code != 503:
                _raise_for_store_status(response)
                return response
            delay = random.uniform(1.0, 5.0)
            if response is not None:
                delay = max(delay, _claim_retry_delay(response))
            if delay >= deadline - time.monotonic():
                raise StoreError("ClaimSession operation remained unavailable")
            if self._stop.wait(delay):
                raise StoreError("ClaimSession closed during request")

    def _upload_until(self, payload: ArtifactFile, *, deadline: float) -> None:
        headers = {
            "X-Artifact-Media-Type": payload.media_type,
            "X-Artifact-Byte-Size": str(payload.byte_size),
        }
        try:
            response = self.store._claim_transport.request(
                "PUT",
                f"/v1/artifacts/{payload.digest}",
                deadline=deadline,
                headers=headers,
                content=_async_file_chunks(payload.path),
            )
        except (httpx.HTTPError, TimeoutError) as error:
            raise StoreError("ClaimSession artifact upload failed") from error
        _raise_for_store_status(response)
        uploaded = ArtifactUploadResponse.model_validate(response.json()).artifact
        expected = ArtifactReferenceModel(
            digest=payload.digest,
            media_type=payload.media_type,
            byte_size=payload.byte_size,
        )
        if uploaded != expected:
            raise StoreIntegrityError("shared Store returned wrong artifact metadata")

    def _upload_artifact(self, payload: ArtifactFile) -> None:
        upload_started = time.monotonic()
        _authority, authority_deadline = self._authority_request_and_deadline()
        upload_deadline = min(
            upload_started + 30.0,
            authority_deadline - _FINALIZE_RECONCILIATION_RUNWAY_SECONDS,
        )
        if upload_deadline <= upload_started:
            raise EvidenceClaimLostError(
                "ClaimSession artifact upload has no reconciliation runway"
            )
        self._upload_until(payload, deadline=upload_deadline)

    def close(self) -> None:
        with self.store._claim_transport.operation():
            self._close()

    def _close(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        self._thread.join(timeout=min(self.store._timeout_seconds, 5.0))
        try:
            request = ClaimSessionCloseRequest.model_validate(self._authority_request())
            self.store._claim_transport.request(
                "POST",
                "/v1/internal/claim-sessions/close",
                json=request.model_dump(mode="json"),
                deadline=time.monotonic() + min(self.store._timeout_seconds, 5.0),
            )
        except (httpx.HTTPError, TimeoutError):
            pass


def _claim_retry_delay(response: httpx.Response) -> float:
    raw = response.headers.get("retry-after")
    try:
        base = float(raw) if raw is not None else _DEFAULT_CLAIM_RETRY_SECONDS
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw or "")
        except (TypeError, ValueError, OverflowError):
            base = _DEFAULT_CLAIM_RETRY_SECONDS
        else:
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            base = (retry_at - datetime.now(retry_at.tzinfo)).total_seconds()
    base = max(0.01, base)
    jitter = base * _CLAIM_RETRY_JITTER_RATIO
    return base + random.uniform(0.0, jitter)


def _is_receipt_capacity(response: httpx.Response) -> bool:
    if response.status_code != 503:
        return False
    try:
        payload = response.json()
    except ValueError:
        return False
    detail = payload.get("detail", payload) if isinstance(payload, dict) else None
    return isinstance(detail, dict) and detail.get("code") == "claim_receipt_capacity"


def _record_matches_commit(record: EvidenceRecord, commit: EvidenceCommit) -> bool:
    return (
        record.status == commit.status
        and record.payload_digest == commit.payload_digest
    )


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while chunk := handle.read(_TRANSFER_CHUNK_SIZE):
            yield chunk


async def _async_file_chunks(path: Path) -> Any:
    process = await asyncio.create_subprocess_exec(
        *_artifact_reader_command(path),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    complete = False
    try:
        while True:
            try:
                header = await process.stdout.readexactly(_READER_HEADER.size)
            except asyncio.IncompleteReadError as error:
                raise StoreIntegrityError(
                    "artifact reader closed its pipe unexpectedly"
                ) from error
            marker = _READER_HEADER.unpack(header)[0]
            if marker == _READER_EOF:
                complete = True
                break
            if marker == _READER_ERROR:
                size = _READER_HEADER.unpack(
                    await process.stdout.readexactly(_READER_HEADER.size)
                )[0]
                detail = (await process.stdout.readexactly(size)).decode(
                    "utf-8", errors="replace"
                )
                raise StoreIntegrityError(f"artifact reader failed: {detail}")
            if marker < 0 or marker > _TRANSFER_CHUNK_SIZE:
                raise StoreIntegrityError("artifact reader returned an invalid frame")
            yield await process.stdout.readexactly(marker)
    finally:
        cleanup = asyncio.create_task(
            _stop_artifact_reader(process, cancel=not complete)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise


async def _stop_artifact_reader(
    process: asyncio.subprocess.Process, *, cancel: bool
) -> None:
    await _stop_subprocess(process, cancel=cancel)
    if not cancel and process.returncode != 0:
        stderr = b""
        if process.stderr is not None:
            stderr = await process.stderr.read(4096)
        detail = stderr.decode("utf-8", errors="replace").strip()
        raise StoreIntegrityError(
            f"artifact reader exited with status {process.returncode}"
            + (f": {detail}" if detail else "")
        )


async def _stop_subprocess(
    process: asyncio.subprocess.Process, *, cancel: bool
) -> None:
    wait_task = asyncio.create_task(process.wait())
    if cancel and process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(
                asyncio.shield(wait_task), _READER_STOP_GRACE_SECONDS
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    # Deliberately unbounded for Linux D-state: this synchronous owner may not
    # claim deadline completion while its reader still exists.
    await wait_task


def _raise_for_store_status(
    response: httpx.Response, *, include_body: bool = True
) -> None:
    if response.is_success:
        return
    detail = response.text if include_body else response.reason_phrase
    if response.status_code == 409:
        raise EvidenceConflictError(detail)
    raise _StoreResponseError(response.status_code, detail)


def _artifact_identity(artifact: ArtifactFile) -> tuple[str, str, int]:
    return artifact.digest, artifact.media_type, artifact.byte_size
