"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
import math
import os
import random
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
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
    canonical_query_digest,
)

_CLAIM_RUNWAY_SECONDS = 5.0
_HANDOFF_SCHEDULING_SECONDS = 1.0
_CLAIM_MUTATION_CHUNK_SIZE = 250
_MAX_CONCURRENT_CLAIM_REQUESTS = 4
_RESERVED_CLAIM_RENEWAL_SLOTS = 2
_MAX_CLAIM_BACKPRESSURE_RETRIES = 3
_DEFAULT_CLAIM_RETRY_SECONDS = 0.25
_CLAIM_RETRY_JITTER_RATIO = 0.1
_TRANSFER_CHUNK_SIZE = 1024 * 1024


class HttpEvidenceStore:
    """Remote Store client with exact artifact integrity verification."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 120.0,
        maximum_artifact_bytes: int | None = None,
        maximum_batch_size: int | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        self._timeout_seconds = timeout_seconds
        self._closing = threading.Event()
        self._uploaded_artifact_digests: set[str] = set()
        self._download_directory = tempfile.TemporaryDirectory(
            prefix="seqevi-http-artifacts-"
        )
        self._download_root = Path(self._download_directory.name)
        self._downloaded_artifacts: dict[str, ArtifactFile] = {}
        self._owns_client = client is None
        self.client = client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )
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
            capability_response = self.client.request(
                "GET",
                "/v1/internal/claim-sessions/capabilities",
                timeout=httpx.Timeout(min(timeout_seconds, 30.0)),
            )
        except httpx.HTTPError as error:
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
        if self._claim_session_capabilities is None:
            raise StoreError("shared Store does not support ClaimSession")
        response = self._request("GET", "/v1/internal/claim-sessions/capabilities")
        capabilities = ClaimSessionCapabilitiesResponse.model_validate(response.json())
        self._claim_session_capabilities = capabilities
        return _HttpClaimSession(self, capabilities)

    def close(self) -> None:
        self._closing.set()
        try:
            if self._owns_client:
                self.client.close()
        finally:
            self._download_directory.cleanup()

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
        request_started = time.monotonic()
        open_deadline = request_started + 30.0
        request = ClaimSessionOpenRequest(
            open_request_id=uuid4().hex,
            server_time=capabilities.server_time,
            open_not_after=capabilities.server_time + timedelta(seconds=30),
        )
        response = self._request_until(
            "POST",
            "/v1/internal/claim-sessions/open",
            deadline=open_deadline,
            json=request.model_dump(mode="json"),
        )
        opened = ClaimSessionOpenResponse.model_validate(response.json()).to_domain()
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

    def _heartbeat(self) -> None:
        while True:
            with self._lock:
                delay = min(
                    self._authority.heartbeat_after_seconds,
                    max(self._renew_deadline - time.monotonic(), 0.0),
                )
            if self._stop.wait(delay):
                return
            started = time.monotonic()
            try:
                response = self._request_until(
                    "POST",
                    "/v1/internal/claim-sessions/renew",
                    deadline=self._renew_deadline,
                    json=ClaimSessionRenewRequest.model_validate(
                        self._authority_request()
                    ).model_dump(mode="json"),
                )
                renewed = ClaimSessionRenewResponse.model_validate(
                    response.json()
                ).to_domain()
                with self._lock:
                    self._authority = renewed
                    self._renew_deadline = self._authority_deadline(renewed, started)
            except BaseException as error:
                self._lost = error
                self.cancellation_signal.set()
                return

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> tuple[SessionClaimAcquireResult, ...]:
        self.raise_if_lost()
        requested = tuple(requested_queries)
        results: list[SessionClaimAcquireResult] = []
        maximum = min(
            self.capabilities.maximum_batch_size, self.store.maximum_batch_size
        )
        for offset in range(0, len(requested), maximum):
            chunk = requested[offset : offset + maximum]
            models = [EvidenceQueryModel.from_domain(query) for query in chunk]
            while True:
                started = time.monotonic()
                request = ClaimSessionAcquireRequest.model_validate(
                    {
                        **self._authority_request(),
                        "acquire_request_id": uuid4().hex,
                        "query_digest": canonical_query_digest(models),
                        "queries": [model.model_dump(mode="json") for model in models],
                    }
                )
                try:
                    response = self._request_until(
                        "POST",
                        "/v1/internal/claim-sessions/acquire",
                        deadline=min(started + 30.0, self._renew_deadline),
                        json=request.model_dump(mode="json"),
                    )
                    break
                except ClaimReceiptCapacityError:
                    delay = min(1.0, self._renew_deadline - time.monotonic())
                    if delay <= 0 or self._stop.wait(delay):
                        raise EvidenceClaimLostError(
                            "ClaimSession authority runway expired during receipt admission"
                        )
            payload = ClaimSessionAcquireResponse.model_validate(response.json())
            if len(payload.results) != len(chunk):
                raise StoreIntegrityError(
                    "shared Store returned incomplete acquire results"
                )
            for query, model in zip(chunk, payload.results, strict=True):
                record = None if model.record is None else model.record.to_domain()
                claim = None if model.claim is None else model.claim.to_domain()
                busy = None if model.busy is None else model.busy.to_domain()
                result = SessionClaimAcquireResult(
                    model.disposition, record=record, claim=claim, busy=busy
                )
                if record is not None:
                    key = record.key
                elif claim is not None:
                    key = claim.key
                else:
                    assert busy is not None
                    key = busy.key
                if key != query.key:
                    raise StoreIntegrityError(
                        "shared Store returned a mismatched acquire result"
                    )
                if claim is not None:
                    self._claims[claim.key] = claim
                results.append(result)
        return tuple(results)

    def finalize_many(
        self, proposed: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        self.raise_if_lost()
        commits = tuple(proposed)
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
                    self.store._upload(payload)
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
        deadline = min(started + 30.0, self._renew_deadline)
        request_id = uuid4().hex
        pending = list(zip(commits, items, strict=True))
        resolved: dict[EvidenceKey, CommitOutcome] = {}
        while pending:
            request = ClaimSessionFinalizeRequest.model_validate(
                {
                    **self._authority_request(),
                    "finalize_request_id": request_id,
                    "commits": [item.model_dump(mode="json") for _, item in pending],
                }
            )
            try:
                response = self._request_until(
                    "POST",
                    "/v1/internal/claim-sessions/finalize",
                    deadline=deadline,
                    json=request.model_dump(mode="json"),
                )
                returned = tuple(
                    ClaimSessionFinalizeResponse.model_validate(
                        response.json()
                    ).outcomes
                )
                if len(returned) != len(pending):
                    raise StoreIntegrityError(
                        "shared Store returned incomplete finalize outcomes"
                    )
                resolved.update(
                    (commit.key, outcome)
                    for (commit, _item), outcome in zip(pending, returned, strict=True)
                )
                break
            except (StoreError, ValueError) as error:
                found = self.store.lookup_many(
                    EvidenceQuery(commit.identity, commit.key) for commit, _ in pending
                )
                remaining = []
                for commit, item in pending:
                    record = found.get(commit.key)
                    if record is None:
                        remaining.append((commit, item))
                        continue
                    if not _record_matches_commit(record, commit):
                        raise EvidenceConflictError(
                            "finalize readback found conflicting immutable evidence"
                        ) from error
                    resolved[commit.key] = CommitOutcome.EXISTING
                    self._claims.pop(commit.key, None)
                if not remaining:
                    break
                if len(remaining) == len(pending) or time.monotonic() >= deadline:
                    raise
                pending = remaining
        for commit in commits:
            self._claims.pop(commit.key, None)
        return tuple(resolved[commit.key] for commit in commits)

    def _request_until(
        self, method: str, path: str, *, deadline: float, **kwargs: Any
    ) -> httpx.Response:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise EvidenceClaimLostError("ClaimSession operation deadline expired")
            try:
                response = self.store.client.request(
                    method, path, timeout=httpx.Timeout(remaining), **kwargs
                )
            except httpx.HTTPError:
                response = None
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

    def close(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        self._thread.join(timeout=min(self.store._timeout_seconds, 5.0))
        try:
            request = ClaimSessionCloseRequest.model_validate(self._authority_request())
            self.store.client.request(
                "POST",
                "/v1/internal/claim-sessions/close",
                json=request.model_dump(mode="json"),
                timeout=httpx.Timeout(min(self.store._timeout_seconds, 5.0)),
            )
        except httpx.HTTPError:
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
        and record.normalized_artifact_digest
        == (
            None
            if commit.normalized_artifact is None
            else commit.normalized_artifact.digest
        )
        and record.raw_artifact_digest
        == (None if commit.raw_artifact is None else commit.raw_artifact.digest)
    )


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while chunk := handle.read(_TRANSFER_CHUNK_SIZE):
            yield chunk


def _raise_for_store_status(
    response: httpx.Response, *, include_body: bool = True
) -> None:
    if response.is_success:
        return
    detail = response.text if include_body else response.reason_phrase
    if response.status_code == 409:
        raise EvidenceConflictError(detail)
    raise StoreError(f"shared Store returned HTTP {response.status_code}: {detail}")


def _artifact_identity(artifact: ArtifactFile) -> tuple[str, str, int]:
    return artifact.digest, artifact.media_type, artifact.byte_size
