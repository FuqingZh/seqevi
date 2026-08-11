"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
import math
import os
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from typing import Any

import httpx

from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
    ClaimAcquireResult,
    ClaimDisposition,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceClaim,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
)

from .transport import (
    ArtifactReferenceModel,
    ArtifactUploadResponse,
    CommitModel,
    CommitRequest,
    CommitResponse,
    ClaimAcquireRequest,
    ClaimAcquireResponse,
    ClaimCapabilitiesResponse,
    ClaimFinalizeRequest,
    ClaimedCommitModel,
    ClaimMutationRequest,
    ClaimReleaseResponse,
    ClaimRenewResponse,
    EvidenceClaimModel,
    EvidenceKeyModel,
    EvidenceQueryModel,
    FetchManyRequest,
    FetchManyResponse,
    HealthResponse,
    LookupRequest,
    LookupResponse,
)

_CLAIM_RUNWAY_SECONDS = 5.0
_HANDOFF_SCHEDULING_SECONDS = 1.0
_MAX_CONCURRENT_CLAIM_REQUESTS = 32


class _ChunkAcquireRenewer:
    """Keep early HTTP acquisition chunks alive until the whole call returns."""

    def __init__(self, store: HttpEvidenceStore) -> None:
        self._store = store
        self._claims: dict[EvidenceKey, EvidenceClaim] = {}
        self._queries: dict[EvidenceKey, EvidenceQuery] = {}
        self._terminal: dict[EvidenceKey, EvidenceRecord] = {}
        self._deadlines: dict[EvidenceKey, float] = {}
        self._lock = threading.Lock()
        self._changed = threading.Event()
        self._stopped = threading.Event()
        self._failure: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def add(self, claim: EvidenceClaim, query: EvidenceQuery) -> None:
        with self._lock:
            self._claims[claim.key] = claim
            self._queries[claim.key] = query
            self._deadlines[claim.key] = time.monotonic() + _claim_renewal_delay(claim)
        self._changed.set()

    def stop(self) -> None:
        self._stopped.set()
        self._changed.set()
        self._thread.join()

    def claims(self) -> dict[EvidenceKey, EvidenceClaim]:
        with self._lock:
            return dict(self._claims)

    def terminal_records(self) -> dict[EvidenceKey, EvidenceRecord]:
        with self._lock:
            return dict(self._terminal)

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise self._failure

    def release_best_effort(self) -> None:
        now = datetime.now(UTC)
        claims = tuple(
            claim for claim in self.claims().values() if claim.expires_at > now
        )
        if not claims:
            return
        try:
            self._store.release_many(claims)
        except Exception:
            pass

    def flush_unsafe(self) -> None:
        """Synchronously refresh claims too close to expiry for safe handoff."""

        now = datetime.now(UTC)
        with self._lock:
            pending = tuple(
                claim
                for claim in self._claims.values()
                if (claim.expires_at - now).total_seconds()
                <= _CLAIM_RUNWAY_SECONDS + _HANDOFF_SCHEDULING_SECONDS
            )
            expired = tuple(claim for claim in pending if claim.expires_at <= now)
            expired_queries = tuple(self._queries[claim.key] for claim in expired)
        if expired:
            terminal = self._store.lookup_many(expired_queries)
            with self._lock:
                for key, record in terminal.items():
                    self._claims.pop(key, None)
                    self._queries.pop(key, None)
                    self._deadlines.pop(key, None)
                    self._terminal[key] = record
            expired_nonterminal = tuple(
                claim for claim in expired if claim.key not in terminal
            )
            if expired_nonterminal:
                raise EvidenceClaimLostError(
                    "claim acquisition received an expired authority runway"
                )
            pending = tuple(claim for claim in pending if claim.key not in terminal)
        while pending:
            try:
                renewed = self._store.renew_many(pending)
            except EvidenceClaimLostError:
                with self._lock:
                    queries = tuple(self._queries[claim.key] for claim in pending)
                terminal = self._store.lookup_many(queries)
                if not terminal:
                    raise
                with self._lock:
                    for key, record in terminal.items():
                        self._claims.pop(key, None)
                        self._queries.pop(key, None)
                        self._deadlines.pop(key, None)
                        self._terminal[key] = record
                pending = tuple(claim for claim in pending if claim.key not in terminal)
            else:
                with self._lock:
                    renewed_at = time.monotonic()
                    for claim in renewed:
                        self._claims[claim.key] = claim
                        self._deadlines[claim.key] = renewed_at + _claim_renewal_delay(
                            claim
                        )
                return

    def _run(self) -> None:
        while not self._stopped.is_set():
            self._changed.clear()
            with self._lock:
                now = time.monotonic()
                due_keys = tuple(
                    key for key, deadline in self._deadlines.items() if deadline <= now
                )
                snapshot = tuple(self._claims[key] for key in due_keys)
                next_deadline = min(self._deadlines.values(), default=now + 1.0)
            if not snapshot:
                self._changed.wait(max(0.0, next_deadline - time.monotonic()))
                continue
            while snapshot:
                try:
                    renewed = self._store.renew_many(snapshot)
                    break
                except EvidenceClaimLostError as error:
                    with self._lock:
                        queries = tuple(self._queries[claim.key] for claim in snapshot)
                    try:
                        terminal = self._store.lookup_many(queries)
                    except BaseException as lookup_error:
                        self._failure = lookup_error
                        self._stopped.set()
                        return
                    if not terminal:
                        self._failure = error
                        self._stopped.set()
                        return
                    with self._lock:
                        for key, record in terminal.items():
                            self._claims.pop(key, None)
                            self._queries.pop(key, None)
                            self._deadlines.pop(key, None)
                            self._terminal[key] = record
                    snapshot = tuple(
                        claim for claim in snapshot if claim.key not in terminal
                    )
                except BaseException as error:
                    self._failure = error
                    self._stopped.set()
                    return
            else:
                continue
            with self._lock:
                renewed_at = time.monotonic()
                for claim in renewed:
                    if not _claim_has_runway(claim):
                        self._failure = EvidenceClaimLostError(
                            "claim renewal returned insufficient authority runway"
                        )
                        self._stopped.set()
                        return
                    if claim.key in self._claims:
                        self._claims[claim.key] = claim
                        self._deadlines[claim.key] = renewed_at + _claim_renewal_delay(
                            claim
                        )


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
                "GET", "/v1/evidence/claims/capabilities"
            )
        except httpx.HTTPError as error:
            raise StoreError(f"shared Store request failed: {error}") from error
        if capability_response.status_code == 404:
            self._claim_capabilities = None
        else:
            _raise_for_store_status(capability_response)
            self._claim_capabilities = ClaimCapabilitiesResponse.model_validate(
                capability_response.json()
            )
        self._renewal_executor = ThreadPoolExecutor(
            max_workers=_MAX_CONCURRENT_CLAIM_REQUESTS,
            thread_name_prefix="seqevi-claim-renewal",
        )

    @property
    def supports_claims(self) -> bool:
        """Return whether the service exposes atomic claim endpoints.

        Examples:
            A 404 capability probe selects legacy Store behavior:

            >>> store.supports_claims
            False
        """

        return self._claim_capabilities is not None

    def close(self) -> None:
        try:
            self._renewal_executor.shutdown(wait=True, cancel_futures=True)
        finally:
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

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]:
        """Acquire exact work in service-bounded chunks.

        Examples:
            Busy results contain wait metadata but no owner credential:

            >>> results = store.acquire_many(queries, owner_token="worker")
        """

        requested = tuple(requested_queries)
        if len({query.key for query in requested}) != len(requested):
            raise ValueError("acquire batch contains a duplicate evidence key")
        _validate_owner_token(owner_token)
        capabilities = self._require_claims()
        results = []
        renewer = _ChunkAcquireRenewer(self)
        maximum = min(capabilities.maximum_batch_size, self.maximum_batch_size, 1000)
        renewer.start()
        try:
            for offset in range(0, len(requested), maximum):
                renewer.raise_if_failed()
                chunk = requested[offset : offset + maximum]
                request = ClaimAcquireRequest(
                    owner_token=owner_token,
                    queries=[EvidenceQueryModel.from_domain(query) for query in chunk],
                )
                response = self._claim_request(
                    "POST",
                    "/v1/evidence/claims/acquire",
                    json=request.model_dump(mode="json"),
                )
                payload = ClaimAcquireResponse.model_validate(response.json())
                if len(payload.results) != len(chunk):
                    raise StoreIntegrityError(
                        "shared Store returned incomplete claim results"
                    )
                for query, model in zip(chunk, payload.results, strict=True):
                    result = model.to_domain()
                    if result.record is not None:
                        key = result.record.key
                    elif result.claim is not None:
                        key = result.claim.key
                    else:
                        assert result.busy is not None
                        key = result.busy.key
                    if key != query.key:
                        raise StoreIntegrityError(
                            "shared Store returned a mismatched claim result"
                        )
                    if (
                        result.claim is not None
                        and result.claim.owner_token != owner_token
                    ):
                        raise StoreIntegrityError(
                            "shared Store returned claim credentials for another owner"
                        )
                    results.append(result)
                    if result.claim is not None:
                        renewer.add(result.claim, query)
                renewer.raise_if_failed()
        except BaseException:
            renewer.stop()
            renewer.release_best_effort()
            raise
        renewer.stop()
        try:
            renewer.raise_if_failed()
            renewer.flush_unsafe()
        except BaseException:
            renewer.release_best_effort()
            raise
        current = renewer.claims()
        terminal = renewer.terminal_records()
        completion_time = datetime.now(UTC)
        if any(
            (claim.expires_at - completion_time).total_seconds() < _CLAIM_RUNWAY_SECONDS
            for claim in current.values()
        ):
            renewer.release_best_effort()
            raise EvidenceClaimLostError(
                "claim acquisition completed without a safe authority runway"
            )
        return tuple(
            ClaimAcquireResult(
                ClaimDisposition.CACHED, record=terminal[result.claim.key]
            )
            if result.claim is not None and result.claim.key in terminal
            else ClaimAcquireResult(result.disposition, claim=current[result.claim.key])
            if result.claim is not None
            else result
            for result in results
        )

    def renew_many(self, claims: Iterable[EvidenceClaim]) -> tuple[EvidenceClaim, ...]:
        """Renew authoritative claims in service-bounded chunks.

        Examples:
            Returned leases replace the previous expiry values:

            >>> claims = store.renew_many(claims)
        """

        return self._mutate_claims(tuple(claims), renew=True)

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None:
        """Release authoritative claims in service-bounded chunks.

        Examples:
            Normal failure can release all still-owned work:

            >>> store.release_many(claims)
        """

        self._mutate_claims(tuple(claims), renew=False)

    def finalize_many(
        self, proposed: Iterable[ClaimedEvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        """Upload artifacts then atomically finalize matching claims.

        Examples:
            Matching generations retire their claim with terminal evidence:

            >>> outcomes = store.finalize_many(proposed)
        """

        items = tuple(proposed)
        if len({item.commit.key for item in items}) != len(items):
            raise ValueError("finalize batch contains a duplicate evidence key")
        self._require_claims()
        payloads: dict[str, ArtifactFile] = {}
        for item in items:
            for payload in (item.commit.normalized_artifact, item.commit.raw_artifact):
                if payload is not None:
                    existing = payloads.setdefault(payload.digest, payload)
                    if _artifact_identity(existing) != _artifact_identity(payload):
                        raise StoreIntegrityError(
                            f"artifact digest has conflicting payloads: {payload.digest}"
                        )
        for payload in payloads.values():
            if payload.digest not in self._uploaded_artifact_digests:
                self._upload(payload)
                self._uploaded_artifact_digests.add(payload.digest)
        outcomes = []
        maximum = min(
            self._claim_capabilities.maximum_batch_size,  # type: ignore[union-attr]
            self.maximum_batch_size,
            1000,
        )
        for offset in range(0, len(items), maximum):
            chunk = items[offset : offset + maximum]
            request = ClaimFinalizeRequest(
                commits=[ClaimedCommitModel.from_domain(item) for item in chunk]
            )
            response = self._claim_request(
                "POST",
                "/v1/evidence/claims/finalize",
                json=request.model_dump(mode="json"),
            )
            returned = CommitResponse.model_validate(response.json()).outcomes
            if len(returned) != len(chunk):
                raise StoreIntegrityError(
                    "shared Store returned incomplete finalize outcomes"
                )
            outcomes.extend(returned)
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

    def _require_claims(self) -> ClaimCapabilitiesResponse:
        if self._claim_capabilities is None:
            raise StoreError("shared Store does not support evidence claims")
        return self._claim_capabilities

    def _mutate_claims(
        self, claims: tuple[EvidenceClaim, ...], *, renew: bool
    ) -> tuple[EvidenceClaim, ...]:
        if len({claim.key for claim in claims}) != len(claims):
            raise ValueError("claim mutation contains a duplicate evidence key")
        capabilities = self._require_claims()
        returned: list[EvidenceClaim] = []
        maximum = min(capabilities.maximum_batch_size, self.maximum_batch_size, 1000)
        if renew:
            chunks = tuple(
                claims[offset : offset + maximum]
                for offset in range(0, len(claims), maximum)
            )
            if not chunks:
                return ()
            scheduled = sorted(
                enumerate(chunks),
                key=lambda item: min(claim.expires_at for claim in item[1]),
            )
            renewed_by_index: dict[int, tuple[EvidenceClaim, ...]] = {}
            completed: Queue[Future[tuple[int, tuple[EvidenceClaim, ...]]]] = Queue()
            futures: set[Future[tuple[int, tuple[EvidenceClaim, ...]]]] = set()
            try:
                for item in scheduled:
                    future = self._renewal_executor.submit(
                        self._renew_indexed_claim_chunk, item
                    )
                    future.add_done_callback(completed.put)
                    futures.add(future)
            except RuntimeError as error:
                for future in futures:
                    future.cancel()
                raise StoreError("shared Store closed during claim renewal") from error
            try:
                for _completed_count in range(len(futures)):
                    future = completed.get()
                    try:
                        index, renewed_chunk = future.result()
                    except CancelledError as error:
                        raise StoreError(
                            "shared Store closed during claim renewal"
                        ) from error
                    renewed_by_index[index] = renewed_chunk
            except BaseException:
                for future in futures:
                    future.cancel()
                raise
            renewed_chunks = tuple(
                renewed_by_index[index] for index in range(len(chunks))
            )
            renewed = tuple(claim for chunk in renewed_chunks for claim in chunk)
            if any(not _claim_has_runway(claim) for claim in renewed):
                raise EvidenceClaimLostError(
                    "claim renewal completed without a safe authority runway"
                )
            return renewed
        for offset in range(0, len(claims), maximum):
            chunk = claims[offset : offset + maximum]
            request = ClaimMutationRequest(
                claims=[EvidenceClaimModel.from_domain(claim) for claim in chunk]
            )
            path = (
                "/v1/evidence/claims/renew" if renew else "/v1/evidence/claims/release"
            )
            response = self._claim_request(
                "POST", path, json=request.model_dump(mode="json")
            )
            payload = ClaimReleaseResponse.model_validate(response.json())
            if tuple(
                (ack.key.to_domain(), ack.generation) for ack in payload.released
            ) != tuple((claim.key, claim.generation) for claim in chunk):
                raise StoreIntegrityError(
                    "shared Store returned mismatched claim release"
                )
        return tuple(returned)

    def _renew_indexed_claim_chunk(
        self, indexed: tuple[int, tuple[EvidenceClaim, ...]]
    ) -> tuple[int, tuple[EvidenceClaim, ...]]:
        index, chunk = indexed
        return index, self._renew_claim_chunk(chunk)

    def _renew_claim_chunk(
        self, chunk: tuple[EvidenceClaim, ...]
    ) -> tuple[EvidenceClaim, ...]:
        timeout = min(
            self._timeout_seconds,
            _claim_request_budget(chunk),
        )
        if timeout <= 0:
            raise EvidenceClaimLostError(
                "claim renewal could not start before its authority runway"
            )
        request = ClaimMutationRequest(
            claims=[EvidenceClaimModel.from_domain(claim) for claim in chunk]
        )
        response = self._claim_request(
            "POST",
            "/v1/evidence/claims/renew",
            json=request.model_dump(mode="json"),
            timeout=timeout,
        )
        payload = ClaimRenewResponse.model_validate(response.json())
        if len(payload.claims) != len(chunk):
            raise StoreIntegrityError("shared Store returned incomplete renewed claims")
        renewed = tuple(model.to_domain() for model in payload.claims)
        if tuple(
            (claim.key, claim.owner_token, claim.generation) for claim in renewed
        ) != tuple((claim.key, claim.owner_token, claim.generation) for claim in chunk):
            raise StoreIntegrityError("shared Store returned mismatched renewed claims")
        return renewed

    def _claim_request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        kwargs.setdefault("timeout", self._timeout_seconds)
        try:
            response = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            raise StoreError(f"shared Store request failed: {error}") from error
        if response.status_code == 412:
            raise EvidenceClaimLostError(response.text)
        _raise_for_store_status(response)
        return response

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


def _validate_owner_token(owner_token: str) -> None:
    if not owner_token or len(owner_token) > 255:
        raise ValueError("owner_token must contain 1 to 255 characters")


def _claim_renewal_delay(claim: EvidenceClaim) -> float:
    remaining = (claim.expires_at - datetime.now(UTC)).total_seconds()
    return max(
        0.0,
        min(claim.renewal_after_seconds, remaining - _CLAIM_RUNWAY_SECONDS),
    )


def _claim_has_runway(claim: EvidenceClaim) -> bool:
    return (
        claim.expires_at - datetime.now(UTC)
    ).total_seconds() >= _CLAIM_RUNWAY_SECONDS


def _claim_request_budget(claims: tuple[EvidenceClaim, ...]) -> float:
    remaining = min(
        (claim.expires_at - datetime.now(UTC)).total_seconds() for claim in claims
    )
    if remaining > _CLAIM_RUNWAY_SECONDS:
        return remaining - _CLAIM_RUNWAY_SECONDS
    return remaining


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
