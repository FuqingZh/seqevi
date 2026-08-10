"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
from collections.abc import Iterable, Iterator
from pathlib import Path
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


class _ChunkAcquireRenewer:
    """Keep early HTTP acquisition chunks alive until the whole call returns."""

    def __init__(self, store: HttpEvidenceStore) -> None:
        self._store = store
        self._claims: dict[EvidenceKey, EvidenceClaim] = {}
        self._lock = threading.Lock()
        self._changed = threading.Event()
        self._stopped = threading.Event()
        self._failure: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def add(self, claim: EvidenceClaim) -> None:
        with self._lock:
            self._claims[claim.key] = claim
        self._changed.set()

    def stop(self) -> None:
        self._stopped.set()
        self._changed.set()
        self._thread.join()

    def claims(self) -> dict[EvidenceKey, EvidenceClaim]:
        with self._lock:
            return dict(self._claims)

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise self._failure

    def _run(self) -> None:
        while not self._stopped.is_set():
            self._changed.clear()
            with self._lock:
                snapshot = tuple(self._claims.values())
            cadence = min(
                (claim.renewal_after_seconds for claim in snapshot), default=1.0
            )
            if self._changed.wait(cadence) or self._stopped.is_set():
                continue
            try:
                renewed = self._store.renew_many(snapshot)
            except BaseException as error:
                self._failure = error
                self._stopped.set()
                return
            with self._lock:
                for claim in renewed:
                    if claim.key in self._claims:
                        self._claims[claim.key] = claim


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
        if len(set(requested)) != len(requested):
            raise ValueError("acquire batch contains a duplicate evidence query")
        capabilities = self._require_claims()
        results = []
        renewer = _ChunkAcquireRenewer(self)
        maximum = min(capabilities.maximum_batch_size, self.maximum_batch_size, 1000)
        renewer.start()
        try:
            for offset in range(0, len(requested), maximum):
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
                        renewer.add(result.claim)
        finally:
            renewer.stop()
        renewer.raise_if_failed()
        current = renewer.claims()
        return tuple(
            ClaimAcquireResult(result.disposition, claim=current[result.claim.key])
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
            if renew:
                payload = ClaimRenewResponse.model_validate(response.json())
                if len(payload.claims) != len(chunk):
                    raise StoreIntegrityError(
                        "shared Store returned incomplete renewed claims"
                    )
                renewed = tuple(model.to_domain() for model in payload.claims)
                if tuple(
                    (claim.key, claim.owner_token, claim.generation)
                    for claim in renewed
                ) != tuple(
                    (claim.key, claim.owner_token, claim.generation) for claim in chunk
                ):
                    raise StoreIntegrityError(
                        "shared Store returned mismatched renewed claims"
                    )
                returned.extend(renewed)
            else:
                payload = ClaimReleaseResponse.model_validate(response.json())
                if tuple(
                    (ack.key.to_domain(), ack.generation) for ack in payload.released
                ) != tuple((claim.key, claim.generation) for claim in chunk):
                    raise StoreIntegrityError(
                        "shared Store returned mismatched claim release"
                    )
        return tuple(returned)

    def _claim_request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
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
