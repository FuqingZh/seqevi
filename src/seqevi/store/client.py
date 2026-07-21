"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from typing import Any

import httpx

from seqevi.errors import EvidenceConflictError, StoreError, StoreIntegrityError
from seqevi.evidence import (
    ArtifactPayload,
    CommitOutcome,
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
    EvidenceKeyModel,
    EvidenceQueryModel,
    FetchRequest,
    FetchResponse,
    LookupRequest,
    LookupResponse,
)

_TRANSFER_CHUNK_SIZE = 1024 * 1024


class HttpEvidenceStore:
    """Remote Store client with exact artifact integrity verification."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 120.0,
        maximum_artifact_bytes: int = 512 * 1024 * 1024,
        client: httpx.Client | None = None,
    ) -> None:
        self.maximum_artifact_bytes = maximum_artifact_bytes
        self._owns_client = client is None
        self.client = client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def __enter__(self) -> HttpEvidenceStore:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        requested = tuple(dict.fromkeys(requested_queries))
        request = LookupRequest(
            queries=[EvidenceQueryModel.from_domain(query) for query in requested]
        )
        response = self._request(
            "POST", "/v1/evidence/lookup", json=request.model_dump(mode="json")
        )
        payload = LookupResponse.model_validate(response.json())
        records = [record.to_domain() for record in payload.records]
        expected = {query.key for query in requested}
        observed = {record.key for record in records}
        if len(observed) != len(records) or not observed.issubset(expected):
            raise StoreIntegrityError("shared Store returned unexpected lookup records")
        return {record.key: record for record in records}

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        commits = tuple(proposed_commits)
        payloads: dict[str, ArtifactPayload] = {}
        for commit in commits:
            for payload in (commit.normalized_artifact, commit.raw_artifact):
                if payload is None:
                    continue
                existing = payloads.setdefault(payload.digest, payload)
                if existing != payload:
                    raise StoreIntegrityError(
                        f"artifact digest has conflicting payloads: {payload.digest}"
                    )
        for payload in payloads.values():
            self._upload(payload)
        request = CommitRequest(
            commits=[CommitModel.from_domain(item) for item in commits]
        )
        response = self._request(
            "POST", "/v1/evidence/commit", json=request.model_dump(mode="json")
        )
        outcomes = tuple(CommitResponse.model_validate(response.json()).outcomes)
        if len(outcomes) != len(commits):
            raise StoreIntegrityError(
                "shared Store returned incomplete commit outcomes"
            )
        return outcomes

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None:
        request = FetchRequest(key=EvidenceKeyModel.from_domain(key))
        response = self._request(
            "POST", "/v1/evidence/fetch", json=request.model_dump(mode="json")
        )
        model = FetchResponse.model_validate(response.json()).record
        if model is None:
            return None
        record = model.to_domain()
        if record.key != key:
            raise StoreIntegrityError("shared Store returned the wrong evidence key")
        return FetchedEvidence(
            record=record,
            normalized_artifact=self._download(record.normalized_artifact_digest),
            raw_artifact=self._download(record.raw_artifact_digest),
        )

    def _upload(self, payload: ArtifactPayload) -> None:
        headers = {
            "X-Artifact-Media-Type": payload.media_type,
            "X-Artifact-Byte-Size": str(len(payload.data)),
        }
        response = self._request(
            "PUT",
            f"/v1/artifacts/{payload.digest}",
            headers=headers,
            content=_chunks(payload.data),
        )
        uploaded = ArtifactUploadResponse.model_validate(response.json()).artifact
        expected = ArtifactReferenceModel(
            digest=payload.digest,
            media_type=payload.media_type,
            byte_size=len(payload.data),
        )
        if uploaded != expected:
            raise StoreIntegrityError("shared Store returned wrong artifact metadata")

    def _download(self, digest: str | None) -> bytes | None:
        if digest is None:
            return None
        hasher = hashlib.sha256()
        payload = bytearray()
        try:
            with self.client.stream("GET", f"/v1/artifacts/{digest}") as response:
                _raise_for_store_status(response)
                for chunk in response.iter_bytes(_TRANSFER_CHUNK_SIZE):
                    payload.extend(chunk)
                    if len(payload) > self.maximum_artifact_bytes:
                        raise StoreIntegrityError(
                            "artifact exceeds configured client download limit"
                        )
                    hasher.update(chunk)
        except httpx.HTTPError as error:
            raise StoreError(f"shared Store request failed: {error}") from error
        if hasher.hexdigest() != digest:
            raise StoreIntegrityError(f"artifact digest mismatch: {digest}")
        return bytes(payload)

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            raise StoreError(f"shared Store request failed: {error}") from error
        _raise_for_store_status(response)
        return response


def _chunks(data: bytes) -> Iterator[bytes]:
    for offset in range(0, len(data), _TRANSFER_CHUNK_SIZE):
        yield data[offset : offset + _TRANSFER_CHUNK_SIZE]


def _raise_for_store_status(response: httpx.Response) -> None:
    if response.is_success:
        return
    detail = response.text
    if response.status_code == 409:
        raise EvidenceConflictError(detail)
    raise StoreError(f"shared Store returned HTTP {response.status_code}: {detail}")
