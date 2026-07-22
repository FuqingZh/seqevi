"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import httpx

from seqevi.errors import EvidenceConflictError, StoreError, StoreIntegrityError
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
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
    FetchManyRequest,
    FetchManyResponse,
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
        maximum_batch_size: int = 1000,
        client: httpx.Client | None = None,
    ) -> None:
        if maximum_batch_size < 1:
            raise ValueError("maximum_batch_size must be positive")
        self.maximum_artifact_bytes = maximum_artifact_bytes
        self.maximum_batch_size = maximum_batch_size
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
                    _raise_for_store_status(response)
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


def _raise_for_store_status(response: httpx.Response) -> None:
    if response.is_success:
        return
    detail = response.text
    if response.status_code == 409:
        raise EvidenceConflictError(detail)
    raise StoreError(f"shared Store returned HTTP {response.status_code}: {detail}")


def _artifact_identity(artifact: ArtifactFile) -> tuple[str, str, int]:
    return artifact.digest, artifact.media_type, artifact.byte_size
