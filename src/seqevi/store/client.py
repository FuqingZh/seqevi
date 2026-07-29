"""Synchronous HTTP client implementing the logical evidence Store contract."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

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
    HealthResponse,
    LookupRequest,
    LookupResponse,
)

_TRANSFER_CHUNK_SIZE = 1024 * 1024


class HttpEvidenceStore:
    """Remote Store client with exact artifact integrity verification.

    Store URLs must not contain credentials. For deployment Basic
    authentication, pass ``basic_auth_file`` as an absolute path to a regular,
    non-symlink file owned by the process UID with no group/other permissions.
    The file contains exactly two non-empty UTF-8 lines: username, then
    password.

    Example:
        >>> store = HttpEvidenceStore(
        ...     "https://node4.cluster.local:18443",
        ...     basic_auth_file="/run/secrets/seqevi-basic-auth",
        ... )
        >>> store.close()
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 120.0,
        maximum_artifact_bytes: int | None = None,
        maximum_batch_size: int | None = None,
        basic_auth_file: str | Path | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        """Initialize one shared Store client.

        Args:
            base_url: Credential-free HTTP(S) Store URL.
            timeout_seconds: Complete request timeout.
            maximum_artifact_bytes: Optional override for health discovery.
            maximum_batch_size: Optional override for health discovery.
            basic_auth_file: Optional owner-only two-line Basic-auth file.
            client: Optional preconfigured HTTPX client for embedding or tests.

        Raises:
            ValueError: If the URL contains credentials, the auth file is
                unsafe or malformed, or ``basic_auth_file`` is combined with
                ``client``.
        """
        parsed_url = urlsplit(base_url)
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError("shared Store URL must not contain credentials")
        if client is not None and basic_auth_file is not None:
            raise ValueError("basic_auth_file cannot be combined with a custom client")
        self._uploaded_artifact_digests: set[str] = set()
        self._download_directory = tempfile.TemporaryDirectory(
            prefix="seqevi-http-artifacts-"
        )
        self._download_root = Path(self._download_directory.name)
        self._downloaded_artifacts: dict[str, ArtifactFile] = {}
        self._owns_client = client is None
        auth = _load_basic_auth_file(Path(basic_auth_file)) if basic_auth_file else None
        self.client = client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
            auth=auth,
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


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while chunk := handle.read(_TRANSFER_CHUNK_SIZE):
            yield chunk


def _load_basic_auth_file(path: Path) -> httpx.BasicAuth:
    """Load an owner-only two-line Basic-auth credential file."""

    if not path.is_absolute():
        raise ValueError("shared Store Basic-auth file path must be absolute")
    try:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise ValueError("shared Store Basic-auth file must be a regular file")
        if metadata.st_uid != os.geteuid():
            raise ValueError("shared Store Basic-auth file must be owned by this user")
        if metadata.st_mode & 0o077:
            raise ValueError(
                "shared Store Basic-auth file must not permit group/other access"
            )
        if not metadata.st_mode & stat.S_IRUSR:
            raise ValueError("shared Store Basic-auth file must be owner-readable")
        payload = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError("cannot read shared Store Basic-auth file") from error
    lines = payload.splitlines()
    if len(lines) != 2 or not lines[0] or not lines[1]:
        raise ValueError(
            "shared Store Basic-auth file must contain username and password lines"
        )
    username, password = lines
    if ":" in username or any(
        character in username + password for character in "\r\n\x00"
    ):
        raise ValueError("shared Store Basic-auth file contains invalid credentials")
    return httpx.BasicAuth(username, password)


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
