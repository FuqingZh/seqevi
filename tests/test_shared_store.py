from __future__ import annotations

import asyncio
import importlib
import hashlib
import os
import logging
import json
import runpy
import socket
import subprocess
import sys
import threading
import tempfile
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from inspect import getclosurevars, signature
from typing import Any, cast

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import (
    BigInteger,
    create_engine,
    event,
    func,
    inspect,
    make_url,
    text,
    select,
)
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, TimeoutError as SQLAlchemyTimeoutError

from seqevi.annotate import run_annotation
from seqevi.errors import (
    ClaimReceiptCapacityError,
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreError,
    StoreBackpressureError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    SessionEvidenceClaim,
    StoredArtifact,
    sha256_digest,
)
from seqevi.sequence import SequenceIdentity, identify_protein_sequence
from seqevi.service import (
    ServiceSettings,
    configure_claim_logging,
    create_service_app,
)
from seqevi.service import persistence as persistence_module
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.service.persistence import ServicePersistence
from seqevi.store import HttpEvidenceStore, LocalStore
from seqevi.store import migration as store_migration
from seqevi.store.schema import (
    artifacts,
    claim_session_acquire_receipt_items,
    claim_session_acquire_receipts,
    claim_sessions,
    evidence_claim_generations,
    session_claims,
)
from seqevi.store.transport import (
    ClaimSessionFinalizeItem,
    ClaimSessionOpenResponse,
    CommitModel,
    EvidenceRecordModel,
    EvidenceQueryModel,
    SessionEvidenceClaimModel,
    canonical_query_digest,
)

from polars.testing import assert_frame_equal

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
    read_result_table,
    write_artifact_file,
    write_fixture_database,
    write_fixture_tool,
)


class MemoryPersistence:
    def __init__(self) -> None:
        self.records: dict[EvidenceKey, EvidenceRecord] = {}
        self.sequences: dict[str, object] = {}
        self.artifacts: dict[str, StoredArtifact] = {}
        self.fetch_many_calls = 0
        self.lookup_many_calls = 0
        self.closed = False

    @property
    def supports_claim_sessions(self) -> bool:
        return False

    def lookup_many(
        self, queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        self.lookup_many_calls += 1
        found = {}
        for query in queries:
            persisted = self.sequences.get(query.identity.sequence_id)
            if persisted is not None and persisted != query.identity:
                raise StoreIntegrityError("SequenceID collision in lookup")
            if query.key in self.records:
                found[query.key] = self.records[query.key]
        return found

    def commit_many(
        self,
        commits: Iterable[CommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]:
        outcomes = []
        self.artifacts.update(stored_artifacts)
        for commit in commits:
            identity = commit.identity.to_domain()
            key = commit.key.to_domain()
            persisted_identity = self.sequences.setdefault(
                identity.sequence_id, identity
            )
            if persisted_identity != identity:
                raise StoreIntegrityError("SequenceID collision in shared Store")
            record = EvidenceRecord(
                key=key,
                status=commit.status,
                payload_digest=commit.payload_digest,
                normalized_artifact_digest=(
                    commit.normalized_artifact.digest
                    if commit.normalized_artifact
                    else None
                ),
                raw_artifact_digest=(
                    commit.raw_artifact.digest if commit.raw_artifact else None
                ),
                created_at=datetime.now(UTC),
            )
            existing = self.records.setdefault(key, record)
            if existing != record:
                comparable = (existing.status, existing.payload_digest)
                proposed = (record.status, record.payload_digest)
                if comparable != proposed:
                    raise EvidenceConflictError("immutable payload conflict")
            outcomes.append(
                CommitOutcome.CREATED if existing is record else CommitOutcome.EXISTING
            )
        return tuple(outcomes)

    def fetch_record(self, key: EvidenceKey) -> EvidenceRecord | None:
        return self.records.get(key)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        self.fetch_many_calls += 1
        return {key: self.records[key] for key in keys if key in self.records}

    def artifact_metadata(self, digest: str) -> StoredArtifact | None:
        return self.artifacts.get(digest)

    def database_time(self) -> datetime:
        return datetime.now(UTC)

    def open_claim_session(self, **_kwargs):
        raise NotImplementedError

    def renew_claim_session(self, _authority):
        raise NotImplementedError

    def close_claim_session(self, _authority) -> None:
        raise NotImplementedError

    def acquire_claim_session(self, _authority, _queries):
        raise NotImplementedError

    def finalize_claim_session(self, _authority, _commits, _stored_artifacts):
        raise NotImplementedError

    def close(self) -> None:
        self.closed = True


def _memory_persistence() -> ServicePersistence:
    return cast(ServicePersistence, MemoryPersistence())


def _key(sequence: str) -> tuple[SequenceIdentity, EvidenceKey]:
    identity = identify_protein_sequence(sequence)
    key = EvidenceKey.from_parameters(
        sequence_id=identity.sequence_id,
        adapter_contract_version="fixture/1",
        tool_runtime_digest="sha256:" + "a" * 64,
        resource_id="fixture/1",
        semantic_parameters={"threshold": 0.01},
    )
    return identity, key


def _distinct_query_with_same_key(query: EvidenceQuery) -> EvidenceQuery:
    class DistinctQuery:
        identity = query.identity
        key = query.key

    return cast(EvidenceQuery, DistinctQuery())


def _hit_commit(
    artifact_dir: Path,
    sequence: str = "MPEPTIDE",
    *,
    normalized_data: bytes = b"normalized",
    raw_data: bytes = b"raw",
) -> EvidenceCommit:
    identity, key = _key(sequence)
    normalized_digest = sha256_digest(normalized_data)
    raw_digest = sha256_digest(raw_data)
    normalized = write_artifact_file(
        artifact_dir / f"{normalized_digest}.parquet",
        normalized_data,
        "application/vnd.apache.parquet",
    )
    raw = write_artifact_file(
        artifact_dir / f"{raw_digest}.tsv",
        raw_data,
        "text/tab-separated-values",
    )
    return EvidenceCommit(
        identity=identity,
        key=key,
        status=EvidenceStatus.HIT,
        payload_digest=sha256_digest(b"scientific-result"),
        normalized_artifact=normalized,
        raw_artifact=raw,
    )


def _settings(
    tmp_path: Path,
    *,
    maximum_batch_size: int = 1000,
    maximum_artifact_bytes: int = 512 * 1024 * 1024,
) -> ServiceSettings:
    return ServiceSettings(
        database_url="postgresql+psycopg://unused/seqevi",
        artifacts_dir=tmp_path / "artifacts",
        maximum_batch_size=maximum_batch_size,
        maximum_artifact_bytes=maximum_artifact_bytes,
    )


def test_http_store_matches_lookup_commit_and_fetch_contract(tmp_path: Path) -> None:
    persistence = MemoryPersistence()
    app = create_service_app(
        _settings(tmp_path), persistence=cast(ServicePersistence, persistence)
    )
    commit = _hit_commit(tmp_path / "sources")

    with TestClient(app) as test_client:
        store = HttpEvidenceStore(
            "http://testserver",
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        assert store.commit_many((commit,)) == (CommitOutcome.CREATED,)
        assert store.commit_many((commit,)) == (CommitOutcome.EXISTING,)
        query = EvidenceQuery(commit.identity, commit.key)
        found = store.lookup_many((query, query))
        fetched = store.fetch(commit.key)

    assert found[commit.key].status is EvidenceStatus.HIT
    assert len(found) == 1
    assert fetched is not None
    assert fetched.normalized_artifact is not None
    assert fetched.raw_artifact is not None
    assert fetched.normalized_artifact.path.read_bytes() == b"normalized"
    assert fetched.raw_artifact.path.read_bytes() == b"raw"
    assert persistence.closed


def test_http_store_preserves_no_hit_without_normalized_artifact(
    tmp_path: Path,
) -> None:
    identity, key = _key("MNOHIT")
    commit = EvidenceCommit(
        identity=identity,
        key=key,
        status=EvidenceStatus.NO_HIT,
        payload_digest=sha256_digest(b"no-hit"),
        raw_artifact=write_artifact_file(
            tmp_path / "sources" / "completed.txt", b"completed", "text/plain"
        ),
    )
    persistence = MemoryPersistence()
    app = create_service_app(
        _settings(tmp_path), persistence=cast(ServicePersistence, persistence)
    )
    with TestClient(app) as test_client:
        store = HttpEvidenceStore(
            "http://testserver",
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        store.commit_many((commit,))
        fetched = store.fetch(key)

    assert fetched is not None
    assert fetched.record.status is EvidenceStatus.NO_HIT
    assert fetched.normalized_artifact is None
    assert fetched.raw_artifact is not None
    assert fetched.raw_artifact.path.read_bytes() == b"completed"


def test_http_fetch_many_chunks_metadata_requests_without_n_plus_one(
    tmp_path: Path,
) -> None:
    persistence = MemoryPersistence()
    app = create_service_app(
        _settings(tmp_path, maximum_batch_size=2),
        persistence=cast(ServicePersistence, persistence),
    )
    first = _hit_commit(tmp_path / "sources", "MONE")
    second = _hit_commit(tmp_path / "sources", "MTWO")
    third = _hit_commit(tmp_path / "sources", "MTHREE")

    with TestClient(app) as test_client:
        store = HttpEvidenceStore(
            "http://testserver",
            maximum_batch_size=2,
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        store.commit_many((first, second))
        store.commit_many((third,))
        fetched = store.fetch_many((first.key, second.key, third.key))

    assert set(fetched) == {first.key, second.key, third.key}
    assert persistence.fetch_many_calls == 2


def test_http_lookup_and_commit_follow_discovered_service_batch_size(
    tmp_path: Path,
) -> None:
    persistence = MemoryPersistence()
    app = create_service_app(
        _settings(tmp_path, maximum_batch_size=2),
        persistence=cast(ServicePersistence, persistence),
    )
    commits = tuple(
        _hit_commit(tmp_path / "sources", sequence)
        for sequence in ("MONE", "MTWO", "MTHREE")
    )

    with TestClient(app) as test_client:
        store = HttpEvidenceStore(
            "http://testserver",
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        assert store.maximum_batch_size == 2
        assert store.commit_many(commits) == (CommitOutcome.CREATED,) * 3
        found = store.lookup_many(
            EvidenceQuery(commit.identity, commit.key) for commit in commits
        )

    assert set(found) == {commit.key for commit in commits}
    assert persistence.lookup_many_calls == 2


def test_http_store_uses_discovered_artifact_limit_and_formats_stream_errors(
    tmp_path: Path,
) -> None:
    persistence = MemoryPersistence()
    identity, key = _key("MMISSING")
    persistence.records[key] = EvidenceRecord(
        key=key,
        status=EvidenceStatus.NO_HIT,
        payload_digest=sha256_digest(b"missing"),
        normalized_artifact_digest=None,
        raw_artifact_digest="f" * 64,
        created_at=datetime.now(UTC),
    )
    app = create_service_app(
        _settings(tmp_path, maximum_artifact_bytes=1234),
        persistence=cast(ServicePersistence, persistence),
    )

    with TestClient(app) as test_client:
        store = HttpEvidenceStore(
            "http://testserver",
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        assert store.maximum_artifact_bytes == 1234
        with pytest.raises(StoreError, match="HTTP 404"):
            store.fetch(key)


def test_service_rejects_bad_artifact_digest_and_oversized_batches(
    tmp_path: Path,
) -> None:
    app = create_service_app(
        _settings(tmp_path, maximum_batch_size=1),
        persistence=_memory_persistence(),
    )
    with TestClient(app) as client:
        response = client.put(
            "/v1/artifacts/" + "0" * 64,
            content=b"wrong",
            headers={
                "X-Artifact-Media-Type": "application/octet-stream",
                "X-Artifact-Byte-Size": "5",
            },
        )
        oversized = client.post(
            "/v1/evidence/lookup",
            json={
                "queries": [
                    {
                        "identity": {
                            "sequence_id": item.identity.sequence_id,
                            "md5": item.identity.md5,
                            "length": item.identity.length,
                            "sequence": item.identity.sequence,
                        },
                        "key": {
                            "sequence_id": item.key.sequence_id,
                            "adapter_contract_version": item.key.adapter_contract_version,
                            "tool_runtime_digest": item.key.tool_runtime_digest,
                            "resource_id": item.key.resource_id,
                            "semantic_parameters_json": item.key.semantic_parameters_json,
                        },
                    }
                    for item in (
                        EvidenceQuery(
                            _hit_commit(tmp_path / "sources", "MONE").identity,
                            _hit_commit(tmp_path / "sources", "MONE").key,
                        ),
                        EvidenceQuery(
                            _hit_commit(tmp_path / "sources", "MTWO").identity,
                            _hit_commit(tmp_path / "sources", "MTWO").key,
                        ),
                    )
                ]
            },
        )
        oversized_fetch = client.post(
            "/v1/evidence/fetch-many",
            json={
                "keys": [
                    {
                        "sequence_id": item.key.sequence_id,
                        "adapter_contract_version": item.key.adapter_contract_version,
                        "tool_runtime_digest": item.key.tool_runtime_digest,
                        "resource_id": item.key.resource_id,
                        "semantic_parameters_json": item.key.semantic_parameters_json,
                    }
                    for item in (
                        _hit_commit(tmp_path / "sources", "MONE"),
                        _hit_commit(tmp_path / "sources", "MTWO"),
                    )
                ]
            },
        )

    assert response.status_code == 409
    assert oversized.status_code == 413
    assert oversized_fetch.status_code == 413


def test_service_openapi_preserves_legacy_and_adds_claim_operations(
    tmp_path: Path,
) -> None:
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    paths = set(app.openapi()["paths"])
    assert paths == {
        "/health",
        "/v1/storage/discovery",
        "/v1/artifacts/resolve",
        "/v1/artifacts/{digest}",
        "/v1/evidence/commit",
        "/v1/evidence/fetch",
        "/v1/evidence/fetch-many",
        "/v1/evidence/lookup",
        "/v1/internal/claim-sessions/capabilities",
        "/v1/internal/claim-sessions/open",
        "/v1/internal/claim-sessions/renew",
        "/v1/internal/claim-sessions/acquire",
        "/v1/internal/claim-sessions/authority",
        "/v1/internal/claim-sessions/finalize",
        "/v1/internal/claim-sessions/close",
    }


def test_claim_session_transport_enforces_authority_timing_limits() -> None:
    authority = {
        "session_id": "session",
        "owner_token": "owner",
        "generation": 1,
    }
    timing = {
        **authority,
        "expires_at": datetime.now(UTC),
        "heartbeat_after_seconds": 30,
    }
    ClaimSessionOpenResponse.model_validate(
        {
            **timing,
            "remaining_lease_seconds": 120,
            "renew_deadline_seconds": 90,
        }
    )
    for field, value in (
        ("remaining_lease_seconds", 120.000001),
        ("renew_deadline_seconds", 90.000001),
    ):
        with pytest.raises(ValidationError):
            ClaimSessionOpenResponse.model_validate(
                {
                    **timing,
                    "remaining_lease_seconds": 120,
                    "renew_deadline_seconds": 90,
                    field: value,
                }
            )


def test_configure_claim_logging_attaches_an_info_handler() -> None:
    logger = logging.getLogger("seqevi.service.claims")
    handlers = list(logger.handlers)
    propagate = logger.propagate
    try:
        logger.handlers.clear()
        logger.propagate = True
        configure_claim_logging()
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        assert logger.handlers[0].level == logging.INFO
        assert logger.handlers[0].formatter is not None
        assert not logger.propagate
    finally:
        logger.handlers.clear()
        logger.handlers.extend(handlers)
        logger.propagate = propagate


def test_claim_request_logging_uses_validated_batch_size_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ObservedPersistence(MemoryPersistence):
        finalize_calls = 0

        @property
        def supports_claim_sessions(self) -> bool:
            return True

        def acquire_claim_session(  # pyright: ignore[reportIncompatibleMethodOverride]
            self, _authority, **_kwargs
        ):
            return ()

        def finalize_claim_session(self, _authority, commits, _stored_artifacts):
            proposed = tuple(commits)
            self.finalize_calls += 1
            return (CommitOutcome.CREATED,) * len(proposed)

    observed: list[tuple[str, int | None, int]] = []
    service_app_module = importlib.import_module("seqevi.service.app")

    def record(operation: str, **fields: object) -> None:
        observed.append(
            (
                operation,
                cast(int | None, fields["batch_size"]),
                cast(int, fields["status_code"]),
            )
        )

    monkeypatch.setattr(service_app_module, "_log_claim_request", record)
    persistence = ObservedPersistence()
    app = create_service_app(
        _settings(tmp_path), persistence=cast(ServicePersistence, persistence)
    )
    identity, key = _key("MOBSERVEDCLAIMBATCH")
    query = EvidenceQuery(identity, key)
    commit = _hit_commit(tmp_path / "source", "MOBSERVEDCLAIMBATCH")
    authority = {"session_id": "session", "owner_token": "owner", "generation": 1}

    with TestClient(app) as client:
        acquire = client.post(
            "/v1/internal/claim-sessions/acquire",
            json={
                **authority,
                "acquire_request_id": "acquire-observed",
                "query_digest": canonical_query_digest(
                    [EvidenceQueryModel.from_domain(query)]
                ),
                "queries": [
                    EvidenceQueryModel.from_domain(query).model_dump(mode="json")
                ],
            },
        )
        empty = client.post(
            "/v1/internal/claim-sessions/finalize",
            json={
                **authority,
                "finalize_request_id": "finalize-empty",
                "commits": [],
            },
        )
        for artifact in (commit.normalized_artifact, commit.raw_artifact):
            assert artifact is not None
            uploaded = client.put(
                f"/v1/artifacts/{artifact.digest}",
                headers={
                    "X-Artifact-Media-Type": artifact.media_type,
                    "X-Artifact-Byte-Size": str(artifact.byte_size),
                },
                content=artifact.path.read_bytes(),
            )
            assert uploaded.status_code == 200
        finalized = client.post(
            "/v1/internal/claim-sessions/finalize",
            json={
                **authority,
                "finalize_request_id": "finalize-observed",
                "commits": [
                    ClaimSessionFinalizeItem(
                        commit=CommitModel.from_domain(commit), claim_generation=1
                    ).model_dump(mode="json")
                ],
            },
        )

    assert acquire.status_code == 200
    assert empty.status_code == 422
    assert finalized.status_code == 200
    assert persistence.finalize_calls == 1
    assert [item for item in observed if item[0] in {"acquire", "finalize"}] == [
        ("acquire", 1, 200),
        ("finalize", 0, 422),
        ("finalize", 1, 200),
    ]


def _claim_mock_client(handler: httpx.MockTransport) -> httpx.Client:
    def legacy_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/storage/discovery":
            return httpx.Response(404)
        response = handler.handle_request(request)
        if request.url.path == "/health":
            # Claim-only handlers predate storage discovery; retain explicit
            # health limits but supply the healthy legacy bootstrap otherwise.
            if response.status_code == 200 and "maximum_batch_size" in response.json():
                return response
            return httpx.Response(200, json=_claim_health())
        return response

    return httpx.Client(
        transport=httpx.MockTransport(legacy_handler), base_url="http://testserver"
    )


def _claim_health(maximum_batch_size: int = 1000) -> dict[str, object]:
    return {
        "status": "ok",
        "api_version": "v1",
        "maximum_batch_size": maximum_batch_size,
        "maximum_artifact_bytes": 1024,
    }


def test_http_store_released_constructor_compatibility_boundary() -> None:
    parameters = signature(HttpEvidenceStore).parameters
    assert [name for name in parameters if not name.startswith("_")] == [
        "base_url",
        "timeout_seconds",
        "maximum_artifact_bytes",
        "maximum_batch_size",
        "oci_files",
    ]
    assert "client" not in parameters
    with httpx.Client() as released_client:
        with pytest.raises(TypeError, match="client"):
            HttpEvidenceStore(
                "http://testserver",
                client=released_client,  # type: ignore[call-arg]
            )
    store_module = importlib.import_module("seqevi.store")
    assert not hasattr(store_module, "ClaimCapableEvidenceStore")
    assert not hasattr(store_module, "is_claim_capable_store")


class _BlockingAsyncBody(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.cancelled = threading.Event()
        self.started = threading.Event()

    async def __aiter__(self):
        self.started.set()
        yield b"{"
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()


def test_claim_transport_deadline_aborts_body_and_joins_runtime() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    body = _BlockingAsyncBody()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=body)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        runtime.request("GET", "/slow", deadline=started + 0.05)
    assert time.monotonic() - started < 0.5
    assert body.cancelled.wait(0.1)
    runtime.close()
    assert not runtime._thread.is_alive()


def test_claim_transport_cancellation_is_request_local() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    blocked = threading.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/slow":
            blocked.set()
            await asyncio.Event().wait()
        return httpx.Response(200, json={"path": request.url.path})

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    failures: list[BaseException] = []

    def slow_request() -> None:
        try:
            runtime.request("GET", "/slow", deadline=time.monotonic() + 0.1)
        except BaseException as error:
            failures.append(error)

    worker = threading.Thread(target=slow_request)
    worker.start()
    assert blocked.wait(0.2)
    response = runtime.request("GET", "/fast", deadline=time.monotonic() + 0.2)
    worker.join(0.5)
    assert response.json() == {"path": "/fast"}
    assert len(failures) == 1 and isinstance(failures[0], TimeoutError)
    assert not worker.is_alive()
    runtime.close()


def test_claim_transport_close_cancels_and_joins_outstanding_request() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    blocked = threading.Event()
    cancelled = threading.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        blocked.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("blocked request unexpectedly resumed")

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    worker = threading.Thread(
        target=lambda: pytest.raises(
            BaseException,
            runtime.request,
            "GET",
            "/blocked",
            deadline=time.monotonic() + 10.0,
        )
    )
    worker.start()
    assert blocked.wait(0.2)
    runtime.close()
    worker.join(0.2)
    assert cancelled.is_set()
    assert not worker.is_alive()
    assert not runtime._thread.is_alive()


def test_artifact_reader_timeout_kills_and_reaps_stalled_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"payload")
    pid_file = tmp_path / "reader.pid"
    monkeypatch.setattr(
        store_client_module,
        "_artifact_reader_command",
        lambda path: (
            sys.executable,
            "-c",
            (
                "import os, pathlib, signal, sys; "
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), "
                "encoding='ascii'); signal.pause()"
            ),
            str(pid_file),
        ),
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        return httpx.Response(200)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    errors: list[BaseException] = []

    def request_upload() -> None:
        try:
            runtime.request(
                "PUT",
                "/upload",
                deadline=time.monotonic() + 10.0,
                content=store_client_module._async_file_chunks(artifact),
            )
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(
        target=request_upload,
    )
    worker.start()
    for _attempt in range(50):
        if pid_file.exists() and pid_file.stat().st_size:
            break
        time.sleep(0.01)
    reader_pid = int(pid_file.read_text(encoding="ascii"))
    runtime.close()
    worker.join(1.0)
    assert not worker.is_alive()
    assert errors
    with pytest.raises(ProcessLookupError):
        os.kill(reader_pid, 0)


def test_artifact_reader_streams_more_than_one_chunk_exactly(tmp_path: Path) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    content = os.urandom(store_client_module._TRANSFER_CHUNK_SIZE + 17)
    artifact = tmp_path / "artifact"
    artifact.write_bytes(content)
    observed: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed.append(await request.aread())
        return httpx.Response(200)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    runtime.request(
        "PUT",
        "/upload",
        deadline=time.monotonic() + 10.0,
        content=store_client_module._async_file_chunks(artifact),
    )
    runtime.close()
    assert observed == [content]


def test_private_helpers_ignore_shadow_package_in_cwd_and_pythonpath(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    shadow_root = tmp_path / "shadow"
    shadow_package = shadow_root / "seqevi" / "store"
    shadow_package.mkdir(parents=True)
    marker = tmp_path / "shadow-loaded"
    (shadow_root / "seqevi" / "__init__.py").write_text(
        f"from pathlib import Path; Path({str(marker)!r}).write_text('loaded')\n",
        encoding="utf-8",
    )
    (shadow_package / "__init__.py").write_text("", encoding="utf-8")
    (shadow_package / "_artifact_reader.py").write_text(
        "raise RuntimeError('shadow artifact reader loaded')\n", encoding="utf-8"
    )
    (shadow_package / "_resolver.py").write_text(
        "raise RuntimeError('shadow resolver loaded')\n", encoding="utf-8"
    )
    monkeypatch.chdir(shadow_root)
    monkeypatch.setenv("PYTHONPATH", str(shadow_root))
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"isolated payload")
    observed: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed.append(await request.aread())
        return httpx.Response(200)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    runtime.request(
        "PUT",
        "/upload",
        deadline=time.monotonic() + 10.0,
        content=store_client_module._async_file_chunks(artifact),
    )
    resolved = runtime.run(runtime._loop.getaddrinfo(b"localhost", 443, type=1))
    runtime.close()
    assert observed == [b"isolated payload"]
    assert resolved
    assert not marker.exists()


def test_request_closes_upload_stream_when_transport_returns_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"payload")
    pid_file = tmp_path / "reader.pid"
    monkeypatch.setattr(
        store_client_module,
        "_artifact_reader_command",
        lambda _path: (
            sys.executable,
            "-c",
            (
                "import os,pathlib,signal,struct,sys;"
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()),encoding='ascii');"
                "sys.stdout.buffer.write(struct.pack('!q',7)+b'payload');"
                "sys.stdout.buffer.flush();signal.pause()"
            ),
            str(pid_file),
        ),
    )

    class EarlyResponseTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            stream = cast(httpx.AsyncByteStream, request.stream).__aiter__()
            assert await stream.__anext__() == b"payload"
            return httpx.Response(200, json={"accepted": True})

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, EarlyResponseTransport()
    )
    response = runtime.request(
        "PUT",
        "/upload",
        deadline=time.monotonic() + 10.0,
        content=store_client_module._async_file_chunks(artifact),
    )
    assert response.json() == {"accepted": True}
    reader_pid = int(pid_file.read_text(encoding="ascii"))
    with pytest.raises(ProcessLookupError):
        os.kill(reader_pid, 0)
    runtime.close()


def test_artifact_reader_reports_error_and_is_reaped(tmp_path: Path) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        return httpx.Response(200)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver", 10.0, httpx.MockTransport(handler)
    )
    with pytest.raises(StoreIntegrityError, match="artifact reader failed"):
        runtime.request(
            "PUT",
            "/upload",
            deadline=time.monotonic() + 10.0,
            content=store_client_module._async_file_chunks(tmp_path / "missing"),
        )
    runtime.close()


def test_claim_transport_request_close_admission_race_does_not_strand() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    runtime = store_client_module._ClaimTransportRuntime(
        "http://testserver",
        10.0,
        httpx.MockTransport(lambda _request: httpx.Response(200)),
    )
    runtime._admission_lock.acquire()
    request_done = threading.Event()
    close_done = threading.Event()
    request_errors: list[BaseException] = []
    close_errors: list[BaseException] = []

    def request_once() -> None:
        try:
            runtime.request("GET", "/race", deadline=time.monotonic() + 1.0)
        except BaseException as error:
            request_errors.append(error)
        finally:
            request_done.set()

    def close_once() -> None:
        try:
            runtime.close()
        except BaseException as error:
            close_errors.append(error)
        finally:
            close_done.set()

    request = threading.Thread(target=request_once)
    close = threading.Thread(target=close_once)
    request.start()
    close.start()
    runtime._admission_lock.release()
    request.join(0.5)
    close.join(0.5)
    assert request_done.is_set()
    assert close_done.is_set()
    assert not request.is_alive()
    assert not close.is_alive()
    assert not close_errors
    assert not request_errors or all(
        isinstance(error, RuntimeError) for error in request_errors
    )
    runtime.close()


def test_claim_resolver_normal_result_shape() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    runtime = store_client_module._ClaimTransportRuntime(
        "http://127.0.0.1", 10.0, httpx.MockTransport(lambda _: httpx.Response(200))
    )
    resolved = runtime.run(runtime._loop.getaddrinfo(b"localhost", 443, type=1))
    runtime.close()
    assert resolved
    assert all(len(item) == 5 and isinstance(item[4], tuple) for item in resolved)


def test_claim_resolver_close_kills_and_reaps_stalled_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    pid_file = tmp_path / "resolver.pid"
    monkeypatch.setattr(
        store_client_module,
        "_resolver_command",
        lambda *_args: (
            sys.executable,
            "-c",
            (
                "import os,pathlib,signal,sys;"
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()),encoding='ascii');"
                "signal.pause()"
            ),
            str(pid_file),
        ),
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        await asyncio.get_running_loop().getaddrinfo(b"stalled.invalid", 443)
        return httpx.Response(200)

    runtime = store_client_module._ClaimTransportRuntime(
        "http://127.0.0.1", 10.0, httpx.MockTransport(handler)
    )
    errors: list[BaseException] = []
    close_errors: list[BaseException] = []
    close_done = threading.Event()

    def resolve() -> None:
        try:
            runtime.request("GET", "/resolve", deadline=time.monotonic() + 0.2)
        except BaseException as error:
            errors.append(error)

    def close_runtime() -> None:
        try:
            runtime.close()
        except BaseException as error:
            close_errors.append(error)
        finally:
            close_done.set()

    worker = threading.Thread(target=resolve)
    worker.start()
    for _attempt in range(50):
        if pid_file.exists() and pid_file.stat().st_size:
            break
        time.sleep(0.01)
    resolver_pid = int(pid_file.read_text(encoding="ascii"))
    closer = threading.Thread(target=close_runtime)
    closer.start()
    worker.join(1.0)
    closer.join(1.0)
    assert len(errors) == 1 and isinstance(errors[0], TimeoutError)
    assert not close_errors
    assert not worker.is_alive()
    assert not closer.is_alive()
    assert close_done.is_set()
    with pytest.raises(ProcessLookupError):
        os.kill(resolver_pid, 0)


def test_claim_transport_concurrent_close_waits_for_owner() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    runtime = store_client_module._ClaimTransportRuntime(
        "http://127.0.0.1", 10.0, httpx.MockTransport(lambda _: httpx.Response(200))
    )
    operation_started = threading.Event()
    release = threading.Event()

    def operation() -> None:
        with runtime.operation():
            operation_started.set()
            release.wait()

    worker = threading.Thread(target=operation)
    worker.start()
    assert operation_started.wait(0.2)
    closed: list[int] = []
    close_errors: list[BaseException] = []

    def close_runtime(index: int) -> None:
        try:
            runtime.close()
            closed.append(index)
        except BaseException as error:
            close_errors.append(error)

    closers = [
        threading.Thread(target=close_runtime, args=(index,)) for index in range(2)
    ]
    for closer in closers:
        closer.start()
    time.sleep(0.05)
    assert not closed
    release.set()
    for closer in closers:
        closer.join(0.5)
    worker.join(0.5)
    assert not worker.is_alive()
    assert all(not closer.is_alive() for closer in closers)
    assert not close_errors
    assert sorted(closed) == [0, 1]


def test_http_store_concurrent_close_waits_for_all_cleanup_and_shares_error() -> None:
    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(lambda _: httpx.Response(404)),
    )
    download_root = store._download_root
    close_started = threading.Event()
    release_close = threading.Event()

    class BlockingFailingClient:
        def close(self) -> None:
            close_started.set()
            release_close.wait()
            raise RuntimeError("sync close failed")

    store.client = cast(Any, BlockingFailingClient())
    store._owns_client = True
    errors: list[BaseException] = []
    done = [threading.Event(), threading.Event()]

    def close_store(index: int) -> None:
        try:
            store.close()
        except BaseException as error:
            errors.append(error)
        finally:
            done[index].set()

    closers = [
        threading.Thread(target=close_store, args=(index,)) for index in range(2)
    ]
    closers[0].start()
    assert close_started.wait(0.5)
    closers[1].start()
    time.sleep(0.05)
    assert not done[0].is_set()
    assert not done[1].is_set()
    assert download_root.exists()
    release_close.set()
    for closer in closers:
        closer.join(1.0)
    assert all(not closer.is_alive() for closer in closers)
    assert all(event.is_set() for event in done)
    assert len(errors) == 2
    assert all(str(error) == "sync close failed" for error in errors)
    assert not download_root.exists()


def test_http_store_close_publishes_unexpected_cleanup_failure_to_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(lambda _: httpx.Response(404)),
    )
    original_cleanup = store._cleanup_resources
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    errors: list[BaseException] = []

    def fail_cleanup() -> BaseException | None:
        cleanup_started.set()
        release_cleanup.wait()
        raise RuntimeError("unexpected cleanup failure")

    def close_store() -> None:
        try:
            store.close()
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(store, "_cleanup_resources", fail_cleanup)
    closers = [threading.Thread(target=close_store) for _index in range(2)]
    closers[0].start()
    assert cleanup_started.wait(0.5)
    closers[1].start()
    release_cleanup.set()
    for closer in closers:
        closer.join(1.0)
    assert all(not closer.is_alive() for closer in closers)
    assert len(errors) == 2
    assert all(str(error) == "unexpected cleanup failure" for error in errors)
    assert store._closed
    original_cleanup()


def test_http_store_reentrant_close_does_not_publish_or_clean_up() -> None:
    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(lambda _: httpx.Response(404)),
    )
    runtime = store._claim_transport
    download_root = store._download_root
    with runtime.operation():
        with pytest.raises(RuntimeError, match="from an operation"):
            store.close()
        assert runtime._thread.is_alive()
        assert download_root.exists()
        assert not store._closing.is_set()
        assert not store._close_started
        assert not store._closed
    store.close()
    assert not runtime._thread.is_alive()
    assert not download_root.exists()
    assert store._closed


def test_http_store_initialization_error_survives_complete_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    real_temporary_directory = store_client_module.tempfile.TemporaryDirectory
    real_cleanup_resources = HttpEvidenceStore._cleanup_resources
    roots: list[Path] = []
    runtimes: list[Any] = []
    sync_closed = threading.Event()

    def tracking_temporary_directory(*args: Any, **kwargs: Any) -> Any:
        directory = real_temporary_directory(*args, dir=tmp_path, **kwargs)
        roots.append(Path(directory.name))
        return directory

    class TrackingRuntime(store_client_module._ClaimTransportRuntime):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            runtimes.append(self)

    class OwningClient:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def close(self) -> None:
            sync_closed.set()

    class FailingAsyncClose(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(200)

        async def aclose(self) -> None:
            raise RuntimeError("async cleanup failed")

    def cleanup_then_fail(store: HttpEvidenceStore) -> BaseException | None:
        real_cleanup_resources(store)
        raise RuntimeError("unexpected cleanup wrapper failure")

    monkeypatch.setattr(
        store_client_module.tempfile,
        "TemporaryDirectory",
        tracking_temporary_directory,
    )
    monkeypatch.setattr(store_client_module, "_ClaimTransportRuntime", TrackingRuntime)
    monkeypatch.setattr(store_client_module.httpx, "Client", OwningClient)
    monkeypatch.setattr(HttpEvidenceStore, "_cleanup_resources", cleanup_then_fail)
    with pytest.raises(ValueError, match="maximum_artifact_bytes must be positive"):
        HttpEvidenceStore(
            "http://testserver",
            maximum_artifact_bytes=0,
            maximum_batch_size=1,
            _async_transport=FailingAsyncClose(),
        )
    assert sync_closed.is_set()
    assert len(roots) == 1 and not roots[0].exists()
    assert len(runtimes) == 1 and not runtimes[0]._thread.is_alive()


def test_claim_transport_aclose_failure_still_joins_runtime() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")

    class FailingCloseTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(200)

        async def aclose(self) -> None:
            raise RuntimeError("close failed")

    runtime = store_client_module._ClaimTransportRuntime(
        "http://127.0.0.1", 10.0, FailingCloseTransport()
    )
    with pytest.raises(RuntimeError, match="close failed"):
        runtime.close()
    assert not runtime._thread.is_alive()
    with pytest.raises(RuntimeError, match="close failed"):
        runtime.close()


def test_claim_artifact_upload_uses_staging_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    payload = write_artifact_file(
        tmp_path / "artifact", b"payload", "application/octet-stream"
    )
    session = object.__new__(store_client_module._HttpClaimSession)
    upload_deadline = 104.0
    session._operation_condition = threading.Condition()
    session._active_operations = {}
    session._stop = threading.Event()
    session.store = object.__new__(HttpEvidenceStore)
    session.store._closing = threading.Event()
    session.store._registry = None
    observed: list[float] = []
    monkeypatch.setattr(store_client_module.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(
        session,
        "_upload_until",
        lambda _payload, *, deadline: observed.append(deadline),
    )

    session._upload_artifact(payload, deadline=upload_deadline)

    assert observed == [upload_deadline]


def test_claim_artifact_upload_rejects_expired_staging_before_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    payload = write_artifact_file(
        tmp_path / "artifact", b"payload", "application/octet-stream"
    )
    session = object.__new__(store_client_module._HttpClaimSession)
    upload_called = False
    session._operation_condition = threading.Condition()
    session._active_operations = {}
    session._stop = threading.Event()
    session.store = object.__new__(HttpEvidenceStore)
    session.store._closing = threading.Event()
    monkeypatch.setattr(store_client_module.time, "monotonic", lambda: 100.0)

    def record_upload(_payload: object, *, deadline: float) -> None:
        nonlocal upload_called
        upload_called = True

    monkeypatch.setattr(session, "_upload_until", record_upload)

    with pytest.raises(StoreError, match="staging deadline expired"):
        session._upload_artifact(payload, deadline=100.0)
    assert not upload_called


@pytest.mark.parametrize("cause", ["transport", "http", "metadata", "conflict"])
def test_claim_upload_diagnostics_are_correlated_and_redacted(
    tmp_path: Path, cause: str
) -> None:
    module = importlib.import_module("seqevi.store.client")
    payload = write_artifact_file(tmp_path / "payload", b"payload", "text/plain")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(404)
        requests.append(request)
        if cause == "transport":
            try:
                raise OSError("secret://password@host")
            except OSError as inner:
                raise httpx.ReadTimeout("secret://password@host") from inner
        if cause == "http":
            return httpx.Response(503, text="secret://password@host")
        if cause == "conflict":
            return httpx.Response(409, text="secret://password@host")
        return httpx.Response(200, json={"secret": "secret://password@host"})

    with HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = object.__new__(module._HttpClaimSession)
        session.store = store
        session._staging_cancel = threading.Event()
        with pytest.raises(StoreError) as caught:
            session._upload_until(payload, deadline=time.monotonic() + 900)
        message = str(caught.value)
        assert f"digest={payload.digest} bytes=7" in message
        assert "phase=staging" in message and "elapsed=" in message
        assert "remaining=" in message and "cause=" in message
        assert "secret" not in message and "password" not in message
        assert caught.value.__cause__ is not None
        if cause == "conflict":
            assert isinstance(caught.value, EvidenceConflictError)
        assert len(requests) == 1  # R1 does not silently add upload retries.
        assert requests[0].headers["X-Request-ID"] in message
        assert requests[0].extensions["timeout"] == {
            "connect": 60.0,
            "read": 60.0,
            "write": 60.0,
            "pool": 60.0,
        }
        if cause == "transport":
            assert "ReadTimeout<-OSError" in message


@pytest.mark.parametrize("request_id", ["a" * 32, "secret://password@host"])
def test_service_artifact_upload_logs_safe_request_identity(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, request_id: str
) -> None:
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    payload = b"payload"
    digest = sha256_digest(payload)
    with (
        TestClient(app) as service,
        caplog.at_level(logging.INFO, logger="seqevi.service.claims"),
    ):
        response = service.put(
            f"/v1/artifacts/{digest}",
            content=payload,
            headers={
                "X-Artifact-Media-Type": "text/plain",
                "X-Artifact-Byte-Size": "7",
                "X-Request-ID": request_id,
            },
        )
    assert response.status_code == 200
    records = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "seqevi.service.claims"
    ]
    logged = next(
        record for record in records if record["event"] == "seqevi.artifact_request"
    )
    if request_id == "a" * 32:
        assert logged["request_id"] == request_id
    else:
        assert len(logged["request_id"]) == 32
        assert set(logged["request_id"]) <= set("0123456789abcdef")
        assert "secret" not in json.dumps(records)
    assert logged["digest"] == digest and logged["byte_size"] == 7
    assert logged["outcome"] == "created" and logged["duration_ms"] >= 0


def test_claim_artifact_staging_total_timeout_cancels_http_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("seqevi.store.client")
    payload = write_artifact_file(tmp_path / "payload", b"payload", "text/plain")
    cancelled = threading.Event()
    timeout_values: list[float] = []

    async def chunks(_path: Path):
        yield b"payload"

    # This test isolates HTTP cancellation from interpreter startup; the
    # reader's subprocess/reap boundary has its own real-reader tests.
    monkeypatch.setattr(module, "_async_file_chunks", chunks)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(404)
        timeout_values.append(request.extensions["timeout"]["read"])
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")

    with HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = object.__new__(module._HttpClaimSession)
        session.store = store
        session._staging_cancel = threading.Event()
        with pytest.raises(StoreError, match="cause=TimeoutError"):
            session._upload_until(payload, deadline=time.monotonic() + 0.5)
        assert cancelled.is_set()
        assert not store._claim_transport._requests
        assert len(timeout_values) == 1 and 0 < timeout_values[0] <= 0.5


def test_blocking_artifact_fsync_does_not_block_service_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module("seqevi.store.artifact")
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    entered = threading.Event()
    release = threading.Event()
    original = module.os.fsync

    def block(descriptor: int) -> None:
        entered.set()
        assert release.wait(3)
        original(descriptor)

    monkeypatch.setattr(module.os, "fsync", block)

    async def exercise() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            upload = asyncio.create_task(
                client.put(
                    f"/v1/artifacts/{sha256_digest(b'payload')}",
                    content=b"payload",
                    headers={
                        "X-Artifact-Media-Type": "text/plain",
                        "X-Artifact-Byte-Size": "7",
                    },
                )
            )
            try:
                async with asyncio.timeout(2):
                    while not entered.is_set():
                        await asyncio.sleep(0.005)
                    response = await client.get("/health")
                assert response.status_code == 200
                assert not upload.done()
            finally:
                release.set()
                assert (await upload).status_code == 200

    asyncio.run(exercise())


@pytest.mark.slow
@pytest.mark.timeout(180)
def test_claim_artifact_staging_512_mib_round_trip(tmp_path: Path) -> None:
    """Local ASGI scale check, not a network/restart durability benchmark."""
    module = importlib.import_module("seqevi.store.client")
    with tempfile.TemporaryDirectory(
        dir=tmp_path, prefix="artifact-scale-"
    ) as directory:
        root = Path(directory)
        source = root / "source"
        chunk = bytes(range(256)) * 4096
        with source.open("wb") as handle:
            for _ in range(512):
                handle.write(chunk)
        payload = ArtifactFile.from_path(source, "application/octet-stream")
        assert payload.byte_size == 512 * 1024 * 1024
        settings = _settings(root)
        app = create_service_app(settings, persistence=_memory_persistence())
        with (
            TestClient(app) as service,
            HttpEvidenceStore(
                "http://testserver",
                _client=service,
                _async_transport=httpx.ASGITransport(app=app),
            ) as store,
        ):
            session = object.__new__(module._HttpClaimSession)
            session.store = store
            session._staging_cancel = threading.Event()
            started = time.monotonic()
            session._upload_until(payload, deadline=started + 900)
            elapsed = time.monotonic() - started
        target = (
            settings.artifacts_dir
            / "sha256"
            / payload.digest[:2]
            / payload.digest[2:4]
            / payload.digest
        )
        with target.open("rb") as handle:
            actual_digest = hashlib.file_digest(handle, "sha256").hexdigest()
        assert target.stat().st_size == payload.byte_size
        assert actual_digest == payload.digest
        print(
            f"artifact_staging bytes={payload.byte_size} elapsed={elapsed:.3f}s digest={actual_digest}"
        )


def test_slow_renew_body_promptly_publishes_session_loss() -> None:
    now = datetime.now(UTC)
    renew_body = _BlockingAsyncBody()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "slow-renew",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 5.2,
                    "heartbeat_after_seconds": 0.01,
                    "renew_deadline_seconds": 0.1,
                },
            )
        if request.url.path.endswith("/renew"):
            return httpx.Response(200, stream=renew_body)
        return httpx.Response(200, json={"closed": True})

    with HttpEvidenceStore(
        "http://testserver",
        timeout_seconds=1.0,
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert session.cancellation_signal.wait(0.5)
        assert renew_body.cancelled.wait(0.1)
        with pytest.raises(EvidenceClaimLostError):
            session.raise_if_lost()
        session.close()
        assert not session._thread.is_alive()


def test_session_close_after_store_close_stops_heartbeat_without_admission() -> None:
    now = datetime.now(UTC)
    remote_close_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal remote_close_calls
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "close-after-store",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 1000,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/close"):
            remote_close_calls += 1
        return httpx.Response(200, json={"closed": True})

    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    )
    session = store.claim_session()
    store.close()
    started = time.monotonic()
    session.close()
    assert time.monotonic() - started < 0.5
    assert not session._thread.is_alive()
    assert remote_close_calls == 0


def test_session_close_sends_remote_close_once_and_is_idempotent() -> None:
    now = datetime.now(UTC)
    remote_close_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal remote_close_calls
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "close-once",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 1000,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/close"):
            remote_close_calls += 1
        return httpx.Response(200, json={"closed": True})

    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    )
    session = store.claim_session()
    session.close()
    session.close()
    assert remote_close_calls == 1
    assert not session._thread.is_alive()
    store.close()


def test_session_close_replays_unexpected_admission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "close-error",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 1000,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"closed": True})

    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    )
    session = store.claim_session()

    @contextmanager
    def fail_admission() -> Iterator[None]:
        raise RuntimeError("unexpected admission failure")
        yield

    monkeypatch.setattr(store._claim_transport, "operation", fail_admission)
    for _attempt in range(2):
        with pytest.raises(RuntimeError, match="unexpected admission failure"):
            session.close()
    assert not session._thread.is_alive()
    monkeypatch.undo()
    store.close()


def test_finalize_commit_then_slow_body_reconciles_inside_original_budget(
    tmp_path: Path,
) -> None:
    commit = _hit_commit(tmp_path / "source", "MSLOWFINALIZEBODY")
    assert commit.normalized_artifact is not None
    assert commit.raw_artifact is not None
    normalized_artifact = commit.normalized_artifact
    raw_artifact = commit.raw_artifact
    query = EvidenceQuery(commit.identity, commit.key)
    now = datetime.now(UTC)
    slow_body = _BlockingAsyncBody()
    lookup_remaining: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "slow-finalize",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": EvidenceQueryModel.from_domain(
                                    query
                                ).key.model_dump(mode="json"),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        if request.url.path.endswith("/finalize"):
            return httpx.Response(200, stream=slow_body)
        if request.url.path.endswith("/lookup"):
            lookup_remaining.append(request.extensions["timeout"]["read"])
            record = EvidenceRecord(
                key=commit.key,
                status=commit.status,
                payload_digest=commit.payload_digest,
                normalized_artifact_digest=normalized_artifact.digest,
                raw_artifact_digest=raw_artifact.digest,
                created_at=now,
            )
            return httpx.Response(
                200,
                json={
                    "records": [
                        EvidenceRecordModel.from_domain(record).model_dump(mode="json")
                    ]
                },
            )
        return httpx.Response(
            200, json={"session_id": "slow-finalize", "generation": 1}
        )

    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    )
    session = store.claim_session()
    session.acquire_many((query,))
    store._uploaded_artifact_digests.update(
        (normalized_artifact.digest, raw_artifact.digest)
    )
    session._renew_deadline = time.monotonic() + 1.3
    outcomes: list[tuple[CommitOutcome, ...]] = []
    errors: list[BaseException] = []

    def finalize_once() -> None:
        try:
            outcomes.append(session.finalize_many((commit,)))
        except BaseException as error:
            errors.append(error)

    def close_once() -> None:
        try:
            store.close()
            closed.set()
        except BaseException as error:
            errors.append(error)

    finalize = threading.Thread(target=finalize_once)
    finalize.start()
    assert slow_body.started.wait(0.5)
    closed = threading.Event()
    close = threading.Thread(target=close_once)
    close.start()
    time.sleep(0.05)
    assert not closed.is_set()
    finalize.join(2.0)
    close.join(2.0)
    session._stop.set()
    session._thread.join(0.5)
    assert outcomes == [(CommitOutcome.EXISTING,)]
    assert not finalize.is_alive()
    assert not close.is_alive()
    assert not errors
    assert closed.is_set()
    assert slow_body.cancelled.is_set()
    assert lookup_remaining and all(
        0 < remaining <= 1.0 for remaining in lookup_remaining
    )


def test_http_claim_session_refresh_uses_bounded_capability_timeout() -> None:
    now = datetime.now(UTC)
    capability_timeouts: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            capability_timeouts.append(request.extensions["timeout"]["read"])
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        timeout_seconds=120,
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session():
            pass
    assert len(capability_timeouts) == 2
    assert all(29.9 < timeout <= 30.0 for timeout in capability_timeouts)


@pytest.mark.parametrize(
    "missing",
    [
        "protocol",
        "maximum_batch_size",
        "retention_seconds",
        "maximum_session_receipt_headers",
        "maximum_session_receipt_items",
        "server_time",
    ],
)
def test_http_claim_session_rejects_partial_capability_advertisement(
    missing: str,
) -> None:
    payload = {
        "protocol": "claim-session-v1",
        "maximum_batch_size": 1000,
        "retention_seconds": 60,
        "maximum_session_receipt_headers": 1000,
        "maximum_session_receipt_items": 32000,
        "server_time": datetime.now(UTC).isoformat(),
    }
    del payload[missing]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        return httpx.Response(200, json=payload)

    with pytest.raises(ValidationError):
        HttpEvidenceStore(
            "http://testserver",
            _client=_claim_mock_client(httpx.MockTransport(handler)),
            _async_transport=httpx.MockTransport(handler),
        )


@pytest.mark.parametrize("operation", ["open", "renew", "acquire", "authority"])
def test_claim_request_rejects_response_after_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    clock = [0.0]

    class SlowResponseClient:
        def request(self, *_args, **_kwargs) -> httpx.Response:
            clock[0] = 11.0
            return httpx.Response(200, json={})

    class Store:
        _claim_transport = SlowResponseClient()

    session = object.__new__(store_client_module._HttpClaimSession)
    session.store = Store()
    session._stop = threading.Event()
    session._staging_cancel = threading.Event()
    monkeypatch.setattr(store_client_module.time, "monotonic", lambda: clock[0])

    expected_error = StoreError if operation == "acquire" else EvidenceClaimLostError
    with pytest.raises(expected_error, match="response exceeded"):
        session._request_until(
            "POST",
            f"/v1/internal/claim-sessions/{operation}",
            deadline=10.0,
            deadline_loses_authority=operation != "acquire",
        )


def test_http_open_replays_malformed_success_with_same_request_id() -> None:
    now = datetime.now(UTC)
    open_ids: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            open_ids.append(json.loads(request.content)["open_request_id"])
            if len(open_ids) == 1:
                return httpx.Response(200, json={})
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session():
            pass
    assert len(open_ids) == 2
    assert open_ids[0] == open_ids[1]


def test_http_empty_finalize_is_a_noop() -> None:
    now = datetime.now(UTC)
    finalize_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal finalize_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/finalize"):
            finalize_calls += 1
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            assert session.finalize_many(()) == ()
    assert finalize_calls == 0


def test_http_finalize_rejects_logical_duplicate_before_chunk_or_upload(
    tmp_path: Path,
) -> None:
    commit = _hit_commit(tmp_path / "source", "MDUPLICATEFINALIZE")
    now = datetime.now(UTC)
    artifact_calls = 0
    finalize_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal artifact_calls, finalize_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(maximum_batch_size=1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.startswith("/v1/artifacts/"):
            artifact_calls += 1
        if request.url.path.endswith("/finalize"):
            finalize_calls += 1
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            with pytest.raises(ValueError, match="duplicate evidence key"):
                session.finalize_many((commit, commit))

    assert artifact_calls == 0
    assert finalize_calls == 0


def test_claim_session_capability_clock_backpressure_is_retryable(
    tmp_path: Path,
) -> None:
    class BackpressuredCapabilityPersistence(MemoryPersistence):
        @property
        def supports_claim_sessions(self) -> bool:
            return True

        def database_time(self) -> datetime:
            raise StoreBackpressureError("clock unavailable")

    persistence = cast(ServicePersistence, BackpressuredCapabilityPersistence())
    with TestClient(
        create_service_app(_settings(tmp_path), persistence=persistence)
    ) as client:
        response = client.get("/v1/internal/claim-sessions/capabilities")

    assert response.status_code == 503
    assert response.json()["detail"] == "clock unavailable"
    assert response.headers["Retry-After"] == "1"


def test_http_receipt_capacity_restarts_acquire_with_new_request_id() -> None:
    identity, key = _key("MCAPACITYCLIENT")
    query = EvidenceQuery(identity, key)
    acquire_ids: list[str] = []
    now = datetime.now(UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            payload = json.loads(request.content)
            acquire_ids.append(payload["acquire_request_id"])
            if len(acquire_ids) == 1:
                return httpx.Response(
                    503,
                    json={
                        "detail": {
                            "code": "claim_receipt_capacity",
                            "detail": "wait",
                        }
                    },
                    headers={"Retry-After": "0"},
                )
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": EvidenceQueryModel.from_domain(
                                    query
                                ).key.model_dump(mode="json"),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert (
            session.acquire_many((query,))[0].disposition is ClaimDisposition.ACQUIRED
        )
        session.close()
    assert len(acquire_ids) == 2
    assert acquire_ids[0] != acquire_ids[1]


def test_receipt_capacity_retry_snapshots_renew_deadline_under_lock() -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    session = object.__new__(store_client_module._HttpClaimSession)
    session._lock = threading.Lock()
    session._renew_deadline = 10.0
    observed: list[float] = []

    session._lock.acquire()
    thread = threading.Thread(
        target=lambda: observed.append(session._renew_deadline_snapshot())
    )
    thread.start()
    session._renew_deadline = 20.0
    assert thread.is_alive()
    session._lock.release()
    thread.join(0.5)

    assert observed == [20.0]


@pytest.mark.parametrize(
    "malformation", ["missing", "wrong-count", "wrong-key", "wrong-disposition"]
)
def test_http_acquire_replays_malformed_success_with_same_request_id(
    malformation: str,
) -> None:
    identity, key = _key("MAMBIGUOUSACQUIRE")
    query = EvidenceQuery(identity, key)
    acquire_ids: list[str] = []
    now = datetime.now(UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            acquire_ids.append(json.loads(request.content)["acquire_request_id"])
            if len(acquire_ids) == 1:
                wrong_identity, wrong_key = _key("MOTHERACQUIREKEY")
                malformed = {
                    "missing": {},
                    "wrong-count": {"results": []},
                    "wrong-key": {
                        "results": [
                            {
                                "disposition": "acquired",
                                "claim": {
                                    "key": EvidenceQueryModel.from_domain(
                                        EvidenceQuery(wrong_identity, wrong_key)
                                    ).key.model_dump(mode="json"),
                                    "generation": 1,
                                },
                            }
                        ]
                    },
                    "wrong-disposition": {
                        "results": [
                            {
                                "disposition": "acquired",
                                "busy": {
                                    "key": EvidenceQueryModel.from_domain(
                                        query
                                    ).key.model_dump(mode="json"),
                                    "expires_at": (
                                        now + timedelta(seconds=30)
                                    ).isoformat(),
                                    "retry_after_seconds": 1,
                                },
                            }
                        ]
                    },
                }[malformation]
                return httpx.Response(
                    200,
                    json=malformed,
                )
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": EvidenceQueryModel.from_domain(
                                    query
                                ).key.model_dump(mode="json"),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            assert (
                session.acquire_many((query,))[0].disposition
                is ClaimDisposition.ACQUIRED
            )
    assert len(acquire_ids) == 2
    assert acquire_ids[0] == acquire_ids[1]


def test_http_acquire_rejects_duplicate_keys_before_logical_batch_chunking() -> None:
    identity, key = _key("MDUPLICATELOGICALACQUIRE")
    query = EvidenceQuery(identity, key)
    now = datetime.now(UTC)
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(maximum_batch_size=1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            acquire_calls += 1
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            with pytest.raises(ValueError, match="duplicate evidence key"):
                session.acquire_many((query, query))
    assert acquire_calls == 0


@pytest.mark.parametrize(
    "first_outcome", ["transport", "503", "412", "transport-then-412"]
)
@pytest.mark.parametrize("first_lookup", ["503", "malformed"])
@pytest.mark.parametrize("first_authority", ["valid", "malformed"])
def test_http_finalize_reconciles_apparent_success_after_unknown_outcome(
    tmp_path: Path,
    first_outcome: str,
    first_lookup: str,
    first_authority: str,
) -> None:
    commit = _hit_commit(tmp_path / "source", "MUNKNOWNTHENSUCCESS")
    query = EvidenceQuery(commit.identity, commit.key)
    now = datetime.now(UTC)
    finalize_calls = 0
    lookup_calls = 0
    authority_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal finalize_calls, lookup_calls, authority_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": EvidenceQueryModel.from_domain(
                                    query
                                ).key.model_dump(mode="json"),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        if request.url.path.endswith("/finalize"):
            finalize_calls += 1
            if finalize_calls == 1:
                if first_outcome in {"transport", "transport-then-412"}:
                    raise httpx.ReadError("response lost")
                if first_outcome == "412":
                    return httpx.Response(412, text="authority uncertain")
                return httpx.Response(503, text="temporarily unavailable")
            if first_outcome == "transport-then-412":
                return httpx.Response(412, text="authority uncertain")
            return httpx.Response(200, json={"outcomes": ["created"]})
        if request.url.path.endswith("/authority"):
            authority_calls += 1
            if first_authority == "malformed" and authority_calls == 1:
                return httpx.Response(200, json={})
            return httpx.Response(200, json={"live": True})
        if request.url.path.endswith("/lookup"):
            lookup_calls += 1
            if lookup_calls == 1:
                if first_lookup == "malformed":
                    return httpx.Response(200, json={})
                return httpx.Response(503, text="transient readback")
            records = []
            if lookup_calls == 3:
                records = [
                    EvidenceRecordModel.from_domain(
                        EvidenceRecord(
                            key=commit.key,
                            status=commit.status,
                            payload_digest=commit.payload_digest,
                            normalized_artifact_digest=commit.normalized_artifact.digest
                            if commit.normalized_artifact
                            else None,
                            raw_artifact_digest=commit.raw_artifact.digest
                            if commit.raw_artifact
                            else None,
                            created_at=now,
                        )
                    ).model_dump(mode="json")
                ]
            return httpx.Response(200, json={"records": records})
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            session.acquire_many((query,))
            store._uploaded_artifact_digests.update(
                payload.digest
                for payload in (commit.normalized_artifact, commit.raw_artifact)
                if payload is not None
            )
            assert session.finalize_many((commit,)) == (CommitOutcome.EXISTING,)
    assert finalize_calls == 2
    assert lookup_calls == 3
    assert authority_calls == (2 if first_authority == "malformed" else 1)


@pytest.mark.parametrize("stalled_stage", ["lookup", "authority"])
def test_http_finalize_readback_timeout_is_clamped_to_authority_deadline(
    tmp_path: Path,
    stalled_stage: str,
) -> None:
    commit = _hit_commit(tmp_path / "source", "MSTALLEDREADBACK")
    query = EvidenceQuery(commit.identity, commit.key)
    now = datetime.now(UTC)
    lookup_timeouts: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": EvidenceQueryModel.from_domain(
                                    query
                                ).key.model_dump(mode="json"),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        if request.url.path.endswith("/finalize"):
            return httpx.Response(503, text="unknown")
        if request.url.path.endswith("/lookup"):
            lookup_timeouts.append(request.extensions["timeout"]["read"])
            if stalled_stage == "lookup":
                raise httpx.ReadTimeout("stalled")
            return httpx.Response(200, json={"records": []})
        if request.url.path.endswith("/authority"):
            lookup_timeouts.append(request.extensions["timeout"]["read"])
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"live": True})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            session.acquire_many((query,))
            store._uploaded_artifact_digests.update(
                payload.digest
                for payload in (commit.normalized_artifact, commit.raw_artifact)
                if payload is not None
            )
            session._renew_deadline = time.monotonic() + 1.4
            with pytest.raises(StoreError):
                session.finalize_many((commit,))
    assert lookup_timeouts
    assert all(0 < timeout <= 1.4 for timeout in lookup_timeouts)


@pytest.mark.parametrize(
    "staging_result", ["success", "transport_failure", "authority_lost"]
)
def test_finalize_stages_before_fresh_authority_and_shared_metadata_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, staging_result: str
) -> None:
    store_client_module = importlib.import_module("seqevi.store.client")
    commits = (
        _hit_commit(tmp_path / "first", "MSHAREDBUDGETONE"),
        _hit_commit(tmp_path / "second", "MSHAREDBUDGETTWO"),
    )
    queries = tuple(EvidenceQuery(commit.identity, commit.key) for commit in commits)
    now = datetime.now(UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "shared-budget",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            payload = json.loads(await request.aread())
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {"key": model["key"], "generation": 1},
                        }
                        for model in payload["queries"]
                    ]
                },
            )
        if request.url.path.endswith("/finalize"):
            return httpx.Response(200, json={"outcomes": ["created"]})
        return httpx.Response(200, json={"closed": True})

    store = HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1,
        _client=_claim_mock_client(httpx.MockTransport(lambda _: httpx.Response(404))),
        _async_transport=httpx.MockTransport(handler),
    )
    session = store.claim_session()
    session.acquire_many(queries)
    clock = [100.0]
    deadlines: list[tuple[str, float]] = []
    monkeypatch.setattr(store_client_module.time, "monotonic", lambda: clock[0])
    session._renew_deadline = 125.0
    original_request = store._claim_transport.request

    def capture_request(
        method: str, path: str, *, deadline: float, **kwargs: Any
    ) -> httpx.Response:
        if path.endswith("/finalize"):
            assert kwargs["json"]["owner_token"] == "renewed-owner"
            deadlines.append(("finalize", deadline))
            clock[0] += 3.0
        return original_request(method, path, deadline=deadline, **kwargs)

    def capture_upload(_payload: object, *, deadline: float) -> None:
        deadlines.append(("upload", deadline))
        assert deadline == clock[0] + 900.0
        clock[0] += 40.0
        # Model the heartbeat renewing while a transfer exceeds the old cutoff.
        session._renew_deadline = clock[0] + 90.0
        session._authority = replace(session._authority, owner_token="renewed-owner")
        if staging_result == "transport_failure":
            raise StoreError("upload unavailable")
        if staging_result == "authority_lost":
            session._lost = StoreError("lost during upload")

    monkeypatch.setattr(store._claim_transport, "request", capture_request)
    monkeypatch.setattr(session, "_upload_until", capture_upload)
    try:
        if staging_result != "success":
            with pytest.raises(StoreError):
                session.finalize_many(commits)
            assert [kind for kind, _ in deadlines] == ["upload"]
            return
        assert session.finalize_many(commits) == (
            CommitOutcome.CREATED,
            CommitOutcome.CREATED,
        )
        uploads = [deadline for kind, deadline in deadlines if kind == "upload"]
        assert len(uploads) >= 2
        metadata_started = 100.0 + len(uploads) * 40.0
        cutoff = metadata_started + 29.0
        assert [deadline for kind, deadline in deadlines if kind == "finalize"] == [
            cutoff,
            cutoff,
        ]
        assert [kind for kind, _ in deadlines] == ["upload"] * len(uploads) + [
            "finalize",
            "finalize",
        ]
    finally:
        session._stop.set()
        session._thread.join(0.5)
        store.close()


def test_http_finalize_does_not_reconcile_deterministic_422(tmp_path: Path) -> None:
    commit = _hit_commit(tmp_path / "source", "MDETERMINISTICFINALIZE")
    now = datetime.now(UTC)
    lookup_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal lookup_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        if request.url.path.endswith("/acquire"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "disposition": "acquired",
                            "claim": {
                                "key": CommitModel.from_domain(commit).key.model_dump(
                                    mode="json"
                                ),
                                "generation": 1,
                            },
                        }
                    ]
                },
            )
        if request.url.path.endswith("/finalize"):
            return httpx.Response(422, text="invalid commit")
        if request.url.path.endswith("/lookup"):
            lookup_calls += 1
            return httpx.Response(200, json={"records": []})
        if request.method == "PUT":
            return httpx.Response(
                201,
                json={
                    "status": "created",
                    "digest": request.url.path.rsplit("/", 1)[-1],
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        with store.claim_session() as session:
            session.acquire_many((EvidenceQuery(commit.identity, commit.key),))
            store._uploaded_artifact_digests.update(
                payload.digest
                for payload in (commit.normalized_artifact, commit.raw_artifact)
                if payload is not None
            )
            with pytest.raises(StoreError, match="HTTP 422"):
                session.finalize_many((commit,))
    assert lookup_calls == 0


def test_http_heartbeat_renews_before_request_deadline() -> None:
    renewed = threading.Event()
    now = datetime.now(UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 0.5,
                },
            )
        if request.url.path.endswith("/renew"):
            renewed.set()
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert renewed.wait(1.0)
        session.close()


@pytest.mark.parametrize(
    ("failure", "expected_calls", "loses_authority"),
    [
        ("transient-503", 3, False),
        ("fenced-412", 1, True),
        ("exhausted-503", 2, True),
    ],
)
def test_http_heartbeat_renew_failure_policy(
    failure: str,
    expected_calls: int,
    loses_authority: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    renew_calls = 0
    renewed = threading.Event()
    monkeypatch.setattr("seqevi.store.client.random.uniform", lambda _a, _b: 0.01)

    def authority_payload() -> dict[str, object]:
        return {
            "session_id": "renew-fault",
            "owner_token": "owner",
            "generation": 1,
            "expires_at": (now + timedelta(seconds=120)).isoformat(),
            "remaining_lease_seconds": 120,
            "heartbeat_after_seconds": 0.01,
            "renew_deadline_seconds": 0.08,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal renew_calls
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json=authority_payload())
        if request.url.path.endswith("/renew"):
            renew_calls += 1
            if failure == "fenced-412":
                renewed.set()
                return httpx.Response(412, text="fenced")
            if failure == "exhausted-503" or renew_calls < 3:
                if renew_calls >= expected_calls:
                    renewed.set()
                return httpx.Response(503, headers={"Retry-After": "0.01"})
            renewed.set()
            return httpx.Response(200, json=authority_payload())
        return httpx.Response(200, json={"closed": True})

    with HttpEvidenceStore(
        "http://testserver",
        maximum_artifact_bytes=1024,
        maximum_batch_size=1000,
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert renewed.wait(1.0)
        if loses_authority:
            assert session.cancellation_signal.wait(0.5)
            with pytest.raises(EvidenceClaimLostError):
                session.raise_if_lost()
        else:
            assert not session.cancellation_signal.is_set()
            session.raise_if_lost()
        session.close()

    assert renew_calls >= expected_calls


@pytest.mark.parametrize("renewal_failure", ["malformed", "authority-switch"])
def test_http_heartbeat_invalid_success_is_terminal_session_loss(
    renewal_failure: str,
) -> None:
    malformed = threading.Event()
    now = datetime.now(UTC)
    renewal_payloads: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open"):
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 0.75,
                },
            )
        if request.url.path.endswith("/renew"):
            renewal_payloads.append(json.loads(request.content))
            malformed.set()
            if renewal_failure == "malformed":
                return httpx.Response(200, json={})
            return httpx.Response(
                200,
                json={
                    "session_id": "other-session",
                    "owner_token": "other-owner",
                    "generation": 2,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 30,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert malformed.wait(1.5)
        assert session.cancellation_signal.wait(0.5)
        with pytest.raises(EvidenceClaimLostError):
            session.raise_if_lost()
        session.close()

    assert len(renewal_payloads) == 1


def test_http_heartbeat_applies_deterministic_session_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    renewed = threading.Event()
    now = datetime.now(UTC)
    started = time.monotonic()
    monkeypatch.setattr("seqevi.store.client.random.uniform", lambda _a, _b: 0.8)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
                    "retention_seconds": 60,
                    "maximum_session_receipt_headers": 1000,
                    "maximum_session_receipt_items": 32000,
                    "server_time": now.isoformat(),
                },
            )
        if request.url.path.endswith("/open") or request.url.path.endswith("/renew"):
            if request.url.path.endswith("/renew"):
                renewed.set()
            return httpx.Response(
                200,
                json={
                    "session_id": "session",
                    "owner_token": "owner",
                    "generation": 1,
                    "expires_at": (now + timedelta(seconds=120)).isoformat(),
                    "remaining_lease_seconds": 120,
                    "heartbeat_after_seconds": 0.2,
                    "renew_deadline_seconds": 90,
                },
            )
        return httpx.Response(200, json={"session_id": "session", "generation": 1})

    with HttpEvidenceStore(
        "http://testserver",
        _client=_claim_mock_client(httpx.MockTransport(handler)),
        _async_transport=httpx.MockTransport(handler),
    ) as store:
        session = store.claim_session()
        assert renewed.wait(1.0)
        session.close()
    assert time.monotonic() - started < 0.5


def test_shared_store_configuration_requires_postgres_and_postgres_engine(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="must use PostgreSQL"):
        ServiceSettings(
            database_url="sqlite:///store.sqlite3",
            artifacts_dir=tmp_path,
        )
    normalized = ServiceSettings(
        database_url="postgresql://seqevi@postgres/seqevi",
        artifacts_dir=tmp_path,
    )
    assert normalized.database_url == "postgresql+psycopg://seqevi@postgres/seqevi"
    assert normalized.database_pool_size == 16
    assert normalized.database_max_overflow == 8
    assert normalized.database_pool_timeout_seconds == 5.0
    assert normalized.database_lock_timeout_seconds == 5.0
    assert normalized.database_statement_timeout_seconds == 15.0
    assert normalized.database_transaction_timeout_seconds == 25.0
    with pytest.raises(ValidationError, match="must total at most 30 seconds"):
        ServiceSettings(
            database_url="postgresql://seqevi@postgres/seqevi",
            artifacts_dir=tmp_path,
            database_pool_timeout_seconds=30,
            database_transaction_timeout_seconds=30,
        )
    assert isinstance(artifacts.c.byte_size.type, BigInteger)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        with pytest.raises(ValueError, match="requires PostgreSQL"):
            PostgresEvidencePersistence(engine)
    finally:
        engine.dispose()


def test_postgres_version_requirement_rejects_pre_17() -> None:
    with pytest.raises(RuntimeError, match="requires PostgreSQL 17 or newer"):
        persistence_module._require_supported_postgres_version(  # pyright: ignore[reportPrivateUsage]
            160012
        )

    persistence_module._require_supported_postgres_version(170000)  # pyright: ignore[reportPrivateUsage]


def test_postgres_advisory_locks_use_actual_signed_bigint_order() -> None:
    keys = tuple(_key("MACTUAL" + "A" * index)[1] for index in range(1, 33))
    expected = sorted(
        {
            persistence_module._advisory_lock_id(  # pyright: ignore[reportPrivateUsage]
                key
            )
            for key in keys
        }
    )
    observed: list[list[int]] = []

    class RecordingConnection:
        def execute(self, _statement: object, parameters: dict[str, list[int]]) -> None:
            observed.append(parameters["lock_ids"])

    persistence_module._lock_evidence_keys(  # pyright: ignore[reportPrivateUsage]
        cast(Connection, RecordingConnection()), reversed(keys)
    )

    assert any(lock_id < 0 for lock_id in expected)
    assert observed == [expected]


def test_postgres_advisory_locks_deduplicate_forced_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = tuple(_key(sequence)[1] for sequence in ("MLOCKA", "MLOCKB", "MLOCKC"))
    lock_ids = {keys[0]: 4, keys[1]: -9, keys[2]: 4}
    observed: list[list[int]] = []

    class RecordingConnection:
        def execute(self, _statement: object, parameters: dict[str, list[int]]) -> None:
            observed.append(parameters["lock_ids"])

    monkeypatch.setattr(
        persistence_module, "_advisory_lock_id", lambda key: lock_ids[key]
    )
    persistence_module._lock_evidence_keys(  # pyright: ignore[reportPrivateUsage]
        cast(Connection, RecordingConnection()), reversed(keys)
    )

    assert observed == [[-9, 4]]


def test_service_returns_422_for_invalid_domain_values_and_missing_raw_artifact(
    tmp_path: Path,
) -> None:
    commit = _hit_commit(tmp_path / "sources")
    model = CommitModel.from_domain(commit).model_dump(mode="json")
    invalid_identity = {
        **model,
        "identity": {
            **model["identity"],
            "md5": "0" * 32,
        },
    }
    missing_raw = {**model, "raw_artifact": None}
    coerced_length = {
        **model,
        "identity": {
            **model["identity"],
            "length": str(model["identity"]["length"]),
        },
    }
    assert model["raw_artifact"] is not None
    coerced_byte_size = {
        **model,
        "raw_artifact": {
            **model["raw_artifact"],
            "byte_size": str(model["raw_artifact"]["byte_size"]),
        },
    }
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())

    with TestClient(app) as client:
        invalid_response = client.post(
            "/v1/evidence/commit",
            json={"commits": [invalid_identity]},
        )
        missing_raw_response = client.post(
            "/v1/evidence/commit",
            json={"commits": [missing_raw]},
        )
        coerced_length_response = client.post(
            "/v1/evidence/commit",
            json={"commits": [coerced_length]},
        )
        coerced_byte_size_response = client.post(
            "/v1/evidence/commit",
            json={"commits": [coerced_byte_size]},
        )

    assert invalid_response.status_code == 422
    assert missing_raw_response.status_code == 422
    assert coerced_length_response.status_code == 422
    assert coerced_byte_size_response.status_code == 422


def test_annotation_results_are_equivalent_for_local_and_http_store(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">hit\nMPEPTIDE\n>none\nMNOHITX\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    with LocalStore.open(tmp_path / "local-store") as local_store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "local-output",
            adapter=adapter,
            store=local_store,
        )

    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    with TestClient(app) as test_client:
        remote_store = HttpEvidenceStore(
            "http://testserver",
            _client=cast(httpx.Client, test_client),
            _async_transport=httpx.ASGITransport(app=app),
        )
        first = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "remote-output",
            adapter=adapter,
            store=remote_store,
        )
        second = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "remote-cached-output",
            adapter=NeverRunAdapter(adapter),
            store=remote_store,
        )

    assert first.computed == 2
    assert second.cache_hits == 2
    for relation in ("main.evidence", "main.no_hits"):
        assert_frame_equal(
            read_result_table(tmp_path / "local-output", relation),
            read_result_table(tmp_path / "remote-output", relation),
        )
    assert_frame_equal(
        read_result_table(tmp_path / "local-output", "main.sequence_map"),
        read_result_table(tmp_path / "remote-output", "main.sequence_map"),
    )


def test_v020_request_contract_uses_only_released_legacy_routes(tmp_path: Path) -> None:
    """Fixed request fixture derived from the released v0.2.0 HTTP client."""

    persistence = _memory_persistence()
    app = create_service_app(_settings(tmp_path), persistence=persistence)
    _identity, key = _key("MVTWOCOMPATIBILITY")
    key_json = EvidenceQueryModel.from_domain(
        EvidenceQuery(_identity, key)
    ).key.model_dump(mode="json")
    with TestClient(app) as service:
        health = service.get("/health")
        lookup = service.post("/v1/evidence/lookup", json={"queries": []})
        fetch = service.post("/v1/evidence/fetch", json={"key": key_json})
        fetch_many = service.post("/v1/evidence/fetch-many", json={"keys": []})
        commit = service.post("/v1/evidence/commit", json={"commits": []})
        artifact = service.get("/v1/artifacts/" + "a" * 64)
        removed = [
            service.request(method, path, json={})
            for method, path in (
                ("GET", "/v1/evidence/claims/capabilities"),
                ("POST", "/v1/evidence/claims/acquire"),
                ("POST", "/v1/evidence/claims/renew"),
                ("POST", "/v1/evidence/claims/release"),
                ("POST", "/v1/evidence/claims/finalize"),
            )
        ]
    assert health.json() == {
        "status": "ok",
        "api_version": "v1",
        "maximum_batch_size": 1000,
        "maximum_artifact_bytes": 512 * 1024 * 1024,
    }
    assert lookup.status_code == 200 and lookup.json() == {"records": []}
    assert fetch.status_code == 200 and fetch.json() == {"record": None}
    assert fetch_many.status_code == 200 and fetch_many.json() == {"records": []}
    assert commit.status_code == 200 and commit.json() == {"outcomes": []}
    assert artifact.status_code == 404
    assert all(response.status_code == 404 for response in removed)


@pytest.mark.parametrize(
    "route",
    ("open", "renew", "close", "acquire", "finalize"),
)
def test_claim_session_malformed_bodies_remain_422(tmp_path: Path, route: str) -> None:
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    with TestClient(app) as service:
        response = service.post(f"/v1/internal/claim-sessions/{route}", json={})
    assert response.status_code == 422


def test_claim_session_open_terminal_receipt_is_412(tmp_path: Path) -> None:
    class TerminalOpenPersistence(MemoryPersistence):
        @property
        def supports_claim_sessions(self) -> bool:
            return True

        def open_claim_session(self, **_kwargs):
            raise EvidenceClaimLostError("ClaimSession open receipt is terminal")

    now = datetime.now(UTC)
    app = create_service_app(
        _settings(tmp_path),
        persistence=cast(ServicePersistence, TerminalOpenPersistence()),
    )
    with TestClient(app) as service:
        response = service.post(
            "/v1/internal/claim-sessions/open",
            json={
                "open_request_id": "terminal-open",
                "server_time": now.isoformat(),
                "open_not_after": (now + timedelta(seconds=30)).isoformat(),
            },
        )
    assert response.status_code == 412


def test_claim_session_finalize_invalid_artifact_reference_is_422(
    tmp_path: Path,
) -> None:
    commit = _hit_commit(tmp_path / "sources", "MINVALIDARTIFACT")
    model = CommitModel.from_domain(commit).model_dump(mode="json")
    assert model["normalized_artifact"] is not None
    model["normalized_artifact"]["digest"] = "f" * 64
    app = create_service_app(_settings(tmp_path), persistence=_memory_persistence())
    with TestClient(app) as service:
        response = service.post(
            "/v1/internal/claim-sessions/finalize",
            json={
                "session_id": "session",
                "owner_token": "owner",
                "generation": 1,
                "finalize_request_id": "invalid-artifact",
                "commits": [{"commit": model, "claim_generation": 1}],
            },
        )
    assert response.status_code == 422


def test_claim_session_sweeper_retries_after_connection_failure(tmp_path: Path) -> None:
    class RecoveringPersistence(MemoryPersistence):
        def __init__(self) -> None:
            super().__init__()
            self.sweeps = 0
            self.recovered = threading.Event()

        def sweep_claim_sessions(self) -> int:
            self.sweeps += 1
            if self.sweeps == 1:
                raise StoreError("connection lost")
            self.recovered.set()
            return 0

    persistence = RecoveringPersistence()
    app = create_service_app(
        _settings(tmp_path), persistence=cast(ServicePersistence, persistence)
    )
    with TestClient(app):
        assert persistence.recovered.wait(2.0)
    assert persistence.sweeps >= 2


def test_claim_session_authority_enforces_protocol_batch_ceiling(
    tmp_path: Path,
) -> None:
    class AuthorityPersistence(MemoryPersistence):
        def claim_session_authority_is_live(self, _authority, claims):
            assert len(tuple(claims)) == 1000
            return True

    _, key = _key("MAUTHORITYBOUNDARY")
    claim = SessionEvidenceClaimModel.from_domain(SessionEvidenceClaim(key, 1))
    payload = {
        "session_id": "session",
        "owner_token": "owner",
        "generation": 1,
        "claims": [claim.model_dump(mode="json")] * 1000,
    }
    persistence = AuthorityPersistence()
    app = create_service_app(
        _settings(tmp_path), persistence=cast(ServicePersistence, persistence)
    )
    with TestClient(app) as service:
        accepted = service.post("/v1/internal/claim-sessions/authority", json=payload)
        rejected = service.post(
            "/v1/internal/claim-sessions/authority",
            json={
                **payload,
                "claims": [claim.model_dump(mode="json")] * 1001,
            },
        )
    assert accepted.status_code == 200
    assert rejected.status_code == 422


def test_claim_session_authority_backpressure_is_503(tmp_path: Path) -> None:
    class BackpressuredAuthorityPersistence(MemoryPersistence):
        def claim_session_authority_is_live(self, _authority, _claims):
            raise StoreBackpressureError("busy")

    app = create_service_app(
        _settings(tmp_path),
        persistence=cast(ServicePersistence, BackpressuredAuthorityPersistence()),
    )
    with TestClient(app) as service:
        response = service.post(
            "/v1/internal/claim-sessions/authority",
            json={
                "session_id": "session",
                "owner_token": "owner",
                "generation": 1,
                "claims": [],
            },
        )
    assert response.status_code == 503


@contextmanager
def _isolated_postgres_url() -> Iterator[str]:
    database_url = os.environ.get("SEQEVI_TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("SEQEVI_TEST_POSTGRES_URL is not configured")

    schema = f"seqevi_test_{os.urandom(8).hex()}"
    admin_engine = create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped_url = (
        make_url(database_url)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    try:
        yield scoped_url
    finally:
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin_engine.dispose()


def test_pressure_fresh_database_guard_accepts_empty_and_rejects_data() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    expected_tables = (
        "sequence",
        "artifact",
        "evidence",
        "evidence_claim_generations",
        "claim_sessions",
        "session_claims",
        "claim_session_open_receipts",
        "claim_session_acquire_receipts",
        "claim_session_acquire_receipt_items",
    )
    assert module["_FRESH_DATABASE_TABLES"] == expected_tables

    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            module["_require_fresh_database"](persistence)
            with persistence.engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO sequence "
                        "(sequence_id, md5, length, sequence) "
                        "VALUES ('sha256:occupied', :md5, 1, 'M')"
                    ),
                    {"md5": "0" * 32},
                )
                connection.execute(
                    text(
                        "INSERT INTO artifact "
                        "(digest, media_type, byte_size, relative_path, storage_kind) "
                        "VALUES (:digest, 'application/octet-stream', 0, "
                        "'occupied/artifact', 'posix')"
                    ),
                    {"digest": "1" * 64},
                )

            with pytest.raises(
                RuntimeError,
                match=("fresh PostgreSQL database: sequence=1, artifact=1"),
            ):
                module["_require_fresh_database"](persistence)
        finally:
            persistence.close()


def _seed_0002_evidence(connection) -> None:
    hit_identity, hit_key = _key("MMIGRATIONHIT")
    no_hit_identity, no_hit_key = _key("MMIGRATIONNOHIT")
    created_at = datetime(2026, 8, 10, 1, 2, 3, tzinfo=UTC)
    for identity in (hit_identity, no_hit_identity):
        connection.execute(
            text(
                "INSERT INTO sequence "
                "(sequence_id, md5, length, sequence, created_at) "
                "VALUES (:sequence_id, :md5, :length, :sequence, :created_at)"
            ),
            {
                "sequence_id": identity.sequence_id,
                "md5": identity.md5,
                "length": identity.length,
                "sequence": identity.sequence,
                "created_at": created_at,
            },
        )
    artifacts_to_seed = (
        ("1" * 64, "application/vnd.apache.parquet", 11, "seed/hit.parquet"),
        ("2" * 64, "text/tab-separated-values", 7, "seed/raw.tsv"),
    )
    for digest, media_type, byte_size, relative_path in artifacts_to_seed:
        connection.execute(
            text(
                "INSERT INTO artifact "
                "(digest, media_type, byte_size, relative_path, created_at) "
                "VALUES (:digest, :media_type, :byte_size, :relative_path, :created_at)"
            ),
            {
                "digest": digest,
                "media_type": media_type,
                "byte_size": byte_size,
                "relative_path": relative_path,
                "created_at": created_at,
            },
        )
    for key, status, normalized in (
        (hit_key, "hit", "1" * 64),
        (no_hit_key, "no_hit", None),
    ):
        connection.execute(
            text(
                "INSERT INTO evidence "
                "(sequence_id, adapter_contract_version, tool_runtime_digest, "
                "resource_id, semantic_parameters_hash, semantic_parameters_json, "
                "status, payload_digest, normalized_artifact_digest, "
                "raw_artifact_digest, created_at) VALUES "
                "(:sequence_id, :adapter_contract_version, :tool_runtime_digest, "
                ":resource_id, :semantic_parameters_hash, :semantic_parameters_json, "
                ":status, :payload_digest, :normalized_artifact_digest, "
                ":raw_artifact_digest, :created_at)"
            ),
            {
                "sequence_id": key.sequence_id,
                "adapter_contract_version": key.adapter_contract_version,
                "tool_runtime_digest": key.tool_runtime_digest,
                "resource_id": key.resource_id,
                "semantic_parameters_hash": key.semantic_parameters_hash,
                "semantic_parameters_json": key.semantic_parameters_json,
                "status": status,
                "payload_digest": "3" * 64 if normalized else "4" * 64,
                "normalized_artifact_digest": normalized,
                "raw_artifact_digest": "2" * 64,
                "created_at": created_at,
            },
        )


def _snapshot_legacy_rows(connection) -> dict[str, tuple[tuple[object, ...], ...]]:
    statements = {
        "sequence": "SELECT * FROM sequence ORDER BY 1",
        "artifact": (
            "SELECT digest, media_type, byte_size, relative_path, created_at "
            "FROM artifact ORDER BY 1"
        ),
        "evidence": "SELECT * FROM evidence ORDER BY 1",
    }
    return {
        table_name: tuple(tuple(row) for row in connection.execute(text(statement)))
        for table_name, statement in statements.items()
    }


def test_sqlite_acknowledged_0002_preparation_and_rollback_preserve_rows(
    tmp_path: Path,
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    engine = create_engine(f"sqlite+pysqlite:///{store_root / 'store.sqlite3'}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0002_artifact_byte_size_bigint")
        _seed_0002_evidence(connection)
        before = _snapshot_legacy_rows(connection)
        connection.commit()
    identity = store_migration._database_identity(engine, store_root)  # pyright: ignore[reportPrivateUsage]

    store_migration.maintenance_prepare_database(
        engine,
        store_root,
        store_migration.MaintenanceAcknowledgement(
            identity, "0002_artifact_byte_size_bigint"
        ),
    )
    with engine.connect() as connection:
        assert store_migration._revision(connection) == "0003_evidence_claim_leases"  # pyright: ignore[reportPrivateUsage]
        assert "evidence_claim" in inspect(connection).get_table_names()
        assert _snapshot_legacy_rows(connection) == before

    store_migration.maintenance_prepare_database(
        engine,
        store_root,
        store_migration.MaintenanceAcknowledgement(
            identity, "0003_evidence_claim_leases"
        ),
        rollback=True,
    )
    with engine.connect() as connection:
        assert store_migration._revision(connection) == "0002_artifact_byte_size_bigint"  # pyright: ignore[reportPrivateUsage]
        assert "evidence_claim" not in inspect(connection).get_table_names()
        assert _snapshot_legacy_rows(connection) == before
    engine.dispose()


def test_sqlite_preparation_rejects_stale_acknowledgement(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    engine = create_engine(f"sqlite+pysqlite:///{store_root / 'store.sqlite3'}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0003_evidence_claim_leases")
        connection.commit()
    identity = store_migration._database_identity(engine, store_root)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError, match="stale revision"):
        store_migration.maintenance_prepare_database(
            engine,
            store_root,
            store_migration.MaintenanceAcknowledgement(
                identity, "0002_artifact_byte_size_bigint"
            ),
        )
    with engine.connect() as connection:
        assert store_migration._revision(connection) == "0003_evidence_claim_leases"  # pyright: ignore[reportPrivateUsage]
    engine.dispose()


def test_sqlite_preparation_rollback_refuses_unexpired_claim(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    engine = create_engine(f"sqlite+pysqlite:///{store_root / 'store.sqlite3'}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0003_evidence_claim_leases")
        connection.execute(
            text(
                "INSERT INTO evidence_claim (sequence_id, adapter_contract_version, "
                "tool_runtime_digest, resource_id, semantic_parameters_hash, "
                "semantic_parameters_json, owner_token, generation, expires_at) "
                "VALUES (:sequence_id, :adapter, :runtime, :resource, :parameters, "
                ":parameters_json, :owner, 1, :expires_at)"
            ),
            {
                "sequence_id": "sha256:" + "1" * 64,
                "adapter": "eggnog-mapper/v1",
                "runtime": "sha256:" + "2" * 64,
                "resource": "sha256:" + "3" * 64,
                "parameters": "4" * 64,
                "parameters_json": "{}",
                "owner": "live-owner",
                "expires_at": datetime.now(UTC) + timedelta(minutes=5),
            },
        )
        connection.commit()
    identity = store_migration._database_identity(engine, store_root)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError, match="refuses unexpired evidence claims"):
        store_migration.maintenance_prepare_database(
            engine,
            store_root,
            store_migration.MaintenanceAcknowledgement(
                identity, "0003_evidence_claim_leases"
            ),
            rollback=True,
        )

    with engine.connect() as connection:
        assert store_migration._revision(connection) == "0003_evidence_claim_leases"  # pyright: ignore[reportPrivateUsage]
        assert "evidence_claim" in inspect(connection).get_table_names()
        assert (
            connection.execute(text("SELECT count(*) FROM evidence_claim")).scalar_one()
            == 1
        )
    engine.dispose()


def test_sqlite_preparation_bounds_database_lock_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    engine = create_engine(f"sqlite+pysqlite:///{database_path}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0002_artifact_byte_size_bigint")
        connection.commit()
    identity = store_migration._database_identity(engine, store_root)  # pyright: ignore[reportPrivateUsage]
    blocker = create_engine(f"sqlite+pysqlite:///{database_path}").connect()
    blocker.exec_driver_sql("BEGIN EXCLUSIVE")
    monkeypatch.setattr(store_migration, "_MAINTENANCE_TIMEOUT_SECONDS", 0.1)
    started = time.monotonic()
    try:
        with pytest.raises(DBAPIError, match="database is locked"):
            store_migration.maintenance_prepare_database(
                engine,
                store_root,
                store_migration.MaintenanceAcknowledgement(
                    identity, "0002_artifact_byte_size_bigint"
                ),
            )
        assert time.monotonic() - started < 1.0
    finally:
        blocker.rollback()
        blocker.close()
    with engine.connect() as connection:
        assert store_migration._revision(connection) == "0002_artifact_byte_size_bigint"  # pyright: ignore[reportPrivateUsage]
    engine.dispose()


def test_postgres_preparation_invalidates_unknown_advisory_lock_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailedResult:
        def scalar_one(self) -> bool:
            raise RuntimeError("advisory lock result was lost")

    class FakeConnection:
        invalidated = False

        def exec_driver_sql(self, statement: str, _parameters: object) -> FailedResult:
            assert "pg_try_advisory_lock" in statement
            return FailedResult()

        def invalidate(self) -> None:
            self.invalidated = True

    class FakeWatchdog:
        expired = threading.Event()

        def __init__(self, _connection: object, _deadline: float) -> None:
            pass

        def cancel(self) -> None:
            pass

    connection = FakeConnection()

    @contextmanager
    def fake_connect(_engine: object, _deadline: float):
        yield connection

    def verify_cleanup(
        cleanup_connection: FakeConnection, acquired: bool, _deadline: float
    ) -> None:
        assert cleanup_connection.invalidated
        assert not acquired

    monkeypatch.setattr(store_migration, "_bounded_postgres_connect", fake_connect)
    monkeypatch.setattr(store_migration, "_MaintenanceWatchdog", FakeWatchdog)
    monkeypatch.setattr(
        store_migration, "_cleanup_postgres_maintenance", verify_cleanup
    )

    with pytest.raises(RuntimeError, match="advisory lock result was lost"):
        store_migration._maintenance_prepare_postgres(  # pyright: ignore[reportPrivateUsage]
            cast(Any, object()),
            "0002_artifact_byte_size_bigint",
            "0003_evidence_claim_leases",
            time.monotonic() + 5,
        )
    assert connection.invalidated


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("0002_artifact_byte_size_bigint", "0003_evidence_claim_leases"),
        ("0003_evidence_claim_leases", "0002_artifact_byte_size_bigint"),
    ],
)
def test_postgres_preparation_retries_transient_table_fence(
    monkeypatch: pytest.MonkeyPatch, source: str, target: str
) -> None:
    class LockUnavailable(Exception):
        sqlstate = "55P03"

    class AdvisoryResult:
        def scalar_one(self) -> bool:
            return True

    class FakeConnection:
        invalidated = False
        lock_attempts = 0
        rollbacks = 0

        def exec_driver_sql(
            self, statement: str, _parameters: object | None = None
        ) -> AdvisoryResult | None:
            if "pg_try_advisory_lock" in statement:
                return AdvisoryResult()
            if statement.startswith("LOCK TABLE"):
                self.lock_attempts += 1
                if self.lock_attempts == 1:
                    raise DBAPIError(statement, {}, LockUnavailable(), False)
            return None

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            self.rollbacks += 1

        def invalidate(self) -> None:
            self.invalidated = True

    class FakeWatchdog:
        expired = threading.Event()

        def __init__(self, _connection: object, _deadline: float) -> None:
            self.cancelled = False

        def commit(self) -> None:
            pass

        def cancel(self) -> None:
            self.cancelled = True

    connection = FakeConnection()
    transitions: list[tuple[str, str]] = []

    @contextmanager
    def fake_connect(_engine: object, _deadline: float):
        yield connection

    def fake_transition(
        _connection: object,
        transition_source: str,
        transition_target: str,
        _deadline: float,
        commit: Any,
    ) -> None:
        transitions.append((transition_source, transition_target))
        commit()

    def verify_cleanup(
        cleanup_connection: FakeConnection, acquired: bool, _deadline: float
    ) -> None:
        assert cleanup_connection is connection
        assert acquired

    monkeypatch.setattr(store_migration, "_bounded_postgres_connect", fake_connect)
    monkeypatch.setattr(store_migration, "_MaintenanceWatchdog", FakeWatchdog)
    monkeypatch.setattr(
        store_migration, "_arm_postgres_transaction_timeout", FakeWatchdog
    )
    monkeypatch.setattr(store_migration, "_run_preparation_transition", fake_transition)
    monkeypatch.setattr(
        store_migration, "_cleanup_postgres_maintenance", verify_cleanup
    )
    monkeypatch.setattr(store_migration.random, "uniform", lambda *_args: 0.0)
    monkeypatch.setattr(store_migration.time, "sleep", lambda _seconds: None)

    store_migration._maintenance_prepare_postgres(  # pyright: ignore[reportPrivateUsage]
        cast(Any, object()), source, target, time.monotonic() + 5
    )

    assert connection.lock_attempts == 2
    assert connection.rollbacks == 1
    assert not connection.invalidated
    assert transitions == [(source, target)]


@pytest.mark.requires_postgres
def test_postgres_acknowledged_0002_preparation_and_rollback_preserve_rows() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0002_artifact_byte_size_bigint")
            _seed_0002_evidence(connection)
            before = _snapshot_legacy_rows(connection)
            connection.commit()
        identity = store_migration._database_identity(engine)  # pyright: ignore[reportPrivateUsage]
        store_migration.maintenance_prepare_database(
            engine,
            None,
            store_migration.MaintenanceAcknowledgement(
                identity, "0002_artifact_byte_size_bigint"
            ),
        )
        with engine.connect() as connection:
            assert store_migration._revision(connection) == "0003_evidence_claim_leases"  # pyright: ignore[reportPrivateUsage]
            assert _snapshot_legacy_rows(connection) == before
        store_migration.maintenance_prepare_database(
            engine,
            None,
            store_migration.MaintenanceAcknowledgement(
                identity, "0003_evidence_claim_leases"
            ),
            rollback=True,
        )
        with engine.connect() as connection:
            assert (
                store_migration._revision(connection)
                == "0002_artifact_byte_size_bigint"
            )  # pyright: ignore[reportPrivateUsage]
            assert _snapshot_legacy_rows(connection) == before
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_migrates_artifact_byte_size_from_integer_to_bigint() -> None:
    with _isolated_postgres_url() as database_url:
        scoped_engine = create_engine(database_url)
        try:
            config = Config()
            config.set_main_option(
                "script_location",
                str(Path(store_migration.__file__).with_name("migrations")),
            )
            with scoped_engine.connect() as connection:
                config.attributes["connection"] = connection
                command.upgrade(config, "0001_initial_store")
                connection.commit()
                initial_type = connection.exec_driver_sql(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = current_schema() "
                    "AND table_name = 'artifact' AND column_name = 'byte_size'"
                ).scalar_one()
                command.upgrade(config, "0002_artifact_byte_size_bigint")
                connection.commit()
                upgraded_type = connection.exec_driver_sql(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = current_schema() "
                    "AND table_name = 'artifact' AND column_name = 'byte_size'"
                ).scalar_one()
                _seed_0002_evidence(connection)
                connection.commit()
                rows_before = _snapshot_legacy_rows(connection)
                command.upgrade(config, "head")
                connection.commit()
                rows_after = _snapshot_legacy_rows(connection)
                tables = inspect(connection).get_table_names()
            assert initial_type == "integer"
            assert upgraded_type == "bigint"
            assert "evidence_claim" not in tables
            assert {
                "claim_sessions",
                "session_claims",
                "evidence_claim_generations",
                "claim_session_open_receipts",
                "claim_session_acquire_receipts",
                "claim_session_acquire_receipt_items",
            } <= set(tables)
            assert rows_after == rows_before
        finally:
            scoped_engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_upgrade_arms_syncs_resets_and_preserves_rows() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            _seed_0002_evidence(connection)
            before = _snapshot_legacy_rows(connection)
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0003_evidence_claim_leases",
        )
        store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0004_claim_sessions"
            )
            assert _snapshot_legacy_rows(connection) == before
            assert connection.exec_driver_sql(
                "SHOW transaction_timeout"
            ).scalar_one() in {"0", "0ms"}
        engine.dispose()


@pytest.mark.requires_postgres
@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_postgres_maintenance_classifies_commit_after_operation_deadline(
    monkeypatch: pytest.MonkeyPatch, direction: str
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        source_revision = (
            "0003_evidence_claim_leases"
            if direction == "upgrade"
            else "0004_claim_sessions"
        )
        target_revision = (
            "0004_claim_sessions"
            if direction == "upgrade"
            else "0003_evidence_claim_leases"
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, source_revision)
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision=source_revision,
        )
        run_name = f"_run_maintenance_{direction}"
        original = getattr(store_migration, run_name)

        def committed_late(*args, **kwargs):
            original(*args, **kwargs)
            time.sleep(1.05)
            raise store_migration._AmbiguousMaintenanceCommit(  # pyright: ignore[reportPrivateUsage]
                "commit returned after operation deadline"
            )

        monkeypatch.setattr(store_migration, "_MAINTENANCE_TIMEOUT_SECONDS", 1.0)
        monkeypatch.setattr(
            store_migration, "_MAINTENANCE_READBACK_TIMEOUT_SECONDS", 6.0
        )
        monkeypatch.setattr(store_migration, run_name, committed_late)
        if direction == "upgrade":
            store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        else:
            store_migration.maintenance_downgrade_database(
                engine, None, acknowledgement
            )
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == target_revision
            )
        engine.dispose()


def test_sqlite_maintenance_classifies_commit_after_operation_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    engine = create_engine(f"sqlite+pysqlite:///{store_root / 'store.sqlite3'}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0003_evidence_claim_leases")
        connection.commit()
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
        expected_revision="0003_evidence_claim_leases",
    )
    original = store_migration._run_maintenance_upgrade  # pyright: ignore[reportPrivateUsage]
    clock = [time.monotonic()]
    # Force the intended post-commit deadline crossing, independently of disk
    # latency. Keep real migration/readback; do not change production budgets
    # or patch the process-global clock used by unrelated runtime threads.
    monkeypatch.setattr(
        store_migration,
        "time",
        SimpleNamespace(monotonic=lambda: clock[0], sleep=time.sleep),
    )

    def committed_late(*args, **kwargs):
        original(*args, **kwargs)
        clock[0] += 0.25
        raise store_migration._AmbiguousMaintenanceCommit(  # pyright: ignore[reportPrivateUsage]
            "commit returned after operation deadline"
        )

    monkeypatch.setattr(store_migration, "_MAINTENANCE_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(store_migration, "_MAINTENANCE_READBACK_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(store_migration, "_run_maintenance_upgrade", committed_late)
    store_migration.maintenance_upgrade_database(engine, store_root, acknowledgement)
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0004_claim_sessions"
        )
    engine.dispose()


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_sqlite_maintenance_canonicalizes_relative_database_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, direction: str
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    absolute_engine = create_engine(f"sqlite+pysqlite:///{database_path}")
    config = Config()
    config.set_main_option(
        "script_location", str(Path(store_migration.__file__).with_name("migrations"))
    )
    with absolute_engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(
            config,
            "0003_evidence_claim_leases"
            if direction == "upgrade"
            else "0004_claim_sessions",
        )
        connection.commit()
    absolute_engine.dispose()
    monkeypatch.chdir(store_root)
    engine = create_engine("sqlite+pysqlite:///store.sqlite3")
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        database_identity=f"sqlite+pysqlite:///{database_path}",
        expected_revision=(
            "0003_evidence_claim_leases"
            if direction == "upgrade"
            else "0004_claim_sessions"
        ),
    )
    operation = (
        store_migration.maintenance_upgrade_database
        if direction == "upgrade"
        else store_migration.maintenance_downgrade_database
    )
    operation(engine, store_root, acknowledgement)
    engine.dispose()


@pytest.mark.parametrize("failure", ["mismatch", "escape", "symlink", "missing"])
def test_sqlite_maintenance_identity_fails_closed_before_ddl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    database_path.touch()
    target = database_path
    acknowledgement_identity = f"sqlite+pysqlite:///{database_path}"
    if failure == "mismatch":
        acknowledgement_identity += "-other"
    elif failure == "escape":
        target = tmp_path / "outside.sqlite3"
        target.touch()
        acknowledgement_identity = f"sqlite+pysqlite:///{target}"
    elif failure == "symlink":
        real = tmp_path / "real.sqlite3"
        real.touch()
        database_path.unlink()
        database_path.symlink_to(real)
    else:
        database_path.unlink()
    engine = create_engine(f"sqlite+pysqlite:///{target}")
    called = False

    def unexpected_ddl(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(store_migration, "_run_maintenance_upgrade", unexpected_ddl)
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        acknowledgement_identity, "0003_evidence_claim_leases"
    )
    with pytest.raises(RuntimeError):
        store_migration.maintenance_upgrade_database(
            engine, store_root, acknowledgement
        )
    assert not called
    engine.dispose()


def test_sqlite_maintenance_revalidates_target_after_file_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    replacement_path = tmp_path / "replacement.sqlite3"
    config = Config()
    config.set_main_option(
        "script_location", str(Path(store_migration.__file__).with_name("migrations"))
    )
    for path in (database_path, replacement_path):
        seeded = create_engine(f"sqlite+pysqlite:///{path}")
        with seeded.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        seeded.dispose()
    engine = create_engine(f"sqlite+pysqlite:///{database_path}")
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        store_migration._database_identity(engine, store_root),  # pyright: ignore[reportPrivateUsage]
        "0003_evidence_claim_leases",
    )

    original_upgrade = store_migration._run_maintenance_upgrade  # pyright: ignore[reportPrivateUsage]

    def replace_after_mutation(*args, **kwargs):
        original_upgrade(*args, **kwargs)
        database_path.replace(tmp_path / "original.sqlite3")
        replacement_path.replace(database_path)

    monkeypatch.setattr(
        store_migration, "_run_maintenance_upgrade", replace_after_mutation
    )
    with pytest.raises(RuntimeError, match="target changed while fenced"):
        store_migration.maintenance_upgrade_database(
            engine, store_root, acknowledgement
        )
    engine.dispose()


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_sqlite_maintenance_pins_acknowledged_file_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, direction: str
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    replacement_path = tmp_path / "replacement.sqlite3"
    for path in (database_path, replacement_path):
        path.touch()
    engine = create_engine(f"sqlite+pysqlite:///{database_path}")
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        store_migration._database_identity(engine, store_root),  # pyright: ignore[reportPrivateUsage]
        "0003_evidence_claim_leases"
        if direction == "upgrade"
        else "0004_claim_sessions",
    )
    original_pin = store_migration._pinned_sqlite_database  # pyright: ignore[reportPrivateUsage]

    def replace_before_pin(path: Path, identity: tuple[int, int]):
        database_path.replace(tmp_path / "acknowledged.sqlite3")
        replacement_path.replace(database_path)
        return original_pin(path, identity)

    monkeypatch.setattr(store_migration, "_pinned_sqlite_database", replace_before_pin)
    operation = (
        store_migration.maintenance_upgrade_database
        if direction == "upgrade"
        else store_migration.maintenance_downgrade_database
    )
    with pytest.raises(RuntimeError, match="changed while opening"):
        operation(engine, store_root, acknowledgement)
    engine.dispose()


def test_sqlite_maintenance_discards_stale_pooled_database_handle(
    tmp_path: Path,
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    replacement_path = tmp_path / "replacement.sqlite3"
    original_path = tmp_path / "original.sqlite3"
    config = Config()
    config.set_main_option(
        "script_location", str(Path(store_migration.__file__).with_name("migrations"))
    )
    for path in (database_path, replacement_path):
        seeded = create_engine(f"sqlite+pysqlite:///{path}")
        with seeded.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        seeded.dispose()
    engine = create_engine(f"sqlite+pysqlite:///{database_path}")
    with engine.connect() as stale:
        assert stale.exec_driver_sql("PRAGMA database_list").one()[2] == str(
            database_path
        )
    database_path.replace(original_path)
    replacement_path.replace(database_path)
    acknowledgement = store_migration.MaintenanceAcknowledgement(
        store_migration._database_identity(engine, store_root),  # pyright: ignore[reportPrivateUsage]
        "0003_evidence_claim_leases",
    )
    store_migration.maintenance_upgrade_database(engine, store_root, acknowledgement)
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0004_claim_sessions"
        )
    original = create_engine(f"sqlite+pysqlite:///{original_path}")
    with original.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            == "0003_evidence_claim_leases"
        )
    original.dispose()
    engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_fenced_downgrade_recreates_empty_0003_coordination() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0004_claim_sessions")
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0004_claim_sessions",
        )
        store_migration.maintenance_downgrade_database(engine, None, acknowledgement)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0003_evidence_claim_leases"
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM evidence_claim")
                ).scalar_one()
                == 0
            )
            tables = set(inspect(connection).get_table_names())
            assert "claim_sessions" not in tables
            assert "evidence_claim" in tables
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_upgrade_stale_ack_on_0004_is_not_reconciled_as_success() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        persistence.close()
        engine = create_engine(database_url)
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0003_evidence_claim_leases",
        )
        with pytest.raises(Exception):
            store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_downgrade_stale_ack_on_0003_is_not_reconciled_as_success() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0004_claim_sessions",
        )
        with pytest.raises(Exception):
            store_migration.maintenance_downgrade_database(
                engine, None, acknowledgement
            )
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_reset_failure_invalidates_after_committed_upgrade() -> (
    None
):
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0003_evidence_claim_leases",
        )
        failed_connection_ids: list[int] = []

        def fail_reset(
            connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            if statement == "RESET transaction_timeout":
                failed_connection_ids.append(id(connection))
                raise RuntimeError("injected reset failure")

        event.listen(engine, "before_cursor_execute", fail_reset)
        with pytest.raises(RuntimeError, match="injected reset failure"):
            store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        event.remove(engine, "before_cursor_execute", fail_reset)
        with engine.connect() as replacement:
            assert id(replacement.connection) not in failed_connection_ids
            assert replacement.exec_driver_sql(
                "SHOW transaction_timeout"
            ).scalar_one() in {"0", "0ms"}
            assert (
                replacement.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0004_claim_sessions"
            )
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_service_commit_lookup_fetch_contract(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        commit = _hit_commit(tmp_path / "sources", "MPOSTGRESPHASEFIVE")
        app = create_service_app(
            ServiceSettings(
                database_url=database_url,
                artifacts_dir=tmp_path / "artifacts",
            ),
            persistence=persistence,
        )
        with TestClient(app) as test_client:
            store = HttpEvidenceStore(
                "http://testserver",
                _client=cast(httpx.Client, test_client),
                _async_transport=httpx.ASGITransport(app=app),
            )
            assert store.commit_many((commit,)) in {
                (CommitOutcome.CREATED,),
                (CommitOutcome.EXISTING,),
            }
            query = EvidenceQuery(commit.identity, commit.key)
            assert store.lookup_many((query,))[commit.key].status is EvidenceStatus.HIT
            fetched = store.fetch(commit.key)
    assert fetched is not None
    assert fetched.normalized_artifact is not None
    assert fetched.raw_artifact is not None
    assert fetched.normalized_artifact.path.read_bytes() == b"normalized"
    assert fetched.raw_artifact.path.read_bytes() == b"raw"


@pytest.mark.requires_postgres
def test_postgres_concurrent_identical_commits_are_idempotent(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        unique_sequence = "M" + "".join(
            chr(ord("A") + value % 26) for value in os.urandom(24)
        )
        commit = _hit_commit(
            tmp_path / "sources",
            unique_sequence,
            normalized_data=b"concurrent-normalized",
            raw_data=b"concurrent-raw",
        )
        model = CommitModel.from_domain(commit)
        assert model.normalized_artifact is not None
        assert model.raw_artifact is not None
        stored = {
            reference.digest: StoredArtifact(
                digest=reference.digest,
                media_type=reference.media_type,
                byte_size=reference.byte_size,
                relative_path=f"fixture/{reference.digest}",
            )
            for reference in (model.normalized_artifact, model.raw_artifact)
        }

        def commit_once() -> CommitOutcome:
            persistence = PostgresEvidencePersistence.open(database_url)
            try:
                return persistence.commit_many((model,), stored)[0]
            finally:
                persistence.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(lambda _index: commit_once(), range(2)))
    assert sorted(outcomes) == [CommitOutcome.CREATED, CommitOutcome.EXISTING]


@pytest.mark.requires_postgres
def test_postgres_http_claim_session_end_to_end(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        app = create_service_app(_settings(tmp_path), persistence=persistence)
        commit = _hit_commit(tmp_path / "session-sources", "MPOSTGRESSESSION")
        query = EvidenceQuery(commit.identity, commit.key)
        with (
            TestClient(app) as service,
            HttpEvidenceStore(
                "http://testserver",
                _client=service,
                _async_transport=httpx.ASGITransport(app=app),
            ) as store,
        ):
            assert store.supports_claim_sessions
            with store.claim_session() as session:
                decision = session.acquire_many((query,))[0]
                assert decision.disposition is ClaimDisposition.ACQUIRED
                assert session.finalize_many((commit,)) == (CommitOutcome.CREATED,)
            fetched = store.fetch(commit.key)
            assert fetched is not None
            assert fetched.record.payload_digest == commit.payload_digest


@pytest.mark.requires_postgres
def test_postgres_staged_artifacts_cannot_finalize_after_local_authority_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        app = create_service_app(_settings(tmp_path), persistence=persistence)
        commit = _hit_commit(tmp_path / "sources", "MSTAGINGLOSS")
        query = EvidenceQuery(commit.identity, commit.key)
        with (
            TestClient(app) as service,
            HttpEvidenceStore(
                "http://testserver",
                _client=service,
                _async_transport=httpx.ASGITransport(app=app),
            ) as store,
        ):
            with store.claim_session() as session:
                session.acquire_many((query,))
                original = session._upload_until
                staged = 0

                def stage_then_lose(payload: ArtifactFile, *, deadline: float) -> None:
                    nonlocal staged
                    original(payload, deadline=deadline)
                    staged += 1
                    if staged == 2:
                        session._lost = StoreError("local authority loss after staging")

                monkeypatch.setattr(session, "_upload_until", stage_then_lose)
                with pytest.raises(EvidenceClaimLostError):
                    session.finalize_many((commit,))
                assert store.lookup_many((query,)) == {}
                assert staged == 2
            # Closing the first session releases its claims. Reuse already
            # staged bytes in a new session, but publish evidence only now.
            with store.claim_session() as resumed:

                def unexpected_upload(*_args: object, **_kwargs: object) -> None:
                    raise AssertionError("successful staging should be reused")

                monkeypatch.setattr(resumed, "_upload_until", unexpected_upload)
                resumed.acquire_many((query,))
                assert resumed.finalize_many((commit,)) == (CommitOutcome.CREATED,)
            fetched = store.fetch(commit.key)
            assert fetched is not None
            assert fetched.record.payload_digest == commit.payload_digest


@pytest.mark.requires_postgres
def test_postgres_http_annotation_uses_one_claim_session(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        app = create_service_app(_settings(tmp_path), persistence=persistence)
        fasta = tmp_path / "claim-session.fasta"
        fasta.write_text(">one\nMPEPTIDE\n>two\nMSEQUENCE\n", encoding="utf-8")
        adapter = FixtureAdapter(
            executable=write_fixture_tool(tmp_path / "fixture-tool-session"),
            database=write_fixture_database(tmp_path / "database-session"),
        )
        with TestClient(app) as service:
            capability = service.get("/v1/internal/claim-sessions/capabilities")
            with HttpEvidenceStore(
                "http://testserver",
                _client=service,
                _async_transport=httpx.ASGITransport(app=app),
            ) as store:
                summary = run_annotation(
                    fasta_path=fasta,
                    output_dir=tmp_path / "session-output",
                    adapter=adapter,
                    store=store,
                )
            with persistence.engine.connect() as connection:
                session_count = connection.execute(
                    select(func.count()).select_from(claim_sessions)
                ).scalar_one()
        assert capability.status_code == 200
        assert capability.json()["protocol"] == "claim-session-v1"
        assert summary.computed == 2
        assert session_count <= 1


@pytest.mark.requires_postgres
def test_postgres_acquire_receipt_replays_fixed_width_result(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        commit = _hit_commit(tmp_path / "receipt-sources", "MRECEIPTREPLAY")
        query = EvidenceQuery(commit.identity, commit.key)
        model = EvidenceQueryModel.from_domain(query)
        digest = canonical_query_digest([model])
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-receipt-replay",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        try:
            first = persistence.acquire_claim_session(
                authority,
                acquire_request_id="acquire-receipt-replay",
                query_digest=digest,
                queries=(query,),
            )
            replay = persistence.acquire_claim_session(
                authority,
                acquire_request_id="acquire-receipt-replay",
                query_digest=digest,
                queries=(query,),
            )
            assert replay == first
            with persistence.engine.connect() as connection:
                assert (
                    connection.execute(
                        select(claim_session_acquire_receipts.c.query_digest)
                    ).scalar_one()
                    == digest
                )
                item = (
                    connection.execute(select(claim_session_acquire_receipt_items))
                    .mappings()
                    .one()
                )
                assert set(item) == {
                    "session_id",
                    "request_id",
                    "input_index",
                    "outcome",
                    "generation",
                    "busy_expires_at",
                    "evidence_created_at",
                }
            persistence.close_claim_session(authority)
            with persistence.engine.connect() as connection:
                assert (
                    connection.execute(select(claim_sessions.c.state)).scalar_one()
                    == "closing"
                )
        finally:
            persistence.close()


@pytest.mark.requires_postgres
def test_postgres_empty_acquire_is_a_write_free_noop() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-empty-acquire",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        assert authority.remaining_lease_seconds <= 120
        assert authority.renew_deadline_seconds <= 90
        statements: list[str] = []

        def record_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            statements.append(statement)

        event.listen(persistence.engine, "before_cursor_execute", record_statement)
        try:
            assert (
                persistence.acquire_claim_session(
                    authority,
                    acquire_request_id="empty-acquire",
                    query_digest=canonical_query_digest([]),
                    queries=(),
                )
                == ()
            )
        finally:
            event.remove(persistence.engine, "before_cursor_execute", record_statement)
        assert statements == []
        with persistence.engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count()).select_from(claim_session_acquire_receipts)
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    select(func.count()).select_from(
                        claim_session_acquire_receipt_items
                    )
                ).scalar_one()
                == 0
            )
        persistence.close()


@pytest.mark.requires_postgres
def test_postgres_finalize_rejects_conflicting_artifact_metadata(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        commit = _hit_commit(tmp_path / "artifact-conflict", "MARTIFACTCONFLICT")
        query = EvidenceQuery(commit.identity, commit.key)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-artifact-conflict",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        claim = persistence.acquire_claim_session(
            authority,
            acquire_request_id="acquire-artifact-conflict",
            query_digest=canonical_query_digest(
                [EvidenceQueryModel.from_domain(query)]
            ),
            queries=(query,),
        )[0].claim
        assert claim is not None and commit.normalized_artifact is not None
        artifact = commit.normalized_artifact
        with persistence.engine.begin() as connection:
            connection.execute(
                artifacts.insert().values(
                    digest=artifact.digest,
                    media_type="application/conflict",
                    byte_size=artifact.byte_size,
                    relative_path="conflict/path",
                )
            )
        item = ClaimSessionFinalizeItem(
            commit=CommitModel.from_domain(commit), claim_generation=claim.generation
        )
        stored = {
            artifact.digest: StoredArtifact(
                artifact.digest,
                artifact.media_type,
                artifact.byte_size,
                "expected/path",
            )
        }
        with pytest.raises(StoreIntegrityError, match="artifact metadata conflict"):
            persistence.finalize_claim_session(authority, (item,), stored)
        persistence.close()


@pytest.mark.requires_postgres
@pytest.mark.parametrize("claim_count", [0, 1, 250, 1000])
def test_postgres_renew_and_close_statement_counts_are_o1(
    tmp_path: Path, claim_count: int
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id=f"open-o1-{claim_count}",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        if claim_count:
            queries = tuple(
                EvidenceQuery(identity, key)
                for identity, key in (
                    _key("M" + "A" * (index + 1) + "C") for index in range(claim_count)
                )
            )
            persistence.acquire_claim_session(
                authority,
                acquire_request_id=f"acquire-o1-{claim_count}",
                query_digest=canonical_query_digest(
                    [EvidenceQueryModel.from_domain(query) for query in queries]
                ),
                queries=queries,
            )
        statements: list[str] = []

        def record_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            statements.append(statement)

        event.listen(persistence.engine, "before_cursor_execute", record_statement)
        try:
            renewed = persistence.renew_claim_session(authority)
            renew_count = len(statements)
            statements.clear()
            persistence.close_claim_session(renewed)
            close_count = len(statements)
        finally:
            event.remove(persistence.engine, "before_cursor_execute", record_statement)
            persistence.close()
        assert renew_count == 5
        assert close_count == 5


@pytest.mark.requires_postgres
def test_postgres_renew_uses_live_clock_and_sweeper_skips_locked_session() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-renew-sweep-interleave",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        renew_statements: list[str] = []

        def record_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            renew_statements.append(statement)

        event.listen(persistence.engine, "before_cursor_execute", record_statement)
        try:
            persistence.renew_claim_session(authority)
        finally:
            event.remove(persistence.engine, "before_cursor_execute", record_statement)
        assert any(
            "UPDATE claim_sessions" in statement
            and statement.count("clock_timestamp()") >= 2
            for statement in renew_statements
        )

        with persistence.engine.begin() as blocker:
            blocker.execute(
                select(claim_sessions.c.session_id)
                .where(claim_sessions.c.session_id == authority.session_id)
                .with_for_update()
            )
            blocker.execute(
                claim_sessions.update()
                .where(claim_sessions.c.session_id == authority.session_id)
                .values(state="closing")
            )
            assert persistence.sweep_claim_sessions() == 0
        assert persistence.sweep_claim_sessions() >= 0
        persistence.close()


@pytest.mark.requires_postgres
def test_postgres_receipt_capacity_is_pre_mutation(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-capacity",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        with persistence.engine.begin() as connection:
            connection.execute(
                claim_session_acquire_receipts.insert(),
                [
                    {
                        "session_id": authority.session_id,
                        "request_id": f"capacity-{index}",
                        "query_digest": "a" * 64,
                        "created_at": server_time,
                    }
                    for index in range(1000)
                ],
            )
        identity, key = _key("MCAPACITY")
        query = EvidenceQuery(identity, key)
        with pytest.raises(ClaimReceiptCapacityError):
            persistence.acquire_claim_session(
                authority,
                acquire_request_id="capacity-overflow",
                query_digest=canonical_query_digest(
                    [EvidenceQueryModel.from_domain(query)]
                ),
                queries=(query,),
            )
        with persistence.engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count()).select_from(evidence_claim_generations)
                ).scalar_one()
                == 0
            )
        persistence.close()


@pytest.mark.requires_postgres
def test_postgres_exact_session_and_claim_authority_readback() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-authority-readback",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        identity, key = _key("MAUTHORITYREADBACK")
        query = EvidenceQuery(identity, key)
        acquired = persistence.acquire_claim_session(
            authority,
            acquire_request_id="acquire-authority-readback",
            query_digest=canonical_query_digest(
                [EvidenceQueryModel.from_domain(query)]
            ),
            queries=(query,),
        )[0]
        assert acquired.claim is not None
        statements: list[tuple[str, object]] = []

        def record_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            statements.append((statement, parameters))

        event.listen(persistence.engine, "before_cursor_execute", record_statement)
        try:
            assert persistence.claim_session_authority_is_live(
                authority, (acquired.claim,)
            )
            assert not persistence.claim_session_authority_is_live(
                authority,
                (replace(acquired.claim, generation=acquired.claim.generation + 1),),
            )
        finally:
            event.remove(persistence.engine, "before_cursor_execute", record_statement)
        authority_queries = [
            (statement, parameters)
            for statement, parameters in statements
            if "FROM session_claims" in statement
        ]
        assert len(authority_queries) == 2
        assert all(" IN (" in statement for statement, _ in authority_queries)
        assert all(
            isinstance(parameters, dict) and len(parameters) == 6
            for _, parameters in authority_queries
        )
        persistence.close_claim_session(authority)
        assert not persistence.claim_session_authority_is_live(
            authority, (acquired.claim,)
        )
        persistence.close()


@pytest.mark.requires_postgres
def test_postgres_authority_readback_chunks_large_accepted_input() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        authority = persistence.open_claim_session(
            open_request_id="open-authority-large",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        claims = tuple(
            SessionEvidenceClaim(_key("M" + "A" * index + "C")[1], 1)
            for index in range(1, 1002)
        )
        with persistence.engine.begin() as connection:
            connection.execute(
                session_claims.insert(),
                [
                    {
                        **{
                            name: value
                            for name, value in zip(
                                persistence_module._CLAIM_KEY_NAMES,  # pyright: ignore[reportPrivateUsage]
                                (
                                    claim.key.sequence_id,
                                    claim.key.adapter_contract_version,
                                    claim.key.tool_runtime_digest,
                                    claim.key.resource_id,
                                    claim.key.semantic_parameters_hash,
                                ),
                                strict=True,
                            )
                        },
                        "semantic_parameters_json": claim.key.semantic_parameters_json,
                        "session_id": authority.session_id,
                        "generation": claim.generation,
                    }
                    for claim in claims
                ],
            )
        statements: list[str] = []

        def record_statement(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _many: bool,
        ) -> None:
            if "FROM session_claims" in statement:
                statements.append(statement)

        event.listen(persistence.engine, "before_cursor_execute", record_statement)
        try:
            assert persistence.claim_session_authority_is_live(authority, claims)
        finally:
            event.remove(persistence.engine, "before_cursor_execute", record_statement)
            persistence.close()
        assert len(statements) == 2


@pytest.mark.requires_postgres
def test_postgres_open_receipt_rejects_changed_timing_fields() -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        server_time = persistence.database_time()
        persistence.open_claim_session(
            open_request_id="timing-bound-open",
            server_time=server_time,
            open_not_after=server_time + timedelta(seconds=30),
        )
        with pytest.raises(EvidenceConflictError):
            persistence.open_claim_session(
                open_request_id="timing-bound-open",
                server_time=server_time,
                open_not_after=server_time + timedelta(seconds=29),
            )
        persistence.close()


@pytest.mark.requires_postgres
def test_postgres_http_open_receipt_changed_timing_is_409(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        app = create_service_app(_settings(tmp_path), persistence=persistence)
        server_time = persistence.database_time()
        payload = {
            "open_request_id": "http-timing-bound-open",
            "server_time": server_time.isoformat(),
            "open_not_after": (server_time + timedelta(seconds=30)).isoformat(),
        }
        with TestClient(app) as service:
            assert (
                service.post(
                    "/v1/internal/claim-sessions/open", json=payload
                ).status_code
                == 200
            )
            changed = {
                **payload,
                "open_not_after": (server_time + timedelta(seconds=29)).isoformat(),
            }
            assert (
                service.post(
                    "/v1/internal/claim-sessions/open", json=changed
                ).status_code
                == 409
            )


@pytest.mark.requires_postgres
def test_postgres_maintenance_deadline_covers_advisory_lock_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0003_evidence_claim_leases",
        )

        @event.listens_for(engine, "before_cursor_execute")
        def delay_advisory_lock(
            _connection, _cursor, statement, _parameters, _context, _executemany
        ) -> None:
            if "pg_try_advisory_lock" in statement:
                time.sleep(0.15)

        monkeypatch.setattr(store_migration, "_MAINTENANCE_TIMEOUT_SECONDS", 0.05)
        with pytest.raises((TimeoutError, DBAPIError)):
            store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "0003_evidence_claim_leases"
            )
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_completion_readback_uses_fresh_bounded_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0004_claim_sessions")
            connection.commit()

        revision, tables = store_migration._maintenance_state(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        )
        assert revision == "0004_claim_sessions"
        assert "claim_sessions" in tables

        @event.listens_for(engine, "before_cursor_execute")
        def delay_revision_readback(
            _connection, _cursor, statement, _parameters, _context, _executemany
        ) -> None:
            if "SELECT version_num FROM alembic_version" in statement:
                time.sleep(0.15)

        started = time.monotonic()
        with pytest.raises((TimeoutError, DBAPIError)):
            store_migration._maintenance_state(  # pyright: ignore[reportPrivateUsage]
                engine, time.monotonic() + 0.05
            )
        assert time.monotonic() - started < 0.5
        engine.dispose()


@pytest.mark.requires_postgres
@pytest.mark.parametrize("phase", ["mutation", "reconciliation"])
def test_postgres_maintenance_pool_checkout_uses_phase_remainder(
    phase: str,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        held = engine.connect()
        deadline = time.monotonic() + 1.1
        started = time.monotonic()
        try:
            with pytest.raises((TimeoutError, SQLAlchemyTimeoutError)):
                if phase == "mutation":
                    store_migration._maintenance_upgrade_postgres(  # pyright: ignore[reportPrivateUsage]
                        engine,
                        store_migration.MaintenanceAcknowledgement(
                            database_identity="unused",
                            expected_revision="0003_evidence_claim_leases",
                        ),
                        deadline,
                    )
                else:
                    store_migration._maintenance_state(  # pyright: ignore[reportPrivateUsage]
                        engine, deadline
                    )
        finally:
            held.close()
            engine.dispose()
        elapsed = time.monotonic() - started
        assert 0.9 <= elapsed < 1.6


@pytest.mark.requires_postgres
@pytest.mark.parametrize("phase", ["mutation", "reconciliation"])
def test_postgres_maintenance_physical_connect_uses_phase_remainder(
    phase: str,
) -> None:
    with _isolated_postgres_url() as database_url:
        database_url = (
            make_url(database_url)
            .set(host="127.0.0.1")
            .render_as_string(hide_password=False)
        )
        engine = create_engine(database_url)
        observed: list[int] = []

        original_connect = engine.dialect.connect

        def stall_physical_connect(*cargs, **cparams):
            timeout = int(cparams["connect_timeout"])
            observed.append(timeout)
            time.sleep(timeout)
            raise TimeoutError("deterministic stalled physical connection")

        engine.dialect.connect = stall_physical_connect

        deadline = time.monotonic() + 2.5
        started = time.monotonic()
        try:
            with pytest.raises(
                TimeoutError, match="deterministic stalled physical connection"
            ):
                if phase == "mutation":
                    store_migration._maintenance_upgrade_postgres(  # pyright: ignore[reportPrivateUsage]
                        engine,
                        store_migration.MaintenanceAcknowledgement(
                            database_identity="unused",
                            expected_revision="0003_evidence_claim_leases",
                        ),
                        deadline,
                    )
                else:
                    store_migration._maintenance_state(  # pyright: ignore[reportPrivateUsage]
                        engine, deadline
                    )
        finally:
            engine.dialect.connect = original_connect
            engine.dispose()
        assert observed == [2]
        assert 1.9 <= time.monotonic() - started < 2.5


@pytest.mark.requires_postgres
def test_postgres_maintenance_fresh_readback_after_invalidation_is_bounded() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        with engine.connect() as connection:
            config = Config()
            config.set_main_option(
                "script_location",
                str(Path(store_migration.__file__).with_name("migrations")),
            )
            config.attributes["connection"] = connection
            command.upgrade(config, "0004_claim_sessions")
            connection.commit()
            assert (
                170000
                <= int(
                    connection.exec_driver_sql("SHOW server_version_num").scalar_one()
                )
                < 180000
            )
            connection.invalidate()

        observed: list[int] = []

        original_connect = engine.dialect.connect

        def observe_fresh_connect(*cargs, **cparams):
            observed.append(int(cparams["connect_timeout"]))
            return original_connect(*cargs, **cparams)

        engine.dialect.connect = observe_fresh_connect

        revision, tables = store_migration._maintenance_state(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        )
        assert revision == "0004_claim_sessions"
        assert "claim_sessions" in tables
        assert observed and 1 <= observed[0] <= 4
        engine.dialect.connect = original_connect
        engine.dispose()


def test_postgres_maintenance_acquisition_lock_wait_uses_phase_remainder() -> None:
    engine = create_engine("postgresql+psycopg://unused@127.0.0.1/unused")
    store_migration._POSTGRES_ACQUISITION_LOCK.acquire()  # pyright: ignore[reportPrivateUsage]
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="serialization"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, started + 0.1
            ):
                pytest.fail("connection acquisition unexpectedly succeeded")
    finally:
        store_migration._POSTGRES_ACQUISITION_LOCK.release()  # pyright: ignore[reportPrivateUsage]
        engine.dispose()
    assert 0.08 <= time.monotonic() - started < 0.5


@pytest.mark.requires_postgres
def test_postgres_maintenance_post_checkout_expiry_discards_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        with engine.connect():
            pass
        original_remaining = store_migration._remaining  # pyright: ignore[reportPrivateUsage]
        calls = 0

        def expire_after_checkout(deadline: float) -> float:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise TimeoutError("deadline expired after checkout")
            return original_remaining(deadline)

        monkeypatch.setattr(store_migration, "_remaining", expire_after_checkout)
        with pytest.raises(TimeoutError, match="expired after checkout"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, time.monotonic() + 5
            ):
                pytest.fail("expired connection was yielded")
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_acquisition_only_narrows_pool_timeout() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_timeout=0.25)
        observed: list[float] = []
        original_connect = engine.dialect.connect

        def observe_pool_timeout(*args: Any, **kwargs: Any) -> Any:
            observed.append(cast(Any, engine.pool)._timeout)
            return original_connect(*args, **kwargs)

        engine.dialect.connect = observe_pool_timeout

        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pass
        assert observed == [0.25]
        assert cast(Any, engine.pool)._timeout == 0.25
        engine.dispose()


def test_postgres_maintenance_refreshes_pool_remainder_before_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused", pool_timeout=30
    )
    remainders = iter((5.0, 4.0, 0.75))
    observed: list[float] = []

    monkeypatch.setattr(
        store_migration, "_remaining", lambda _deadline: next(remainders)
    )

    def stop_at_checkout() -> None:
        observed.append(cast(Any, engine.pool)._timeout)
        raise RuntimeError("stop at checkout")

    monkeypatch.setattr(engine, "connect", stop_at_checkout)
    with pytest.raises(RuntimeError, match="stop at checkout"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 10
        ):
            pytest.fail("test checkout unexpectedly succeeded")
    assert observed == [0.75]
    assert cast(Any, engine.pool)._timeout == 30
    engine.dispose()


def test_postgres_maintenance_rejects_unrepresentable_physical_connect_budget() -> None:
    engine = create_engine("postgresql+psycopg://unused@127.0.0.1/unused")
    connected = False

    def unexpected_connect(*_cargs, **_cparams):
        nonlocal connected
        connected = True
        raise AssertionError("physical connect should have been rejected")

    engine.dialect.connect = unexpected_connect
    with pytest.raises(TimeoutError, match="insufficient physical-connect budget"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 1.5
        ):
            pytest.fail("unrepresentable physical connection was acquired")
    assert not connected
    engine.dispose()


def test_postgres_maintenance_replaces_zero_physical_connect_timeout() -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        connect_args={"connect_timeout": 0},
    )
    observed: list[int] = []

    def observe_connect_timeout(*_cargs, **cparams):
        observed.append(int(cparams["connect_timeout"]))
        raise TimeoutError("stop after observing bounded connect timeout")

    engine.dialect.connect = observe_connect_timeout
    with pytest.raises(TimeoutError, match="stop after observing"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 3.5
        ):
            pytest.fail("test physical connection unexpectedly succeeded")
    assert observed == [3]
    engine.dispose()


def test_postgres_maintenance_rejects_unbounded_client_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from psycopg import capabilities

    engine = create_engine("postgresql+psycopg://unused@127.0.0.1/unused")
    connected = False

    def unexpected_connect(*_args: Any, **_kwargs: Any) -> None:
        nonlocal connected
        connected = True

    monkeypatch.setattr(capabilities, "has_cancel_safe", lambda: False)
    monkeypatch.setattr(engine, "connect", unexpected_connect)
    with pytest.raises(RuntimeError, match="libpq 17 bounded cancellation"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("unsupported client cancellation acquired a connection")
    assert not connected
    engine.dispose()


@pytest.mark.parametrize("source", ["parameter", "positional"])
def test_postgres_maintenance_rejects_embedded_conninfo_before_connect(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        connect_args={"conninfo": "host=hidden.example"}
        if source == "parameter"
        else {},
    )
    connected = False

    def unexpected_connect(*_args: Any, **_kwargs: Any) -> None:
        nonlocal connected
        connected = True

    if source == "positional":
        creator_args = getclosurevars(engine.pool._creator).nonlocals[  # pyright: ignore[reportPrivateUsage]
            "cargs_tup"
        ]
        creator_args.append("host=hidden.example")
    monkeypatch.setattr(engine.dialect, "connect", unexpected_connect)
    with pytest.raises(RuntimeError, match="rejects embedded conninfo targets"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("embedded conninfo reached physical connect")
    assert not connected
    engine.dispose()


@pytest.mark.parametrize("event_name", ["checkout", "do_connect"])
def test_postgres_maintenance_rejects_preexisting_instance_listener(
    monkeypatch: pytest.MonkeyPatch,
    event_name: str,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        pool_timeout=0.25,
    )

    def existing_listener(*_args: Any, **_kwargs: Any) -> None:
        pass

    event.listen(engine, event_name, existing_listener)
    original_checkout = tuple(engine.pool.dispatch.checkout.listeners)
    original_do_connect = tuple(engine.dialect.dispatch.do_connect.listeners)
    connected = False
    temporary_listener_added = False

    def unexpected_connect() -> None:
        nonlocal connected
        connected = True

    def unexpected_listener_setup(*_args: Any, **_kwargs: Any) -> None:
        nonlocal temporary_listener_added
        temporary_listener_added = True

    monkeypatch.setattr(engine, "connect", unexpected_connect)
    monkeypatch.setattr(store_migration.event, "listen", unexpected_listener_setup)
    with pytest.raises(RuntimeError, match="requires a fresh Engine"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("pre-existing instance listener reached acquisition")
    assert not connected
    assert not temporary_listener_added
    assert cast(Any, engine.pool)._timeout == 0.25
    assert tuple(engine.pool.dispatch.checkout.listeners) == original_checkout
    assert tuple(engine.dialect.dispatch.do_connect.listeners) == original_do_connect
    engine.dispose()


@pytest.mark.parametrize(
    ("connect_args", "resolved", "attempts"),
    [
        ({"host": "one"}, {"one": ("192.0.2.1", "192.0.2.2")}, 2),
        (
            {"host": "one,two"},
            {"one": ("192.0.2.1", "192.0.2.2"), "two": ("192.0.2.3",)},
            3,
        ),
        ({"hostaddr": "192.0.2.1,192.0.2.2"}, {}, 2),
        (
            {
                "host": "one,two,three",
                "hostaddr": "192.0.2.1,192.0.2.2,192.0.2.3",
                "target_session_attrs": "prefer-standby",
            },
            {},
            6,
        ),
    ],
)
def test_postgres_maintenance_divides_timeout_across_all_host_attempts(
    monkeypatch: pytest.MonkeyPatch,
    connect_args: dict[str, str],
    resolved: dict[str, tuple[str, ...]],
    attempts: int,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@/unused", connect_args=connect_args
    )
    observed: list[int] = []
    observed_params: list[dict[str, Any]] = []

    monkeypatch.setattr(
        store_migration,
        "_resolve_postgres_host",
        lambda host, _port, _deadline: resolved[host],
    )

    def observe_connect_timeout(*_cargs, **cparams):
        observed.append(int(cparams["connect_timeout"]))
        observed_params.append(dict(cparams))
        raise TimeoutError("stop after observing per-attempt timeout")

    engine.dialect.connect = observe_connect_timeout
    total_budget = attempts * 2 + 1.5
    with pytest.raises(TimeoutError, match="stop after observing"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + total_budget
        ):
            pytest.fail("test physical connection unexpectedly succeeded")
    whole_budget = int(total_budget)
    assert observed == [whole_budget // attempts]
    assert observed[0] * attempts <= whole_budget
    expected_targets = (
        attempts // 2
        if connect_args.get("target_session_attrs") == "prefer-standby"
        else attempts
    )
    assert len(observed_params[0]["hostaddr"].split(",")) == expected_targets
    assert len(observed_params[0]["host"].split(",")) == expected_targets
    engine.dispose()


def test_postgres_maintenance_expiry_before_physical_connect_is_irreversible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("postgresql+psycopg://unused@127.0.0.1/unused")
    remainders = iter((5.0, 0.05, 5.0))
    connected = False

    monkeypatch.setattr(
        store_migration, "_remaining", lambda _deadline: next(remainders)
    )

    def delayed_target_preparation(
        cparams: dict[str, Any], _deadline: float
    ) -> tuple[dict[str, Any], int]:
        time.sleep(0.1)
        return dict(cparams), 1

    def unexpected_connect(*_args: Any, **_kwargs: Any) -> None:
        nonlocal connected
        connected = True

    monkeypatch.setattr(
        store_migration,
        "_resolve_postgres_connect_targets",
        delayed_target_preparation,
    )
    monkeypatch.setattr(engine.dialect, "connect", unexpected_connect)
    with pytest.raises(TimeoutError, match="initialization exceeded deadline"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 10
        ):
            pytest.fail("expired acquisition started physical connect")
    assert not connected
    assert cast(Any, engine.pool).checkedout() == 0
    assert not any(
        thread.name == "seqevi-postgres-acquisition-watchdog" and thread.is_alive()
        for thread in threading.enumerate()
    )
    engine.dispose()


def test_postgres_maintenance_stalled_dns_is_killed_and_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid_file = tmp_path / "postgres-resolver.pid"
    monkeypatch.setattr(
        store_migration,
        "_postgres_resolver_command",
        lambda _host, _port: (
            sys.executable,
            "-c",
            (
                "import os,pathlib,signal,sys;"
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()),encoding='ascii');"
                "signal.pause()"
            ),
            str(pid_file),
        ),
    )
    with pytest.raises(TimeoutError, match="DNS resolution exceeded deadline"):
        store_migration._resolve_postgres_host(  # pyright: ignore[reportPrivateUsage]
            "stalled.invalid", 5432, time.monotonic() + 0.2
        )
    resolver_pid = int(pid_file.read_text(encoding="ascii"))
    with pytest.raises(ProcessLookupError):
        os.kill(resolver_pid, 0)


def test_postgres_maintenance_resolver_matches_psycopg_getaddrinfo_flags() -> None:
    command = store_migration._postgres_resolver_command(  # pyright: ignore[reportPrivateUsage]
        "example.invalid", 5432
    )
    assert command[-4:] == (
        str(socket.AF_UNSPEC),
        str(socket.SOCK_STREAM),
        "0",
        "0",
    )


def test_postgres_maintenance_resolver_preserves_ipv6_scope_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ResolvedProcess:
        returncode = 0

        def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
            assert timeout is not None
            payload = [
                [socket.AF_INET6, socket.SOCK_STREAM, 6, "", ["fe80::1", 5432, 0, 3]],
                [socket.AF_INET6, socket.SOCK_STREAM, 6, "", ["fe80::1", 5432, 0, 4]],
                [socket.AF_INET, socket.SOCK_STREAM, 6, "", ["192.0.2.1", 5432]],
            ]
            return json.dumps(payload).encode(), b""

    monkeypatch.setattr(
        store_migration.subprocess,
        "Popen",
        lambda *_args, **_kwargs: ResolvedProcess(),
    )
    assert store_migration._resolve_postgres_host(  # pyright: ignore[reportPrivateUsage]
        "scoped.example", 5432, time.monotonic() + 5
    ) == ("fe80::1%3", "fe80::1%4", "192.0.2.1")


@pytest.mark.parametrize("source", ["parameter", "environment"])
def test_postgres_maintenance_rejects_abstract_socket_before_driver_connect(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    connect_args: dict[str, Any] = {}
    if source == "parameter":
        connect_args["host"] = "@seqevi"
    else:
        monkeypatch.setenv("PGHOST", "@seqevi")
    engine = create_engine(
        "postgresql+psycopg://unused@/unused", connect_args=connect_args
    )
    connected = False

    def unexpected_connect(*_args: Any, **_kwargs: Any) -> None:
        nonlocal connected
        connected = True

    monkeypatch.setattr(engine.dialect, "connect", unexpected_connect)
    with pytest.raises(RuntimeError, match="does not support abstract Unix socket"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("abstract Unix socket reached the driver")
    assert not connected
    engine.dispose()


def test_postgres_maintenance_preserves_filesystem_socket_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_resolution(*_args: Any) -> tuple[str, ...]:
        pytest.fail("filesystem Unix socket was sent to DNS resolution")

    monkeypatch.setattr(
        store_migration, "_resolve_postgres_host", unexpected_resolution
    )
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        {"host": "/var/run/postgresql"}, time.monotonic() + 10
    )
    assert bounded["host"] == "/var/run/postgresql"
    assert bounded["hostaddr"] == ""
    assert attempts == 1


@pytest.mark.parametrize(
    "connect_args", [{}, {"connect_timeout": None}, {"connect_timeout": ""}]
)
@pytest.mark.parametrize("environment_timeout", ["2", "2.5"])
def test_postgres_maintenance_preserves_environment_connect_timeout(
    monkeypatch: pytest.MonkeyPatch,
    connect_args: dict[str, Any],
    environment_timeout: str,
) -> None:
    monkeypatch.setenv("PGCONNECT_TIMEOUT", environment_timeout)
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        connect_args=connect_args,
    )
    observed: list[int] = []

    def observe_connect_timeout(*_args: Any, **cparams: Any) -> None:
        observed.append(int(cparams["connect_timeout"]))
        raise TimeoutError("stop after observing effective connect timeout")

    monkeypatch.setattr(engine.dialect, "connect", observe_connect_timeout)
    with pytest.raises(TimeoutError, match="stop after observing effective"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("test physical connection unexpectedly succeeded")
    assert observed == [2]
    engine.dispose()


@pytest.mark.parametrize("configured_timeout", [2, "2.5"])
def test_postgres_maintenance_preserves_explicit_connect_timeout_cap(
    monkeypatch: pytest.MonkeyPatch, configured_timeout: int | str
) -> None:
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "4")
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        connect_args={"connect_timeout": configured_timeout},
    )
    observed: list[int] = []

    def observe_connect_timeout(*_args: Any, **cparams: Any) -> None:
        observed.append(int(cparams["connect_timeout"]))
        raise TimeoutError("stop after observing explicit connect timeout")

    monkeypatch.setattr(engine.dialect, "connect", observe_connect_timeout)
    with pytest.raises(TimeoutError, match="stop after observing explicit"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("test physical connection unexpectedly succeeded")
    assert observed == [2]
    engine.dispose()


def test_postgres_maintenance_rejects_invalid_connect_timeout_without_coercion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused",
        connect_args={"connect_timeout": "invalid"},
    )
    connected = False

    def unexpected_connect(*_args: Any, **_cparams: Any) -> None:
        nonlocal connected
        connected = True

    monkeypatch.setattr(engine.dialect, "connect", unexpected_connect)
    with pytest.raises(ValueError, match="could not convert string to float"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("invalid timeout unexpectedly reached the driver")
    assert not connected
    engine.dispose()


def test_postgres_maintenance_uses_client_compiled_default_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, int]] = []
    monkeypatch.setattr(store_migration, "_postgres_default_port", lambda: 6543)

    def resolve(host: str, port: int, _deadline: float) -> tuple[str, ...]:
        observed.append((host, port))
        return ("192.0.2.1",)

    monkeypatch.setattr(store_migration, "_resolve_postgres_host", resolve)
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        {"host": "one,two", "port": "5433,"}, time.monotonic() + 10
    )
    assert attempts == 2
    assert observed == [("one", 5433), ("two", 6543)]
    assert bounded["port"] == "5433,"


def test_postgres_maintenance_uses_environment_attempts_and_prefer_standby(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PGHOST", "one,two")
    monkeypatch.setenv("PGPORT", "5432,5433")
    monkeypatch.setenv("PGTARGETSESSIONATTRS", "prefer-standby")
    monkeypatch.setattr(
        store_migration,
        "_resolve_postgres_host",
        lambda host, _port, _deadline: {
            "one": ("192.0.2.1", "192.0.2.2"),
            "two": ("192.0.2.3",),
        }[host],
    )
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        {}, time.monotonic() + 10
    )
    assert attempts == 6
    assert bounded["host"] == "one,one,two"
    assert bounded["hostaddr"] == "192.0.2.1,192.0.2.2,192.0.2.3"
    assert bounded["port"] == "5432,5432,5433"
    assert bounded["target_session_attrs"] == "prefer-standby"


@pytest.mark.parametrize(
    "cparams",
    [
        {"host": "one,two", "hostaddr": "192.0.2.1"},
        {"host": "one", "hostaddr": "192.0.2.1,192.0.2.2"},
    ],
)
def test_postgres_maintenance_rejects_mismatched_explicit_target_lists(
    cparams: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="host and hostaddr lists must have equal"):
        store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
            cparams, time.monotonic() + 10
        )


@pytest.mark.parametrize("parameter_value", [None, ""])
@pytest.mark.parametrize(
    ("key", "envvar", "envvalue", "expected", "attempts"),
    [
        ("host", "PGHOST", "one,two", "one,two", 2),
        (
            "hostaddr",
            "PGHOSTADDR",
            "192.0.2.1,192.0.2.2",
            "192.0.2.1,192.0.2.2",
            2,
        ),
        ("port", "PGPORT", "5432,5433", "5432,5433", 2),
        (
            "target_session_attrs",
            "PGTARGETSESSIONATTRS",
            "prefer-standby",
            "prefer-standby",
            2,
        ),
    ],
)
def test_postgres_maintenance_empty_parameter_uses_environment_fallback(
    monkeypatch: pytest.MonkeyPatch,
    parameter_value: str | None,
    key: str,
    envvar: str,
    envvalue: str,
    expected: str,
    attempts: int,
) -> None:
    monkeypatch.setenv(envvar, envvalue)
    if key == "port":
        monkeypatch.setenv("PGHOSTADDR", "192.0.2.1,192.0.2.2")
    elif key == "target_session_attrs":
        monkeypatch.delenv("PGHOST", raising=False)
        monkeypatch.delenv("PGHOSTADDR", raising=False)
    elif key == "host":
        monkeypatch.setattr(
            store_migration,
            "_resolve_postgres_host",
            lambda host, _port, _deadline: {
                "one": ("192.0.2.1",),
                "two": ("192.0.2.2",),
            }[host],
        )
    bounded, observed_attempts = (  # pyright: ignore[reportPrivateUsage]
        store_migration._resolve_postgres_connect_targets(
            {key: parameter_value}, time.monotonic() + 10
        )
    )
    assert bounded[key] == expected
    assert observed_attempts == attempts


def test_postgres_maintenance_skips_failed_dns_target_when_another_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def resolve(host: str, _port: int, _deadline: float) -> tuple[str, ...]:
        if host == "missing.invalid":
            raise OSError("name not found")
        return ("192.0.2.10",)

    monkeypatch.setattr(store_migration, "_resolve_postgres_host", resolve)
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        {"host": "missing.invalid,healthy.example"}, time.monotonic() + 10
    )
    assert attempts == 1
    assert bounded["host"] == "healthy.example"
    assert bounded["hostaddr"] == "192.0.2.10"


def test_postgres_maintenance_fails_when_all_dns_targets_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        store_migration,
        "_resolve_postgres_host",
        lambda *_args: (_ for _ in ()).throw(OSError("name not found")),
    )
    with pytest.raises(OSError, match="no usable connection targets"):
        store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
            {"host": "missing.invalid,also-missing.invalid"},
            time.monotonic() + 10,
        )


@pytest.mark.parametrize("source", ["parameter", "environment"])
def test_postgres_maintenance_counts_hostless_prefer_standby(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    cparams: dict[str, Any] = {}
    if source == "parameter":
        cparams["target_session_attrs"] = "prefer-standby"
    else:
        monkeypatch.setenv("PGTARGETSESSIONATTRS", "prefer-standby")
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        cparams, time.monotonic() + 10
    )
    assert bounded["target_session_attrs"] == "prefer-standby"
    assert "host" not in bounded and "hostaddr" not in bounded
    assert attempts == 2


def test_postgres_maintenance_resolves_mixed_explicit_and_default_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, int]] = []

    def resolve(host: str, port: int, _deadline: float) -> tuple[str, ...]:
        observed.append((host, port))
        return ("192.0.2.1" if host == "one" else "192.0.2.2",)

    monkeypatch.setattr(store_migration, "_resolve_postgres_host", resolve)
    bounded, attempts = store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
        {"host": "one,two", "port": "5433,"}, time.monotonic() + 10
    )
    assert attempts == 2
    assert observed == [("one", 5433), ("two", 5432)]
    assert bounded["port"] == "5433,"


@pytest.mark.requires_postgres
def test_postgres_maintenance_deadline_cancels_first_connect_initialization() -> None:
    with _isolated_postgres_url() as database_url:
        database_url = (
            make_url(database_url)
            .set(host="127.0.0.1")
            .render_as_string(hide_password=False)
        )
        engine = create_engine(database_url)

        @event.listens_for(engine, "connect", insert=True)
        def stall_first_connect(dbapi_connection, _connection_record) -> None:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SELECT pg_sleep(10)")

        started = time.monotonic()
        with pytest.raises(TimeoutError, match="initialization exceeded deadline"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, started + 2.25
            ):
                pytest.fail("stalled first-connect initialization succeeded")
        assert 2.0 <= time.monotonic() - started < 3.0
        assert cast(Any, engine.pool).checkedout() == 0
        assert not any(
            thread.name == "seqevi-postgres-acquisition-watchdog" and thread.is_alive()
            for thread in threading.enumerate()
        )
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_discards_connection_returned_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    with _isolated_postgres_url() as database_url:
        direct_url = database_url.replace("postgresql+psycopg://", "postgresql://")
        late_raw = psycopg.connect(direct_url)
        engine = create_engine(database_url)

        def delayed_connect(*_args: Any, **_kwargs: Any) -> Any:
            time.sleep(2.5)
            return late_raw

        monkeypatch.setattr(engine.dialect, "connect", delayed_connect)
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="initialization exceeded deadline"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, started + 2.25
            ):
                pytest.fail("late physical connection was accepted")
        assert late_raw.closed
        assert time.monotonic() - started < 3.0
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_expiry_before_initialization_query_is_irreversible() -> (
    None
):
    with _isolated_postgres_url() as database_url:
        database_url = (
            make_url(database_url)
            .set(host="127.0.0.1")
            .render_as_string(hide_password=False)
        )
        engine = create_engine(database_url)

        @event.listens_for(engine, "connect", insert=True)
        def start_initialization_after_expiry(
            dbapi_connection, _connection_record
        ) -> None:
            time.sleep(2.35)
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SELECT pg_sleep(10)")

        started = time.monotonic()
        with pytest.raises(TimeoutError, match="initialization exceeded deadline"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, started + 2.25
            ):
                pytest.fail("initialization began after acquisition expiry")
        assert 2.0 <= time.monotonic() - started < 3.0
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_does_not_mutate_configured_statement_timeout() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(
            database_url,
            connect_args={"options": "-c statement_timeout=1250ms"},
            pool_size=1,
            max_overflow=0,
        )
        with engine.connect():
            pass
        pre_ping_was_own = "_do_ping_w_event" in vars(engine.dialect)
        original_own_pre_ping = vars(engine.dialect).get("_do_ping_w_event")
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ) as connection:
            assert connection.execute(text("SHOW statement_timeout")).scalar_one() == (
                "1250ms"
            )
        assert ("_do_ping_w_event" in vars(engine.dialect)) is pre_ping_was_own
        assert vars(engine.dialect).get("_do_ping_w_event") is original_own_pre_ping
        assert not any(
            thread.name == "seqevi-postgres-acquisition-watchdog" and thread.is_alive()
            for thread in threading.enumerate()
        )
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_deadline_cancels_stalled_pool_pre_ping() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(
            database_url, pool_pre_ping=True, pool_size=1, max_overflow=0
        )
        with engine.connect():
            pass
        pre_ping_was_own = "_do_ping_w_event" in vars(engine.dialect)
        original_own_pre_ping = vars(engine.dialect).get("_do_ping_w_event")
        original_do_ping = engine.dialect.do_ping

        def stall_pre_ping(dbapi_connection: Any) -> bool:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SELECT pg_sleep(10)")
            return True

        engine.dialect.do_ping = stall_pre_ping
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="initialization exceeded deadline"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, started + 0.5
            ):
                pytest.fail("stalled pool pre-ping succeeded")
        assert 0.4 <= time.monotonic() - started < 1.5
        assert ("_do_ping_w_event" in vars(engine.dialect)) is pre_ping_was_own
        assert vars(engine.dialect).get("_do_ping_w_event") is original_own_pre_ping
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dialect.do_ping = original_do_ping
        with engine.connect() as connection:
            assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_pre_ping_reconnects_stale_pooled_connection() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(
            database_url, pool_pre_ping=True, pool_size=1, max_overflow=0
        )
        with engine.connect() as connection:
            stale_raw = cast(Any, connection.connection.driver_connection)
        stale_raw.close()
        pre_ping_was_own = "_do_ping_w_event" in vars(engine.dialect)
        original_own_pre_ping = vars(engine.dialect).get("_do_ping_w_event")

        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ) as connection:
            assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1
            assert connection.connection.driver_connection is not stale_raw
            assert (
                connection.exec_driver_sql("SHOW statement_timeout").scalar_one() == "0"
            )
        assert cast(Any, engine.pool).checkedout() == 0
        assert ("_do_ping_w_event" in vars(engine.dialect)) is pre_ping_was_own
        assert vars(engine.dialect).get("_do_ping_w_event") is original_own_pre_ping
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_watchdog_join_failure_discards_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnstoppableTimer:
        name = ""
        daemon = False

        def __init__(self, _interval: float, _callback: Any) -> None:
            pass

        def start(self) -> None:
            pass

        def cancel(self) -> None:
            pass

        def join(self, _timeout: float) -> None:
            pass

        def is_alive(self) -> bool:
            return True

    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        with engine.connect():
            pass
        pre_ping_was_own = "_do_ping_w_event" in vars(engine.dialect)
        original_own_pre_ping = vars(engine.dialect).get("_do_ping_w_event")
        monkeypatch.setattr(store_migration.threading, "Timer", UnstoppableTimer)
        with pytest.raises(RuntimeError, match="watchdog failed to stop"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, time.monotonic() + 5
            ):
                pytest.fail("unstopped watchdog yielded a connection")
        assert cast(Any, engine.pool).checkedout() == 0
        assert ("_do_ping_w_event" in vars(engine.dialect)) is pre_ping_was_own
        assert vars(engine.dialect).get("_do_ping_w_event") is original_own_pre_ping
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_error_path_join_failure_discards_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnstoppableTimer:
        name = ""
        daemon = False

        def __init__(self, _interval: float, _callback: Any) -> None:
            pass

        def start(self) -> None:
            pass

        def cancel(self) -> None:
            pass

        def join(self, _timeout: float) -> None:
            pass

        def is_alive(self) -> bool:
            return True

    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        with engine.connect():
            pass
        original_remove = store_migration.event.remove

        def fail_connect_listener_removal(target: Any, name: str, fn: Any) -> None:
            original_remove(target, name, fn)
            if name == "do_connect":
                raise RuntimeError("listener removal failed")

        monkeypatch.setattr(store_migration.threading, "Timer", UnstoppableTimer)
        monkeypatch.setattr(
            store_migration.event, "remove", fail_connect_listener_removal
        )
        with pytest.raises(RuntimeError, match="listener removal failed") as raised:
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, time.monotonic() + 5
            ):
                pytest.fail("error-path watchdog failure yielded a connection")
        assert any("watchdog cleanup failed" in note for note in raised.value.__notes__)
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dispose()


@pytest.mark.parametrize("source", ["parameter", "environment"])
def test_postgres_maintenance_rejects_service_derived_targets(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    cparams: dict[str, Any] = {}
    if source == "parameter":
        cparams["service"] = "seqevi"
    else:
        monkeypatch.setenv("PGSERVICE", "seqevi")
    with pytest.raises(RuntimeError, match="service-derived connection targets"):
        store_migration._resolve_postgres_connect_targets(  # pyright: ignore[reportPrivateUsage]
            cparams, time.monotonic() + 10
        )


def test_postgres_maintenance_late_resolver_failure_is_deadline_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LateFailureProcess:
        returncode = 1

        def communicate(self, timeout=None):
            assert timeout is not None
            return b"", b"late resolver failure"

    monkeypatch.setattr(
        store_migration.subprocess,
        "Popen",
        lambda *_args, **_kwargs: LateFailureProcess(),
    )
    monkeypatch.setattr(
        store_migration,
        "_remaining",
        lambda _deadline: (_ for _ in ()).throw(
            TimeoutError("ClaimSession maintenance deadline expired")
        ),
    )
    with pytest.raises(TimeoutError, match="maintenance deadline expired"):
        store_migration._resolve_postgres_host(  # pyright: ignore[reportPrivateUsage]
            "late.invalid", 5432, time.monotonic() + 10
        )


def test_postgres_maintenance_resolver_exit_race_preserves_deadline_and_reaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExitedResolverProcess:
        returncode = 1

        def __init__(self) -> None:
            self.communications = 0
            self.reaped = False

        def communicate(self, timeout: float | None = None):
            self.communications += 1
            if self.communications == 1:
                assert timeout is not None
                raise subprocess.TimeoutExpired("resolver", timeout)
            self.reaped = True
            return b"", b"resolver exited"

        def terminate(self) -> None:
            raise ProcessLookupError

    process = ExitedResolverProcess()
    monkeypatch.setattr(
        store_migration.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
    )
    with pytest.raises(TimeoutError, match="DNS resolution exceeded deadline"):
        store_migration._resolve_postgres_host(  # pyright: ignore[reportPrivateUsage]
            "raced.invalid", 5432, time.monotonic() + 10
        )
    assert process.communications == 2
    assert process.reaped


def test_postgres_maintenance_resolver_interruption_is_exactly_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InterruptedResolverProcess:
        returncode = None

        def __init__(self) -> None:
            self.communications = 0
            self.terminated = False
            self.reaped = False

        def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
            self.communications += 1
            if self.communications == 1:
                assert timeout is not None
                raise KeyboardInterrupt("resolver interrupted")
            assert timeout is not None
            self.reaped = True
            self.returncode = -15
            return b"", b""

        def terminate(self) -> None:
            self.terminated = True

    process = InterruptedResolverProcess()
    monkeypatch.setattr(
        store_migration.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
    )
    with pytest.raises(KeyboardInterrupt, match="resolver interrupted"):
        store_migration._resolve_postgres_host(  # pyright: ignore[reportPrivateUsage]
            "interrupted.invalid", 5432, time.monotonic() + 10
        )
    assert process.terminated
    assert process.communications == 2
    assert process.reaped


def test_postgres_maintenance_rejects_resolved_attempts_that_cannot_fit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("postgresql+psycopg://unused@many.invalid/unused")
    monkeypatch.setattr(
        store_migration,
        "_resolve_postgres_host",
        lambda _host, _port, _deadline: ("192.0.2.1", "192.0.2.2"),
    )
    connected = False

    def unexpected_connect(*_cargs, **_cparams):
        nonlocal connected
        connected = True
        raise AssertionError("physical connect should have been rejected")

    engine.dialect.connect = unexpected_connect
    with pytest.raises(TimeoutError, match="insufficient physical-connect budget"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 3.5
        ):
            pytest.fail("unrepresentable resolved attempts were acquired")
    assert not connected
    engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_physical_connect_does_not_mutate_later_params() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        original_connect = engine.dialect.connect
        observed: list[dict[str, Any]] = []

        def observe_connect_params(*cargs, **cparams):
            observed.append(dict(cparams))
            return original_connect(*cargs, **cparams)

        engine.dialect.connect = observe_connect_params
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pass
        engine.dispose()
        with engine.connect():
            pass
        assert int(observed[0]["connect_timeout"]) <= 4
        assert "connect_timeout" not in observed[1]
        engine.dialect.connect = original_connect
        engine.dispose()


def test_postgres_maintenance_listener_setup_failure_restores_pool_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused@127.0.0.1/unused", pool_timeout=0.25
    )

    def fail_listener_setup(*_args, **_kwargs) -> None:
        raise RuntimeError("listener setup failed")

    monkeypatch.setattr(store_migration.event, "listen", fail_listener_setup)
    with pytest.raises(RuntimeError, match="listener setup failed"):
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ):
            pytest.fail("listener setup failure unexpectedly acquired a connection")
    assert cast(Any, engine.pool)._timeout == 0.25
    engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_maintenance_listener_removal_failure_restores_and_discards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(
            database_url, pool_size=1, max_overflow=0, pool_timeout=0.25
        )

        def fail_listener_removal(*_args, **_kwargs) -> None:
            raise RuntimeError("listener removal failed")

        monkeypatch.setattr(store_migration.event, "remove", fail_listener_removal)
        with pytest.raises(RuntimeError, match="listener removal failed"):
            with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
                engine, time.monotonic() + 5
            ):
                pytest.fail("listener removal failure yielded a connection")
        assert cast(Any, engine.pool)._timeout == 0.25
        assert cast(Any, engine.pool).checkedout() == 0
        monkeypatch.setattr(
            store_migration,
            "_remaining",
            lambda _deadline: (_ for _ in ()).throw(
                TimeoutError("captured acquisition deadline expired")
            ),
        )
        with engine.connect() as later_connection:
            assert later_connection.exec_driver_sql("SELECT 1").scalar_one() == 1
        engine.dispose()


def test_postgres_maintenance_discard_attempts_close_after_invalidate_failure() -> None:
    class FailingConnection:
        closed = False

        def invalidate(self) -> None:
            raise RuntimeError("invalidate failed")

        def close(self) -> None:
            self.closed = True

    connection = FailingConnection()
    primary = TimeoutError("deadline expired after checkout")
    store_migration._discard_postgres_connection(  # pyright: ignore[reportPrivateUsage]
        cast(Any, connection), primary
    )
    assert connection.closed
    assert primary.__notes__ == [
        "maintenance connection invalidation failed: RuntimeError('invalidate failed')"
    ]


@pytest.mark.requires_postgres
def test_postgres_maintenance_watchdog_remains_armed_through_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            database_identity=store_migration._database_identity(engine),  # pyright: ignore[reportPrivateUsage]
            expected_revision="0003_evidence_claim_leases",
        )
        original_upgrade = store_migration.command.upgrade

        def stalled_upgrade(*args, **kwargs):
            time.sleep(0.15)
            return original_upgrade(*args, **kwargs)

        monkeypatch.setattr(store_migration, "_MAINTENANCE_TIMEOUT_SECONDS", 0.05)
        monkeypatch.setattr(store_migration.command, "upgrade", stalled_upgrade)
        with pytest.raises((TimeoutError, DBAPIError)):
            store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        engine.dispose()


def test_maintenance_timeout_show_mismatch_discards_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timers = []

    class FakeTimer:
        daemon = False

        def __init__(self, _delay, _callback):
            self.cancelled = False
            timers.append(self)

        def start(self):
            pass

        def cancel(self):
            self.cancelled = True

    class Result:
        def __init__(self, value):
            self.value = value

        def scalar_one(self):
            return self.value

    class Raw:
        def cancel(self):
            pass

    class Holder:
        driver_connection = Raw()

    class FakeConnection:
        _dbapi_connection = Holder()

        def __init__(self):
            self.invalidated = False

        def execution_options(self, **_kwargs):
            return self

        def exec_driver_sql(self, statement):
            return Result("wrong" if statement == "SHOW transaction_timeout" else None)

        def invalidate(self):
            self.invalidated = True

    monkeypatch.setattr(store_migration.threading, "Timer", FakeTimer)
    connection = FakeConnection()
    with pytest.raises(RuntimeError, match="setup readback"):
        store_migration._arm_postgres_transaction_timeout(  # pyright: ignore[reportPrivateUsage]
            cast(Any, connection), time.monotonic() + 10
        )
    assert connection.invalidated
    assert timers[0].cancelled


def test_maintenance_watchdog_classifies_expiry_during_commit_as_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeTimer:
        daemon = False

        def __init__(self, _delay, _callback):
            self.cancelled = False

        def start(self):
            pass

        def cancel(self):
            self.cancelled = True

    class Raw:
        def cancel(self):
            pass

    class Holder:
        driver_connection = Raw()

    class FakeConnection:
        _dbapi_connection = Holder()

        def __init__(self):
            self.invalidated = False
            self.watchdog: Any = None

        def commit(self):
            self.watchdog.expired.set()

        def invalidate(self):
            self.invalidated = True

    monkeypatch.setattr(store_migration.threading, "Timer", FakeTimer)
    connection = FakeConnection()
    watchdog = store_migration._MaintenanceWatchdog(  # pyright: ignore[reportPrivateUsage]
        cast(Any, connection), time.monotonic() + 10
    )
    connection.watchdog = watchdog
    with pytest.raises(
        store_migration._AmbiguousMaintenanceCommit  # pyright: ignore[reportPrivateUsage]
    ):
        watchdog.commit()
    assert connection.invalidated


def test_postgres_maintenance_cleanup_watchdog_invalidates_stalled_connection() -> None:
    cancelled = threading.Event()

    class Raw:
        def cancel(self) -> None:
            cancelled.set()

    class Holder:
        driver_connection = Raw()

    class FakeConnection:
        _dbapi_connection = Holder()

        def __init__(self) -> None:
            self.invalidated = False

        def rollback(self) -> None:
            time.sleep(0.1)

        def invalidate(self) -> None:
            self.invalidated = True

    connection = FakeConnection()
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        store_migration._cleanup_postgres_maintenance(  # pyright: ignore[reportPrivateUsage]
            cast(Any, connection), False, started + 0.05
        )
    assert time.monotonic() - started < 0.5
    assert cancelled.is_set()
    assert connection.invalidated


@pytest.mark.requires_postgres
def test_postgres_maintenance_cleanup_does_not_reconnect_invalidated_connection() -> (
    None
):
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url, pool_size=1, max_overflow=0)
        reconnected = False
        with store_migration._bounded_postgres_connect(  # pyright: ignore[reportPrivateUsage]
            engine, time.monotonic() + 5
        ) as connection:
            connection.commit()
            connection.invalidate()
            original_connect = engine.dialect.connect

            def unexpected_reconnect(*_args: Any, **_kwargs: Any) -> Any:
                nonlocal reconnected
                reconnected = True
                pytest.fail(
                    "invalidated maintenance cleanup re-entered dialect.connect"
                )

            engine.dialect.connect = unexpected_reconnect
            try:
                store_migration._cleanup_postgres_maintenance(  # pyright: ignore[reportPrivateUsage]
                    connection, True, time.monotonic() + 5
                )
            finally:
                engine.dialect.connect = original_connect
            assert connection.invalidated
        assert not reconnected
        assert cast(Any, engine.pool).checkedout() == 0
        engine.dispose()


@pytest.mark.requires_postgres
def test_postgres_read_pool_timeout_returns_retryable_service_responses(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(
            database_url,
            pool_size=1,
            max_overflow=0,
            pool_timeout_seconds=0.05,
        )
        identity, key = _key("MREADPOOLTIMEOUT")
        query = EvidenceQuery(identity, key)
        query_model = EvidenceQueryModel.from_domain(query)
        app = create_service_app(_settings(tmp_path), persistence=persistence)

        with TestClient(app) as client, persistence.engine.connect():
            responses = (
                client.post(
                    "/v1/evidence/lookup",
                    json={"queries": [query_model.model_dump(mode="json")]},
                ),
                client.post(
                    "/v1/evidence/fetch-many",
                    json={"keys": [query_model.key.model_dump(mode="json")]},
                ),
                client.get("/v1/artifacts/" + "a" * 64),
            )

    assert all(response.status_code == 503 for response in responses)
    assert all(response.headers["retry-after"] == "1" for response in responses)
    assert all(
        "pool is saturated" in response.json()["detail"] for response in responses
    )
