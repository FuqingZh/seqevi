from __future__ import annotations

import os
import logging
import json
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

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
from sqlalchemy.exc import DBAPIError

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
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
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
)
from seqevi.store.transport import (
    ClaimSessionFinalizeItem,
    CommitModel,
    EvidenceRecordModel,
    EvidenceQueryModel,
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
            client=cast(httpx.Client, test_client),
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
            client=cast(httpx.Client, test_client),
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
            client=cast(httpx.Client, test_client),
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
            client=cast(httpx.Client, test_client),
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
            client=cast(httpx.Client, test_client),
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


def _claim_mock_client(handler: httpx.MockTransport) -> httpx.Client:
    return httpx.Client(transport=handler, base_url="http://testserver")


def _claim_health(maximum_batch_size: int = 1000) -> dict[str, object]:
    return {
        "status": "ok",
        "api_version": "v1",
        "maximum_batch_size": maximum_batch_size,
        "maximum_artifact_bytes": 1024,
    }


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
        "http://testserver", client=_claim_mock_client(httpx.MockTransport(handler))
    ) as store:
        session = store.claim_session()
        assert (
            session.acquire_many((query,))[0].disposition is ClaimDisposition.ACQUIRED
        )
        session.close()
    assert len(acquire_ids) == 2
    assert acquire_ids[0] != acquire_ids[1]


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
        "http://testserver", client=_claim_mock_client(httpx.MockTransport(handler))
    ) as store:
        with store.claim_session() as session:
            assert (
                session.acquire_many((query,))[0].disposition
                is ClaimDisposition.ACQUIRED
            )
    assert len(acquire_ids) == 2
    assert acquire_ids[0] == acquire_ids[1]


@pytest.mark.parametrize("first_outcome", ["transport", "503"])
def test_http_finalize_reconciles_apparent_success_after_unknown_outcome(
    tmp_path: Path,
    first_outcome: str,
) -> None:
    commit = _hit_commit(tmp_path / "source", "MUNKNOWNTHENSUCCESS")
    query = EvidenceQuery(commit.identity, commit.key)
    now = datetime.now(UTC)
    finalize_calls = 0
    lookup_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal finalize_calls, lookup_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "claim-session-v1",
                    "maximum_batch_size": 1000,
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
                if first_outcome == "transport":
                    raise httpx.ReadError("response lost")
                return httpx.Response(503, text="temporarily unavailable")
            return httpx.Response(200, json={"outcomes": ["created"]})
        if request.url.path.endswith("/authority"):
            return httpx.Response(200, json={"live": True})
        if request.url.path.endswith("/lookup"):
            lookup_calls += 1
            if lookup_calls == 1:
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
        "http://testserver", client=_claim_mock_client(httpx.MockTransport(handler))
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
        "http://testserver", client=_claim_mock_client(httpx.MockTransport(handler))
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
        "http://testserver", client=_claim_mock_client(httpx.MockTransport(handler))
    ) as store:
        session = store.claim_session()
        assert renewed.wait(1.0)
        session.close()


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
            client=cast(httpx.Client, test_client),
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
    return {
        table_name: tuple(
            tuple(row)
            for row in connection.execute(
                text(f"SELECT * FROM {table_name} ORDER BY 1")  # noqa: S608
            )
        )
        for table_name in ("sequence", "artifact", "evidence")
    }


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
def test_postgres_fenced_downgrade_recreates_empty_0003_coordination() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        persistence = PostgresEvidencePersistence.open(database_url)
        persistence.close()
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
                client=cast(httpx.Client, test_client),
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
            HttpEvidenceStore("http://testserver", client=service) as store,
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
            with HttpEvidenceStore("http://testserver", client=service) as store:
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
        assert persistence.claim_session_authority_is_live(authority, (acquired.claim,))
        persistence.close_claim_session(authority)
        assert not persistence.claim_session_authority_is_live(
            authority, (acquired.claim,)
        )
        persistence.close()


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
