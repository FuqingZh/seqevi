from __future__ import annotations

import os
import json
import logging
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import (
    BigInteger,
    create_engine,
    event,
    func,
    inspect,
    make_url,
    select,
    text,
    update,
)
from sqlalchemy.engine import Connection

from seqevi.annotate import run_annotation
from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreBackpressureError,
    StoreError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ClaimAcquireResult,
    ClaimDisposition,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceClaim,
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
from seqevi.store import HttpEvidenceStore, LocalStore
from seqevi.store import client as client_module
from seqevi.store import migration as store_migration
from seqevi.store.schema import artifacts, evidence_claims
from seqevi.store.transport import (
    ClaimAcquireResultModel,
    ClaimAcquireRequest,
    ClaimedCommitModel,
    CommitModel,
    EvidenceClaimModel,
    EvidenceRecordModel,
    EvidenceQueryModel,
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
    def supports_claims(self) -> bool:
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

    def acquire_many(
        self, queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]:
        raise NotImplementedError

    def renew_many(self, claims: Iterable[EvidenceClaim]) -> tuple[EvidenceClaim, ...]:
        raise NotImplementedError

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None:
        raise NotImplementedError

    def finalize_many(
        self,
        commits: Iterable[ClaimedCommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]:
        raise NotImplementedError

    def close(self) -> None:
        self.closed = True


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
    app = create_service_app(_settings(tmp_path), persistence=persistence)
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
    app = create_service_app(_settings(tmp_path), persistence=persistence)
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
        _settings(tmp_path, maximum_batch_size=2), persistence=persistence
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
        persistence=persistence,
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
        persistence=persistence,
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
        persistence=MemoryPersistence(),
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
    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
    paths = set(app.openapi()["paths"])
    assert paths == {
        "/health",
        "/v1/artifacts/{digest}",
        "/v1/evidence/commit",
        "/v1/evidence/fetch",
        "/v1/evidence/fetch-many",
        "/v1/evidence/lookup",
        "/v1/evidence/claims/capabilities",
        "/v1/evidence/claims/acquire",
        "/v1/evidence/claims/renew",
        "/v1/evidence/claims/release",
        "/v1/evidence/claims/finalize",
    }


def test_service_claim_requests_emit_secret_free_timing_records(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    class ClaimPersistence(MemoryPersistence):
        def acquire_many(
            self,
            queries: Iterable[EvidenceQuery],
            *,
            owner_token: str,
        ) -> tuple[ClaimAcquireResult, ...]:
            return tuple(
                ClaimAcquireResult(
                    ClaimDisposition.ACQUIRED,
                    claim=EvidenceClaim(
                        query.key,
                        owner_token,
                        1,
                        datetime.now(UTC) + timedelta(seconds=60),
                        20.0,
                    ),
                )
                for query in queries
            )

    identity, key = _key("MCLAIMLOG")
    app = create_service_app(_settings(tmp_path), persistence=ClaimPersistence())
    caplog.set_level(logging.INFO, logger="seqevi.service.claims")

    with TestClient(app) as client:
        response = client.post(
            "/v1/evidence/claims/acquire",
            json={
                "owner_token": "secret-owner-token",
                "queries": [
                    EvidenceQueryModel.from_domain(
                        EvidenceQuery(identity, key)
                    ).model_dump(mode="json")
                ],
            },
        )

    assert response.status_code == 200
    records = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "seqevi.service.claims"
    ]
    assert len(records) == 1
    assert records[0]["event"] == "seqevi.claim_request"
    assert records[0]["operation"] == "acquire"
    assert records[0]["batch_size"] == 1
    assert records[0]["outcome"] == "ok"
    assert records[0]["status_code"] == 200
    assert records[0]["duration_ms"] >= 0
    assert records[0]["request_id"]
    assert "secret-owner-token" not in caplog.records[0].message


def test_service_claim_validation_errors_emit_timing_records(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
    caplog.set_level(logging.INFO, logger="seqevi.service.claims")

    with TestClient(app) as client:
        response = client.post(
            "/v1/evidence/claims/acquire",
            content="{not-json}",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 422
    records = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "seqevi.service.claims"
    ]
    assert len(records) == 1
    assert records[0]["operation"] == "acquire"
    assert records[0]["batch_size"] is None
    assert records[0]["outcome"] == "http_error"
    assert records[0]["status_code"] == 422
    assert records[0]["duration_ms"] >= 0


def test_service_claim_validation_preserves_batch_size(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    identity, key = _key("MCLAIMINVALIDOWNER")
    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
    caplog.set_level(logging.INFO, logger="seqevi.service.claims")

    with TestClient(app) as client:
        response = client.post(
            "/v1/evidence/claims/acquire",
            json={
                "owner_token": "x" * 256,
                "queries": [
                    EvidenceQueryModel.from_domain(
                        EvidenceQuery(identity, key)
                    ).model_dump(mode="json")
                ],
            },
        )

    assert response.status_code == 422
    records = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "seqevi.service.claims"
    ]
    assert len(records) == 1
    assert records[0]["operation"] == "acquire"
    assert records[0]["batch_size"] == 1
    assert records[0]["duration_ms"] >= 0


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


def test_service_claim_backpressure_returns_retryable_response(tmp_path: Path) -> None:
    class BusyPersistence(MemoryPersistence):
        def acquire_many(
            self,
            queries: Iterable[EvidenceQuery],
            *,
            owner_token: str,
        ) -> tuple[ClaimAcquireResult, ...]:
            raise StoreBackpressureError("database mutation is busy")

    identity, key = _key("MBACKPRESSURE")
    app = create_service_app(_settings(tmp_path), persistence=BusyPersistence())

    with TestClient(app) as client:
        response = client.post(
            "/v1/evidence/claims/acquire",
            json={
                "owner_token": "owner",
                "queries": [
                    EvidenceQueryModel.from_domain(
                        EvidenceQuery(identity, key)
                    ).model_dump(mode="json")
                ],
            },
        )

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.json()["detail"] == "database mutation is busy"


def test_old_health_shape_and_missing_claim_capability_fall_back(
    tmp_path: Path,
) -> None:
    class ReleasedHealthResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        status: Literal["ok"]
        api_version: Literal["v1"]
        maximum_batch_size: int
        maximum_artifact_bytes: int

    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
    commit = _hit_commit(tmp_path / "sources", "MLEGACYFALLBACK")
    query = EvidenceQuery(commit.identity, commit.key)
    with TestClient(app) as test_client:
        health = test_client.get("/health")
        parsed = ReleasedHealthResponse.model_validate(health.json())
        store = HttpEvidenceStore(
            "http://testserver", client=cast(httpx.Client, test_client)
        )
        assert store.commit_many((commit,)) == (CommitOutcome.CREATED,)
        found = store.lookup_many((query, query))

    assert health.json() == {
        "status": "ok",
        "api_version": "v1",
        "maximum_batch_size": 1000,
        "maximum_artifact_bytes": 512 * 1024 * 1024,
    }
    assert store.supports_claims is False
    assert parsed.status == "ok"
    assert found[commit.key].status is EvidenceStatus.HIT


@pytest.mark.parametrize("capability_status", [401, 500])
def test_claim_capability_auth_and_server_errors_do_not_fall_back(
    capability_status: int,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        return httpx.Response(capability_status, text="capability failed")

    client = _claim_mock_client(httpx.MockTransport(handler))
    with pytest.raises(StoreError):
        HttpEvidenceStore("http://testserver", client=client)


@pytest.mark.parametrize(
    "capabilities",
    [
        {
            "maximum_batch_size": 1000,
            "lease_seconds": 5.0,
            "renewal_after_seconds": 1.0,
        },
        {
            "maximum_batch_size": 1000,
            "lease_seconds": 10.0,
            "renewal_after_seconds": 6.0,
        },
    ],
)
def test_http_claim_capabilities_require_fixed_runway(
    capabilities: dict[str, object],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        return httpx.Response(200, json=capabilities)

    client = _claim_mock_client(httpx.MockTransport(handler))
    with pytest.raises(ValidationError, match="5-second runway"):
        HttpEvidenceStore("http://testserver", client=client)


def _claim_mock_client(handler: httpx.MockTransport) -> httpx.Client:
    return httpx.Client(transport=handler, base_url="http://testserver")


def _claim_health(maximum_batch_size: int = 1000) -> dict[str, object]:
    return {
        "status": "ok",
        "api_version": "v1",
        "maximum_batch_size": maximum_batch_size,
        "maximum_artifact_bytes": 1024,
    }


def _claim_capabilities() -> dict[str, object]:
    return {
        "maximum_batch_size": 1000,
        "lease_seconds": 60.0,
        "renewal_after_seconds": 20.0,
    }


def test_http_claim_acquire_rejects_another_owner_credentials() -> None:
    identity, key = _key("MOWNERBOUNDARY")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        wrong = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                key,
                "other-owner",
                1,
                datetime.now(UTC) + timedelta(seconds=60),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(wrong).model_dump(mode="json")
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(StoreIntegrityError, match="another owner"):
            store.acquire_many((EvidenceQuery(identity, key),), owner_token="owner")


def test_http_claim_acquire_rejects_duplicates_before_request() -> None:
    identity, key = _key("MDUPLICATECLAIM")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        raise AssertionError("duplicate acquisition must not reach the service")

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        query = EvidenceQuery(identity, key)
        with pytest.raises(ValueError, match="duplicate"):
            store.acquire_many(
                (query, _distinct_query_with_same_key(query)), owner_token="owner"
            )
        with pytest.raises(ValueError, match="owner_token"):
            store.acquire_many((), owner_token="")

    model = EvidenceQueryModel.from_domain(query)

    with pytest.raises(ValidationError, match="duplicate evidence key"):
        ClaimAcquireRequest(owner_token="owner", queries=[model, model.model_copy()])


def test_http_claim_mutations_reject_duplicates_before_network_or_upload(
    tmp_path: Path,
) -> None:
    commit = _hit_commit(tmp_path / "sources", "MDUPLICATEMUTATION")
    claim = EvidenceClaim(
        commit.key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        raise AssertionError("duplicate mutation must not reach the service")

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(ValueError, match="duplicate"):
            store.renew_many((claim, claim))
        with pytest.raises(ValueError, match="duplicate"):
            store.release_many((claim, claim))
        proposed = ClaimedEvidenceCommit(commit, claim)
        with pytest.raises(ValueError, match="duplicate"):
            store.finalize_many((proposed, proposed))


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [(409, EvidenceConflictError), (412, EvidenceClaimLostError)],
)
def test_http_claim_status_preserves_conflict_contract(
    status_code: int, error_type: type[StoreError]
) -> None:
    _identity, key = _key("MSTATUSMAPPING")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        return httpx.Response(status_code, text="claim mutation failed")

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(error_type):
            store.renew_many((claim,))


def test_http_claim_transport_error_identifies_method_and_path() -> None:
    _identity, key = _key("MCLAIMTIMEOUT")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        raise httpx.ReadTimeout("upstream did not respond", request=request)

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(
            StoreError,
            match=r"shared Store request failed during POST /v1/evidence/claims/renew",
        ):
            store.renew_many((claim,))


def test_http_claim_capability_probe_error_identifies_method_and_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        raise httpx.ReadTimeout("capability probe did not respond", request=request)

    with pytest.raises(
        StoreError,
        match=r"shared Store request failed during GET /v1/evidence/claims/capabilities",
    ):
        HttpEvidenceStore(
            "http://testserver",
            client=_claim_mock_client(httpx.MockTransport(handler)),
        )


def test_http_claim_retries_retryable_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _identity, key = _key("MCLAIMRETRY")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )
    attempts = 0
    sleeps: list[float] = []
    request_timeouts: list[float] = []
    clock = [100.0]

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        attempts += 1
        timeout = cast(dict[str, float], request.extensions["timeout"])
        request_timeouts.append(timeout["read"])
        if attempts == 1:
            return httpx.Response(503, headers={"Retry-After": "1"})
        renewed = EvidenceClaim(
            claim.key,
            claim.owner_token,
            claim.generation,
            datetime.now(UTC) + timedelta(seconds=60),
            claim.renewal_after_seconds,
        )
        return httpx.Response(
            200,
            json={
                "claims": [
                    EvidenceClaimModel.from_domain(renewed).model_dump(mode="json")
                ]
            },
        )

    def wait_for_retry(_store: HttpEvidenceStore, delay: float) -> bool:
        sleeps.append(delay)
        clock[0] += delay
        return False

    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        client_module.HttpEvidenceStore,
        "_wait_for_claim_retry",
        wait_for_retry,
    )
    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        renewed = store.renew_many((claim,))

    assert len(renewed) == 1
    assert attempts == 2
    assert len(sleeps) == 1
    assert 0.9 <= sleeps[0] <= 1.1
    assert request_timeouts[0] > 30.0
    assert request_timeouts[1] <= request_timeouts[0] - sleeps[0] / 4


def test_http_claim_scheduler_recomputes_timeout_after_queueing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "_MAX_CONCURRENT_CLAIM_REQUESTS", 1)
    first_started = threading.Event()
    release_first = threading.Event()
    observed_timeout: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        if request.url.path.endswith("/acquire"):
            first_started.set()
            assert release_first.wait(timeout=2)
        else:
            observed_timeout.append(
                cast(dict[str, float], request.extensions["timeout"])["read"]
            )
        return httpx.Response(200, json={})

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/acquire",
                timeout=2.0,
            )
            assert first_started.wait(timeout=2)
            second = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/renew",
                timeout=0.5,
            )
            time.sleep(0.2)
            release_first.set()
            assert first.result(timeout=2).status_code == 200
            assert second.result(timeout=2).status_code == 200

    assert len(observed_timeout) == 1
    assert 0.15 < observed_timeout[0] < 0.45


def test_http_claim_scheduler_releases_slot_at_request_deadline() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        if request.url.path.endswith("/acquire"):
            first_started.set()
            assert release_first.wait(timeout=3)
        else:
            second_started.set()
        return httpx.Response(200, json={})

    client = _claim_mock_client(httpx.MockTransport(handler))
    store = HttpEvidenceStore("http://testserver", client=client)
    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/acquire",
                timeout=0.1,
            )
            assert first_started.wait(timeout=2)
            second = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/renew",
                timeout=1.0,
            )
            assert second_started.wait(timeout=1)
            assert second.result(timeout=2).status_code == 200
            with pytest.raises(StoreError, match="remained unavailable"):
                first.result(timeout=2)
    finally:
        release_first.set()
        store.close()


def test_http_claim_scheduler_keeps_expired_transport_within_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "_MAX_CONCURRENT_CLAIM_REQUESTS", 1)
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        if request.url.path.endswith("/acquire"):
            first_started.set()
            assert release_first.wait(timeout=3)
        else:
            second_started.set()
        return httpx.Response(200, json={})

    store = HttpEvidenceStore(
        "http://testserver",
        client=_claim_mock_client(httpx.MockTransport(handler)),
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/acquire",
                timeout=0.1,
            )
            assert first_started.wait(timeout=2)
            second = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/renew",
                timeout=0.2,
            )
            with pytest.raises(EvidenceClaimLostError):
                second.result(timeout=2)
            assert not second_started.is_set()
            release_first.set()
            with pytest.raises(StoreError, match="remained unavailable"):
                first.result(timeout=2)
    finally:
        release_first.set()
        store.close()


def test_http_claim_renewal_enforces_one_wall_clock_deadline() -> None:
    _identity, key = _key("MCLAIMTOTALDEADLINE")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=5.25),
        20.0,
    )
    request_started = threading.Event()
    release_request = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        request_started.set()
        assert release_request.wait(timeout=3)
        return httpx.Response(503, headers={"Retry-After": "1"})

    store = HttpEvidenceStore(
        "http://testserver",
        client=_claim_mock_client(httpx.MockTransport(handler)),
    )
    started = time.monotonic()
    try:
        with pytest.raises(EvidenceClaimLostError, match="authority runway"):
            store.renew_many((claim,))
        elapsed = time.monotonic() - started
        assert request_started.is_set()
        assert elapsed < 1.0
    finally:
        release_request.set()
        store.close()


def test_http_claim_scheduler_prioritizes_renewal_and_caps_all_mutations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "_MAX_CONCURRENT_CLAIM_REQUESTS", 1)
    first_started = threading.Event()
    release_first = threading.Event()
    order: list[str] = []
    active = 0
    maximum_active = 0
    lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        with lock:
            active += 1
            maximum_active = max(maximum_active, active)
            order.append(request.url.path)
        try:
            if request.url.path.endswith("/acquire"):
                first_started.set()
                assert release_first.wait(timeout=3)
            return httpx.Response(200, json={})
        finally:
            with lock:
                active -= 1

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with ThreadPoolExecutor(max_workers=4) as callers:
            acquire = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/acquire",
            )
            assert first_started.wait(timeout=2)
            release = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/release",
            )
            finalize = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/finalize",
            )
            deadline = time.monotonic() + 2
            while store._claim_request_scheduler._queue.qsize() < 2:  # pyright: ignore[reportPrivateUsage]
                assert time.monotonic() < deadline
                time.sleep(0.001)
            renew = callers.submit(
                store._claim_request,  # pyright: ignore[reportPrivateUsage]
                "POST",
                "/v1/evidence/claims/renew",
            )
            deadline = time.monotonic() + 2
            while store._claim_request_scheduler._queue.qsize() < 3:  # pyright: ignore[reportPrivateUsage]
                assert time.monotonic() < deadline
                time.sleep(0.001)
            release_first.set()
            for future in (acquire, renew, release, finalize):
                assert future.result(timeout=3).status_code == 200

    assert maximum_active == 1
    assert order == [
        "/v1/evidence/claims/acquire",
        "/v1/evidence/claims/renew",
        "/v1/evidence/claims/release",
        "/v1/evidence/claims/finalize",
    ]


def test_http_claim_chunks_honor_client_and_capability_limits() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (_key(f"MCHUNK{letter}") for letter in "ABCDE")
    )
    observed_sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(2))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        observed_sizes.append(len(body["queries"]))
        results = []
        for query_data in body["queries"]:
            query = EvidenceQueryModel.model_validate(query_data).to_domain()
            result = ClaimAcquireResult(
                ClaimDisposition.ACQUIRED,
                claim=EvidenceClaim(
                    query.key,
                    body["owner_token"],
                    1,
                    datetime.now(UTC) + timedelta(seconds=60),
                    20.0,
                ),
            )
            results.append(
                ClaimAcquireResultModel.from_domain(result).model_dump(mode="json")
            )
        return httpx.Response(200, json={"results": results})

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        results = store.acquire_many(queries, owner_token="owner")

    assert len(results) == 5
    assert observed_sizes == [2, 2, 1]


def test_http_claim_chunks_apply_internal_contention_cap() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (
            _key("MCONTENTION" + "A" * index) for index in range(1, 252)
        )
    )
    observed_sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        observed_sizes.append(len(body["queries"]))
        results = []
        for query_data in body["queries"]:
            query = EvidenceQueryModel.model_validate(query_data).to_domain()
            acquired = ClaimAcquireResult(
                ClaimDisposition.ACQUIRED,
                claim=EvidenceClaim(
                    query.key,
                    body["owner_token"],
                    1,
                    datetime.now(UTC) + timedelta(seconds=60),
                    20.0,
                ),
            )
            results.append(
                ClaimAcquireResultModel.from_domain(acquired).model_dump(mode="json")
            )
        return httpx.Response(200, json={"results": results})

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        results = store.acquire_many(queries, owner_token="owner")

    assert len(results) == len(queries)
    assert observed_sizes == [250, 1]


def test_http_claim_chunks_renew_early_authority_while_acquisition_blocks() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (_key(f"MALIVE{letter}") for letter in "AB")
    )
    renew_observed = threading.Event()
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 20.0,
                },
            )
        body = json.loads(request.content)
        if request.url.path.endswith("/renew"):
            renewed = []
            for data in body["claims"]:
                claim = EvidenceClaimModel.model_validate(data).to_domain()
                renewed.append(
                    EvidenceClaimModel.from_domain(
                        EvidenceClaim(
                            claim.key,
                            claim.owner_token,
                            claim.generation,
                            datetime.now(UTC) + timedelta(seconds=60),
                            20.0,
                        )
                    ).model_dump(mode="json")
                )
            renew_observed.set()
            return httpx.Response(200, json={"claims": renewed})
        acquire_calls += 1
        if acquire_calls == 2:
            assert renew_observed.wait(timeout=1)
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=0.05),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        results = store.acquire_many(queries, owner_token="owner")

    assert renew_observed.is_set()
    assert len(results) == 2


def test_http_single_final_chunk_flushes_near_expiry_without_daemon_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity, key = _key("MFINALFLUSH")
    renew_observed = threading.Event()
    monkeypatch.setattr(
        client_module._ChunkAcquireRenewer,  # pyright: ignore[reportPrivateUsage]
        "_run",
        lambda self: self._stopped.wait(),  # pyright: ignore[reportPrivateUsage]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/renew"):
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            refreshed = EvidenceClaim(
                claim.key,
                claim.owner_token,
                claim.generation,
                datetime.now(UTC) + timedelta(seconds=60),
                20.0,
            )
            renew_observed.set()
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(refreshed).model_dump(
                            mode="json"
                        )
                    ]
                },
            )
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=0.05),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        result = store.acquire_many(
            (EvidenceQuery(identity, key),), owner_token="owner"
        )[0]

    assert renew_observed.is_set()
    assert result.claim is not None
    assert result.claim.expires_at > datetime.now(UTC) + timedelta(seconds=30)


def test_http_final_flush_reconciles_expired_terminal_and_renews_live_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminal_identity, terminal_key = _key("MFLUSHTERMINAL")
    live_identity, live_key = _key("MFLUSHLIVE")
    terminal = EvidenceRecord(
        terminal_key,
        EvidenceStatus.NO_HIT,
        sha256_digest(b"flush-terminal"),
        None,
        None,
        datetime.now(UTC),
    )
    renewed_keys: list[EvidenceKey] = []
    monkeypatch.setattr(
        client_module._ChunkAcquireRenewer,  # pyright: ignore[reportPrivateUsage]
        "_run",
        lambda self: self._stopped.wait(),  # pyright: ignore[reportPrivateUsage]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/lookup"):
            requested = tuple(
                EvidenceQueryModel.model_validate(data).to_domain()
                for data in body["queries"]
            )
            assert tuple(query.key for query in requested) == (terminal_key,)
            return httpx.Response(
                200,
                json={
                    "records": [
                        EvidenceRecordModel.from_domain(terminal).model_dump(
                            mode="json"
                        )
                    ]
                },
            )
        if request.url.path.endswith("/renew"):
            claims = tuple(
                EvidenceClaimModel.model_validate(data).to_domain()
                for data in body["claims"]
            )
            renewed_keys.extend(claim.key for claim in claims)
            refreshed = tuple(
                EvidenceClaim(
                    claim.key,
                    claim.owner_token,
                    claim.generation,
                    datetime.now(UTC) + timedelta(seconds=60),
                    20.0,
                )
                for claim in claims
            )
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(claim).model_dump(mode="json")
                        for claim in refreshed
                    ]
                },
            )
        queries = tuple(
            EvidenceQueryModel.model_validate(data).to_domain()
            for data in body["queries"]
        )
        expires = (
            datetime.now(UTC) - timedelta(milliseconds=1),
            datetime.now(UTC) + timedelta(seconds=1),
        )
        results = tuple(
            ClaimAcquireResult(
                ClaimDisposition.ACQUIRED,
                claim=EvidenceClaim(
                    query.key,
                    body["owner_token"],
                    1,
                    expiry,
                    20.0,
                ),
            )
            for query, expiry in zip(queries, expires, strict=True)
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(result).model_dump(mode="json")
                    for result in results
                ]
            },
        )

    queries = (
        EvidenceQuery(terminal_identity, terminal_key),
        EvidenceQuery(live_identity, live_key),
    )
    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        results = store.acquire_many(queries, owner_token="owner")

    assert results[0].disposition is ClaimDisposition.CACHED
    assert results[0].record == terminal
    assert results[1].disposition is ClaimDisposition.ACQUIRED
    assert results[1].claim is not None
    assert results[1].claim.expires_at > datetime.now(UTC) + timedelta(seconds=30)
    assert renewed_keys == [live_key]


def test_http_claim_chunk_additions_cannot_postpone_due_renewal() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (_key("MSTARVE" + "A" * index) for index in range(1, 13))
    )
    acquire_calls = 0
    renew_at_acquire_count: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 0.01,
                },
            )
        body = json.loads(request.content)
        if request.url.path.endswith("/renew"):
            renew_at_acquire_count.append(acquire_calls)
            renewed = []
            for data in body["claims"]:
                claim = EvidenceClaimModel.model_validate(data).to_domain()
                renewed.append(
                    EvidenceClaimModel.from_domain(
                        EvidenceClaim(
                            claim.key,
                            claim.owner_token,
                            claim.generation,
                            datetime.now(UTC) + timedelta(seconds=60),
                            0.01,
                        )
                    ).model_dump(mode="json")
                )
            return httpx.Response(200, json={"claims": renewed})
        acquire_calls += 1
        time.sleep(0.003)
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=1),
                0.01,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        assert len(store.acquire_many(queries, owner_token="owner")) == len(queries)

    assert renew_at_acquire_count
    assert min(renew_at_acquire_count) < len(queries)


def test_http_renew_chunks_execute_concurrently_and_preserve_order() -> None:
    claims = tuple(
        EvidenceClaim(
            key,
            "owner",
            1,
            datetime.now(UTC) + timedelta(seconds=60),
            20.0,
        )
        for _identity, key in (
            _key(sequence) for sequence in ("MPARALLELA", "MPARALLELB")
        )
    )
    barrier = threading.Barrier(2)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        barrier.wait(timeout=1)
        return httpx.Response(200, json={"claims": body["claims"]})

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        renewed = store.renew_many(claims)

    assert tuple(claim.key for claim in renewed) == tuple(claim.key for claim in claims)


def test_http_renew_bounded_scheduler_drains_fast_slots_before_blocked_chunk() -> None:
    claims = tuple(
        EvidenceClaim(
            key,
            "owner",
            1,
            datetime.now(UTC) + timedelta(seconds=60),
            20.0,
        )
        for _identity, key in (
            _key("MQUEUE" + "A" * (index + 1)) for index in range(33)
        )
    )
    blocked_key = claims[0].key
    final_key = claims[-1].key
    final_started = threading.Event()
    active = 0
    maximum_active = 0
    active_lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 20.0,
                },
            )
        body = json.loads(request.content)
        returned = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
        nonlocal active, maximum_active
        with active_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            if returned.key == final_key:
                final_started.set()
            if returned.key == blocked_key:
                time.sleep(0.02)
            renewed = EvidenceClaim(
                returned.key,
                returned.owner_token,
                returned.generation,
                datetime.now(UTC) + timedelta(seconds=60),
                returned.renewal_after_seconds,
            )
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(renewed).model_dump(mode="json")
                    ]
                },
            )
        finally:
            with active_lock:
                active -= 1

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        renewed = store.renew_many(claims)

    assert tuple(claim.key for claim in renewed) == tuple(claim.key for claim in claims)
    assert final_started.is_set()
    assert maximum_active <= client_module._MAX_CONCURRENT_CLAIM_REQUESTS


def test_http_renew_shared_executor_bounds_concurrent_logical_calls() -> None:
    claim_groups = tuple(
        tuple(
            EvidenceClaim(
                key,
                "owner",
                1,
                datetime.now(UTC) + timedelta(seconds=60),
                20.0,
            )
            for _identity, key in (
                _key("MSHARED" + letter + "A" * (index + 1)) for index in range(20)
            )
        )
        for letter in "ABCD"
    )
    active = 0
    maximum_active = 0
    active_lock = threading.Lock()
    callers_ready = threading.Barrier(len(claim_groups))

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 20.0,
                },
            )
        body = json.loads(request.content)
        returned = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
        with active_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.02)
            renewed = EvidenceClaim(
                returned.key,
                returned.owner_token,
                returned.generation,
                datetime.now(UTC) + timedelta(seconds=60),
                returned.renewal_after_seconds,
            )
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(renewed).model_dump(mode="json")
                    ]
                },
            )
        finally:
            with active_lock:
                active -= 1

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:

        def renew_group(group):
            callers_ready.wait(timeout=2)
            return store.renew_many(group)

        with ThreadPoolExecutor(max_workers=len(claim_groups)) as callers:
            results = tuple(callers.map(renew_group, claim_groups))

    assert maximum_active <= client_module._MAX_CONCURRENT_CLAIM_REQUESTS
    assert maximum_active > 1
    assert tuple(tuple(claim.key for claim in group) for group in results) == tuple(
        tuple(claim.key for claim in group) for group in claim_groups
    )


def test_http_store_close_cancels_pending_renewal_then_drains_active_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = tuple(
        EvidenceClaim(
            key,
            "owner",
            1,
            datetime.now(UTC) + timedelta(seconds=60),
            20.0,
        )
        for _identity, key in (_key(sequence) for sequence in ("MCLOSEA", "MCLOSEB"))
    )
    monkeypatch.setattr(client_module, "_MAX_CONCURRENT_CLAIM_REQUESTS", 1)
    active_started = threading.Event()
    active_finished = threading.Event()
    release_active = threading.Event()
    request_count = 0
    request_lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 20.0,
                },
            )
        body = json.loads(request.content)
        returned = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
        with request_lock:
            request_count += 1
        active_started.set()
        assert release_active.wait(timeout=3)
        renewed = EvidenceClaim(
            returned.key,
            returned.owner_token,
            returned.generation,
            datetime.now(UTC) + timedelta(seconds=60),
            returned.renewal_after_seconds,
        )
        active_finished.set()
        return httpx.Response(
            200,
            json={
                "claims": [
                    EvidenceClaimModel.from_domain(renewed).model_dump(mode="json")
                ]
            },
        )

    owned_client = _claim_mock_client(httpx.MockTransport(handler))
    transport_closed_after_active: list[bool] = []
    original_close = owned_client.close

    def tracking_close() -> None:
        transport_closed_after_active.append(active_finished.is_set())
        original_close()

    monkeypatch.setattr(owned_client, "close", tracking_close)
    monkeypatch.setattr(client_module.httpx, "Client", lambda **_kwargs: owned_client)
    store = HttpEvidenceStore("http://testserver")
    renewal_errors: list[BaseException] = []

    def renew() -> None:
        try:
            store.renew_many(claims)
        except BaseException as error:
            renewal_errors.append(error)

    renewal_thread = threading.Thread(target=renew)
    renewal_thread.start()
    assert active_started.wait(timeout=2)
    close_thread = threading.Thread(target=store.close)
    close_thread.start()
    deadline = time.monotonic() + 2
    while not store._renewal_executor._shutdown:  # pyright: ignore[reportPrivateUsage]
        assert time.monotonic() < deadline
        time.sleep(0.001)
    assert close_thread.is_alive()
    assert not owned_client.is_closed

    release_active.set()
    renewal_thread.join(timeout=3)
    close_thread.join(timeout=3)

    assert not renewal_thread.is_alive()
    assert not close_thread.is_alive()
    assert request_count == 1
    assert len(renewal_errors) == 1
    assert isinstance(renewal_errors[0], StoreError)
    assert "closed during claim renewal" in str(renewal_errors[0])
    assert transport_closed_after_active == [True]
    assert owned_client.is_closed
    assert not any(
        thread.name.startswith("seqevi-claim-renewal") and thread.is_alive()
        for thread in threading.enumerate()
    )
    assert not any(  # pyright: ignore[reportPrivateUsage]
        thread.is_alive() for thread in store._claim_request_scheduler._threads
    )


def test_http_store_close_cancels_queued_scheduler_renewal_before_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "_MAX_CONCURRENT_CLAIM_REQUESTS", 1)
    active_started = threading.Event()
    release_active = threading.Event()
    renewal_started = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        if request.url.path.endswith("/acquire"):
            active_started.set()
            assert release_active.wait(timeout=3)
        else:
            renewal_started.set()
        return httpx.Response(200, json={})

    client = _claim_mock_client(httpx.MockTransport(handler))
    store = HttpEvidenceStore("http://testserver", client=client)
    active_errors: list[BaseException] = []

    def active_request() -> None:
        try:
            store._claim_request(  # pyright: ignore[reportPrivateUsage]
                "POST", "/v1/evidence/claims/acquire", timeout=5.0
            )
        except BaseException as error:
            active_errors.append(error)

    active_thread = threading.Thread(target=active_request)
    active_thread.start()
    assert active_started.wait(timeout=2)

    _identity, key = _key("MCLOSEQUEUED")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )
    renewal_errors: list[BaseException] = []

    def renewal_request() -> None:
        try:
            store.renew_many((claim,))
        except BaseException as error:
            renewal_errors.append(error)

    renewal_thread = threading.Thread(target=renewal_request)
    renewal_thread.start()
    deadline = time.monotonic() + 2
    while store._claim_request_scheduler._queue.qsize() < 1:  # pyright: ignore[reportPrivateUsage]
        assert time.monotonic() < deadline
        time.sleep(0.001)

    close_thread = threading.Thread(target=store.close)
    close_thread.start()
    renewal_thread.join(timeout=2)
    assert not renewal_thread.is_alive()
    assert not renewal_started.is_set()
    assert close_thread.is_alive()

    release_active.set()
    active_thread.join(timeout=3)
    close_thread.join(timeout=3)
    assert not active_thread.is_alive()
    assert not close_thread.is_alive()
    assert not active_errors
    assert len(renewal_errors) == 1


def test_http_store_close_interrupts_claim_retry_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_waiting = threading.Event()
    release_retry = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health())
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        return httpx.Response(503, headers={"Retry-After": "5"})

    original_wait = client_module.HttpEvidenceStore._wait_for_claim_retry

    def observe_wait(store: HttpEvidenceStore, delay: float) -> bool:
        retry_waiting.set()
        result = original_wait(store, delay)
        release_retry.set()
        return result

    monkeypatch.setattr(
        client_module.HttpEvidenceStore,
        "_wait_for_claim_retry",
        observe_wait,
    )
    _identity, key = _key("MCLOSERETRY")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )
    store = HttpEvidenceStore(
        "http://testserver",
        client=_claim_mock_client(httpx.MockTransport(handler)),
    )
    errors: list[BaseException] = []

    def renew() -> None:
        try:
            store.renew_many((claim,))
        except BaseException as error:
            errors.append(error)

    renewal_thread = threading.Thread(target=renew)
    renewal_thread.start()
    assert retry_waiting.wait(timeout=2)
    store.close()
    renewal_thread.join(timeout=2)

    assert release_retry.is_set()
    assert not renewal_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], StoreError)
    assert "closed during claim request" in str(errors[0])


def test_http_renew_bounded_overload_fails_before_stale_chunk_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expiry = datetime.now(UTC) + timedelta(seconds=60)
    claims = tuple(
        EvidenceClaim(key, "owner", 1, expiry, 20.0)
        for _identity, key in (
            _key("MOVERLOAD" + "A" * (index + 1)) for index in range(33)
        )
    )
    first_wave = threading.Barrier(client_module._MAX_CONCURRENT_CLAIM_REQUESTS)
    active = 0
    maximum_active = 0
    request_count = 0
    active_lock = threading.Lock()
    original_budget = client_module._claim_request_budget  # pyright: ignore[reportPrivateUsage]

    def controlled_budget(chunk):
        if chunk[0].key == claims[-1].key:
            return -1.0
        return original_budget(chunk)

    monkeypatch.setattr(client_module, "_claim_request_budget", controlled_budget)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active, request_count
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 20.0,
                },
            )
        body = json.loads(request.content)
        returned = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
        with active_lock:
            request_count += 1
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            first_wave.wait(timeout=3)
            renewed = EvidenceClaim(
                returned.key,
                returned.owner_token,
                returned.generation,
                datetime.now(UTC) + timedelta(seconds=60),
                returned.renewal_after_seconds,
            )
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(renewed).model_dump(mode="json")
                    ]
                },
            )
        finally:
            with active_lock:
                active -= 1

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(EvidenceClaimLostError, match="could not start"):
            store.renew_many(claims)

    assert request_count == 32
    assert maximum_active == client_module._MAX_CONCURRENT_CLAIM_REQUESTS


def test_http_renew_many_fails_instead_of_returning_stale_handles() -> None:
    _identity, key = _key("MSTALERENEW")
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        stale = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
        returned = EvidenceClaim(
            stale.key,
            stale.owner_token,
            stale.generation,
            datetime.now(UTC) - timedelta(seconds=1),
            stale.renewal_after_seconds,
        )
        return httpx.Response(
            200,
            json={
                "claims": [
                    EvidenceClaimModel.from_domain(returned).model_dump(mode="json")
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(EvidenceClaimLostError, match="authority runway"):
            store.renew_many((claim,))


def test_http_chunk_acquire_reconciles_terminal_winner_during_renewal() -> None:
    first_identity, first_key = _key("MCHUNKTERMINAL")
    second_identity, second_key = _key("MCHUNKREMAINDER")
    queries = (
        EvidenceQuery(first_identity, first_key),
        EvidenceQuery(second_identity, second_key),
    )
    terminal = EvidenceRecord(
        first_key,
        EvidenceStatus.NO_HIT,
        sha256_digest(b"terminal"),
        None,
        None,
        datetime.now(UTC),
    )
    renewal_lost = threading.Event()
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 0.01,
                },
            )
        if request.url.path.endswith("/renew"):
            body = json.loads(request.content)
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            if claim.key == first_key:
                renewal_lost.set()
                return httpx.Response(412, text="terminal won")
            refreshed = EvidenceClaim(
                claim.key,
                claim.owner_token,
                claim.generation,
                datetime.now(UTC) + timedelta(seconds=60),
                20.0,
            )
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(refreshed).model_dump(
                            mode="json"
                        )
                    ]
                },
            )
        if request.url.path.endswith("/lookup"):
            return httpx.Response(
                200,
                json={
                    "records": [
                        EvidenceRecordModel.from_domain(terminal).model_dump(
                            mode="json"
                        )
                    ]
                },
            )
        body = json.loads(request.content)
        acquire_calls += 1
        if acquire_calls == 2:
            assert renewal_lost.wait(timeout=1)
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=1),
                0.01,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        results = store.acquire_many(queries, owner_token="owner")

    assert results[0].disposition is ClaimDisposition.CACHED
    assert results[0].record == terminal
    assert results[1].disposition is ClaimDisposition.ACQUIRED


def test_http_chunk_acquire_propagates_lookup_failure_and_releases_claim() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (
            _key(sequence) for sequence in ("MLOOKUPFAILA", "MLOOKUPFAILB")
        )
    )
    renewal_lost = threading.Event()
    released = threading.Event()
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 0.01,
                },
            )
        if request.url.path.endswith("/renew"):
            renewal_lost.set()
            return httpx.Response(412, text="claim lost")
        if request.url.path.endswith("/lookup"):
            return httpx.Response(500, text="metadata lookup failed")
        body = json.loads(request.content)
        if request.url.path.endswith("/release"):
            released.set()
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            return httpx.Response(
                200,
                json={
                    "released": [
                        {
                            "key": body["claims"][0]["key"],
                            "generation": claim.generation,
                        }
                    ]
                },
            )
        acquire_calls += 1
        if acquire_calls == 2:
            assert renewal_lost.wait(timeout=1)
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=1),
                0.01,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(StoreError, match="metadata lookup failed"):
            store.acquire_many(queries, owner_token="owner")

    assert released.is_set()


def test_http_acquire_failure_releases_earlier_chunks_without_masking_error() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (_key(sequence) for sequence in ("MCLEANUPA", "MCLEANUPB"))
    )
    released: list[tuple[EvidenceKey, int]] = []
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/lookup"):
            return httpx.Response(200, json={"records": []})
        if request.url.path.endswith("/release"):
            for data in body["claims"]:
                claim = EvidenceClaimModel.model_validate(data).to_domain()
                released.append((claim.key, claim.generation))
            return httpx.Response(500, text="cleanup failed")
        acquire_calls += 1
        if acquire_calls == 2:
            return httpx.Response(500, text="primary acquire failed")
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=60),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(StoreError, match="primary acquire failed"):
            store.acquire_many(queries, owner_token="owner")

    assert released == [(queries[0].key, 1)]


def test_http_background_renew_failure_releases_acquired_chunks() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (_key(sequence) for sequence in ("MRENEWA", "MRENEWB"))
    )
    renew_failed = threading.Event()
    released: list[EvidenceKey] = []
    acquire_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal acquire_calls
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "maximum_batch_size": 1,
                    "lease_seconds": 60.0,
                    "renewal_after_seconds": 0.01,
                },
            )
        body = json.loads(request.content)
        if request.url.path.endswith("/renew"):
            renew_failed.set()
            return httpx.Response(500, text="renew failed")
        if request.url.path.endswith("/release"):
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            released.append(claim.key)
            return httpx.Response(
                200,
                json={
                    "released": [
                        {
                            "key": body["claims"][0]["key"],
                            "generation": claim.generation,
                        }
                    ]
                },
            )
        acquire_calls += 1
        if acquire_calls == 2:
            assert renew_failed.wait(timeout=1)
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=1),
                0.01,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(StoreError, match="renew failed"):
            store.acquire_many(queries, owner_token="owner")

    assert released
    assert queries[0].key in released


def test_http_acquire_rejects_and_releases_expired_returned_authority() -> None:
    identity, key = _key("MEXPIREDHTTP")
    released = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/lookup"):
            return httpx.Response(200, json={"records": []})
        if request.url.path.endswith("/release"):
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            released.set()
            return httpx.Response(
                200,
                json={
                    "released": [
                        {
                            "key": body["claims"][0]["key"],
                            "generation": claim.generation,
                        }
                    ]
                },
            )
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        expired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) - timedelta(seconds=1),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(expired).model_dump(mode="json")
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(EvidenceClaimLostError, match="authority runway"):
            store.acquire_many((EvidenceQuery(identity, key),), owner_token="owner")

    assert not released.is_set()


def test_http_expired_handoff_releases_only_other_live_claims() -> None:
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in (
            _key(sequence) for sequence in ("MMIXEXPIRED", "MMIXLIVE")
        )
    )
    released_keys: list[EvidenceKey] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(2))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/lookup"):
            return httpx.Response(200, json={"records": []})
        if request.url.path.endswith("/release"):
            claims = tuple(
                EvidenceClaimModel.model_validate(data).to_domain()
                for data in body["claims"]
            )
            if any(claim.expires_at <= datetime.now(UTC) for claim in claims):
                return httpx.Response(412, text="expired credential")
            released_keys.extend(claim.key for claim in claims)
            return httpx.Response(
                200,
                json={
                    "released": [
                        {"key": data["key"], "generation": claim.generation}
                        for data, claim in zip(body["claims"], claims, strict=True)
                    ]
                },
            )
        results = []
        for index, data in enumerate(body["queries"]):
            query = EvidenceQueryModel.model_validate(data).to_domain()
            claim = EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=-1 if index == 0 else 60),
                20.0,
            )
            results.append(
                ClaimAcquireResultModel.from_domain(
                    ClaimAcquireResult(ClaimDisposition.ACQUIRED, claim=claim)
                ).model_dump(mode="json")
            )
        return httpx.Response(200, json={"results": results})

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(EvidenceClaimLostError) as raised:
            store.acquire_many(queries, owner_token="owner")

    assert "expired authority runway" in str(raised.value)
    assert released_keys == [queries[1].key]


def test_http_handoff_rejects_six_second_lease_after_two_second_transport() -> None:
    identity, key = _key("MTRANSPORTRUNWAY")
    released = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_claim_health(1))
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=_claim_capabilities())
        body = json.loads(request.content)
        if request.url.path.endswith("/lookup"):
            return httpx.Response(200, json={"records": []})
        if request.url.path.endswith("/release"):
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            released.set()
            return httpx.Response(
                200,
                json={
                    "released": [
                        {
                            "key": body["claims"][0]["key"],
                            "generation": claim.generation,
                        }
                    ]
                },
            )
        if request.url.path.endswith("/renew"):
            claim = EvidenceClaimModel.model_validate(body["claims"][0]).to_domain()
            delayed = EvidenceClaim(
                claim.key,
                claim.owner_token,
                claim.generation,
                datetime.now(UTC) + timedelta(seconds=6),
                20.0,
            )
            time.sleep(2)
            return httpx.Response(
                200,
                json={
                    "claims": [
                        EvidenceClaimModel.from_domain(delayed).model_dump(mode="json")
                    ]
                },
            )
        query = EvidenceQueryModel.model_validate(body["queries"][0]).to_domain()
        acquired = ClaimAcquireResult(
            ClaimDisposition.ACQUIRED,
            claim=EvidenceClaim(
                query.key,
                body["owner_token"],
                1,
                datetime.now(UTC) + timedelta(seconds=6),
                20.0,
            ),
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    ClaimAcquireResultModel.from_domain(acquired).model_dump(
                        mode="json"
                    )
                ]
            },
        )

    client = _claim_mock_client(httpx.MockTransport(handler))
    with HttpEvidenceStore("http://testserver", client=client) as store:
        with pytest.raises(EvidenceClaimLostError, match="authority runway"):
            store.acquire_many((EvidenceQuery(identity, key),), owner_token="owner")

    assert released.is_set()


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
    observed: list[int] = []

    class RecordingConnection:
        def execute(self, _statement: object, parameters: dict[str, int]) -> None:
            observed.append(parameters["lock_id"])

    persistence_module._lock_evidence_keys(  # pyright: ignore[reportPrivateUsage]
        cast(Connection, RecordingConnection()), reversed(keys)
    )

    assert any(lock_id < 0 for lock_id in expected)
    assert observed == expected


def test_postgres_advisory_locks_deduplicate_forced_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = tuple(_key(sequence)[1] for sequence in ("MLOCKA", "MLOCKB", "MLOCKC"))
    lock_ids = {keys[0]: 4, keys[1]: -9, keys[2]: 4}
    observed: list[int] = []

    class RecordingConnection:
        def execute(self, _statement: object, parameters: dict[str, int]) -> None:
            observed.append(parameters["lock_id"])

    monkeypatch.setattr(
        persistence_module, "_advisory_lock_id", lambda key: lock_ids[key]
    )
    persistence_module._lock_evidence_keys(  # pyright: ignore[reportPrivateUsage]
        cast(Connection, RecordingConnection()), reversed(keys)
    )

    assert observed == [-9, 4]


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
    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())

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

    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
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


def test_sqlite_0002_to_0003_preserves_every_legacy_field(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'migration.sqlite3'}")
    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(store_migration.__file__).with_name("migrations")),
    )
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0002_artifact_byte_size_bigint")
            _seed_0002_evidence(connection)
            connection.commit()
            before = _snapshot_legacy_rows(connection)
            command.upgrade(config, "0003_evidence_claim_leases")
            connection.commit()
            after = _snapshot_legacy_rows(connection)
        assert after == before
    finally:
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
            assert "evidence_claim" in tables
            assert rows_after == rows_before
        finally:
            scoped_engine.dispose()


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
def test_postgres_claim_lifecycle_expiry_stale_and_terminal_convergence(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MPOSTGRESLEASE")
        query = EvidenceQuery(commit.identity, commit.key)

        def acquire(owner: str) -> ClaimAcquireResult:
            persistence = PostgresEvidencePersistence.open(database_url)
            try:
                return persistence.acquire_many((query,), owner_token=owner)[0]
            finally:
                persistence.close()

        with ThreadPoolExecutor(max_workers=3) as executor:
            decisions = tuple(executor.map(acquire, ("owner-a", "owner-b", "owner-c")))
        assert [item.disposition for item in decisions].count(
            ClaimDisposition.ACQUIRED
        ) == 1
        assert [item.disposition for item in decisions].count(
            ClaimDisposition.BUSY
        ) == 2
        winner = next(item.claim for item in decisions if item.claim is not None)
        assert winner is not None

        persistence = PostgresEvidencePersistence.open(database_url)
        try:
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
            with pytest.raises(ValueError, match="duplicate"):
                persistence.renew_many((winner, winner))
            with pytest.raises(ValueError, match="duplicate"):
                persistence.release_many((winner, winner))
            duplicate_finalize = ClaimedCommitModel.from_domain(
                ClaimedEvidenceCommit(commit, winner)
            )
            with pytest.raises(ValueError, match="duplicate"):
                persistence.finalize_many(
                    (duplicate_finalize, duplicate_finalize), stored
                )
            renewed = persistence.renew_many((winner,))[0]
            with persistence.engine.begin() as connection:
                connection.execute(
                    update(evidence_claims).values(
                        expires_at=datetime.now(UTC) - timedelta(seconds=1)
                    )
                )
            with pytest.raises(StoreError):
                persistence.renew_many((renewed,))
            with pytest.raises(StoreError):
                persistence.release_many((renewed,))
            with pytest.raises(StoreError):
                persistence.finalize_many(
                    (
                        ClaimedCommitModel.from_domain(
                            ClaimedEvidenceCommit(commit, renewed)
                        ),
                    ),
                    stored,
                )

            takeover = persistence.acquire_many(
                (query,), owner_token=winner.owner_token
            )[0].claim
            assert takeover is not None
            assert takeover.generation == renewed.generation + 1
            persistence.release_many((takeover,))
            reacquired = persistence.acquire_many(
                (query,), owner_token=winner.owner_token
            )[0].claim
            assert reacquired is not None
            assert reacquired.generation == takeover.generation + 1
            with pytest.raises(StoreError):
                persistence.renew_many((takeover,))
            with pytest.raises(StoreError):
                persistence.release_many((takeover,))
            with pytest.raises(StoreError):
                persistence.finalize_many(
                    (
                        ClaimedCommitModel.from_domain(
                            ClaimedEvidenceCommit(commit, takeover)
                        ),
                    ),
                    stored,
                )
            assert persistence.finalize_many(
                (
                    ClaimedCommitModel.from_domain(
                        ClaimedEvidenceCommit(commit, reacquired)
                    ),
                ),
                stored,
            ) == (CommitOutcome.CREATED,)
            cached = persistence.acquire_many((query,), owner_token="peer")[0]
            with persistence.engine.connect() as connection:
                claim_rows = connection.execute(
                    select(func.count()).select_from(evidence_claims)
                ).scalar_one()
        finally:
            persistence.close()

    assert cached.disposition is ClaimDisposition.CACHED
    assert claim_rows == 0


@pytest.mark.requires_postgres
def test_postgres_exact_expiry_loses_all_authority_and_reacquires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MEXACTEXPIRY")
        query = EvidenceQuery(commit.identity, commit.key)
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            claim = persistence.acquire_many((query,), owner_token="owner")[0].claim
            assert claim is not None
            model = CommitModel.from_domain(commit)
            references = (model.normalized_artifact, model.raw_artifact)
            stored = {
                reference.digest: StoredArtifact(
                    digest=reference.digest,
                    media_type=reference.media_type,
                    byte_size=reference.byte_size,
                    relative_path=f"fixture/{reference.digest}",
                )
                for reference in references
                if reference is not None
            }
            decision_time = datetime.now(UTC) + timedelta(seconds=1)
            with persistence.engine.begin() as connection:
                connection.execute(
                    update(evidence_claims).values(expires_at=decision_time)
                )

            class FrozenDateTime(datetime):
                @classmethod
                def now(cls, tz=None):
                    return (
                        decision_time
                        if tz is not None
                        else decision_time.replace(tzinfo=None)
                    )

            monkeypatch.setattr(persistence_module, "datetime", FrozenDateTime)
            with pytest.raises(EvidenceClaimLostError):
                persistence.renew_many((claim,))
            with pytest.raises(EvidenceClaimLostError):
                persistence.release_many((claim,))
            with pytest.raises(EvidenceClaimLostError):
                persistence.finalize_many(
                    (
                        ClaimedCommitModel.from_domain(
                            ClaimedEvidenceCommit(commit, claim)
                        ),
                    ),
                    stored,
                )
            reacquired = persistence.acquire_many((query,), owner_token="owner")[
                0
            ].claim
        finally:
            persistence.close()

    assert reacquired is not None
    assert reacquired.generation == claim.generation + 1


@pytest.mark.requires_postgres
def test_postgres_legacy_commit_retires_active_claim(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MLEGACYCLAIM")
        query = EvidenceQuery(commit.identity, commit.key)
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            with pytest.raises(ValueError, match="duplicate"):
                persistence.acquire_many(
                    (query, _distinct_query_with_same_key(query)),
                    owner_token="duplicate",
                )
            claim = persistence.acquire_many((query,), owner_token="new-client")[
                0
            ].claim
            assert claim is not None
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
            assert persistence.commit_many((model,), stored) == (CommitOutcome.CREATED,)
            cached = persistence.acquire_many((query,), owner_token="peer")[0]
            with persistence.engine.connect() as connection:
                claim_rows = connection.execute(
                    select(func.count()).select_from(evidence_claims)
                ).scalar_one()
            with pytest.raises(EvidenceClaimLostError):
                persistence.renew_many((claim,))
        finally:
            persistence.close()

    assert cached.disposition is ClaimDisposition.CACHED
    assert claim_rows == 0


@pytest.mark.requires_postgres
def test_postgres_legacy_commit_serializes_absent_claim_acquire(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MABSENTCLAIMRACE")
        query = EvidenceQuery(commit.identity, commit.key)
        model = CommitModel.from_domain(commit)
        references = (model.normalized_artifact, model.raw_artifact)
        stored = {
            reference.digest: StoredArtifact(
                digest=reference.digest,
                media_type=reference.media_type,
                byte_size=reference.byte_size,
                relative_path=f"fixture/{reference.digest}",
            )
            for reference in references
            if reference is not None
        }
        writer = PostgresEvidencePersistence.open(database_url)
        acquirer = PostgresEvidencePersistence.open(database_url)
        legacy_thread: list[int] = []
        legacy_has_key_lock = threading.Event()
        allow_legacy_commit = threading.Event()
        acquirer_reached_key_lock = threading.Event()

        def pause_after_key_lock(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if (
                legacy_thread
                and threading.get_ident() == legacy_thread[0]
                and statement.lstrip().startswith("DELETE FROM evidence_claim")
            ):
                legacy_has_key_lock.set()
                assert allow_legacy_commit.wait(timeout=10)

        event.listen(writer.engine, "before_cursor_execute", pause_after_key_lock)

        def observe_acquirer_key_lock(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if "pg_advisory_xact_lock" in statement:
                acquirer_reached_key_lock.set()

        event.listen(
            acquirer.engine, "before_cursor_execute", observe_acquirer_key_lock
        )
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:

                def publish_legacy():
                    legacy_thread.append(threading.get_ident())
                    return writer.commit_many((model,), stored)

                published = executor.submit(publish_legacy)
                assert legacy_has_key_lock.wait(timeout=10)
                acquired = executor.submit(
                    acquirer.acquire_many, (query,), owner_token="calculator"
                )
                assert acquirer_reached_key_lock.wait(timeout=10)
                assert not acquired.done()
                allow_legacy_commit.set()
                assert published.result(timeout=10) == (CommitOutcome.CREATED,)
                decision = acquired.result(timeout=10)[0]
        finally:
            allow_legacy_commit.set()
            event.remove(writer.engine, "before_cursor_execute", pause_after_key_lock)
            event.remove(
                acquirer.engine, "before_cursor_execute", observe_acquirer_key_lock
            )
            writer.close()
            acquirer.close()

    assert decision.disposition is ClaimDisposition.CACHED
    assert decision.claim is None


@pytest.mark.requires_postgres
def test_postgres_legacy_commit_and_finalize_share_lock_order(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commits = (
            _hit_commit(tmp_path / "sources-a", "MCOMMITFINALIZEORDERA"),
            _hit_commit(tmp_path / "sources-b", "MCOMMITFINALIZEORDERB"),
        )
        queries = tuple(
            EvidenceQuery(commit.identity, commit.key) for commit in commits
        )
        persistence = PostgresEvidencePersistence.open(database_url)
        decisions = persistence.acquire_many(queries, owner_token="calculator")
        claims = tuple(decision.claim for decision in decisions)
        assert all(claim is not None for claim in claims)
        models = tuple(CommitModel.from_domain(commit) for commit in commits)
        references = {
            reference.digest: reference
            for model in models
            for reference in (model.normalized_artifact, model.raw_artifact)
            if reference is not None
        }
        stored = {
            reference.digest: StoredArtifact(
                digest=reference.digest,
                media_type=reference.media_type,
                byte_size=reference.byte_size,
                relative_path=f"fixture/{reference.digest}",
            )
            for reference in references.values()
        }
        claimed = tuple(
            ClaimedCommitModel.from_domain(ClaimedEvidenceCommit(commit, claim))
            for commit, claim in zip(commits, claims, strict=True)
            if claim is not None
        )
        start = threading.Barrier(2)
        try:

            def legacy_commit():
                start.wait(timeout=10)
                return persistence.commit_many(models, stored)

            def claimed_finalize():
                start.wait(timeout=10)
                try:
                    return persistence.finalize_many(tuple(reversed(claimed)), stored)
                except EvidenceClaimLostError:
                    return "lost"

            with ThreadPoolExecutor(max_workers=2) as executor:
                legacy = executor.submit(legacy_commit)
                finalize = executor.submit(claimed_finalize)
                legacy_result = legacy.result(timeout=15)
                finalize_result = finalize.result(timeout=15)
        finally:
            cached = persistence.acquire_many(queries, owner_token="peer")
            persistence.close()

    assert legacy_result in {
        (CommitOutcome.CREATED, CommitOutcome.CREATED),
        (CommitOutcome.EXISTING, CommitOutcome.EXISTING),
    }
    assert finalize_result in {
        (CommitOutcome.CREATED, CommitOutcome.CREATED),
        "lost",
    }
    assert all(item.disposition is ClaimDisposition.CACHED for item in cached)


@pytest.mark.requires_postgres
def test_postgres_different_keys_share_sequence_and_artifact_write_order(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        claimed_commit = _hit_commit(tmp_path / "sources", "MSHAREDROWS")
        legacy_key = EvidenceKey.from_parameters(
            sequence_id=claimed_commit.identity.sequence_id,
            adapter_contract_version=claimed_commit.key.adapter_contract_version,
            tool_runtime_digest=claimed_commit.key.tool_runtime_digest,
            resource_id=claimed_commit.key.resource_id,
            semantic_parameters={"threshold": 0.02},
        )
        legacy_commit = EvidenceCommit(
            identity=claimed_commit.identity,
            key=legacy_key,
            status=claimed_commit.status,
            payload_digest=claimed_commit.payload_digest,
            normalized_artifact=claimed_commit.normalized_artifact,
            raw_artifact=claimed_commit.raw_artifact,
        )
        finalizer = PostgresEvidencePersistence.open(database_url)
        legacy = PostgresEvidencePersistence.open(database_url)
        query = EvidenceQuery(claimed_commit.identity, claimed_commit.key)
        claim = finalizer.acquire_many((query,), owner_token="calculator")[0].claim
        assert claim is not None
        claimed_model = ClaimedCommitModel.from_domain(
            ClaimedEvidenceCommit(claimed_commit, claim)
        )
        legacy_model = CommitModel.from_domain(legacy_commit)
        references = (legacy_model.normalized_artifact, legacy_model.raw_artifact)
        stored = {
            reference.digest: StoredArtifact(
                digest=reference.digest,
                media_type=reference.media_type,
                byte_size=reference.byte_size,
                relative_path=f"fixture/{reference.digest}",
            )
            for reference in references
            if reference is not None
        }
        finalizer_inserted_sequence = threading.Event()
        release_finalizer = threading.Event()
        legacy_attempted_sequence = threading.Event()
        operation_threads: dict[str, int] = {}
        shared_insert_order: dict[int, list[str]] = {}

        def record_shared_insert(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            table = next(
                (
                    name
                    for name in ("sequence", "artifact")
                    if statement.startswith(f"INSERT INTO {name}")
                ),
                None,
            )
            if table is None:
                return
            thread_id = threading.get_ident()
            shared_insert_order.setdefault(thread_id, []).append(table)
            if thread_id == operation_threads.get("legacy") and table == "sequence":
                legacy_attempted_sequence.set()

        def pause_finalizer_after_sequence(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if threading.get_ident() == operation_threads.get(
                "finalizer"
            ) and statement.startswith("INSERT INTO sequence"):
                finalizer_inserted_sequence.set()
                assert release_finalizer.wait(timeout=10)

        for engine in (finalizer.engine, legacy.engine):
            event.listen(engine, "before_cursor_execute", record_shared_insert)
        event.listen(
            finalizer.engine, "after_cursor_execute", pause_finalizer_after_sequence
        )
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:

                def finalize_claim():
                    operation_threads["finalizer"] = threading.get_ident()
                    return finalizer.finalize_many((claimed_model,), stored)

                def commit_legacy():
                    operation_threads["legacy"] = threading.get_ident()
                    return legacy.commit_many((legacy_model,), stored)

                finalized = executor.submit(finalize_claim)
                assert finalizer_inserted_sequence.wait(timeout=10)
                committed = executor.submit(commit_legacy)
                assert legacy_attempted_sequence.wait(timeout=10)
                assert not committed.done()
                release_finalizer.set()
                assert finalized.result(timeout=10) == (CommitOutcome.CREATED,)
                assert committed.result(timeout=10) == (CommitOutcome.CREATED,)
        finally:
            release_finalizer.set()
            for engine in (finalizer.engine, legacy.engine):
                event.remove(engine, "before_cursor_execute", record_shared_insert)
            event.remove(
                finalizer.engine,
                "after_cursor_execute",
                pause_finalizer_after_sequence,
            )
            finalizer.close()
            legacy.close()

    assert shared_insert_order[operation_threads["finalizer"]][:2] == [
        "sequence",
        "artifact",
    ]
    assert shared_insert_order[operation_threads["legacy"]][:2] == [
        "sequence",
        "artifact",
    ]


@pytest.mark.requires_postgres
def test_postgres_claim_batches_lock_in_canonical_order(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commits = (
            _hit_commit(tmp_path / "sources-a", "MLOCKORDERA"),
            _hit_commit(tmp_path / "sources-b", "MLOCKORDERB"),
        )
        queries = tuple(
            EvidenceQuery(commit.identity, commit.key) for commit in commits
        )

        first_persistence = PostgresEvidencePersistence.open(database_url)
        second_persistence = PostgresEvidencePersistence.open(database_url)
        start = threading.Barrier(2)

        def acquire(
            persistence: PostgresEvidencePersistence,
            order: tuple[EvidenceQuery, ...],
            owner: str,
        ):
            start.wait(timeout=10)
            return persistence.acquire_many(order, owner_token=owner)

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(acquire, first_persistence, queries, "owner-a")
                second = executor.submit(
                    acquire,
                    second_persistence,
                    tuple(reversed(queries)),
                    "owner-b",
                )
                decisions = (first.result(timeout=15), second.result(timeout=15))
        finally:
            first_persistence.close()
            second_persistence.close()

    assert sorted(
        sum(item.disposition is ClaimDisposition.ACQUIRED for item in result)
        for result in decisions
    ) == [0, 2]


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


@pytest.mark.requires_postgres
def test_postgres_transaction_timeout_returns_retryable_service_response(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        identity, key = _key("MTRANSACTIONTIMEOUT")
        query = EvidenceQuery(identity, key)
        primary = PostgresEvidencePersistence.open(
            database_url,
            lock_timeout_seconds=5.0,
            statement_timeout_seconds=5.0,
            transaction_timeout_seconds=0.05,
        )
        blocker = PostgresEvidencePersistence.open(database_url)
        blocked_claim = blocker.acquire_many((query,), owner_token="blocker")[0].claim
        assert blocked_claim is not None
        lock_connection = blocker.engine.connect()
        transaction = lock_connection.begin()
        lock_connection.execute(
            select(evidence_claims)
            .where(evidence_claims.c.sequence_id == key.sequence_id)
            .with_for_update()
        )
        app = create_service_app(_settings(tmp_path), persistence=primary)
        try:
            with TestClient(app) as client:
                response = client.post(
                    "/v1/evidence/claims/acquire",
                    json=ClaimAcquireRequest(
                        owner_token="calculator",
                        queries=[EvidenceQueryModel.from_domain(query)],
                    ).model_dump(mode="json"),
                )
        finally:
            if transaction.is_active:
                transaction.rollback()
            lock_connection.close()
            blocker.close()

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert "wait budget" in response.json()["detail"]


@pytest.mark.requires_postgres
def test_postgres_refreshes_acquire_expiry_after_later_row_lock_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.service.persistence.CLAIM_LEASE_SECONDS", 0.2)
    with _isolated_postgres_url() as database_url:
        commits = (
            _hit_commit(tmp_path / "sources-a", "MACQUIREWAITA"),
            _hit_commit(tmp_path / "sources-b", "MACQUIREWAITB"),
        )
        queries = tuple(
            sorted(
                (EvidenceQuery(commit.identity, commit.key) for commit in commits),
                key=lambda query: (
                    query.key.sequence_id,
                    query.key.adapter_contract_version,
                    query.key.tool_runtime_digest,
                    query.key.resource_id,
                    query.key.semantic_parameters_hash,
                ),
            )
        )
        primary = PostgresEvidencePersistence.open(database_url)
        blocker = PostgresEvidencePersistence.open(database_url)
        blocked_claim = blocker.acquire_many((queries[1],), owner_token="blocker")[
            0
        ].claim
        assert blocked_claim is not None
        lock_connection = blocker.engine.connect()
        transaction = lock_connection.begin()
        lock_connection.execute(
            select(evidence_claims)
            .where(evidence_claims.c.sequence_id == queries[1].key.sequence_id)
            .with_for_update()
        )
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    primary.acquire_many, queries, owner_token="calculator"
                )
                time.sleep(0.3)
                lock_released_at = datetime.now(UTC)
                transaction.commit()
                decisions = future.result(timeout=10)
        finally:
            if transaction.is_active:
                transaction.rollback()
            lock_connection.close()
            primary.close()
            blocker.close()

    claims = tuple(decision.claim for decision in decisions)
    assert all(claim is not None for claim in claims)
    assert all(claim.expires_at > lock_released_at for claim in claims if claim)


@pytest.mark.requires_postgres
def test_postgres_refreshes_renewal_expiry_after_later_row_lock_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.service.persistence.CLAIM_LEASE_SECONDS", 5.0)
    with _isolated_postgres_url() as database_url:
        commits = (
            _hit_commit(tmp_path / "sources-a", "MRENEWWAITA"),
            _hit_commit(tmp_path / "sources-b", "MRENEWWAITB"),
        )
        queries = tuple(
            sorted(
                (EvidenceQuery(commit.identity, commit.key) for commit in commits),
                key=lambda query: (
                    query.key.sequence_id,
                    query.key.adapter_contract_version,
                    query.key.tool_runtime_digest,
                    query.key.resource_id,
                    query.key.semantic_parameters_hash,
                ),
            )
        )
        primary = PostgresEvidencePersistence.open(database_url)
        locker = PostgresEvidencePersistence.open(database_url)
        decisions = primary.acquire_many(queries, owner_token="calculator")
        claims = tuple(decision.claim for decision in decisions)
        assert all(claim is not None for claim in claims)
        monkeypatch.setattr("seqevi.service.persistence.CLAIM_LEASE_SECONDS", 0.2)
        with primary.engine.begin() as connection:
            connection.execute(
                update(evidence_claims)
                .where(evidence_claims.c.sequence_id == queries[1].key.sequence_id)
                .values(expires_at=datetime.now(UTC) + timedelta(seconds=5))
            )
        lock_connection = locker.engine.connect()
        transaction = lock_connection.begin()
        lock_connection.execute(
            select(evidence_claims)
            .where(evidence_claims.c.sequence_id == queries[1].key.sequence_id)
            .with_for_update()
        )
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    primary.renew_many,
                    tuple(claim for claim in claims if claim is not None),
                )
                time.sleep(0.3)
                lock_released_at = datetime.now(UTC)
                transaction.commit()
                renewed = future.result(timeout=10)
        finally:
            if transaction.is_active:
                transaction.rollback()
            lock_connection.close()
            primary.close()
            locker.close()

    assert all(claim.expires_at > lock_released_at for claim in renewed)


@pytest.mark.requires_postgres
@pytest.mark.parametrize("operation", ["acquire", "renew"])
def test_postgres_tail_refresh_uses_one_shared_deadline_after_update_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    monkeypatch.setattr(persistence_module, "CLAIM_LEASE_SECONDS", 0.2)
    with _isolated_postgres_url() as database_url:
        commits = (
            _hit_commit(tmp_path / "first", "MPGTAILFIRST"),
            _hit_commit(tmp_path / "second", "MPGTAILSECOND"),
        )
        queries = tuple(
            EvidenceQuery(commit.identity, commit.key) for commit in commits
        )
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            initial = None
            if operation == "renew":
                initial = persistence.acquire_many(queries, owner_token="owner")
            delay_finished: datetime | None = None

            def delay_after_shared_refresh(
                _connection: object,
                _cursor: object,
                statement: str,
                _parameters: object,
                _context: object,
                _executemany: object,
            ) -> None:
                nonlocal delay_finished
                if (
                    delay_finished is None
                    and statement.lstrip().startswith("UPDATE evidence_claim")
                    and " IN " in statement
                ):
                    time.sleep(0.1)
                    delay_finished = datetime.now(UTC)

            event.listen(
                persistence.engine, "after_cursor_execute", delay_after_shared_refresh
            )
            try:
                if initial is None:
                    decisions = persistence.acquire_many(queries, owner_token="owner")
                    refreshed = tuple(
                        decision.claim
                        for decision in decisions
                        if decision.claim is not None
                    )
                else:
                    refreshed = persistence.renew_many(
                        tuple(
                            decision.claim
                            for decision in initial
                            if decision.claim is not None
                        )
                    )
            finally:
                event.remove(
                    persistence.engine,
                    "after_cursor_execute",
                    delay_after_shared_refresh,
                )
        finally:
            persistence.close()

    assert delay_finished is not None
    assert len({claim.expires_at for claim in refreshed}) == 1
    assert all(claim.expires_at > delay_finished for claim in refreshed)


@pytest.mark.requires_postgres
def test_postgres_finalize_cannot_beat_expiry_takeover(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MFINALIZERACE")
        query = EvidenceQuery(commit.identity, commit.key)
        persistence = PostgresEvidencePersistence.open(database_url)
        claim = persistence.acquire_many((query,), owner_token="old-owner")[0].claim
        assert claim is not None
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
        takeover_store = PostgresEvidencePersistence.open(database_url)
        with takeover_store.engine.begin() as connection:
            connection.execute(
                update(evidence_claims).values(
                    expires_at=datetime.now(UTC) - timedelta(seconds=1)
                )
            )
        takeover_has_key_lock = threading.Event()
        continue_takeover = threading.Event()

        def pause_takeover_after_key_lock(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            if (
                statement.startswith("SELECT evidence_claim")
                and "FOR UPDATE" in statement
            ):
                takeover_has_key_lock.set()
                assert continue_takeover.wait(10)

        event.listen(
            takeover_store.engine,
            "before_cursor_execute",
            pause_takeover_after_key_lock,
        )
        finalize_errors: list[BaseException] = []
        takeover_results: list[ClaimAcquireResult] = []

        def take_over() -> None:
            takeover_results.extend(
                takeover_store.acquire_many((query,), owner_token="new-owner")
            )

        def finalize() -> None:
            try:
                persistence.finalize_many(
                    (
                        ClaimedCommitModel.from_domain(
                            ClaimedEvidenceCommit(commit, claim)
                        ),
                    ),
                    stored,
                )
            except BaseException as error:
                finalize_errors.append(error)

        takeover_thread = threading.Thread(target=take_over)
        finalize_thread = threading.Thread(target=finalize)
        try:
            takeover_thread.start()
            assert takeover_has_key_lock.wait(10)
            finalize_thread.start()
            assert finalize_thread.is_alive()
            continue_takeover.set()
            takeover_thread.join(10)
            finalize_thread.join(10)
        finally:
            continue_takeover.set()
            event.remove(
                takeover_store.engine,
                "before_cursor_execute",
                pause_takeover_after_key_lock,
            )
            takeover_store.close()
            persistence.close()

    assert not takeover_thread.is_alive()
    assert not finalize_thread.is_alive()
    assert len(finalize_errors) == 1
    assert isinstance(finalize_errors[0], StoreError)
    takeover = takeover_results[0].claim
    assert takeover is not None
    assert takeover.generation == claim.generation + 1
