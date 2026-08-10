from __future__ import annotations

import os
import json
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
from pydantic import BaseModel, ConfigDict
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

from seqevi.annotate import run_annotation
from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
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
from seqevi.service import ServiceSettings, create_service_app
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.store import HttpEvidenceStore, LocalStore
from seqevi.store import migration as store_migration
from seqevi.store.schema import artifacts, evidence_claims
from seqevi.store.transport import (
    ClaimAcquireResultModel,
    ClaimedCommitModel,
    CommitModel,
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
            store.acquire_many((query, query), owner_token="owner")


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
    assert isinstance(artifacts.c.byte_size.type, BigInteger)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        with pytest.raises(ValueError, match="requires PostgreSQL"):
            PostgresEvidencePersistence(engine)
    finally:
        engine.dispose()


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
def test_postgres_legacy_commit_retires_active_claim(tmp_path: Path) -> None:
    with _isolated_postgres_url() as database_url:
        commit = _hit_commit(tmp_path / "sources", "MLEGACYCLAIM")
        query = EvidenceQuery(commit.identity, commit.key)
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            with pytest.raises(ValueError, match="duplicate"):
                persistence.acquire_many((query, query), owner_token="duplicate")
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
        delete_barrier = threading.Barrier(2)
        seen_threads: set[int] = set()
        seen_lock = threading.Lock()

        def align_claim_deletes(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if not statement.lstrip().startswith("DELETE FROM evidence_claim"):
                return
            thread_id = threading.get_ident()
            with seen_lock:
                if thread_id in seen_threads:
                    return
                seen_threads.add(thread_id)
            delete_barrier.wait(timeout=10)

        event.listen(persistence.engine, "before_cursor_execute", align_claim_deletes)
        try:

            def legacy_commit():
                return persistence.commit_many(models, stored)

            def claimed_finalize():
                try:
                    return persistence.finalize_many(claimed, stored)
                except EvidenceClaimLostError:
                    return "lost"

            with ThreadPoolExecutor(max_workers=2) as executor:
                legacy = executor.submit(legacy_commit)
                finalize = executor.submit(claimed_finalize)
                legacy_result = legacy.result(timeout=15)
                finalize_result = finalize.result(timeout=15)
        finally:
            event.remove(
                persistence.engine, "before_cursor_execute", align_claim_deletes
            )
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
    assert all(claim.expires_at > datetime.now(UTC) for claim in claims if claim)


@pytest.mark.requires_postgres
def test_postgres_refreshes_renewal_expiry_after_later_row_lock_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.service.persistence.CLAIM_LEASE_SECONDS", 0.2)
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
                transaction.commit()
                renewed = future.result(timeout=10)
        finally:
            if transaction.is_active:
                transaction.rollback()
            lock_connection.close()
            primary.close()
            locker.close()

    assert all(claim.expires_at > datetime.now(UTC) for claim in renewed)


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
        reached_delete = threading.Event()
        continue_delete = threading.Event()

        def pause_before_claim_delete(
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
                reached_delete.set()
                assert continue_delete.wait(10)

        event.listen(
            persistence.engine, "before_cursor_execute", pause_before_claim_delete
        )
        finalize_errors: list[BaseException] = []

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

        thread = threading.Thread(target=finalize)
        thread.start()
        assert reached_delete.wait(10)
        takeover_store = PostgresEvidencePersistence.open(database_url)
        try:
            with takeover_store.engine.begin() as connection:
                connection.execute(
                    update(evidence_claims).values(
                        expires_at=datetime.now(UTC) - timedelta(seconds=1)
                    )
                )
            takeover = takeover_store.acquire_many((query,), owner_token="new-owner")[
                0
            ].claim
        finally:
            takeover_store.close()
        continue_delete.set()
        thread.join(10)
        event.remove(
            persistence.engine, "before_cursor_execute", pause_before_claim_delete
        )
        persistence.close()

    assert not thread.is_alive()
    assert len(finalize_errors) == 1
    assert isinstance(finalize_errors[0], StoreError)
    assert takeover is not None
    assert takeover.generation == claim.generation + 1
