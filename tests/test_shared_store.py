from __future__ import annotations

import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal
from sqlalchemy import create_engine

from seqevi.annotate import run_annotation
from seqevi.errors import EvidenceConflictError, StoreIntegrityError
from seqevi.evidence import (
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
from seqevi.service import ServiceSettings, create_service_app
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.store import HttpEvidenceStore, LocalStore
from seqevi.store.transport import CommitModel

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
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
        self.closed = False

    def lookup_many(
        self, queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
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
        found = store.lookup_many((EvidenceQuery(commit.identity, commit.key),))
        fetched = store.fetch(commit.key)

    assert found[commit.key].status is EvidenceStatus.HIT
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


def test_service_openapi_freezes_v1_operations(tmp_path: Path) -> None:
    app = create_service_app(_settings(tmp_path), persistence=MemoryPersistence())
    paths = set(app.openapi()["paths"])
    assert paths == {
        "/health",
        "/v1/artifacts/{digest}",
        "/v1/evidence/commit",
        "/v1/evidence/fetch",
        "/v1/evidence/fetch-many",
        "/v1/evidence/lookup",
    }


def test_shared_store_configuration_requires_postgres_and_postgres_engine(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="must use PostgreSQL"):
        ServiceSettings(
            database_url="sqlite:///store.sqlite3",
            artifacts_dir=tmp_path,
        )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        with pytest.raises(ValueError, match="requires PostgreSQL"):
            PostgresEvidencePersistence(engine)
    finally:
        engine.dispose()


def test_annotation_packages_are_equivalent_for_local_and_http_store(
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
    for name in ("evidence.parquet", "no-hits.parquet"):
        assert_frame_equal(
            pl.read_parquet(tmp_path / "local-output" / name),
            pl.read_parquet(tmp_path / "remote-output" / name),
        )
    assert (tmp_path / "local-output" / "sequence-map.tsv").read_text(
        encoding="utf-8"
    ) == (tmp_path / "remote-output" / "sequence-map.tsv").read_text(encoding="utf-8")


@pytest.mark.requires_postgres
def test_postgres_service_commit_lookup_fetch_contract(tmp_path: Path) -> None:
    database_url = os.environ.get("SEQEVI_TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("SEQEVI_TEST_POSTGRES_URL is not configured")
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
    database_url = os.environ.get("SEQEVI_TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("SEQEVI_TEST_POSTGRES_URL is not configured")
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
