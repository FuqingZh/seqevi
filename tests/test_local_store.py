from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from sqlalchemy import text

from seqevi.errors import (
    EvidenceConflictError,
    StoreConfigurationError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceStatus,
    StoredArtifact,
    sha256_digest,
)
from seqevi.sequence import SequenceIdentity, identify_protein_sequence
from seqevi.store import LocalStore, resolve_store_path

from .support import write_artifact_file


def make_key(
    identity: SequenceIdentity,
    *,
    resource_id: str = "fixture-db/1",
) -> EvidenceKey:
    return EvidenceKey.from_parameters(
        sequence_id=identity.sequence_id,
        adapter_contract_version="fixture/1",
        tool_runtime_digest="sha256:" + "a" * 64,
        resource_id=resource_id,
        semantic_parameters={"threshold": 0.01},
    )


def make_hit_commit(
    sequence: str,
    *,
    artifact_dir: Path,
    result: bytes | None = None,
    resource_id: str = "fixture-db/1",
) -> EvidenceCommit:
    identity = identify_protein_sequence(sequence)
    result_bytes = result or f"result:{identity.sequence_id}".encode()
    digest = sha256_digest(result_bytes)
    return EvidenceCommit(
        identity=identity,
        key=make_key(identity, resource_id=resource_id),
        status=EvidenceStatus.HIT,
        payload_digest=sha256_digest(result_bytes),
        normalized_artifact=write_artifact_file(
            artifact_dir / f"{digest}.parquet",
            result_bytes,
            "application/x-parquet",
        ),
        raw_artifact=write_artifact_file(
            artifact_dir / f"{digest}.raw.tsv",
            b"raw:" + result_bytes,
            "text/tab-separated-values",
        ),
    )


def test_store_path_is_explicit() -> None:
    with pytest.raises(StoreConfigurationError, match="required"):
        resolve_store_path(None, environ={})

    assert resolve_store_path(None, environ={"SEQEVI_STORE": "/tmp/example"}) == Path(
        "/tmp/example"
    )
    with pytest.raises(StoreConfigurationError, match="service URL"):
        resolve_store_path("https://seqevi.example.org")


def test_local_store_initializes_migrated_wal_database(tmp_path: Path) -> None:
    with LocalStore.open(tmp_path / "store") as store:
        with store.engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            journal_mode = connection.exec_driver_sql(
                "PRAGMA journal_mode"
            ).scalar_one()

    assert version == "0001_initial_store"
    assert str(journal_mode).lower() == "wal"
    assert (tmp_path / "store" / ".migration.lock").is_file()
    assert (tmp_path / "store" / "artifacts").is_dir()


def test_commit_lookup_and_fetch_hit_evidence(tmp_path: Path) -> None:
    commit = make_hit_commit("MPEPTIDE", artifact_dir=tmp_path / "sources")

    with LocalStore.open(tmp_path / "store") as store:
        assert store.commit_many((commit,)) == (CommitOutcome.CREATED,)
        assert store.commit_many((commit,)) == (CommitOutcome.EXISTING,)

        found = store.lookup_many((EvidenceQuery(commit.identity, commit.key),))
        fetched = store.fetch(commit.key)
        persisted_sequence = store.get_sequence(commit.identity.sequence_id)

    assert found[commit.key].status is EvidenceStatus.HIT
    assert fetched is not None
    assert commit.normalized_artifact is not None
    assert commit.raw_artifact is not None
    assert fetched.normalized_artifact is not None
    assert fetched.raw_artifact is not None
    assert fetched.normalized_artifact.path.read_bytes() == (
        commit.normalized_artifact.path.read_bytes()
    )
    assert fetched.raw_artifact.path.read_bytes() == (
        commit.raw_artifact.path.read_bytes()
    )
    assert persisted_sequence == commit.identity


def test_fetch_many_reads_each_shared_artifact_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = make_hit_commit(
        "MPEPTIDE", artifact_dir=tmp_path / "sources", result=b"shared-result"
    )
    second = make_hit_commit(
        "MSEQUENCE", artifact_dir=tmp_path / "sources", result=b"shared-result"
    )

    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((first, second))
        original_reference = store.artifact_store.reference
        read_digests: list[str] = []

        def tracked_reference(artifact: StoredArtifact) -> ArtifactFile:
            read_digests.append(artifact.digest)
            return original_reference(artifact)

        monkeypatch.setattr(store.artifact_store, "reference", tracked_reference)
        fetched = store.fetch_many((first.key, second.key))

    assert set(fetched) == {first.key, second.key}
    assert len(read_digests) == 2
    assert len(set(read_digests)) == 2


def test_no_hit_is_a_fetchable_terminal_result(tmp_path: Path) -> None:
    identity = identify_protein_sequence("MNOHIT")
    commit = EvidenceCommit(
        identity=identity,
        key=make_key(identity),
        status=EvidenceStatus.NO_HIT,
        payload_digest=sha256_digest(b"no-hit"),
        raw_artifact=write_artifact_file(
            tmp_path / "no-hit.txt", b"completed with no rows", "text/plain"
        ),
    )

    with LocalStore.open(tmp_path / "store") as store:
        assert store.commit_many((commit,)) == (CommitOutcome.CREATED,)
        fetched = store.fetch(commit.key)

    assert fetched is not None
    assert fetched.record.status is EvidenceStatus.NO_HIT
    assert fetched.normalized_artifact is None
    assert fetched.raw_artifact is not None
    assert fetched.raw_artifact.path.read_bytes() == b"completed with no rows"


def test_exact_resource_change_is_a_cache_miss(tmp_path: Path) -> None:
    old = make_hit_commit(
        "MPEPTIDE",
        artifact_dir=tmp_path / "sources",
        resource_id="fixture-db/1",
    )
    new_key = make_key(old.identity, resource_id="fixture-db/2")

    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((old,))
        found = store.lookup_many(
            (
                EvidenceQuery(old.identity, old.key),
                EvidenceQuery(old.identity, new_key),
            )
        )

    assert set(found) == {old.key}


def test_conflicting_batch_rolls_back_all_database_rows(tmp_path: Path) -> None:
    existing = make_hit_commit(
        "MEXISTING", artifact_dir=tmp_path / "sources", result=b"first"
    )
    new = make_hit_commit("MNEW", artifact_dir=tmp_path / "sources", result=b"new")
    conflict = make_hit_commit(
        "MEXISTING", artifact_dir=tmp_path / "sources", result=b"different"
    )

    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((existing,))
        with pytest.raises(EvidenceConflictError):
            store.commit_many((new, conflict))

        assert store.lookup_many((EvidenceQuery(new.identity, new.key),)) == {}
        assert store.get_sequence(new.identity.sequence_id) is None


def test_same_scientific_payload_accepts_different_parquet_encoding(
    tmp_path: Path,
) -> None:
    first = make_hit_commit(
        "MPEPTIDE", artifact_dir=tmp_path / "first", result=b"encoding-one"
    )
    second = make_hit_commit(
        "MPEPTIDE", artifact_dir=tmp_path / "second", result=b"encoding-two"
    )
    second = EvidenceCommit(
        identity=second.identity,
        key=second.key,
        status=second.status,
        payload_digest=first.payload_digest,
        normalized_artifact=second.normalized_artifact,
        raw_artifact=second.raw_artifact,
    )

    with LocalStore.open(tmp_path / "store") as store:
        assert store.commit_many((first,)) == (CommitOutcome.CREATED,)
        assert store.commit_many((second,)) == (CommitOutcome.EXISTING,)
        fetched = store.fetch(first.key)

    assert fetched is not None
    assert fetched.normalized_artifact is not None
    assert fetched.normalized_artifact.path.read_bytes() == b"encoding-one"


def test_fetch_detects_corrupt_registered_artifact(tmp_path: Path) -> None:
    commit = make_hit_commit("MPEPTIDE", artifact_dir=tmp_path / "sources")

    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((commit,))
        record = store.lookup_many((EvidenceQuery(commit.identity, commit.key),))[
            commit.key
        ]
        assert record.normalized_artifact_digest is not None
        artifact_path = (
            store.artifact_store.root
            / "sha256"
            / record.normalized_artifact_digest[:2]
            / record.normalized_artifact_digest[2:4]
            / record.normalized_artifact_digest
        )
        artifact_path.write_bytes(b"corrupt")

        with pytest.raises(StoreIntegrityError, match="artifact is corrupt"):
            store.fetch(commit.key)


def test_lookup_verifies_full_persisted_sequence_content(tmp_path: Path) -> None:
    commit = make_hit_commit("MPEPTIDE", artifact_dir=tmp_path / "sources")

    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((commit,))
        with store.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE sequence SET sequence = 'MPEPTIDF' "
                    "WHERE sequence_id = :sequence_id"
                ),
                {"sequence_id": commit.identity.sequence_id},
            )

        with pytest.raises(StoreIntegrityError, match="collision in Store lookup"):
            store.lookup_many((EvidenceQuery(commit.identity, commit.key),))


def test_concurrent_identical_commits_are_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "store"
    commit = make_hit_commit("MCONCURRENT", artifact_dir=tmp_path / "sources")

    def commit_once() -> CommitOutcome:
        with LocalStore.open(root) as store:
            return store.commit_many((commit,))[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: commit_once(), range(2)))

    assert sorted(outcomes) == [CommitOutcome.CREATED, CommitOutcome.EXISTING]
