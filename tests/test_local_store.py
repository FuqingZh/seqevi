from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
import threading
import time

import pytest
from sqlalchemy import func, select, text

from seqevi.errors import (
    EvidenceClaimLostError,
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
from seqevi.store.schema import claim_session_acquire_receipts, claim_sessions
from seqevi.store import local as local_module

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

    assert version == "0004_claim_sessions"
    assert str(journal_mode).lower() == "wal"
    assert (tmp_path / "store" / ".migration.lock").is_file()
    assert (tmp_path / "store" / "artifacts").is_dir()


def test_local_claim_session_does_not_retain_transport_receipts(tmp_path: Path) -> None:
    identity = identify_protein_sequence("MLOCALSESSION")
    query = EvidenceQuery(identity, make_key(identity))
    with LocalStore.open(tmp_path / "store") as store:
        with store.claim_session() as session:
            assert session.acquire_many((query,))[0].disposition.value == "acquired"
        with store.engine.connect() as connection:
            count = connection.execute(
                select(func.count()).select_from(claim_session_acquire_receipts)
            ).scalar_one()
    assert count == 0


def test_local_claim_finalize_rejects_conflicting_artifact_metadata(
    tmp_path: Path,
) -> None:
    first = make_hit_commit("MLOCALMETAONE", artifact_dir=tmp_path / "sources")
    second = make_hit_commit("MLOCALMETATWO", artifact_dir=tmp_path / "sources")
    assert first.normalized_artifact is not None
    assert second.normalized_artifact is not None
    conflicting = replace(
        second,
        normalized_artifact=replace(
            first.normalized_artifact, media_type="application/conflicting"
        ),
    )
    with LocalStore.open(tmp_path / "store") as store:
        with store.claim_session() as session:
            session.acquire_many(
                (
                    EvidenceQuery(first.identity, first.key),
                    EvidenceQuery(conflicting.identity, conflicting.key),
                )
            )
            with pytest.raises(StoreIntegrityError, match="artifact metadata conflict"):
                session.finalize_many((first, conflicting))


def test_local_claim_handles_survive_rolled_back_finalize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = make_hit_commit("MLOCALROLLBACKONE", artifact_dir=tmp_path / "sources")
    second = make_hit_commit("MLOCALROLLBACKTWO", artifact_dir=tmp_path / "sources")
    with LocalStore.open(tmp_path / "store") as store:
        with store.claim_session() as session:
            session.acquire_many(
                (
                    EvidenceQuery(first.identity, first.key),
                    EvidenceQuery(second.identity, second.key),
                )
            )
            original = store._insert_evidence
            calls = 0

            def fail_second(connection, commit):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise StoreIntegrityError("injected finalize failure")
                return original(connection, commit)

            monkeypatch.setattr(store, "_insert_evidence", fail_second)
            with pytest.raises(StoreIntegrityError, match="injected"):
                session.finalize_many((first, second))
            monkeypatch.setattr(store, "_insert_evidence", original)
            assert session.finalize_many((first, second)) == (
                CommitOutcome.CREATED,
                CommitOutcome.CREATED,
            )


def test_local_terminal_renewal_marks_session_lost_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(local_module, "_CLAIM_RENEWAL_SECONDS", 0.01)
    with LocalStore.open(tmp_path / "store") as store:
        session = store.claim_session()
        with store.engine.begin() as connection:
            connection.execute(claim_sessions.update().values(state="closing"))
        assert session.cancellation_signal.wait(1.0)
        with pytest.raises(EvidenceClaimLostError):
            session.raise_if_lost()
        session.close()


def test_local_close_is_bounded_when_heartbeat_waits_for_writer_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(local_module, "_CLAIM_RENEWAL_SECONDS", 0.01)
    with LocalStore.open(tmp_path / "store") as store:
        session = store.claim_session()
        blocker = store.engine.connect()
        blocker.exec_driver_sql("BEGIN IMMEDIATE")
        released = threading.Event()

        def release_writer() -> None:
            time.sleep(0.5)
            blocker.rollback()
            blocker.close()
            released.set()

        thread = threading.Thread(target=release_writer)
        thread.start()
        started = time.monotonic()
        session.close()
        elapsed = time.monotonic() - started
        thread.join()
    assert released.is_set()
    assert elapsed < 2.0


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


def test_sweeper_close_is_bounded_by_writer_contention_and_recovers_next_open(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    store = LocalStore.open(root)
    with store.engine.begin() as connection:
        connection.execute(
            claim_sessions.insert().values(
                session_id="stale-session",
                owner_token="owner",
                generation=1,
                state="closing",
                expires_at=datetime.now(UTC),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
    blocker = store.engine.connect()
    blocker.exec_driver_sql("BEGIN IMMEDIATE")
    store._sweeper_wake.set()  # pyright: ignore[reportPrivateUsage]
    time.sleep(0.05)
    started = time.monotonic()
    store.close()
    assert time.monotonic() - started < 2.5
    blocker.rollback()
    blocker.close()

    with LocalStore.open(root) as recovered:
        with recovered.engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count()).select_from(claim_sessions)
                ).scalar_one()
                == 0
            )
