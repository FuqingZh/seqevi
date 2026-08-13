from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from seqevi.evidence import ClaimDisposition, EvidenceQuery
from seqevi.store import LocalStore
from seqevi.store.schema import (
    claim_sessions,
    evidence_claim_generations,
    session_claims,
)
from seqevi.store.transport import EvidenceQueryModel, canonical_query_digest

from .test_local_store import make_hit_commit


def _query(tmp_path: Path, sequence: str = "MSESSION"):
    commit = make_hit_commit(sequence, artifact_dir=tmp_path / sequence)
    return commit, EvidenceQuery(commit.identity, commit.key)


def test_local_claim_session_acquire_finalize_and_o1_close(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        session = store.claim_session()
        acquired = session.acquire_many((query,))[0]
        assert acquired.disposition is ClaimDisposition.ACQUIRED
        assert session.finalize_many((commit,))
        session.close()
        with store.engine.connect() as connection:
            state = connection.execute(select(claim_sessions.c.state)).scalar_one()
            assert state == "closing"
            assert (
                connection.execute(
                    select(func.count()).select_from(session_claims)
                ).scalar_one()
                == 0
            )


def test_local_session_generation_high_water_survives_reclamation(
    tmp_path: Path,
) -> None:
    _commit, query = _query(tmp_path)
    root = tmp_path / "store"
    with LocalStore.open(root) as store:
        first = store.claim_session()
        first_claim = first.acquire_many((query,))[0].claim
        assert first_claim is not None
        first.close()
    with LocalStore.open(root) as store:
        second = store.claim_session()
        second_claim = second.acquire_many((query,))[0].claim
        assert second_claim is not None
        assert second_claim.generation > first_claim.generation
        second.close()
        with store.engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count()).select_from(evidence_claim_generations)
                ).scalar_one()
                == 1
            )


def test_local_sessions_have_one_winner_for_an_exact_key(tmp_path: Path) -> None:
    _commit, query = _query(tmp_path)
    root = tmp_path / "store"
    with LocalStore.open(root):
        pass
    acquired = threading.Barrier(3)
    release = threading.Event()

    def acquire(_index: int):
        with LocalStore.open(root) as store:
            with store.claim_session() as session:
                disposition = session.acquire_many((query,))[0].disposition
                acquired.wait(timeout=10)
                release.wait(timeout=10)
                return disposition

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(executor.submit(acquire, index) for index in range(2))
        acquired.wait(timeout=10)
        release.set()
        results = tuple(future.result() for future in futures)
    assert results.count(ClaimDisposition.ACQUIRED) == 1
    assert results.count(ClaimDisposition.BUSY) == 1


def test_query_digest_is_canonical_order_sensitive(tmp_path: Path) -> None:
    _first_commit, first = _query(tmp_path, "MSESSIONA")
    _second_commit, second = _query(tmp_path, "MSESSIONB")
    models = [
        EvidenceQueryModel.from_domain(first),
        EvidenceQueryModel.from_domain(second),
    ]
    assert canonical_query_digest(models) == canonical_query_digest(list(models))
    assert canonical_query_digest(models) != canonical_query_digest(
        list(reversed(models))
    )


def test_duplicate_session_acquire_is_rejected(tmp_path: Path) -> None:
    _commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        with store.claim_session() as session:
            with pytest.raises(ValueError, match="duplicate"):
                session.acquire_many((query, query))


def test_session_capability_models_are_strict() -> None:
    from seqevi.store.transport import ClaimSessionCapabilitiesResponse

    with pytest.raises(ValidationError):
        ClaimSessionCapabilitiesResponse.model_validate(
            {
                "protocol": "unknown",
                "maximum_batch_size": 1000,
                "retention_seconds": 60,
                "maximum_session_receipt_headers": 1000,
                "maximum_session_receipt_items": 32000,
                "server_time": "2026-08-12T00:00:00Z",
            }
        )
