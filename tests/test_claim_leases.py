from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, update

from seqevi.errors import EvidenceClaimLostError
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimAcquireResult,
    ClaimDisposition,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceQuery,
)
from seqevi.store import LocalStore
from seqevi.store.schema import evidence_claims
from seqevi.store.transport import (
    BusyEvidenceClaimModel,
    ClaimAcquireResultModel,
    ClaimedCommitModel,
    ClaimFinalizeRequest,
    ClaimMutationRequest,
    EvidenceClaimModel,
)

from .test_local_store import make_hit_commit


def _query(tmp_path: Path):
    commit = make_hit_commit("MLEASE", artifact_dir=tmp_path / "sources")
    return commit, EvidenceQuery(commit.identity, commit.key)


def _expire(store: LocalStore) -> None:
    with store.engine.begin() as connection:
        connection.execute(
            update(evidence_claims).values(
                expires_at=datetime.now(UTC) - timedelta(seconds=1)
            )
        )


def test_busy_result_has_no_mutation_credential(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    expiry = datetime.now(UTC) + timedelta(seconds=10)
    busy = BusyEvidenceClaim(commit.key, expiry, 1.0)
    result = ClaimAcquireResult(ClaimDisposition.BUSY, busy=busy)
    model = ClaimAcquireResultModel.from_domain(result)

    assert "owner_token" not in model.model_dump_json()
    assert "generation" not in model.model_dump_json()
    with pytest.raises(ValidationError):
        ClaimMutationRequest.model_validate(
            {
                "claims": [
                    BusyEvidenceClaimModel.from_domain(busy).model_dump(mode="json")
                ]
            }
        )
    with pytest.raises(TypeError):
        ClaimedEvidenceCommit(commit=commit, claim=busy)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="ClaimDisposition"):
        ClaimAcquireResult("busy", busy=busy)  # type: ignore[arg-type]


def test_owner_token_is_redacted_and_duplicate_acquire_is_rejected(
    tmp_path: Path,
) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        claim = store.acquire_many((query,), owner_token="sensitive-owner")[0].claim
        assert claim is not None
        assert "sensitive-owner" not in repr(claim)
        assert "sensitive-owner" not in repr(EvidenceClaimModel.from_domain(claim))
        with pytest.raises(ValueError, match="duplicate"):
            store.acquire_many((query, query), owner_token="owner")
        with pytest.raises(ValidationError, match="duplicate"):
            model = EvidenceClaimModel.from_domain(claim)
            ClaimMutationRequest(claims=[model, model])
        with pytest.raises(ValidationError, match="duplicate"):
            proposed = ClaimedCommitModel.from_domain(
                ClaimedEvidenceCommit(commit, claim)
            )
            ClaimFinalizeRequest(commits=[proposed, proposed])
        with pytest.raises(ValueError, match="duplicate"):
            store.renew_many((claim, claim))
        with pytest.raises(ValueError, match="duplicate"):
            store.release_many((claim, claim))
        with pytest.raises(ValueError, match="duplicate"):
            store.finalize_many(
                (
                    ClaimedEvidenceCommit(commit, claim),
                    ClaimedEvidenceCommit(commit, claim),
                )
            )


def test_legacy_lookup_many_deduplicates_queries(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        store.commit_many((commit,))
        found = store.lookup_many((query, query))

    assert found == {query.key: found[query.key]}


def test_legacy_commit_retires_active_local_claim(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        claim = store.acquire_many((query,), owner_token="new-client")[0].claim
        assert claim is not None
        assert store.commit_many((commit,)) == (CommitOutcome.CREATED,)
        cached = store.acquire_many((query,), owner_token="peer")[0]
        with store.engine.connect() as connection:
            claim_rows = connection.execute(
                select(func.count()).select_from(evidence_claims)
            ).scalar_one()
        with pytest.raises(EvidenceClaimLostError):
            store.renew_many((claim,))

    assert cached.disposition is ClaimDisposition.CACHED
    assert claim_rows == 0


def test_sqlite_concurrent_acquire_has_one_winner(tmp_path: Path) -> None:
    _commit, query = _query(tmp_path)
    root = tmp_path / "store"
    with LocalStore.open(root):
        pass

    def acquire(owner: str) -> ClaimAcquireResult:
        with LocalStore.open(root) as store:
            return store.acquire_many((query,), owner_token=owner)[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(acquire, ("owner-a", "owner-b")))

    assert [result.disposition for result in results].count(
        ClaimDisposition.ACQUIRED
    ) == 1
    assert [result.disposition for result in results].count(ClaimDisposition.BUSY) == 1
    busy = next(result.busy for result in results if result.busy is not None)
    assert not hasattr(busy, "owner_token")
    assert not hasattr(busy, "generation")


def test_sqlite_renew_release_expiry_and_same_owner_takeover(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        acquired = store.acquire_many((query,), owner_token="owner")[0].claim
        assert acquired is not None
        renewed = store.renew_many((acquired,))[0]
        assert renewed.expires_at >= acquired.expires_at
        store.release_many((renewed,))
        reacquired = store.acquire_many((query,), owner_token="owner")[0].claim
        assert reacquired is not None
        assert reacquired.generation == renewed.generation + 1
        with pytest.raises(EvidenceClaimLostError):
            store.renew_many((renewed,))
        with pytest.raises(EvidenceClaimLostError):
            store.release_many((renewed,))
        with pytest.raises(EvidenceClaimLostError):
            store.finalize_many((ClaimedEvidenceCommit(commit, renewed),))
        _expire(store)
        takeover = store.acquire_many((query,), owner_token="owner")[0].claim
        assert takeover is not None
        assert takeover.generation == reacquired.generation + 1


@pytest.mark.parametrize("operation", ["renew", "release", "finalize"])
def test_sqlite_expired_claim_cannot_mutate(tmp_path: Path, operation: str) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        claim = store.acquire_many((query,), owner_token="owner")[0].claim
        assert claim is not None
        _expire(store)
        with pytest.raises(EvidenceClaimLostError):
            if operation == "renew":
                store.renew_many((claim,))
            elif operation == "release":
                store.release_many((claim,))
            else:
                store.finalize_many((ClaimedEvidenceCommit(commit, claim),))


def test_sqlite_finalize_is_terminal_and_leaves_no_claim(tmp_path: Path) -> None:
    commit, query = _query(tmp_path)
    with LocalStore.open(tmp_path / "store") as store:
        claim = store.acquire_many((query,), owner_token="owner")[0].claim
        assert claim is not None
        store.finalize_many((ClaimedEvidenceCommit(commit, claim),))
        cached = store.acquire_many((query,), owner_token="peer")[0]
        with store.engine.connect() as connection:
            residual = connection.execute(
                select(func.count()).select_from(evidence_claims)
            ).scalar_one()

    assert cached.disposition is ClaimDisposition.CACHED
    assert residual == 0
