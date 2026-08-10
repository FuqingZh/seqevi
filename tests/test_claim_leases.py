from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
import time
from typing import cast

import pytest
from pydantic import ValidationError
from sqlalchemy import event, func, select, update

from seqevi.errors import EvidenceClaimLostError
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimAcquireResult,
    ClaimDisposition,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceQuery,
    EvidenceKey,
)
from seqevi.sequence import identify_protein_sequence
from seqevi.store import LocalStore
from seqevi.store import local as local_module
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

        class DistinctQuery:
            identity = query.identity
            key = query.key

        with pytest.raises(ValueError, match="duplicate"):
            store.acquire_many(
                (query, cast(EvidenceQuery, DistinctQuery())), owner_token="owner"
            )
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


def test_sqlite_full_1000_key_acquire_and_renew_uses_scalable_tail_refresh(
    tmp_path: Path,
) -> None:
    alphabet = "ACDEFGHIKLMNPQRSTVWY"

    def sequence(value: int) -> str:
        residues = []
        for _position in range(4):
            value, remainder = divmod(value, len(alphabet))
            residues.append(alphabet[remainder])
        return "M" + "".join(residues)

    identities = tuple(
        identify_protein_sequence(sequence(index)) for index in range(1_000)
    )
    queries = tuple(
        EvidenceQuery(
            identity,
            EvidenceKey.from_parameters(
                sequence_id=identity.sequence_id,
                adapter_contract_version="fixture/1",
                tool_runtime_digest="sha256:" + "a" * 64,
                resource_id="fixture/1",
                semantic_parameters={"threshold": 0.01},
            ),
        )
        for identity in identities
    )

    with LocalStore.open(tmp_path / "store") as store:
        decisions = store.acquire_many(queries, owner_token="owner")
        claims = tuple(
            decision.claim for decision in decisions if decision.claim is not None
        )
        renewed = store.renew_many(claims)

    assert len(claims) == 1_000
    assert len(renewed) == 1_000
    assert len({claim.expires_at for claim in renewed}) == 1


def test_sqlite_refreshes_acquire_expiry_after_later_item_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.store.local._CLAIM_LEASE_SECONDS", 0.05)
    commits = (
        make_hit_commit("MLEASEFIRST", artifact_dir=tmp_path / "first"),
        make_hit_commit("MLEASESECOND", artifact_dir=tmp_path / "second"),
    )
    queries = tuple(EvidenceQuery(commit.identity, commit.key) for commit in commits)
    with LocalStore.open(tmp_path / "store") as store:
        claim_selects = 0
        delay_finished: datetime | None = None

        def delay_second_claim_select(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            nonlocal claim_selects, delay_finished
            if "FROM evidence_claim" not in statement:
                return
            claim_selects += 1
            if claim_selects == 2:
                import time

                time.sleep(0.1)
                delay_finished = datetime.now(UTC)

        event.listen(store.engine, "before_cursor_execute", delay_second_claim_select)
        try:
            results = store.acquire_many(queries, owner_token="owner")
        finally:
            event.remove(
                store.engine, "before_cursor_execute", delay_second_claim_select
            )

    claims = tuple(result.claim for result in results)
    assert all(claim is not None for claim in claims)
    assert delay_finished is not None
    assert all(claim.expires_at > delay_finished for claim in claims if claim)


def test_sqlite_refreshes_renewal_expiry_after_later_item_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.store.local._CLAIM_LEASE_SECONDS", 5.0)
    commits = (
        make_hit_commit("MRENEWFIRST", artifact_dir=tmp_path / "first"),
        make_hit_commit("MRENEWSECOND", artifact_dir=tmp_path / "second"),
    )
    queries = tuple(EvidenceQuery(commit.identity, commit.key) for commit in commits)
    with LocalStore.open(tmp_path / "store") as store:
        acquired = store.acquire_many(queries, owner_token="owner")
        claims = tuple(result.claim for result in acquired)
        assert all(claim is not None for claim in claims)
        monkeypatch.setattr("seqevi.store.local._CLAIM_LEASE_SECONDS", 0.05)
        claim_updates = 0
        delay_finished: datetime | None = None

        def delay_second_claim_update(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            nonlocal claim_updates, delay_finished
            if not statement.lstrip().startswith("UPDATE evidence_claim"):
                return
            claim_updates += 1
            if claim_updates == 2:
                import time

                time.sleep(0.1)
                delay_finished = datetime.now(UTC)

        event.listen(store.engine, "before_cursor_execute", delay_second_claim_update)
        try:
            renewed = store.renew_many(
                tuple(claim for claim in claims if claim is not None)
            )
        finally:
            event.remove(
                store.engine, "before_cursor_execute", delay_second_claim_update
            )

    assert delay_finished is not None
    assert all(claim.expires_at > delay_finished for claim in renewed)


@pytest.mark.parametrize("operation", ["acquire", "renew"])
def test_sqlite_tail_refresh_uses_one_shared_deadline_after_update_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    monkeypatch.setattr("seqevi.store.local._CLAIM_LEASE_SECONDS", 0.2)
    commits = (
        make_hit_commit("MTAILFIRST", artifact_dir=tmp_path / "first"),
        make_hit_commit("MTAILSECOND", artifact_dir=tmp_path / "second"),
    )
    queries = tuple(EvidenceQuery(commit.identity, commit.key) for commit in commits)
    with LocalStore.open(tmp_path / "store") as store:
        initial = None
        if operation == "renew":
            initial = store.acquire_many(queries, owner_token="owner")
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

        event.listen(store.engine, "after_cursor_execute", delay_after_shared_refresh)
        try:
            if initial is None:
                decisions = store.acquire_many(queries, owner_token="owner")
                refreshed = tuple(
                    decision.claim
                    for decision in decisions
                    if decision.claim is not None
                )
            else:
                refreshed = store.renew_many(
                    tuple(
                        decision.claim
                        for decision in initial
                        if decision.claim is not None
                    )
                )
        finally:
            event.remove(
                store.engine, "after_cursor_execute", delay_after_shared_refresh
            )

    assert delay_finished is not None
    assert len({claim.expires_at for claim in refreshed}) == 1
    assert all(claim.expires_at > delay_finished for claim in refreshed)


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


def test_sqlite_exact_expiry_loses_all_authority_and_reacquires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commit, query = _query(tmp_path)
    decision_time = datetime.now(UTC) + timedelta(seconds=1)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return (
                decision_time if tz is not None else decision_time.replace(tzinfo=None)
            )

    with LocalStore.open(tmp_path / "store") as store:
        claim = store.acquire_many((query,), owner_token="owner")[0].claim
        assert claim is not None
        with store.engine.begin() as connection:
            connection.execute(update(evidence_claims).values(expires_at=decision_time))
        monkeypatch.setattr(local_module, "datetime", FrozenDateTime)
        with pytest.raises(EvidenceClaimLostError):
            store.renew_many((claim,))
        with pytest.raises(EvidenceClaimLostError):
            store.release_many((claim,))
        with pytest.raises(EvidenceClaimLostError):
            store.finalize_many((ClaimedEvidenceCommit(commit, claim),))
        reacquired = store.acquire_many((query,), owner_token="owner")[0].claim

    assert reacquired is not None
    assert reacquired.generation == claim.generation + 1


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
