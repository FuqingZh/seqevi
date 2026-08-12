"""Logical Store protocol shared by local and future service clients."""

from __future__ import annotations

from collections.abc import Iterable
from threading import Event
from typing import Protocol, TypeGuard, runtime_checkable

from seqevi.evidence import (
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
    SessionClaimAcquireResult,
)


class EvidenceStore(Protocol):
    """Operations required by the annotation orchestrator."""

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]: ...

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]: ...

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, FetchedEvidence]: ...

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None: ...


class ClaimSession(Protocol):
    """Invocation-scoped claim authority with one heartbeat."""

    @property
    def cancellation_signal(self) -> Event: ...

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> tuple[SessionClaimAcquireResult, ...]: ...

    def finalize_many(
        self, proposed: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]: ...

    def raise_if_lost(self) -> None: ...

    def close(self) -> None: ...

    def __enter__(self) -> ClaimSession: ...

    def __exit__(self, *_error: object) -> None: ...


@runtime_checkable
class ClaimSessionCapableEvidenceStore(EvidenceStore, Protocol):
    """Store extension advertising the complete ClaimSession protocol."""

    @property
    def supports_claim_sessions(self) -> bool: ...

    def claim_session(self) -> ClaimSession: ...


def is_claim_session_capable_store(
    store: EvidenceStore,
) -> TypeGuard[ClaimSessionCapableEvidenceStore]:
    return (
        isinstance(store, ClaimSessionCapableEvidenceStore)
        and store.supports_claim_sessions
    )
