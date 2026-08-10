"""Logical Store protocol shared by local and future service clients."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, TypeGuard, runtime_checkable

from seqevi.evidence import (
    ClaimAcquireResult,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceClaim,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
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


@runtime_checkable
class ClaimCapableEvidenceStore(EvidenceStore, Protocol):
    """Optional claim coordination implemented beside the legacy Store API."""

    @property
    def supports_claims(self) -> bool:
        """Return whether this instance can use atomic claim operations.

        Examples:
            A new client can retain legacy Store structural compatibility:

            >>> if is_claim_capable_store(store):
            ...     store.acquire_many(queries, owner_token="worker")
        """
        ...

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]:
        """Atomically return cached, acquired, or busy for each query.

        Examples:
            Results remain aligned with the unique request order:

            >>> results = store.acquire_many(queries, owner_token="worker")
            >>> len(results) == len(tuple(queries))
            True
        """
        ...

    def renew_many(self, claims: Iterable[EvidenceClaim]) -> tuple[EvidenceClaim, ...]:
        """Renew leases still owned by the exact owner and generation.

        Examples:
            A blocking calculator replaces its leases with renewed values:

            >>> claims = store.renew_many(claims)
        """
        ...

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None:
        """Release leases owned by the exact owner and generation.

        Examples:
            Normal adapter failure performs best-effort cleanup:

            >>> store.release_many(claims)
        """
        ...

    def finalize_many(
        self, proposed: Iterable[ClaimedEvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        """Atomically commit terminal evidence and retire matching claims.

        Examples:
            Only the current claim generation may finalize a result:

            >>> outcomes = store.finalize_many(proposed)
        """
        ...


def is_claim_capable_store(
    store: EvidenceStore,
) -> TypeGuard[ClaimCapableEvidenceStore]:
    """Return whether a Store advertises and implements the claim extension.

    Examples:
        Legacy third-party Stores continue to satisfy ``EvidenceStore``:

        >>> is_claim_capable_store(legacy_store)
        False
    """

    return isinstance(store, ClaimCapableEvidenceStore) and store.supports_claims
