"""Logical Store protocol shared by local and future service clients."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from seqevi.evidence import (
    CommitOutcome,
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

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None: ...
