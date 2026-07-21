"""Shallow orchestration for one exact annotation invocation."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .adapters.base import AdapterBatchResult, AnnotationAdapter
from .errors import AnnotationError, OutputPackageError
from .evidence import (
    EvidenceCommit,
    EvidenceQuery,
    EvidenceSource,
    EvidenceStatus,
    FetchedEvidence,
    sha256_digest,
)
from .package import materialize_output_package
from .runner import ToolRunner
from .sequence import (
    SequenceIdentity,
    iter_fasta_lines,
    read_fasta,
    unique_identities,
)
from .store.contract import EvidenceStore


@dataclass(frozen=True, slots=True)
class AnnotationSummary:
    """Counts and output location for one successful invocation."""

    input_records: int
    unique_sequences: int
    cache_hits: int
    computed: int
    hits: int
    no_hits: int
    output_dir: Path


def run_annotation(
    *,
    fasta_path: Path,
    output_dir: Path,
    adapter: AnnotationAdapter,
    store: EvidenceStore,
    runner: ToolRunner | None = None,
    timeout_seconds: float | None = None,
) -> AnnotationSummary:
    """Resolve exact evidence, compute misses, and write the output package."""

    if output_dir.exists():
        raise OutputPackageError(f"output path already exists: {output_dir}")
    if not output_dir.parent.is_dir():
        raise OutputPackageError(
            f"output parent directory does not exist: {output_dir.parent}"
        )

    records = read_fasta(fasta_path)
    identities = unique_identities(records)
    keys_by_sequence_id = {
        identity.sequence_id: adapter.contract.evidence_key(identity)
        for identity in identities
    }
    queries = tuple(
        EvidenceQuery(identity, keys_by_sequence_id[identity.sequence_id])
        for identity in identities
    )
    cached = store.lookup_many(queries)
    missing = tuple(
        identity
        for identity in identities
        if keys_by_sequence_id[identity.sequence_id] not in cached
    )
    ids_missing = {identity.sequence_id for identity in missing}
    source_by_sequence_id = {
        identity.sequence_id: (
            EvidenceSource.COMPUTED
            if identity.sequence_id in ids_missing
            else EvidenceSource.CACHE
        )
        for identity in identities
    }

    work_dir = Path(tempfile.mkdtemp(prefix=".seqevi-annotate-", dir=output_dir.parent))
    try:
        if missing:
            misses_fasta = work_dir / "cache-misses.fasta"
            misses_fasta.write_text(
                "".join(iter_fasta_lines(missing)), encoding="ascii", newline="\n"
            )
            batch = adapter.run_batch(
                identities=missing,
                input_fasta=misses_fasta,
                work_dir=work_dir,
                runner=runner or ToolRunner(),
                timeout_seconds=timeout_seconds,
            )
            commits = _build_commits(
                batch=batch,
                identities=missing,
                adapter=adapter,
            )
            store.commit_many(commits)

        final_records = store.lookup_many(queries)
        missing_keys = [
            query.key for query in queries if query.key not in final_records
        ]
        if missing_keys:
            raise AnnotationError(
                f"Store did not expose {len(missing_keys)} terminal evidence records"
            )

        fetched_by_sequence_id: dict[str, FetchedEvidence] = {}
        for identity in identities:
            key = keys_by_sequence_id[identity.sequence_id]
            fetched = store.fetch(key)
            if fetched is None:
                raise AnnotationError(
                    f"Store could not fetch terminal evidence: {identity.sequence_id}"
                )
            fetched_by_sequence_id[identity.sequence_id] = fetched

        materialize_output_package(
            output_dir=output_dir,
            records=records,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
            evidence_schema=adapter.evidence_schema,
            adapter_contract=adapter.contract,
            input_digest=sha256_digest(fasta_path.read_bytes()),
        )
    except Exception as error:
        raise AnnotationError(
            f"annotation failed; diagnostics retained at {work_dir}: {error}"
        ) from error
    else:
        shutil.rmtree(work_dir, ignore_errors=True)

    statuses = [fetched.record.status for fetched in fetched_by_sequence_id.values()]
    return AnnotationSummary(
        input_records=len(records),
        unique_sequences=len(identities),
        cache_hits=len(identities) - len(missing),
        computed=len(missing),
        hits=statuses.count(EvidenceStatus.HIT),
        no_hits=statuses.count(EvidenceStatus.NO_HIT),
        output_dir=output_dir,
    )


def _build_commits(
    *,
    batch: AdapterBatchResult,
    identities: tuple[SequenceIdentity, ...],
    adapter: AnnotationAdapter,
) -> tuple[EvidenceCommit, ...]:
    identity_by_sequence_id = {
        identity.sequence_id: identity for identity in identities
    }
    result_by_sequence_id = {result.sequence_id: result for result in batch.sequences}
    expected = set(identity_by_sequence_id)
    observed = set(result_by_sequence_id)
    if expected != observed:
        missing = ", ".join(sorted(expected - observed)) or "<none>"
        extra = ", ".join(sorted(observed - expected)) or "<none>"
        raise AnnotationError(
            f"adapter sequence accounting mismatch; missing: {missing}; extra: {extra}"
        )

    commits = []
    for sequence_id in sorted(expected):
        identity = identity_by_sequence_id[sequence_id]
        result = result_by_sequence_id[sequence_id]
        commits.append(
            EvidenceCommit(
                identity=identity,
                key=adapter.contract.evidence_key(identity),
                status=result.status,
                payload_digest=result.payload_digest,
                normalized_artifact=(
                    batch.normalized_artifact
                    if result.status is EvidenceStatus.HIT
                    else None
                ),
                raw_artifact=batch.raw_artifact,
            )
        )
    return tuple(commits)
