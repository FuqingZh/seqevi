"""Shallow orchestration for one exact annotation invocation."""

from __future__ import annotations

import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from itertools import batched
from pathlib import Path

from .adapters.base import AdapterBatchResult, AnnotationAdapter
from .errors import AnnotationError, OutputPackageError
from .evidence import (
    EvidenceCommit,
    EvidenceQuery,
    EvidenceSource,
    EvidenceStatus,
)
from .package import materialize_output_package
from .runner import ToolCommand, ToolRunResult, ToolRunner
from .sequence import (
    SequenceIdentity,
    iter_staged_identities,
    iter_staged_records,
    iter_fasta_lines,
    stage_fasta,
)
from .store.contract import EvidenceStore

_STORE_BATCH_SIZE = 1_000
_ANNOTATION_BATCH_SIZE = 10_000


@dataclass(frozen=True, slots=True)
class AnnotationMetrics:
    """Operational measurements for one successful annotation invocation."""

    elapsed_seconds: float
    fasta_staging_seconds: float
    store_lookup_seconds: float
    adapter_seconds: float
    external_tool_seconds: float
    store_commit_seconds: float
    store_fetch_seconds: float
    package_seconds: float
    peak_rss_kib: int | None
    store_lookup_batches: int
    store_commit_batches: int
    store_fetch_batches: int
    tool_batches: int
    unique_artifact_reads: int
    configured_threads: int


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
    metrics: AnnotationMetrics


@dataclass(frozen=True, slots=True)
class _BatchMetrics:
    commit_batches: int
    adapter_seconds: float
    store_commit_seconds: float


class _MeasuringToolRunner(ToolRunner):
    def __init__(self, delegate: ToolRunner) -> None:
        self.delegate = delegate
        self.duration_seconds = 0.0

    def run(
        self,
        command: ToolCommand,
        *,
        timeout_seconds: float | None = None,
    ) -> ToolRunResult:
        result = self.delegate.run(command, timeout_seconds=timeout_seconds)
        self.duration_seconds += result.duration_seconds
        return result


def run_annotation(
    *,
    fasta_path: Path,
    output_dir: Path,
    adapter: AnnotationAdapter,
    store: EvidenceStore,
    runner: ToolRunner | None = None,
    timeout_seconds: float | None = None,
    threads: int = 1,
) -> AnnotationSummary:
    """Resolve exact evidence, compute misses, and write the output package."""

    if threads < 1:
        raise ValueError("threads must be positive")
    if output_dir.exists():
        raise OutputPackageError(f"output path already exists: {output_dir}")
    if not output_dir.parent.is_dir():
        raise OutputPackageError(
            f"output parent directory does not exist: {output_dir.parent}"
        )

    started = time.perf_counter()
    fasta_started = time.perf_counter()
    fasta_stage_root = Path(
        tempfile.mkdtemp(prefix=".seqevi-fasta-", dir=output_dir.parent)
    )
    stage = stage_fasta(fasta_path, fasta_stage_root)
    fasta_staging_seconds = time.perf_counter() - fasta_started
    try:
        work_dir = Path(
            tempfile.mkdtemp(prefix=".seqevi-annotate-", dir=output_dir.parent)
        )
    except Exception:
        shutil.rmtree(stage.root, ignore_errors=True)
        raise
    lookup_batches = 0
    commit_batches = 0
    tool_batches = 0
    store_lookup_seconds = 0.0
    adapter_seconds = 0.0
    store_commit_seconds = 0.0
    try:
        keys_by_sequence_id = {}
        computed_ids: set[str] = set()
        pending_misses: list[SequenceIdentity] = []
        tool_runner = _MeasuringToolRunner(runner or ToolRunner())

        for identity_batch in batched(iter_staged_identities(stage), _STORE_BATCH_SIZE):
            queries = tuple(
                EvidenceQuery(identity, adapter.contract.evidence_key(identity))
                for identity in identity_batch
            )
            keys_by_sequence_id.update(
                (query.identity.sequence_id, query.key) for query in queries
            )
            lookup_started = time.perf_counter()
            cached = store.lookup_many(queries)
            store_lookup_seconds += time.perf_counter() - lookup_started
            lookup_batches += 1
            for query in queries:
                if query.key not in cached:
                    pending_misses.append(query.identity)
                    computed_ids.add(query.identity.sequence_id)
            while len(pending_misses) >= _ANNOTATION_BATCH_SIZE:
                annotation_identities = tuple(pending_misses[:_ANNOTATION_BATCH_SIZE])
                del pending_misses[:_ANNOTATION_BATCH_SIZE]
                tool_batches += 1
                batch_metrics = _run_annotation_batch(
                    identities=annotation_identities,
                    batch_number=tool_batches,
                    work_dir=work_dir,
                    adapter=adapter,
                    store=store,
                    runner=tool_runner,
                    timeout_seconds=timeout_seconds,
                    threads=threads,
                )
                commit_batches += batch_metrics.commit_batches
                adapter_seconds += batch_metrics.adapter_seconds
                store_commit_seconds += batch_metrics.store_commit_seconds

        if pending_misses:
            tool_batches += 1
            batch_metrics = _run_annotation_batch(
                identities=tuple(pending_misses),
                batch_number=tool_batches,
                work_dir=work_dir,
                adapter=adapter,
                store=store,
                runner=tool_runner,
                timeout_seconds=timeout_seconds,
                threads=threads,
            )
            commit_batches += batch_metrics.commit_batches
            adapter_seconds += batch_metrics.adapter_seconds
            store_commit_seconds += batch_metrics.store_commit_seconds

        fetch_started = time.perf_counter()
        fetched_by_key = store.fetch_many(keys_by_sequence_id.values())
        store_fetch_seconds = time.perf_counter() - fetch_started
        missing_keys = set(keys_by_sequence_id.values()) - fetched_by_key.keys()
        if missing_keys:
            raise AnnotationError(
                f"Store did not expose {len(missing_keys)} terminal evidence records"
            )
        fetched_by_sequence_id = {
            key.sequence_id: fetched for key, fetched in fetched_by_key.items()
        }
        source_by_sequence_id = {
            sequence_id: (
                EvidenceSource.COMPUTED
                if sequence_id in computed_ids
                else EvidenceSource.CACHE
            )
            for sequence_id in keys_by_sequence_id
        }

        package_started = time.perf_counter()
        materialize_output_package(
            output_dir=output_dir,
            records=iter_staged_records(stage),
            identities=iter_staged_identities(stage),
            input_record_count=stage.input_records,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
            evidence_schema=adapter.evidence_schema,
            adapter_contract=adapter.contract,
            input_digest=stage.input_digest,
        )
        package_seconds = time.perf_counter() - package_started
    except Exception as error:
        raise AnnotationError(
            f"annotation failed; diagnostics retained at {work_dir}: {error}"
        ) from error
    else:
        shutil.rmtree(work_dir, ignore_errors=True)
    finally:
        shutil.rmtree(stage.root, ignore_errors=True)

    statuses = [fetched.record.status for fetched in fetched_by_sequence_id.values()]
    artifact_digests = {
        digest
        for fetched in fetched_by_sequence_id.values()
        for digest in (
            fetched.record.normalized_artifact_digest,
            fetched.record.raw_artifact_digest,
        )
        if digest is not None
    }
    return AnnotationSummary(
        input_records=stage.input_records,
        unique_sequences=stage.unique_sequences,
        cache_hits=stage.unique_sequences - len(computed_ids),
        computed=len(computed_ids),
        hits=statuses.count(EvidenceStatus.HIT),
        no_hits=statuses.count(EvidenceStatus.NO_HIT),
        output_dir=output_dir,
        metrics=AnnotationMetrics(
            elapsed_seconds=time.perf_counter() - started,
            fasta_staging_seconds=fasta_staging_seconds,
            store_lookup_seconds=store_lookup_seconds,
            adapter_seconds=adapter_seconds,
            external_tool_seconds=tool_runner.duration_seconds,
            store_commit_seconds=store_commit_seconds,
            store_fetch_seconds=store_fetch_seconds,
            package_seconds=package_seconds,
            peak_rss_kib=_peak_rss_kib(),
            store_lookup_batches=lookup_batches,
            store_commit_batches=commit_batches,
            store_fetch_batches=1,
            tool_batches=tool_batches,
            unique_artifact_reads=len(artifact_digests),
            configured_threads=threads,
        ),
    )


def _peak_rss_kib() -> int | None:
    try:
        import resource
    except ImportError:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak // 1024 if sys.platform == "darwin" else peak


def _run_annotation_batch(
    *,
    identities: tuple[SequenceIdentity, ...],
    batch_number: int,
    work_dir: Path,
    adapter: AnnotationAdapter,
    store: EvidenceStore,
    runner: ToolRunner,
    timeout_seconds: float | None,
    threads: int,
) -> _BatchMetrics:
    batch_dir = work_dir / f"batch-{batch_number:06d}"
    batch_dir.mkdir()
    misses_fasta = batch_dir / "cache-misses.fasta"
    with misses_fasta.open("w", encoding="ascii", newline="\n") as handle:
        handle.writelines(iter_fasta_lines(identities))
    adapter_started = time.perf_counter()
    batch = adapter.run_batch(
        identities=identities,
        input_fasta=misses_fasta,
        work_dir=batch_dir,
        runner=runner,
        timeout_seconds=timeout_seconds,
        threads=threads,
    )
    adapter_seconds = time.perf_counter() - adapter_started
    commits = _build_commits(
        batch=batch,
        identities=identities,
        adapter=adapter,
    )
    batches = 0
    commit_started = time.perf_counter()
    for commit_batch in batched(commits, _STORE_BATCH_SIZE):
        store.commit_many(commit_batch)
        batches += 1
    return _BatchMetrics(
        commit_batches=batches,
        adapter_seconds=adapter_seconds,
        store_commit_seconds=time.perf_counter() - commit_started,
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
