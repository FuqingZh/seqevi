"""Shallow orchestration for one exact annotation invocation."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import batched
from pathlib import Path
from typing import Literal

from . import __version__
from .adapters.base import AdapterBatchResult, AnnotationAdapter
from .errors import AnnotationError, OutputPackageError
from .evidence import (
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceQuery,
    EvidenceSource,
    EvidenceStatus,
)
from .progress import (
    BatchProgress,
    ProgressEvent,
    ProgressPhase,
    ProgressSink,
    ProgressState,
    ProgressUnit,
    WorkProgress,
    emit_progress,
)
from .result import RESULT_FORMAT_VERSION, materialize_result_database
from .runner import ToolCancelledError, ToolCommand, ToolRunResult, ToolRunner
from .sequence import (
    SequenceIdentity,
    iter_staged_identities,
    iter_staged_records,
    iter_fasta_lines,
    stage_fasta,
)
from .store.contract import (
    ClaimSession,
    EvidenceStore,
    is_claim_session_capable_store,
)

_STORE_BATCH_SIZE = 1_000
_ANNOTATION_BATCH_SIZE = 10_000
_CLAIM_RUNWAY_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class AnnotationMetrics:
    """Operational measurements for one successful annotation invocation.

    ``existing_finalizations`` counts proposed terminal records that a
    ClaimSession resolved to immutable evidence already finalized by a peer.
    It is observational and does not change computed/cache provenance.
    """

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
    existing_finalizations: int = 0


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
    peer_completed_sequence_ids: frozenset[str] = frozenset()
    existing_finalizations: int = 0


class _MeasuringToolRunner(ToolRunner):
    def __init__(
        self,
        delegate: ToolRunner,
        *,
        cancellation_signal: threading.Event | None = None,
    ) -> None:
        self.delegate = delegate
        self.cancellation_signal = cancellation_signal
        self.duration_seconds = 0.0

    def run(
        self,
        command: ToolCommand,
        *,
        timeout_seconds: float | None = None,
        cancellation_signal: threading.Event | None = None,
    ) -> ToolRunResult:
        result = self.delegate.run(
            command,
            timeout_seconds=timeout_seconds,
            cancellation_signal=cancellation_signal or self.cancellation_signal,
        )
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
    output_format: Literal["duckdb"] = "duckdb",
    result_metadata: Mapping[str, str] | None = None,
    progress_sink: ProgressSink | None = None,
) -> AnnotationSummary:
    """Resolve exact evidence, compute misses, and publish one DuckDB result."""

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
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.STAGING,
            ProgressState.STARTED,
            "Reading FASTA",
        ),
    )
    fasta_stage_root = Path(
        tempfile.mkdtemp(prefix=".seqevi-fasta-", dir=output_dir.parent)
    )
    stage = stage_fasta(fasta_path, fasta_stage_root)
    fasta_staging_seconds = time.perf_counter() - fasta_started
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.STAGING,
            ProgressState.COMPLETED,
            "FASTA ready",
            evidence_ready=WorkProgress(
                completed=0,
                total=stage.unique_sequences,
                unit=ProgressUnit.SEQUENCES,
            ),
        ),
    )
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
    existing_finalizations = 0
    claim_session: ClaimSession | None = None
    primary_failure: BaseException | None = None
    close_failure: BaseException | None = None
    published_output_identity: tuple[int, int] | None = None
    published_output_marker: Path | None = None

    def unlink_owned_output() -> None:
        if published_output_identity is None or published_output_marker is None:
            return
        try:
            current = output_dir.stat(follow_symlinks=False)
            marker = published_output_marker.stat(follow_symlinks=False)
        except FileNotFoundError:
            return
        if (current.st_dev, current.st_ino) == published_output_identity and (
            marker.st_dev,
            marker.st_ino,
        ) == published_output_identity:
            output_dir.unlink()

    try:
        keys_by_sequence_id = {}
        evidence_ready_sequence_ids: set[str] = set()
        computed_ids: set[str] = set()
        pending_misses: list[SequenceIdentity] = []
        busy_queries: list[EvidenceQuery] = []
        busy_retry_after: float | None = None
        if is_claim_session_capable_store(store):
            claim_session = store.claim_session().__enter__()

        def evidence_ready() -> WorkProgress:
            return WorkProgress(
                completed=len(evidence_ready_sequence_ids),
                total=stage.unique_sequences,
                unit=ProgressUnit.SEQUENCES,
            )

        tool_runner = _MeasuringToolRunner(
            runner or ToolRunner(),
            cancellation_signal=(
                claim_session.cancellation_signal if claim_session is not None else None
            ),
        )
        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.STORE_LOOKUP,
                ProgressState.STARTED,
                "Checking Store",
                evidence_ready=evidence_ready(),
            ),
        )

        for identity_batch in batched(iter_staged_identities(stage), _STORE_BATCH_SIZE):
            queries = tuple(
                EvidenceQuery(identity, adapter.contract.evidence_key(identity))
                for identity in identity_batch
            )
            keys_by_sequence_id.update(
                (query.identity.sequence_id, query.key) for query in queries
            )
            lookup_started = time.perf_counter()
            if claim_session is not None:
                claim_session.raise_if_lost()
                decisions = claim_session.acquire_many(queries)
                cached = {
                    query.key: decision.record
                    for query, decision in zip(queries, decisions, strict=True)
                    if decision.disposition is ClaimDisposition.CACHED
                }
                for query, decision in zip(queries, decisions, strict=True):
                    if decision.disposition is ClaimDisposition.ACQUIRED:
                        assert decision.claim is not None
                        pending_misses.append(query.identity)
                    elif decision.disposition is ClaimDisposition.BUSY:
                        busy_queries.append(query)
                        assert decision.busy is not None
                        busy_retry_after = (
                            decision.busy.retry_after_seconds
                            if busy_retry_after is None
                            else min(
                                busy_retry_after,
                                decision.busy.retry_after_seconds,
                            )
                        )
            else:
                cached = store.lookup_many(queries)
            evidence_ready_sequence_ids.update(
                query.identity.sequence_id for query in queries if query.key in cached
            )
            store_lookup_seconds += time.perf_counter() - lookup_started
            lookup_batches += 1
            emit_progress(
                progress_sink,
                ProgressEvent(
                    ProgressPhase.STORE_LOOKUP,
                    ProgressState.RUNNING,
                    "Checking Store",
                    evidence_ready=evidence_ready(),
                ),
            )
            if claim_session is None:
                for query in queries:
                    if query.key not in cached:
                        pending_misses.append(query.identity)
            while len(pending_misses) >= _ANNOTATION_BATCH_SIZE:
                annotation_identities = tuple(pending_misses[:_ANNOTATION_BATCH_SIZE])
                del pending_misses[:_ANNOTATION_BATCH_SIZE]
                computed_ids.update(
                    identity.sequence_id for identity in annotation_identities
                )
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
                    claim_session=claim_session,
                    progress_sink=progress_sink,
                    evidence_completed=len(evidence_ready_sequence_ids),
                    evidence_total=stage.unique_sequences,
                )
                evidence_ready_sequence_ids.update(
                    identity.sequence_id for identity in annotation_identities
                )
                commit_batches += batch_metrics.commit_batches
                adapter_seconds += batch_metrics.adapter_seconds
                store_commit_seconds += batch_metrics.store_commit_seconds
                existing_finalizations += batch_metrics.existing_finalizations
                computed_ids.difference_update(
                    batch_metrics.peer_completed_sequence_ids
                )

        if pending_misses:
            computed_ids.update(identity.sequence_id for identity in pending_misses)
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
                claim_session=claim_session,
                progress_sink=progress_sink,
                evidence_completed=len(evidence_ready_sequence_ids),
                evidence_total=stage.unique_sequences,
            )
            evidence_ready_sequence_ids.update(
                identity.sequence_id for identity in pending_misses
            )
            commit_batches += batch_metrics.commit_batches
            adapter_seconds += batch_metrics.adapter_seconds
            store_commit_seconds += batch_metrics.store_commit_seconds
            existing_finalizations += batch_metrics.existing_finalizations
            computed_ids.difference_update(batch_metrics.peer_completed_sequence_ids)

        waited_for_claim = bool(busy_queries)
        while busy_queries:
            emit_progress(
                progress_sink,
                ProgressEvent(
                    ProgressPhase.CLAIM_WAIT,
                    ProgressState.RUNNING,
                    "Waiting for peer evidence",
                    evidence_ready=evidence_ready(),
                ),
            )
            time.sleep(busy_retry_after or 1.0)
            next_retry_after: float | None = None
            next_busy = []
            acquired = []
            assert claim_session is not None
            for busy_batch in batched(busy_queries, _STORE_BATCH_SIZE):
                reacquire_started = time.perf_counter()
                claim_session.raise_if_lost()
                decisions = claim_session.acquire_many(busy_batch)
                store_lookup_seconds += time.perf_counter() - reacquire_started
                lookup_batches += 1
                for query, decision in zip(busy_batch, decisions, strict=True):
                    if decision.disposition is ClaimDisposition.ACQUIRED:
                        assert decision.claim is not None
                        acquired.append(query.identity)
                    elif decision.disposition is ClaimDisposition.CACHED:
                        evidence_ready_sequence_ids.add(query.identity.sequence_id)
                    elif decision.disposition is ClaimDisposition.BUSY:
                        assert decision.busy is not None
                        next_retry_after = (
                            decision.busy.retry_after_seconds
                            if next_retry_after is None
                            else min(
                                next_retry_after,
                                decision.busy.retry_after_seconds,
                            )
                        )
                        next_busy.append(query)
            for acquired_batch in batched(acquired, _ANNOTATION_BATCH_SIZE):
                computed_ids.update(identity.sequence_id for identity in acquired_batch)
                tool_batches += 1
                batch_metrics = _run_annotation_batch(
                    identities=acquired_batch,
                    batch_number=tool_batches,
                    work_dir=work_dir,
                    adapter=adapter,
                    store=store,
                    runner=tool_runner,
                    timeout_seconds=timeout_seconds,
                    threads=threads,
                    claim_session=claim_session,
                    progress_sink=progress_sink,
                    evidence_completed=len(evidence_ready_sequence_ids),
                    evidence_total=stage.unique_sequences,
                )
                evidence_ready_sequence_ids.update(
                    identity.sequence_id for identity in acquired_batch
                )
                commit_batches += batch_metrics.commit_batches
                adapter_seconds += batch_metrics.adapter_seconds
                store_commit_seconds += batch_metrics.store_commit_seconds
                existing_finalizations += batch_metrics.existing_finalizations
                computed_ids.difference_update(
                    batch_metrics.peer_completed_sequence_ids
                )
            busy_queries = next_busy
            busy_retry_after = next_retry_after

        if waited_for_claim:
            emit_progress(
                progress_sink,
                ProgressEvent(
                    ProgressPhase.CLAIM_WAIT,
                    ProgressState.COMPLETED,
                    "Peer evidence ready",
                    evidence_ready=evidence_ready(),
                ),
            )

        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.STORE_LOOKUP,
                ProgressState.COMPLETED,
                "Store check complete",
                evidence_ready=evidence_ready(),
            ),
        )

        if claim_session is not None:
            claim_session.raise_if_lost()
        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.STORE_FETCH,
                ProgressState.STARTED,
                "Loading evidence",
                evidence_ready=evidence_ready(),
            ),
        )
        fetch_started = time.perf_counter()
        fetched_by_key = store.fetch_many(keys_by_sequence_id.values())
        store_fetch_seconds = time.perf_counter() - fetch_started
        if claim_session is not None:
            claim_session.raise_if_lost()
        missing_keys = set(keys_by_sequence_id.values()) - fetched_by_key.keys()
        if missing_keys:
            raise AnnotationError(
                f"Store did not expose {len(missing_keys)} terminal evidence records"
            )
        evidence_ready_sequence_ids.update(keys_by_sequence_id)
        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.STORE_FETCH,
                ProgressState.COMPLETED,
                "Evidence loaded",
                evidence_ready=evidence_ready(),
            ),
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

        statuses = [
            fetched.record.status for fetched in fetched_by_sequence_id.values()
        ]
        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.PACKAGE,
                ProgressState.STARTED,
                "Writing result",
                evidence_ready=evidence_ready(),
            ),
        )
        package_started = time.perf_counter()
        if output_format != "duckdb":
            raise AnnotationError(f"unsupported output format: {output_format}")
        metadata = dict(result_metadata or _default_result_metadata(adapter))
        metadata["InputDigest"] = stage.input_digest
        metadata.setdefault("CreatedAt", datetime.now(UTC).isoformat())
        package_output = (
            work_dir / "candidate-result.duckdb"
            if claim_session is not None
            else output_dir
        )
        if claim_session is not None:
            claim_session.raise_if_lost()
            if output_dir.exists():
                raise AnnotationError(f"output path already exists: {output_dir}")
        materialize_result_database(
            output_path=package_output,
            records=iter_staged_records(stage),
            input_record_count=stage.input_records,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
            evidence_schema=adapter.evidence_schema,
            adapter_contract=adapter.contract,
            input_digest=stage.input_digest,
            metadata=metadata,
            run_metrics={
                "input_records": stage.input_records,
                "unique_sequences": stage.unique_sequences,
                "cache_hits": stage.unique_sequences - len(computed_ids),
                "computed": len(computed_ids),
                "hits": statuses.count(EvidenceStatus.HIT),
                "no_hits": statuses.count(EvidenceStatus.NO_HIT),
                "existing_finalizations": existing_finalizations,
            },
        )
        if claim_session is not None:
            try:
                claim_session.raise_if_lost()
                candidate = package_output.stat(follow_symlinks=False)
                published_output_identity = (candidate.st_dev, candidate.st_ino)
                published_output_marker = package_output
                os.link(package_output, output_dir)
                claim_session.raise_if_lost()
            except BaseException:
                unlink_owned_output()
                raise
        package_seconds = time.perf_counter() - package_started
        emit_progress(
            progress_sink,
            ProgressEvent(
                ProgressPhase.PACKAGE,
                ProgressState.COMPLETED,
                "Result written",
                evidence_ready=evidence_ready(),
            ),
        )
    except BaseException as error:
        primary_failure = error
        if not isinstance(error, Exception):
            raise
        raise AnnotationError(
            f"annotation failed; diagnostics retained at {work_dir}: {error}"
        ) from error
    finally:
        if claim_session is not None:
            try:
                claim_session.__exit__(None, None, None)
            except BaseException as close_error:
                if primary_failure is None:
                    close_failure = close_error
                else:
                    primary_failure.add_note(
                        f"ClaimSession cleanup also failed: {close_error!r}"
                    )
        shutil.rmtree(stage.root, ignore_errors=True)

    if close_failure is not None:
        unlink_owned_output()
        raise close_failure
    if claim_session is not None:
        try:
            claim_session.raise_if_lost()
        except BaseException as error:
            unlink_owned_output()
            if not isinstance(error, Exception):
                raise
            raise AnnotationError(
                f"annotation failed; diagnostics retained at {work_dir}: {error}"
            ) from error
    shutil.rmtree(work_dir, ignore_errors=True)

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
            existing_finalizations=existing_finalizations,
        ),
    )


def _default_result_metadata(adapter: AnnotationAdapter) -> dict[str, str]:
    """Build a complete generic result identity for direct orchestration calls."""

    adapter_name = adapter.contract.name
    if adapter_name == "eggnog":
        upstream_tool = "eggNOG-mapper"
        result_schema = "eggnog-mapper/2"
    elif adapter_name == "interpro-pfam":
        upstream_tool = "InterProScan"
        result_schema = "interproscan-pfam/5"
    elif adapter_name == "dbcan-cazyme":
        upstream_tool = "dbCAN"
        result_schema = "dbcan-cazyme/5"
    else:
        upstream_tool = adapter_name
        result_schema = f"{adapter_name}/1"
    return {
        "ResultFormatVersion": RESULT_FORMAT_VERSION,
        "ResultSchemaID": result_schema,
        "SeqEviVersion": __version__,
        "Adapter": adapter_name,
        "AdapterContractVersion": adapter.contract.version,
        "UpstreamTool": upstream_tool,
        "UpstreamToolVersion": "unknown",
        "ToolRuntimeDigest": adapter.contract.tool_runtime_digest,
        "ResourceID": adapter.contract.resource_id,
        "InputDigest": "unknown",
        "CreatedAt": datetime.now(UTC).isoformat(),
    }


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
    claim_session: ClaimSession | None = None,
    progress_sink: ProgressSink | None = None,
    evidence_completed: int,
    evidence_total: int,
) -> _BatchMetrics:
    batch_dir = work_dir / f"batch-{batch_number:06d}"
    batch_dir.mkdir()
    misses_fasta = batch_dir / "cache-misses.fasta"
    if claim_session is not None:
        claim_session.raise_if_lost()
    with misses_fasta.open("w", encoding="ascii", newline="\n") as handle:
        handle.writelines(iter_fasta_lines(identities))
    batch_progress = BatchProgress(number=batch_number, size=len(identities))
    ready_before = WorkProgress(
        completed=evidence_completed,
        total=evidence_total,
        unit=ProgressUnit.SEQUENCES,
    )
    tool_message = f"Running {_tool_display_name(adapter)} batch {batch_number}"
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.STARTED,
            tool_message,
            evidence_ready=ready_before,
            batch=batch_progress,
        ),
    )
    adapter_started = time.perf_counter()
    try:
        batch = adapter.run_batch(
            identities=identities,
            input_fasta=misses_fasta,
            work_dir=batch_dir,
            runner=runner,
            timeout_seconds=timeout_seconds,
            threads=threads,
        )
    except ToolCancelledError:
        if claim_session is not None:
            claim_session.raise_if_lost()
        raise
    adapter_seconds = time.perf_counter() - adapter_started
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.COMPLETED,
            f"{_tool_display_name(adapter)} batch {batch_number} complete",
            evidence_ready=ready_before,
            batch=batch_progress,
        ),
    )
    if claim_session is not None:
        claim_session.raise_if_lost()
    commits = _build_commits(batch=batch, identities=identities, adapter=adapter)
    batches = 0
    peer_completed_sequence_ids: set[str] = set()
    existing_finalizations = 0
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.STORE_COMMIT,
            ProgressState.STARTED,
            f"Saving batch {batch_number}",
            evidence_ready=ready_before,
            batch=batch_progress,
        ),
    )
    commit_started = time.perf_counter()
    for commit_batch in batched(commits, _STORE_BATCH_SIZE):
        if claim_session is None:
            store.commit_many(commit_batch)
            batches += 1
        else:
            claim_session.raise_if_lost()
            outcomes = claim_session.finalize_many(commit_batch)
            claim_session.raise_if_lost()
            peer_completed_sequence_ids.update(
                commit.identity.sequence_id
                for commit, outcome in zip(commit_batch, outcomes, strict=True)
                if outcome is CommitOutcome.EXISTING
            )
            existing_finalizations += sum(
                outcome is CommitOutcome.EXISTING for outcome in outcomes
            )
            batches += 1
    emit_progress(
        progress_sink,
        ProgressEvent(
            ProgressPhase.STORE_COMMIT,
            ProgressState.COMPLETED,
            f"Batch {batch_number} saved",
            evidence_ready=WorkProgress(
                completed=evidence_completed + len(identities),
                total=evidence_total,
                unit=ProgressUnit.SEQUENCES,
            ),
            batch=batch_progress,
        ),
    )
    return _BatchMetrics(
        commit_batches=batches,
        adapter_seconds=adapter_seconds,
        store_commit_seconds=time.perf_counter() - commit_started,
        peer_completed_sequence_ids=frozenset(peer_completed_sequence_ids),
        existing_finalizations=existing_finalizations,
    )


def _tool_display_name(adapter: AnnotationAdapter) -> str:
    return {
        "eggnog": "eggNOG-mapper",
        "interpro-pfam": "InterProScan/Pfam",
        "dbcan-cazyme": "dbCAN",
    }.get(adapter.contract.name, adapter.contract.name)


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
