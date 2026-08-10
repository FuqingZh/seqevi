"""Shallow orchestration for one exact annotation invocation."""

from __future__ import annotations

import shutil
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import batched
from pathlib import Path
from typing import Literal

from . import __version__
from .adapters.base import AdapterBatchResult, AnnotationAdapter
from .errors import AnnotationError, EvidenceClaimLostError, OutputPackageError
from .evidence import (
    ClaimDisposition,
    ClaimedEvidenceCommit,
    EvidenceCommit,
    EvidenceClaim,
    EvidenceKey,
    EvidenceQuery,
    EvidenceSource,
    EvidenceStatus,
)
from .result import RESULT_FORMAT_VERSION, materialize_result_database
from .runner import ToolCommand, ToolRunResult, ToolRunner
from .sequence import (
    SequenceIdentity,
    iter_staged_identities,
    iter_staged_records,
    iter_fasta_lines,
    stage_fasta,
)
from .store.contract import (
    ClaimCapableEvidenceStore,
    EvidenceStore,
    is_claim_capable_store,
)

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
    output_format: Literal["duckdb"] = "duckdb",
    result_metadata: Mapping[str, str] | None = None,
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
    claim_store: ClaimCapableEvidenceStore | None = None
    invocation_renewer: _LeaseRenewer | None = None
    try:
        keys_by_sequence_id = {}
        computed_ids: set[str] = set()
        pending_misses: list[SequenceIdentity] = []
        busy_queries: list[EvidenceQuery] = []
        busy_retry_after: float | None = None
        claim_store = store if is_claim_capable_store(store) else None
        invocation_renewer = (
            _LeaseRenewer(claim_store, (), {}) if claim_store is not None else None
        )
        if invocation_renewer is not None:
            invocation_renewer.__enter__()
        owner_token = uuid.uuid4().hex
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
            if claim_store is not None:
                decisions = claim_store.acquire_many(queries, owner_token=owner_token)
                cached = {
                    query.key: decision.record
                    for query, decision in zip(queries, decisions, strict=True)
                    if decision.disposition is ClaimDisposition.CACHED
                }
                for query, decision in zip(queries, decisions, strict=True):
                    if decision.disposition is ClaimDisposition.ACQUIRED:
                        assert decision.claim is not None
                        pending_misses.append(query.identity)
                        assert invocation_renewer is not None
                        invocation_renewer.add(decision.claim, query)
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
            store_lookup_seconds += time.perf_counter() - lookup_started
            lookup_batches += 1
            if claim_store is None:
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
                    claim_store=claim_store,
                    renewer=invocation_renewer,
                )
                commit_batches += batch_metrics.commit_batches
                adapter_seconds += batch_metrics.adapter_seconds
                store_commit_seconds += batch_metrics.store_commit_seconds

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
                claim_store=claim_store,
                renewer=invocation_renewer,
            )
            commit_batches += batch_metrics.commit_batches
            adapter_seconds += batch_metrics.adapter_seconds
            store_commit_seconds += batch_metrics.store_commit_seconds

        while busy_queries:
            time.sleep(busy_retry_after or 1.0)
            next_retry_after: float | None = None
            next_busy = []
            acquired = []
            assert claim_store is not None
            for busy_batch in batched(busy_queries, _STORE_BATCH_SIZE):
                reacquire_started = time.perf_counter()
                decisions = claim_store.acquire_many(
                    busy_batch, owner_token=owner_token
                )
                store_lookup_seconds += time.perf_counter() - reacquire_started
                lookup_batches += 1
                for query, decision in zip(busy_batch, decisions, strict=True):
                    if decision.disposition is ClaimDisposition.ACQUIRED:
                        assert decision.claim is not None
                        assert invocation_renewer is not None
                        invocation_renewer.add(decision.claim, query)
                        acquired.append(query.identity)
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
                    claim_store=claim_store,
                    renewer=invocation_renewer,
                )
                commit_batches += batch_metrics.commit_batches
                adapter_seconds += batch_metrics.adapter_seconds
                store_commit_seconds += batch_metrics.store_commit_seconds
            busy_queries = next_busy
            busy_retry_after = next_retry_after

        if invocation_renewer is not None:
            invocation_renewer.mark_finalized()
            invocation_renewer.__exit__(None, None, None)

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

        statuses = [
            fetched.record.status for fetched in fetched_by_sequence_id.values()
        ]
        package_started = time.perf_counter()
        if output_format != "duckdb":
            raise AnnotationError(f"unsupported output format: {output_format}")
        metadata = dict(result_metadata or _default_result_metadata(adapter))
        metadata["InputDigest"] = stage.input_digest
        metadata.setdefault("CreatedAt", datetime.now(UTC).isoformat())
        materialize_result_database(
            output_path=output_dir,
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
            },
        )
        package_seconds = time.perf_counter() - package_started
    except Exception as error:
        if invocation_renewer is not None and claim_store is not None:
            invocation_renewer.stop_and_join()
            _release_active_claims(claim_store, invocation_renewer.active_claims())
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
    claim_store: ClaimCapableEvidenceStore | None = None,
    renewer: _LeaseRenewer | None = None,
) -> _BatchMetrics:
    batch_dir = work_dir / f"batch-{batch_number:06d}"
    batch_dir.mkdir()
    misses_fasta = batch_dir / "cache-misses.fasta"
    if renewer is not None:
        renewer.raise_if_failed()
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
    commits = _build_commits(batch=batch, identities=identities, adapter=adapter)
    batches = 0
    commit_started = time.perf_counter()
    for commit_batch in batched(commits, _STORE_BATCH_SIZE):
        if claim_store is None:
            store.commit_many(commit_batch)
        else:
            assert renewer is not None
            renewer.raise_if_failed()
            proposed = tuple(
                ClaimedEvidenceCommit(commit, renewer.claim_for(commit.key))
                for commit in commit_batch
            )
            claim_store.finalize_many(proposed)
            renewer.complete(item.commit.key for item in proposed)
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


class _LeaseRenewer:
    """Keep only currently authoritative claims alive for one invocation."""

    def __init__(
        self,
        store: ClaimCapableEvidenceStore | None,
        claims: tuple[EvidenceClaim, ...],
        queries_by_key: Mapping[EvidenceKey, EvidenceQuery],
    ) -> None:
        self.store = store
        self.claims = {claim.key: claim for claim in claims}
        self.queries_by_key = dict(queries_by_key)
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.thread: threading.Thread | None = None
        self.lock = threading.Lock()
        self.finalized = False

    def __enter__(self) -> _LeaseRenewer:
        self._start_if_needed()
        return self

    def __exit__(self, *_error: object) -> None:
        self.stop_and_join()
        if self.error is not None and not self.finalized:
            raise self.error

    def stop_and_join(self) -> None:
        """Stop renewal without replacing an in-flight annotation failure."""

        self.stop.set()
        if self.thread is not None:
            self.thread.join()

    def claim_for(self, key: EvidenceKey) -> EvidenceClaim:
        with self.lock:
            return self.claims[key]

    def add(self, claim: EvidenceClaim, query: EvidenceQuery) -> None:
        """Begin renewing one newly acquired claim until it is completed."""

        with self.lock:
            self.claims[claim.key] = claim
            self.queries_by_key[claim.key] = query
        self._start_if_needed()

    def complete(self, keys: Iterable[EvidenceKey]) -> None:
        with self.lock:
            for key in keys:
                self.claims.pop(key, None)
                self.queries_by_key.pop(key, None)

    def active_claims(self) -> tuple[EvidenceClaim, ...]:
        with self.lock:
            return tuple(self.claims.values())

    def mark_finalized(self) -> None:
        self.finalized = True

    def raise_if_failed(self) -> None:
        """Raise a renewal failure before more local work or finalization."""

        if self.error is not None:
            raise self.error

    def _start_if_needed(self) -> None:
        with self.lock:
            if self.claims and self.thread is None:
                self.thread = threading.Thread(target=self._run, daemon=True)
                self.thread.start()

    def _run(self) -> None:
        while not self.stop.is_set():
            with self.lock:
                snapshot = tuple(self.claims.values())
            cadence = (
                min(claim.renewal_after_seconds for claim in snapshot)
                if snapshot
                else 1.0
            )
            if self.stop.wait(cadence):
                return
            try:
                with self.lock:
                    snapshot = tuple(
                        (claim, self.queries_by_key[key])
                        for key, claim in self.claims.items()
                    )
                if not snapshot:
                    continue
                assert self.store is not None
                for pair_batch in batched(snapshot, _STORE_BATCH_SIZE):
                    self._renew_batch(pair_batch)
            except BaseException as error:
                self.error = error
                self.stop.set()
                return

    def _renew_batch(
        self, pairs: tuple[tuple[EvidenceClaim, EvidenceQuery], ...]
    ) -> None:
        assert self.store is not None
        claims = tuple(claim for claim, _query in pairs)
        try:
            renewed = self.store.renew_many(claims)
        except EvidenceClaimLostError:
            terminal: set[EvidenceKey] = set()
            queries = tuple(query for _claim, query in pairs)
            for query_batch in batched(queries, _STORE_BATCH_SIZE):
                terminal.update(self.store.lookup_many(query_batch))
            with self.lock:
                for key in terminal:
                    self.claims.pop(key, None)
                    self.queries_by_key.pop(key, None)
            remaining = tuple(pair for pair in pairs if pair[0].key not in terminal)
            for claim, query in remaining:
                try:
                    renewed_one = self.store.renew_many((claim,))
                except EvidenceClaimLostError:
                    record = self.store.lookup_many((query,))
                    if claim.key in record:
                        with self.lock:
                            self.claims.pop(claim.key, None)
                            self.queries_by_key.pop(claim.key, None)
                        continue
                    raise
                self._replace_renewed(renewed_one)
        else:
            self._replace_renewed(renewed)

    def _replace_renewed(self, renewed: tuple[EvidenceClaim, ...]) -> None:
        with self.lock:
            for claim in renewed:
                if claim.key in self.claims:
                    self.claims[claim.key] = claim


def _release_active_claims(
    store: ClaimCapableEvidenceStore, claims: tuple[EvidenceClaim, ...]
) -> None:
    for claim_batch in batched(claims, _STORE_BATCH_SIZE):
        try:
            store.release_many(claim_batch)
        except Exception:
            for claim in claim_batch:
                try:
                    store.release_many((claim,))
                except Exception:
                    continue
