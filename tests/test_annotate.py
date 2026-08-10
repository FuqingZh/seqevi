from __future__ import annotations

import importlib
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
import threading
import time

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from seqevi.adapters import AdapterBatchResult
from seqevi.annotate import run_annotation
from seqevi.errors import (
    AdapterError,
    AnnotationError,
    EvidenceClaimLostError,
    FastaValidationError,
)
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimDisposition,
    ClaimAcquireResult,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceClaim,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    FetchedEvidence,
    sha256_digest,
)
from seqevi.sequence import identify_protein_sequence, read_fasta, unique_identities
from seqevi.store import LocalStore
from seqevi.store.schema import evidence_claims
from sqlalchemy import func, select

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
    read_result_table,
    write_artifact_file,
    write_fixture_database,
    write_fixture_tool,
)

annotate_module = importlib.import_module("seqevi.annotate")


class _CountingStore:
    def __init__(self, delegate: LocalStore) -> None:
        self.delegate = delegate
        self.lookup_sizes: list[int] = []
        self.commit_sizes: list[int] = []
        self.fetch_sizes: list[int] = []

    @property
    def supports_claims(self) -> bool:
        return False

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        queries = tuple(requested_queries)
        self.lookup_sizes.append(len(queries))
        return self.delegate.lookup_many(queries)

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        commits = tuple(proposed_commits)
        self.commit_sizes.append(len(commits))
        return self.delegate.commit_many(commits)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, FetchedEvidence]:
        requested = tuple(keys)
        self.fetch_sizes.append(len(requested))
        return self.delegate.fetch_many(requested)

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None:
        return self.delegate.fetch(key)

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]:
        return self.delegate.acquire_many(requested_queries, owner_token=owner_token)

    def renew_many(self, claims: Iterable[EvidenceClaim]) -> tuple[EvidenceClaim, ...]:
        return self.delegate.renew_many(claims)

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None:
        self.delegate.release_many(claims)

    def finalize_many(
        self, proposed: Iterable[ClaimedEvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        return self.delegate.finalize_many(proposed)


class _FailSecondBatchAdapter:
    def __init__(self, delegate: FixtureAdapter) -> None:
        self.delegate = delegate
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema
        self.calls = 0

    def run_batch(self, **kwargs: object) -> AdapterBatchResult:
        self.calls += 1
        if self.calls == 2:
            raise AdapterError("planned second batch failure")
        return self.delegate.run_batch(**kwargs)  # type: ignore[arg-type]


class _CancelAdapter:
    def __init__(self, delegate: FixtureAdapter, cancellation: BaseException) -> None:
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema
        self.cancellation = cancellation

    def run_batch(self, **_kwargs: object) -> AdapterBatchResult:
        raise self.cancellation


class _RecordingThreadsAdapter:
    def __init__(self, delegate: FixtureAdapter) -> None:
        self.delegate = delegate
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema
        self.threads: list[int] = []

    def run_batch(self, **kwargs: object) -> AdapterBatchResult:
        threads = kwargs["threads"]
        assert isinstance(threads, int)
        self.threads.append(threads)
        return self.delegate.run_batch(**kwargs)  # type: ignore[arg-type]


class _ConcurrentRecordingAdapter:
    def __init__(self, delegate: FixtureAdapter, *, block_first: bool = False) -> None:
        self.delegate = delegate
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema
        self.sequence_calls: dict[str, int] = {}
        self.lock = threading.Lock()
        self.block_first = block_first
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def run_batch(self, **kwargs: object) -> AdapterBatchResult:
        identities = kwargs["identities"]
        assert isinstance(identities, tuple)
        with self.lock:
            self.calls += 1
            first_call = self.calls == 1
            for identity in identities:
                self.sequence_calls[identity.sequence_id] = (
                    self.sequence_calls.get(identity.sequence_id, 0) + 1
                )
        if self.block_first and first_call:
            self.entered.set()
            assert self.release.wait(10)
        return self.delegate.run_batch(**kwargs)  # type: ignore[arg-type]


def write_input(path: Path) -> Path:
    path.write_text(
        ">hit-a first header\nMPEPTIDE\n"
        ">hit-alias duplicate content\nMPEPTIDE\n"
        ">none terminal no-hit\nMNOHITX\n",
        encoding="utf-8",
    )
    return path


def test_failed_cleanup_makes_one_bounded_logical_release_attempt() -> None:
    identity = identify_protein_sequence("MBOUNDEDCLEANUP")
    key = EvidenceKey.from_parameters(
        sequence_id=identity.sequence_id,
        adapter_contract_version="fixture/v1",
        tool_runtime_digest="sha256:" + "a" * 64,
        resource_id="fixture-resource",
        semantic_parameters={},
    )
    claim = EvidenceClaim(
        key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=60),
        20.0,
    )

    class FailingReleaseStore:
        calls: list[int] = []

        def release_many(self, claims):
            self.calls.append(len(tuple(claims)))
            raise EvidenceClaimLostError("cleanup unavailable")

    store = FailingReleaseStore()
    annotate_module._release_active_claims(  # pyright: ignore[reportPrivateUsage]
        store,  # type: ignore[arg-type]
        (claim,) * 10_000,
    )

    assert store.calls == [10_000]


def test_annotation_materializes_complete_result_and_reuses_cache(
    tmp_path: Path,
) -> None:
    fasta = write_input(tmp_path / "proteins.fasta")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    adapter = FixtureAdapter(executable=executable, database=database)
    store_path = tmp_path / "store"

    with LocalStore.open(store_path) as store:
        first = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "first",
            adapter=adapter,
            store=store,
        )
        second = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "second",
            adapter=NeverRunAdapter(adapter),
            store=store,
        )
    with LocalStore.open(tmp_path / "fresh-store") as fresh_store:
        repeated = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "repeated",
            adapter=adapter,
            store=fresh_store,
        )

    assert first.input_records == 3
    assert first.unique_sequences == 2
    assert first.cache_hits == 0
    assert first.computed == 2
    assert first.hits == 1
    assert first.no_hits == 1
    assert second.cache_hits == 2
    assert second.computed == 0
    assert repeated.computed == 2

    assert first.output_dir.is_file()
    df_map = read_result_table(first.output_dir, "main.sequence_map")
    assert df_map.get_column("InputOrder").to_list() == [1, 2, 3]
    assert df_map.get_column("EvidenceSource").to_list() == [
        "computed",
        "computed",
        "computed",
    ]
    assert df_map.get_column("SequenceID").n_unique() == 2
    assert read_result_table(first.output_dir, "main.evidence").height == 1
    assert read_result_table(first.output_dir, "main.no_hits").height == 1

    df_cached_map = read_result_table(second.output_dir, "main.sequence_map")
    assert set(df_cached_map.get_column("EvidenceSource")) == {"cache"}

    metadata = read_result_table(first.output_dir, "_seqevi.metadata").row(
        0, named=True
    )
    assert metadata["Adapter"] == "fixture"
    assert metadata["ResultFormatVersion"] == "seqevi-duckdb/1"
    assert (
        read_result_table(first.output_dir, "_seqevi.table_info")
        .filter(pl.col("RelationName") == "annotations")
        .get_column("RowCount")
        .item()
        == 3
    )
    assert_frame_equal(
        read_result_table(first.output_dir, "main.annotations"),
        read_result_table(repeated.output_dir, "main.annotations"),
    )


def test_concurrent_partial_overlap_computes_overlap_once(tmp_path: Path) -> None:
    first_fasta = tmp_path / "first.fasta"
    first_fasta.write_text(">a\nMPEPTIDE\n>shared-a\nMSHARED\n", encoding="utf-8")
    second_fasta = tmp_path / "second.fasta"
    second_fasta.write_text(">shared-b\nMSHARED\n>c\nMOTHER\n", encoding="utf-8")
    adapter = _ConcurrentRecordingAdapter(
        FixtureAdapter(
            executable=write_fixture_tool(tmp_path / "fixture-tool"),
            database=write_fixture_database(tmp_path / "database"),
        ),
        block_first=True,
    )
    store_path = tmp_path / "store"
    busy_observed = threading.Event()
    with LocalStore.open(store_path):
        pass

    class BusyObservingStore:
        supports_claims = True

        def __init__(self, delegate: LocalStore) -> None:
            self.delegate = delegate

        def __getattr__(self, name: str):
            return getattr(self.delegate, name)

        def lookup_many(self, queries):
            return self.delegate.lookup_many(queries)

        def commit_many(self, commits):
            return self.delegate.commit_many(commits)

        def fetch_many(self, keys):
            return self.delegate.fetch_many(keys)

        def fetch(self, key):
            return self.delegate.fetch(key)

        def acquire_many(self, queries, *, owner_token: str):
            results = self.delegate.acquire_many(queries, owner_token=owner_token)
            if any(result.disposition is ClaimDisposition.BUSY for result in results):
                busy_observed.set()
            return results

        def renew_many(self, claims):
            return self.delegate.renew_many(claims)

        def release_many(self, claims):
            return self.delegate.release_many(claims)

        def finalize_many(self, proposed):
            return self.delegate.finalize_many(proposed)

    def annotate(fasta: Path, output: Path):
        with LocalStore.open(store_path) as store:
            return run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=adapter,
                store=BusyObservingStore(store),  # type: ignore[arg-type]
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(annotate, first_fasta, tmp_path / "first.duckdb")
        assert adapter.entered.wait(10)
        second = executor.submit(annotate, second_fasta, tmp_path / "second.duckdb")
        assert busy_observed.wait(10)
        adapter.release.set()
        futures = (first, second)
        summaries = tuple(future.result() for future in futures)

    shared_id = read_fasta(first_fasta)[1].identity.sequence_id
    assert adapter.sequence_calls[shared_id] == 1
    assert sum(summary.computed for summary in summaries) == 3
    assert sum(summary.cache_hits for summary in summaries) == 1
    first_shared = read_result_table(
        tmp_path / "first.duckdb", "main.annotations"
    ).filter(pl.col("SequenceID") == shared_id)
    second_shared = read_result_table(
        tmp_path / "second.duckdb", "main.annotations"
    ).filter(pl.col("SequenceID") == shared_id)
    assert_frame_equal(
        first_shared.drop("InputOrder", "InputID", "InputHeader", "EvidenceSource"),
        second_shared.drop("InputOrder", "InputID", "InputHeader", "EvidenceSource"),
    )
    assert first_shared.get_column("EvidenceSource").item() == "computed"
    assert second_shared.get_column("EvidenceSource").item() == "cache"
    with LocalStore.open(store_path) as store:
        with store.engine.connect() as connection:
            claim_rows = connection.execute(
                select(func.count()).select_from(evidence_claims)
            ).scalar_one()
    assert claim_rows == 0


def test_lease_renews_through_slow_finalize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("seqevi.store.local._CLAIM_LEASE_SECONDS", 0.2)
    monkeypatch.setattr("seqevi.store.local._CLAIM_RENEWAL_SECONDS", 0.05)
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    class SlowFinalizeStore:
        def __init__(self, delegate: LocalStore) -> None:
            self.delegate = delegate
            self.supports_claims = True

        def __getattr__(self, name: str):
            return getattr(self.delegate, name)

        def finalize_many(self, proposed):
            time.sleep(0.35)
            return self.delegate.finalize_many(proposed)

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "result.duckdb",
            adapter=adapter,
            store=SlowFinalizeStore(store),  # type: ignore[arg-type]
        )

    assert summary.computed == 1


def test_legacy_terminal_wins_while_claimed_adapter_is_blocked(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = _ConcurrentRecordingAdapter(
        FixtureAdapter(
            executable=write_fixture_tool(tmp_path / "fixture-tool"),
            database=write_fixture_database(tmp_path / "database"),
        ),
        block_first=True,
    )
    store_path = tmp_path / "store"
    with LocalStore.open(store_path):
        pass

    def annotate():
        with LocalStore.open(store_path) as store:
            return run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "result.duckdb",
                adapter=adapter,
                store=store,
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(annotate)
        assert adapter.entered.wait(10)
        identity = identify_protein_sequence("MPEPTIDE")
        peer_commit = EvidenceCommit(
            identity=identity,
            key=adapter.contract.evidence_key(identity),
            status=EvidenceStatus.NO_HIT,
            payload_digest=sha256_digest(b"peer-terminal"),
            raw_artifact=write_artifact_file(
                tmp_path / "peer.raw.tsv", b"peer-terminal", "text/plain"
            ),
        )
        with LocalStore.open(store_path) as peer_store:
            assert peer_store.commit_many((peer_commit,)) == (CommitOutcome.CREATED,)
        adapter.release.set()
        summary = future.result(timeout=20)

    with LocalStore.open(store_path) as store:
        fetched = store.fetch(peer_commit.key)
        with store.engine.connect() as connection:
            claim_rows = connection.execute(
                select(func.count()).select_from(evidence_claims)
            ).scalar_one()

    assert summary.computed == 0
    assert summary.cache_hits == 1
    assert summary.no_hits == 1
    assert fetched is not None
    assert fetched.record.payload_digest == peer_commit.payload_digest
    assert claim_rows == 0


def test_busy_retry_cadence_is_carried_without_real_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    sleeps: list[float] = []
    monkeypatch.setattr(annotate_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(annotate_module, "is_claim_capable_store", lambda _store: True)

    class BusyThenAvailableStore:
        supports_claims = True

        def __init__(self, delegate: LocalStore) -> None:
            self.delegate = delegate
            self.calls = 0
            self.blocking_claim: EvidenceClaim | None = None

        def __getattr__(self, name: str):
            return getattr(self.delegate, name)

        def acquire_many(self, queries, *, owner_token: str):
            requested = tuple(queries)
            self.calls += 1
            if self.calls == 1:
                blocker = self.delegate.acquire_many(requested, owner_token="blocker")
                self.blocking_claim = blocker[0].claim
            results = self.delegate.acquire_many(requested, owner_token=owner_token)
            if self.calls in (1, 2):
                result = results[0]
                assert result.busy is not None
                retry_after = 7.0 if self.calls == 1 else 5.0
                busy = BusyEvidenceClaim(
                    result.busy.key, result.busy.expires_at, retry_after
                )
                if self.calls == 2:
                    assert self.blocking_claim is not None
                    self.delegate.release_many((self.blocking_claim,))
                return (ClaimAcquireResult(ClaimDisposition.BUSY, busy=busy),)
            return results

    with LocalStore.open(tmp_path / "store") as delegate:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "result.duckdb",
            adapter=adapter,
            store=BusyThenAvailableStore(delegate),  # type: ignore[arg-type]
        )

    assert sleeps == [7.0, 5.0]
    assert summary.computed == 1
    assert summary.metrics.tool_batches == 1


def test_renewer_narrows_terminal_chunk_and_renews_remaining_claims(
    tmp_path: Path,
) -> None:
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    alphabet = "ACDEFGHIKLMNPQRSTVWY"

    def sequence(index: int) -> str:
        residues = []
        for _position in range(5):
            index, remainder = divmod(index, len(alphabet))
            residues.append(alphabet[remainder])
        return "M" + "".join(residues)

    identities = tuple(
        identify_protein_sequence(sequence(index)) for index in range(1_500)
    )
    queries = tuple(
        EvidenceQuery(identity, adapter.contract.evidence_key(identity))
        for identity in identities
    )
    expiry = datetime.now(UTC) + timedelta(seconds=60)
    claims = tuple(
        EvidenceClaim(query.key, "owner", 1, expiry, 0.01) for query in queries
    )
    terminal_key = claims[0].key
    first_chunk = {terminal_key}
    renewal_started = threading.Event()
    allow_renewal = threading.Event()
    remaining_renewed = threading.Event()
    renewal_sizes: list[int] = []

    class RacingStore:
        def renew_many(self, requested):
            batch = tuple(requested)
            renewal_sizes.append(len(batch))
            if any(claim.key in first_chunk for claim in batch):
                renewal_started.set()
                assert allow_renewal.wait(10)
                raise EvidenceClaimLostError("finalized chunk")
            if len(batch) == 500:
                remaining_renewed.set()
            return batch

        def lookup_many(self, requested):
            return {
                query.key: EvidenceRecord(
                    query.key,
                    EvidenceStatus.NO_HIT,
                    "sha256:" + "0" * 64,
                    None,
                    None,
                    datetime.now(UTC),
                )
                for query in requested
                if query.key in first_chunk
            }

    renewer = annotate_module._LeaseRenewer(
        RacingStore(),  # type: ignore[arg-type]
        claims,
        {query.key: query for query in queries},
    )
    renewer.__enter__()
    assert renewal_started.wait(10)
    assert remaining_renewed.wait(10)
    renewer.complete(first_chunk)
    allow_renewal.set()
    renewer.mark_finalized()
    renewer.__exit__(None, None, None)

    assert {claim.key for claim in renewer.active_claims()} == {
        claim.key for claim in claims[1:]
    }
    assert set(renewer.queries_by_key) == {claim.key for claim in claims[1:]}
    assert sorted(renewal_sizes) == [500, 999, 1_000]


def test_invocation_renewer_submits_outer_batches_by_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(annotate_module, "_STORE_BATCH_SIZE", 2)
    identities = tuple(
        identify_protein_sequence(sequence)
        for sequence in ("MDEADLINELATEA", "MDEADLINELATEB", "MDEADLINEURGENT")
    )
    keys = tuple(
        EvidenceKey.from_parameters(
            sequence_id=identity.sequence_id,
            adapter_contract_version="fixture/v1",
            tool_runtime_digest="sha256:" + "b" * 64,
            resource_id="fixture-resource",
            semantic_parameters={},
        )
        for identity in identities
    )
    queries = tuple(
        EvidenceQuery(identity, key)
        for identity, key in zip(identities, keys, strict=True)
    )
    claims = tuple(
        EvidenceClaim(
            key,
            "owner",
            1,
            datetime.now(UTC) + timedelta(seconds=60),
            20.0,
        )
        for key in keys
    )
    urgent_key = keys[-1]
    urgent_started = threading.Event()
    both_batches = threading.Event()
    call_count = 0
    call_lock = threading.Lock()

    class RecordingStore:
        def renew_many(self, requested):
            nonlocal call_count
            batch = tuple(requested)
            if any(claim.key == urgent_key for claim in batch):
                urgent_started.set()
            else:
                assert urgent_started.wait(timeout=1)
            with call_lock:
                call_count += 1
                if call_count == 2:
                    both_batches.set()
            return batch

        def lookup_many(self, _requested):
            return {}

    renewer = annotate_module._LeaseRenewer(  # pyright: ignore[reportPrivateUsage]
        RecordingStore(),  # type: ignore[arg-type]
        claims,
        {query.key: query for query in queries},
    )
    now = time.monotonic()
    with renewer.lock:
        renewer.deadlines[keys[0]] = now - 1.0
        renewer.deadlines[keys[1]] = now - 1.0
        renewer.deadlines[urgent_key] = now - 2.0
    renewer.__enter__()
    assert both_batches.wait(timeout=2)
    renewer.complete(keys)
    renewer.mark_finalized()
    renewer.__exit__(None, None, None)

    assert call_count == 2


def test_invocation_renewer_uses_near_expiry_before_long_cadence(
    tmp_path: Path,
) -> None:
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    identity = identify_protein_sequence("MNEAREXPIRY")
    query = EvidenceQuery(identity, adapter.contract.evidence_key(identity))
    claim = EvidenceClaim(
        query.key,
        "owner",
        1,
        datetime.now(UTC) + timedelta(seconds=0.05),
        20.0,
    )
    renewed = threading.Event()

    class RecordingStore:
        def renew_many(self, requested):
            current = tuple(requested)
            renewed.set()
            return tuple(
                EvidenceClaim(
                    item.key,
                    item.owner_token,
                    item.generation,
                    datetime.now(UTC) + timedelta(seconds=60),
                    20.0,
                )
                for item in current
            )

        def lookup_many(self, _requested):
            return {}

    renewer = annotate_module._LeaseRenewer(
        RecordingStore(),  # type: ignore[arg-type]
        (claim,),
        {query.key: query},
    )
    renewer.__enter__()
    assert renewed.wait(timeout=1)
    renewer.mark_finalized()
    renewer.__exit__(None, None, None)

    refreshed = renewer.active_claims()[0]
    assert refreshed.expires_at > datetime.now(UTC) + timedelta(seconds=30)


def test_1500_annotation_renews_only_remaining_finalize_chunk(tmp_path: Path) -> None:
    alphabet = "ACDEFGHIKLMNPQRSTVWY"

    def sequence(value: int) -> str:
        residues = []
        for _position in range(5):
            value, remainder = divmod(value, len(alphabet))
            residues.append(alphabet[remainder])
        return "M" + "".join(residues)

    fasta = tmp_path / "input.fasta"
    fasta.write_text(
        "".join(f">protein-{i}\n{sequence(i)}\n" for i in range(1_500)),
        encoding="utf-8",
    )
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    class ChunkRaceStore:
        supports_claims = True

        def __init__(self) -> None:
            self.claims: dict[EvidenceKey, EvidenceClaim] = {}
            self.records: dict[EvidenceKey, FetchedEvidence] = {}
            self.finalize_sizes: list[int] = []
            self.release_sizes: list[int] = []
            self.first_chunk: set[EvidenceKey] = set()
            self.remaining_renewed = threading.Event()
            self.mutation_lock = threading.Lock()

        def lookup_many(self, queries):
            return {
                query.key: self.records[query.key].record
                for query in queries
                if query.key in self.records
            }

        def commit_many(self, commits):
            raise AssertionError("claim-capable annotation must finalize")

        def fetch_many(self, keys):
            return {key: self.records[key] for key in keys if key in self.records}

        def fetch(self, key):
            return self.records.get(key)

        def acquire_many(self, queries, *, owner_token: str):
            results = []
            for query in queries:
                claim = EvidenceClaim(
                    query.key,
                    owner_token,
                    1,
                    datetime.now(UTC) + timedelta(seconds=60),
                    0.01,
                )
                self.claims[query.key] = claim
                results.append(
                    ClaimAcquireResult(ClaimDisposition.ACQUIRED, claim=claim)
                )
            return tuple(results)

        def renew_many(self, claims):
            requested = tuple(claims)
            with self.mutation_lock:
                if any(claim.key not in self.claims for claim in requested):
                    raise EvidenceClaimLostError("finalized claim in renewal snapshot")
                if self.first_chunk and not any(
                    claim.key in self.first_chunk for claim in requested
                ):
                    if len(requested) == 500:
                        self.remaining_renewed.set()
                return requested

        def release_many(self, claims):
            requested = tuple(claims)
            self.release_sizes.append(len(requested))
            for claim in requested:
                self.claims.pop(claim.key, None)

        def finalize_many(self, proposed):
            requested = tuple(proposed)
            self.finalize_sizes.append(len(requested))
            if len(self.finalize_sizes) == 2:
                assert self.remaining_renewed.wait(10)
            with self.mutation_lock:
                for item in requested:
                    commit = item.commit
                    self.claims.pop(commit.key, None)
                    self.records[commit.key] = FetchedEvidence(
                        EvidenceRecord(
                            commit.key,
                            commit.status,
                            commit.payload_digest,
                            None
                            if commit.normalized_artifact is None
                            else commit.normalized_artifact.digest,
                            None
                            if commit.raw_artifact is None
                            else commit.raw_artifact.digest,
                            datetime.now(UTC),
                        ),
                        commit.normalized_artifact,
                        commit.raw_artifact,
                    )
                if len(self.finalize_sizes) == 1:
                    self.first_chunk = {item.commit.key for item in requested}
            return (CommitOutcome.CREATED,) * len(requested)

    store = ChunkRaceStore()
    summary = run_annotation(
        fasta_path=fasta,
        output_dir=tmp_path / "result.duckdb",
        adapter=adapter,
        store=store,  # type: ignore[arg-type]
    )

    assert store.finalize_sizes == [1_000, 500]
    assert store.remaining_renewed.is_set()
    assert summary.computed == 1_500
    assert store.release_sizes == []
    assert store.claims == {}


def test_annotation_passes_operational_threads_without_changing_contract(
    tmp_path: Path,
) -> None:
    fasta = write_input(tmp_path / "proteins.fasta")
    adapter = _RecordingThreadsAdapter(
        FixtureAdapter(
            executable=write_fixture_tool(tmp_path / "fixture-tool"),
            database=write_fixture_database(tmp_path / "database"),
        )
    )
    contract_before = adapter.contract

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=7,
        )

    assert adapter.threads == [7]
    assert adapter.contract == contract_before
    assert summary.metrics.configured_threads == 7


def test_annotation_rejects_non_positive_threads_before_store_access(
    tmp_path: Path,
) -> None:
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    fasta = write_input(tmp_path / "proteins.fasta")
    with LocalStore.open(tmp_path / "store") as store:
        counted = _CountingStore(store)
        with pytest.raises(ValueError, match="threads must be positive"):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=adapter,
                store=counted,
                threads=0,
            )

    assert counted.lookup_sizes == []


@pytest.mark.parametrize(
    ("mode", "timeout_seconds"),
    [("fail", None), ("malformed", None), ("missing-output", None), ("sleep", 0.05)],
)
def test_failed_annotation_does_not_cache_evidence(
    tmp_path: Path,
    mode: str,
    timeout_seconds: float | None,
) -> None:
    fasta = write_input(tmp_path / "proteins.fasta")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database", mode=mode)
    adapter = FixtureAdapter(executable=executable, database=database)
    records = read_fasta(fasta)
    identities = unique_identities(records)

    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match="diagnostics retained"):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=adapter,
                store=store,
                timeout_seconds=timeout_seconds,
            )

        queries = [
            EvidenceQuery(identity, adapter.contract.evidence_key(identity))
            for identity in identities
        ]
        assert store.lookup_many(queries) == {}
        with store.engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count())
                    .select_from(evidence_claims)
                    .where(evidence_claims.c.expires_at > datetime.now(UTC))
                ).scalar_one()
                == 0
            )

    assert not (tmp_path / "output").exists()
    assert list(tmp_path.glob(".seqevi-annotate-*"))


@pytest.mark.parametrize(
    ("sequence", "evidence_rows", "no_hit_rows"),
    [("MPEPTIDE", 1, 0), ("MNOHITX", 0, 1)],
)
def test_annotation_writes_typed_empty_terminal_tables(
    tmp_path: Path,
    sequence: str,
    evidence_rows: int,
    no_hit_rows: int,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(f">protein\n{sequence}\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
        )

    df_evidence = read_result_table(summary.output_dir, "main.evidence")
    df_no_hits = read_result_table(summary.output_dir, "main.no_hits")
    assert df_evidence.schema == adapter.evidence_schema
    assert df_evidence.height == evidence_rows
    assert df_no_hits.schema == {
        "SequenceID": pl.String,
        "MD5": pl.String,
        "Length": pl.Int64,
    }
    assert df_no_hits.height == no_hit_rows


def test_invalid_fasta_never_accesses_store_and_removes_staging(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">valid\nMPEPTIDE\n>last\nM-INVALID\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    class NoAccessStore:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"Store was accessed during FASTA validation: {name}")

    with pytest.raises(FastaValidationError, match="invalid residue"):
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=NoAccessStore(),  # type: ignore[arg-type]
        )

    assert not list(tmp_path.glob(".seqevi-fasta-*"))
    assert not (tmp_path / "output").exists()


def test_annotation_bounds_store_and_tool_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(annotate_module, "_STORE_BATCH_SIZE", 2)
    monkeypatch.setattr(annotate_module, "_ANNOTATION_BATCH_SIZE", 3)
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        "".join(
            f">protein-{index}\nMPEPTID{chr(ord('A') + index)}\n" for index in range(5)
        ),
        encoding="utf-8",
    )
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as delegate:
        store = _CountingStore(delegate)
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
        )

    assert store.lookup_sizes == [2, 2, 1]
    assert store.commit_sizes == [2, 1, 2]
    assert store.fetch_sizes == [5]
    assert summary.metrics.store_lookup_batches == 3
    assert summary.metrics.store_commit_batches == 3
    assert summary.metrics.tool_batches == 2
    assert summary.metrics.unique_artifact_reads == 4


def test_claim_capable_10001_misses_use_two_tool_batches_and_drop_claims(
    tmp_path: Path,
) -> None:
    alphabet = "ACDEFGHIKLMNPQRSTVWY"

    def encoded_sequence(value: int) -> str:
        residues = []
        for _position in range(6):
            value, remainder = divmod(value, len(alphabet))
            residues.append(alphabet[remainder])
        return "M" + "".join(residues)

    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        "".join(
            f">protein-{index}\n{encoded_sequence(index)}\n" for index in range(10_001)
        ),
        encoding="utf-8",
    )
    adapter = _ConcurrentRecordingAdapter(
        FixtureAdapter(
            executable=write_fixture_tool(tmp_path / "fixture-tool"),
            database=write_fixture_database(tmp_path / "database"),
        )
    )

    class RecordingClaimStore:
        supports_claims = True

        def __init__(self) -> None:
            self.records: dict[EvidenceKey, FetchedEvidence] = {}
            self.claims: dict[EvidenceKey, EvidenceClaim] = {}
            self.finalized_keys: list[EvidenceKey] = []
            self.released_keys: list[EvidenceKey] = []
            self.acquire_sizes: list[int] = []
            self.finalize_sizes: list[int] = []

        def lookup_many(self, queries):
            return {
                query.key: self.records[query.key].record
                for query in queries
                if query.key in self.records
            }

        def commit_many(self, commits):
            raise AssertionError("claim-capable annotation must finalize")

        def fetch_many(self, keys):
            return {key: self.records[key] for key in keys if key in self.records}

        def fetch(self, key):
            return self.records.get(key)

        def acquire_many(self, queries, *, owner_token: str):
            queries = tuple(queries)
            self.acquire_sizes.append(len(queries))
            results = []
            for query in queries:
                claim = EvidenceClaim(
                    query.key,
                    owner_token,
                    1,
                    datetime.now(UTC) + timedelta(seconds=60),
                    20.0,
                )
                self.claims[query.key] = claim
                results.append(
                    ClaimAcquireResult(ClaimDisposition.ACQUIRED, claim=claim)
                )
            return tuple(results)

        def renew_many(self, claims):
            return tuple(claims)

        def release_many(self, claims):
            requested = tuple(claims)
            self.released_keys.extend(claim.key for claim in requested)
            for claim in requested:
                self.claims.pop(claim.key, None)

        def finalize_many(self, proposed):
            requested = tuple(proposed)
            self.finalize_sizes.append(len(requested))
            self.finalized_keys.extend(item.commit.key for item in requested)
            for item in requested:
                commit = item.commit
                self.claims.pop(commit.key)
                self.records[commit.key] = FetchedEvidence(
                    EvidenceRecord(
                        commit.key,
                        commit.status,
                        commit.payload_digest,
                        None
                        if commit.normalized_artifact is None
                        else commit.normalized_artifact.digest,
                        None
                        if commit.raw_artifact is None
                        else commit.raw_artifact.digest,
                        datetime.now(UTC),
                    ),
                    commit.normalized_artifact,
                    commit.raw_artifact,
                )
            return (CommitOutcome.CREATED,) * len(requested)

    store = RecordingClaimStore()
    summary = run_annotation(
        fasta_path=fasta,
        output_dir=tmp_path / "result.duckdb",
        adapter=adapter,
        store=store,  # type: ignore[arg-type]
    )

    assert adapter.calls == 2
    assert summary.metrics.tool_batches == 2
    assert summary.computed == 10_001
    assert len(store.finalized_keys) == 10_001
    assert len(set(store.finalized_keys)) == 10_001
    assert store.released_keys == []
    assert store.claims == {}
    assert sum(store.acquire_sizes) == 10_001
    assert sum(store.finalize_sizes) == 10_001
    assert max(store.acquire_sizes) <= 1_000
    assert max(store.finalize_sizes) <= 1_000


def test_completed_batch_is_reused_after_later_tool_batch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(annotate_module, "_STORE_BATCH_SIZE", 2)
    monkeypatch.setattr(annotate_module, "_ANNOTATION_BATCH_SIZE", 2)
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">first\nMPEPTIDE\n>second\nMSEQUENCE\n>third\nMTHIRDSEQ\n",
        encoding="utf-8",
    )
    delegate = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match="planned second batch failure"):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "failed-output",
                adapter=_FailSecondBatchAdapter(delegate),
                store=store,
            )
        recovered = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "recovered-output",
            adapter=delegate,
            store=store,
        )

    assert recovered.cache_hits == 2
    assert recovered.computed == 1
    assert recovered.metrics.tool_batches == 1


@pytest.mark.parametrize(
    "cancellation",
    [KeyboardInterrupt(), SystemExit(7)],
    ids=["keyboard", "system-exit"],
)
def test_cancellation_stops_renewal_and_releases_active_claims(
    tmp_path: Path, cancellation: BaseException
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    delegate = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(type(cancellation)) as raised:
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "cancelled-output",
                adapter=_CancelAdapter(delegate, cancellation),
                store=store,
            )
        assert raised.value is cancellation
        identity = identify_protein_sequence("MPEPTIDE")
        key = EvidenceKey.from_parameters(
            sequence_id=identity.sequence_id,
            adapter_contract_version=delegate.contract.version,
            tool_runtime_digest=delegate.contract.tool_runtime_digest,
            resource_id=delegate.contract.resource_id,
            semantic_parameters=delegate.contract.semantic_parameters,
        )
        reacquired = store.acquire_many(
            (EvidenceQuery(identity, key),), owner_token="peer"
        )[0].claim

    assert reacquired is not None
    assert reacquired.generation == 2
