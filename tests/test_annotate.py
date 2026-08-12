from __future__ import annotations

import importlib
from collections.abc import Iterable
from pathlib import Path
import threading

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from seqevi.adapters import AdapterBatchResult
from seqevi.annotate import run_annotation
from seqevi.errors import (
    AdapterError,
    AnnotationError,
    FastaValidationError,
)
from seqevi.evidence import (
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
)
from seqevi.sequence import identify_protein_sequence
from seqevi.store import LocalStore

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
    read_result_table,
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
    def supports_claim_sessions(self) -> bool:
        return self.delegate.supports_claim_sessions

    def claim_session(self):
        return self.delegate.claim_session()

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

    assert store.lookup_sizes == []
    assert store.commit_sizes == []
    assert store.fetch_sizes == [5]
    assert summary.metrics.store_lookup_batches == 3
    assert summary.metrics.store_commit_batches == 3
    assert summary.metrics.tool_batches == 2
    assert summary.metrics.unique_artifact_reads == 4


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
        with store.claim_session() as peer:
            reacquired = peer.acquire_many((EvidenceQuery(identity, key),))[0].claim

    assert reacquired is not None
    assert reacquired.generation == 2
