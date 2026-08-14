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
    EvidenceClaimLostError,
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
from seqevi.runner import ToolCommand
from seqevi.store import ClaimSession, LocalStore

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

    def claim_session(self) -> ClaimSession:
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


class _CloseFailingStore(_CountingStore):
    def claim_session(self) -> ClaimSession:
        delegate = self.delegate.claim_session()

        class CloseFailingSession:
            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *_error):
                delegate.__exit__(*_error)
                raise RuntimeError("injected close failure")

            def __getattr__(self, name):
                return getattr(delegate, name)

        return CloseFailingSession()  # type: ignore[return-value]


class _PeerWinningStore(_CountingStore):
    def claim_session(self) -> ClaimSession:
        delegate = self.delegate.claim_session()
        store = self.delegate

        class PeerWinningSession:
            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *_error):
                return delegate.__exit__(*_error)

            def finalize_many(
                self, commits: Iterable[EvidenceCommit]
            ) -> tuple[CommitOutcome, ...]:
                proposed = tuple(commits)
                store.commit_many(proposed)
                return delegate.finalize_many(proposed)

            def __getattr__(self, name):
                return getattr(delegate, name)

        return PeerWinningSession()  # type: ignore[return-value]


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


class _SlowToolAdapter:
    def __init__(self, delegate: FixtureAdapter) -> None:
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema

    def run_batch(self, **kwargs: object) -> AdapterBatchResult:
        work_dir = kwargs["work_dir"]
        runner = kwargs["runner"]
        assert isinstance(work_dir, Path)
        runner.run(  # type: ignore[union-attr]
            ToolCommand(
                arguments=(
                    __import__("sys").executable,
                    "-c",
                    "import time; time.sleep(10)",
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "slow.stdout",
                stderr_path=work_dir / "slow.stderr",
            )
        )
        raise AssertionError("cancelled tool unexpectedly returned")


class _AuthorityLosingStore(_CountingStore):
    def __init__(self, delegate: LocalStore) -> None:
        super().__init__(delegate)
        self.finalize_calls = 0

    def claim_session(self) -> ClaimSession:
        delegate = self.delegate.claim_session()
        owner = self

        class AuthorityLosingSession:
            def __init__(self) -> None:
                self.cancellation_signal = threading.Event()
                self.lost: BaseException | None = None
                self.timer: threading.Timer | None = None

            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *_error):
                if self.timer is not None:
                    self.timer.cancel()
                return delegate.__exit__(*_error)

            def acquire_many(self, queries):
                acquired = delegate.acquire_many(queries)

                def lose_authority() -> None:
                    self.lost = RuntimeError("injected renewal exhaustion")
                    self.cancellation_signal.set()

                self.timer = threading.Timer(0.1, lose_authority)
                self.timer.start()
                return acquired

            def raise_if_lost(self) -> None:
                if self.lost is not None:
                    raise EvidenceClaimLostError(
                        "ClaimSession authority was lost"
                    ) from self.lost
                delegate.raise_if_lost()

            def finalize_many(self, commits):
                owner.finalize_calls += 1
                return delegate.finalize_many(commits)

            def __getattr__(self, name):
                return getattr(delegate, name)

        return AuthorityLosingSession()  # type: ignore[return-value]


class _ManualAuthorityLosingStore(_CountingStore):
    def __init__(self, delegate: LocalStore) -> None:
        super().__init__(delegate)
        self.lost = threading.Event()

    def lose_authority(self) -> None:
        self.lost.set()

    def claim_session(self) -> ClaimSession:
        delegate = self.delegate.claim_session()
        owner = self

        class ManualSession:
            cancellation_signal = owner.lost

            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *_error):
                return delegate.__exit__(*_error)

            def raise_if_lost(self) -> None:
                if owner.lost.is_set():
                    raise EvidenceClaimLostError("injected packaging authority loss")
                delegate.raise_if_lost()

            def __getattr__(self, name):
                return getattr(delegate, name)

        return ManualSession()  # type: ignore[return-value]


class _CloseAuthorityLosingStore(_CountingStore):
    def claim_session(self) -> ClaimSession:
        delegate = self.delegate.claim_session()

        class CloseLosingSession:
            cancellation_signal = delegate.cancellation_signal

            def __init__(self) -> None:
                self.lost = False

            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *_error):
                result = delegate.__exit__(*_error)
                self.lost = True
                return result

            def raise_if_lost(self) -> None:
                if self.lost:
                    raise EvidenceClaimLostError("injected close authority loss")
                delegate.raise_if_lost()

            def __getattr__(self, name):
                return getattr(delegate, name)

        return CloseLosingSession()  # type: ignore[return-value]


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
    ("mode", "timeout_seconds"),
    [("fail", None), ("malformed", None), ("missing-output", None), ("sleep", 0.05)],
)
def test_failed_annotation_does_not_cache_evidence(
    tmp_path: Path,
    mode: str,
    timeout_seconds: float | None,
) -> None:
    fasta = write_input(tmp_path / "proteins.fasta")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database", mode=mode),
    )
    identities = tuple(
        identify_protein_sequence(sequence) for sequence in ("MPEPTIDE", "MNOHITX")
    )
    queries = tuple(
        EvidenceQuery(identity, adapter.contract.evidence_key(identity))
        for identity in identities
    )

    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match="diagnostics retained"):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=adapter,
                store=store,
                timeout_seconds=timeout_seconds,
            )
        assert store.lookup_many(queries) == {}

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


def test_annotation_classifies_finalize_peer_winner_as_cache_hit(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as delegate:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=_PeerWinningStore(delegate),
        )

    assert summary.cache_hits == 1
    assert summary.computed == 0
    assert read_result_table(summary.output_dir, "main.sequence_map").get_column(
        "EvidenceSource"
    ).to_list() == ["cache"]


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


def test_claim_authority_loss_cancels_tool_before_finalize(tmp_path: Path) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    delegate = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as local:
        store = _AuthorityLosingStore(local)
        started = __import__("time").monotonic()
        with pytest.raises(AnnotationError) as raised:
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=_SlowToolAdapter(delegate),
                store=store,
                runner=annotate_module.ToolRunner(termination_grace_seconds=0.1),
            )
        cancellation_elapsed = __import__("time").monotonic() - started

    assert isinstance(raised.value.__cause__, EvidenceClaimLostError)
    assert cancellation_elapsed < 1.0
    assert store.finalize_calls == 0


def test_claim_authority_loss_during_packaging_never_publishes_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    original_materialize = annotate_module.materialize_result_database

    with LocalStore.open(tmp_path / "store") as local:
        store = _ManualAuthorityLosingStore(local)

        def materialize(**kwargs: object) -> None:
            original_materialize(**kwargs)  # type: ignore[arg-type]
            store.lose_authority()

        monkeypatch.setattr(annotate_module, "materialize_result_database", materialize)
        output = tmp_path / "output.duckdb"
        with pytest.raises(AnnotationError) as raised:
            run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=adapter,
                store=store,
            )

    assert isinstance(raised.value.__cause__, EvidenceClaimLostError)
    assert not output.exists()


def test_claim_authority_loss_during_close_never_returns_success(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    with LocalStore.open(tmp_path / "store") as local:
        output = tmp_path / "output.duckdb"
        with pytest.raises(AnnotationError) as raised:
            run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=adapter,
                store=_CloseAuthorityLosingStore(local),
            )

    assert isinstance(raised.value.__cause__, EvidenceClaimLostError)
    assert not output.exists()


def test_annotation_preserves_primary_failure_when_session_close_also_fails(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    delegate = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    cancellation = KeyboardInterrupt()
    with LocalStore.open(tmp_path / "store") as local:
        with pytest.raises(KeyboardInterrupt) as raised:
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=_CancelAdapter(delegate, cancellation),
                store=_CloseFailingStore(local),
            )
    assert raised.value is cancellation
    assert any("cleanup also failed" in note for note in raised.value.__notes__)


def test_annotation_surfaces_session_close_failure_after_success(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    with LocalStore.open(tmp_path / "store") as local:
        with pytest.raises(RuntimeError, match="injected close failure"):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=adapter,
                store=_CloseFailingStore(local),
            )
