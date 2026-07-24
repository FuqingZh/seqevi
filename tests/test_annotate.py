from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import polars as pl
import pytest
from dplib.actions.package.check import check_package
from polars.testing import assert_frame_equal

import seqevi.annotate
from seqevi.adapters import AdapterBatchResult
from seqevi.annotate import run_annotation
from seqevi.errors import AdapterError, AnnotationError, FastaValidationError
from seqevi.evidence import (
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
    sha256_digest,
)
from seqevi.sequence import read_fasta, unique_identities
from seqevi.store import LocalStore

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
    write_fixture_database,
    write_fixture_tool,
)


class _CountingStore:
    def __init__(self, delegate: LocalStore) -> None:
        self.delegate = delegate
        self.lookup_sizes: list[int] = []
        self.commit_sizes: list[int] = []
        self.fetch_sizes: list[int] = []

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


def write_input(path: Path) -> Path:
    path.write_text(
        ">hit-a first header\nMPEPTIDE\n"
        ">hit-alias duplicate content\nMPEPTIDE\n"
        ">none terminal no-hit\nMNOHITX\n",
        encoding="utf-8",
    )
    return path


def test_annotation_materializes_complete_package_and_reuses_cache(
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

    expected_files = {
        "datapackage.json",
        "evidence.parquet",
        "no-hits.parquet",
        "sequence-map.tsv",
    }
    assert {path.name for path in first.output_dir.iterdir()} == expected_files
    assert check_package(str(first.output_dir / "datapackage.json")) == []

    df_map = pl.read_csv(first.output_dir / "sequence-map.tsv", separator="\t")
    assert df_map.get_column("InputOrder").to_list() == [1, 2, 3]
    assert df_map.get_column("EvidenceSource").to_list() == [
        "computed",
        "computed",
        "computed",
    ]
    assert df_map.get_column("SequenceID").n_unique() == 2
    assert pl.read_parquet(first.output_dir / "evidence.parquet").height == 1
    assert pl.read_parquet(first.output_dir / "no-hits.parquet").height == 1

    df_cached_map = pl.read_csv(second.output_dir / "sequence-map.tsv", separator="\t")
    assert set(df_cached_map.get_column("EvidenceSource")) == {"cache"}

    descriptor = json.loads(
        (first.output_dir / "datapackage.json").read_text(encoding="utf-8")
    )
    assert descriptor["$schema"].endswith("/2.0/datapackage.json")
    assert descriptor["seqevi"]["adapter"] == "fixture"
    assert descriptor["seqevi"]["extensionVersion"] == "1.0"
    resources_by_name = {
        resource["name"]: resource for resource in descriptor["resources"]
    }
    assert resources_by_name["evidence"]["rowCount"] == 1
    assert resources_by_name["sequence-map"]["rowCount"] == 3
    assert resources_by_name["no-hits"]["rowCount"] == 1
    for resource in resources_by_name.values():
        data = (first.output_dir / resource["path"]).read_bytes()
        assert resource["bytes"] == len(data)
        assert resource["hash"] == f"sha256:{sha256_digest(data)}"

    repeated_descriptor = json.loads(
        (repeated.output_dir / "datapackage.json").read_text(encoding="utf-8")
    )
    for resource in descriptor["resources"]:
        resource.pop("bytes")
        resource.pop("hash")
    for resource in repeated_descriptor["resources"]:
        resource.pop("bytes")
        resource.pop("hash")
    descriptor.pop("created")
    repeated_descriptor.pop("created")
    assert repeated_descriptor == descriptor
    assert_frame_equal(
        pl.read_parquet(first.output_dir / "evidence.parquet"),
        pl.read_parquet(repeated.output_dir / "evidence.parquet"),
    )
    assert_frame_equal(
        pl.read_parquet(first.output_dir / "no-hits.parquet"),
        pl.read_parquet(repeated.output_dir / "no-hits.parquet"),
    )
    assert (first.output_dir / "sequence-map.tsv").read_text(encoding="utf-8") == (
        repeated.output_dir / "sequence-map.tsv"
    ).read_text(encoding="utf-8")


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

    df_evidence = pl.read_parquet(summary.output_dir / "evidence.parquet")
    df_no_hits = pl.read_parquet(summary.output_dir / "no-hits.parquet")
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
    monkeypatch.setattr(seqevi.annotate, "_STORE_BATCH_SIZE", 2)
    monkeypatch.setattr(seqevi.annotate, "_ANNOTATION_BATCH_SIZE", 3)
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


def test_completed_batch_is_reused_after_later_tool_batch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(seqevi.annotate, "_STORE_BATCH_SIZE", 2)
    monkeypatch.setattr(seqevi.annotate, "_ANNOTATION_BATCH_SIZE", 2)
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
