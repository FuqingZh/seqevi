from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from dplib.actions.package.check import check_package

from seqevi.annotate import run_annotation
from seqevi.errors import AnnotationError
from seqevi.evidence import EvidenceQuery, sha256_digest
from seqevi.sequence import read_fasta, unique_identities
from seqevi.store import LocalStore

from .support import (
    FixtureAdapter,
    NeverRunAdapter,
    write_fixture_database,
    write_fixture_tool,
)


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
    descriptor.pop("created")
    repeated_descriptor.pop("created")
    assert repeated_descriptor == descriptor
    for filename in ("evidence.parquet", "sequence-map.tsv", "no-hits.parquet"):
        assert (first.output_dir / filename).read_bytes() == (
            repeated.output_dir / filename
        ).read_bytes()


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
