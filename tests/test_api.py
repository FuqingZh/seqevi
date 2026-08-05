from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import seqevi
import seqevi.api
from seqevi.errors import AnnotationError

from .support import FixtureAdapter, write_fixture_database, write_fixture_tool


def test_public_annotate_returns_native_relation_and_scan_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein first\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")

    monkeypatch.setattr(
        seqevi.api,
        "create_adapter",
        lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
    )

    relation = seqevi.annotate(
        fasta,
        adapter="interpro-pfam",
        executable=executable,
        resource=database,
        store=tmp_path / "store",
        output=tmp_path / "annotations.duckdb",
    )

    assert isinstance(relation, duckdb.DuckDBPyRelation)
    assert "InputID" in relation.columns
    assert relation.filter("EvidenceStatus = 'hit'").count("*").fetchone() == (1,)
    assert relation.select("InputID", "Annotation").fetchall() == [("protein", "MPE")]
    assert relation.pl(lazy=True).collect().height == 1
    assert relation.to_arrow_reader().read_all().num_rows == 1

    scanned = seqevi.scan_annotations(tmp_path / "annotations.duckdb")
    assert scanned.columns == relation.columns
    assert scanned.fetchall() == relation.fetchall()
    relation.close()
    scanned.close()

    with duckdb.connect(
        str(tmp_path / "annotations.duckdb"),
        read_only=True,
        config={"storage_compatibility_version": "v1.0.0"},
    ) as connection:
        assert connection.table("_seqevi.column_info").filter(
            "RelationName = 'annotations'"
        ).count("*").fetchone() == (9,)


def test_public_annotate_rejects_existing_result_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    output = tmp_path / "annotations.duckdb"
    output.write_bytes(b"sentinel")
    monkeypatch.setattr(
        seqevi.api,
        "create_adapter",
        lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
    )

    with pytest.raises(AnnotationError, match="already exists"):
        seqevi.annotate(
            fasta,
            adapter="interpro-pfam",
            executable=executable,
            resource=database,
            store=tmp_path / "store",
            output=output,
        )
    assert output.read_bytes() == b"sentinel"
