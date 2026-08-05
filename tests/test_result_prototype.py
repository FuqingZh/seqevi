from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from seqevi.adapters import EGGNOG_EVIDENCE_SCHEMA, INTERPRO_PFAM_EVIDENCE_SCHEMA
from seqevi.annotate import run_annotation
from seqevi.evidence import EvidenceSource
from seqevi.errors import OutputPackageError
from seqevi.result import (
    RESULT_FORMAT_VERSION,
    build_result_prototype,
    materialize_result_database,
)
from seqevi.sequence import identify_protein_sequence, read_fasta
from seqevi.store import LocalStore

from .support import FixtureAdapter, write_fixture_database, write_fixture_tool


def _frames() -> tuple[pl.DataFrame, pl.DataFrame, dict[str, str]]:
    hit = identify_protein_sequence("MPEPTIDE")
    no_hit = identify_protein_sequence("MNOHITX")
    sequence_map = pl.DataFrame(
        {
            "InputOrder": [1, 2, 3],
            "InputID": ["hit-a", "hit-alias", "none"],
            "InputHeader": [
                "hit-a first header",
                "hit-alias duplicate content",
                "none terminal no-hit",
            ],
            "SequenceID": [hit.sequence_id, hit.sequence_id, no_hit.sequence_id],
            "MD5": [hit.md5, hit.md5, no_hit.md5],
            "Length": [hit.length, hit.length, no_hit.length],
            "EvidenceStatus": ["hit", "hit", "no_hit"],
            "EvidenceSource": ["computed", "computed", "computed"],
        },
        schema={
            "InputOrder": pl.Int64,
            "InputID": pl.String,
            "InputHeader": pl.String,
            "SequenceID": pl.String,
            "MD5": pl.String,
            "Length": pl.Int64,
            "EvidenceStatus": pl.String,
            "EvidenceSource": pl.String,
        },
    )
    evidence = pl.DataFrame(
        {
            "SequenceID": [hit.sequence_id],
            "SignatureAccession": ["PF00001"],
            "SignatureDescription": ["fixture domain"],
        },
        schema={
            "SequenceID": pl.String,
            "SignatureAccession": pl.String,
            "SignatureDescription": pl.String,
        },
    )
    metadata = {
        "ResultFormatVersion": RESULT_FORMAT_VERSION,
        "ResultSchemaID": "interproscan-pfam/5",
        "SeqEviVersion": "0.2.0-dev",
        "Adapter": "interpro-pfam",
        "AdapterContractVersion": "interpro-pfam/1",
        "UpstreamTool": "InterProScan",
        "UpstreamToolVersion": "5.77-108.0",
        "ToolRuntimeDigest": "sha256:" + "a" * 64,
        "ResourceID": "interpro/108.0",
        "InputDigest": "sha256:" + "b" * 64,
        "CreatedAt": "2026-08-04T00:00:00Z",
    }
    return sequence_map, evidence, metadata


def test_result_prototype_exposes_joinable_relation_and_catalog() -> None:
    sequence_map, evidence, metadata = _frames()
    connection, relation = build_result_prototype(
        sequence_map=sequence_map,
        evidence=evidence,
        metadata=metadata,
        semantic_parameters={"application": "Pfam", "disable_precalc": True},
        run_metrics={"input_records": 3, "computed": 2},
    )
    try:
        assert relation.columns == [
            "InputOrder",
            "InputID",
            "InputHeader",
            "SequenceID",
            "MD5",
            "Length",
            "EvidenceStatus",
            "EvidenceSource",
            "SignatureAccession",
            "SignatureDescription",
        ]
        rows = relation.order("InputOrder").fetchall()
        assert [row[1] for row in rows] == ["hit-a", "hit-alias", "none"]
        assert [row[8] for row in rows] == ["PF00001", "PF00001", None]
        assert connection.sql("SELECT * FROM main.no_hits").fetchall() == [
            (sequence_map["SequenceID"][2], sequence_map["MD5"][2], 7)
        ]
        metadata_row = connection.table("_seqevi.metadata").fetchone()
        assert metadata_row is not None
        assert metadata_row[0] == RESULT_FORMAT_VERSION
        assert connection.table("_seqevi.run_metrics").count("*").fetchone() == (1,)
        catalog_columns = connection.table("_seqevi.column_info").filter(
            "RelationName = 'annotations'"
        )
        assert catalog_columns.count("*").fetchone() == (10,)
        input_column = catalog_columns.filter("ColumnName = 'InputID'").fetchone()
        assert input_column is not None
        assert input_column[4].startswith("First whitespace")
    finally:
        connection.close()


def test_result_prototype_can_be_reopened_read_only(tmp_path: Path) -> None:
    sequence_map, evidence, metadata = _frames()
    path = tmp_path / "annotations.duckdb"
    connection, _relation = build_result_prototype(
        sequence_map=sequence_map,
        evidence=evidence,
        metadata=metadata,
        semantic_parameters={},
        run_metrics={},
        database_path=path,
    )
    connection.close()

    read_only = duckdb.connect(str(path), read_only=True)
    try:
        assert read_only.sql("SELECT * FROM main.annotations").count(
            "*"
        ).fetchone() == (3,)
        evidence_table = (
            read_only.table("_seqevi.table_info")
            .filter("RelationName = 'evidence'")
            .fetchone()
        )
        assert evidence_table is not None
        assert evidence_table[4] == 1
    finally:
        read_only.close()


def test_result_prototype_accepts_current_eggnog_and_interpro_schemas() -> None:
    sequence_map, _fixture_evidence, metadata = _frames()
    hit_sequence_id = sequence_map["SequenceID"][0]
    for schema, result_schema_id in (
        (EGGNOG_EVIDENCE_SCHEMA, "eggnog-mapper/2"),
        (INTERPRO_PFAM_EVIDENCE_SCHEMA, "interproscan-pfam/5"),
    ):
        values: dict[str, list[object]] = {}
        for column, _data_type in schema.items():
            if column == "SequenceID":
                values[column] = [hit_sequence_id]
            elif column in {"evalue", "score", "Score"}:
                values[column] = [0.001]
            elif column in {"SequenceLength", "Start", "Stop"}:
                values[column] = [1]
            else:
                values[column] = ["fixture"]
        evidence = pl.DataFrame(values, schema=schema)
        current_metadata = {
            **metadata,
            "ResultSchemaID": result_schema_id,
        }
        connection, relation = build_result_prototype(
            sequence_map=sequence_map,
            evidence=evidence,
            metadata=current_metadata,
            semantic_parameters={"fixture": True},
            run_metrics={"input_records": 3},
        )
        try:
            assert relation.columns == [
                *sequence_map.columns,
                *[column for column in evidence.columns if column != "SequenceID"],
            ]
            assert relation.count("*").fetchone() == (3,)
            assert connection.sql("SELECT count(*) FROM main.no_hits").fetchone() == (
                1,
            )
        finally:
            connection.close()


def test_result_writer_publishes_atomic_file_from_store_artifacts(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">hit-a first header\nMPEPTIDE\n>none terminal no-hit\nMNOHITX\n",
        encoding="utf-8",
    )
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    output = tmp_path / "annotations.duckdb"
    with LocalStore.open(tmp_path / "store") as store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "legacy-output",
            adapter=adapter,
            store=store,
        )
        records = read_fasta(fasta)
        fetched = {
            record.identity.sequence_id: store.fetch(
                adapter.contract.evidence_key(record.identity)
            )
            for record in records
        }
        assert all(value is not None for value in fetched.values())
        fetched_by_sequence_id = {
            sequence_id: value
            for sequence_id, value in fetched.items()
            if value is not None
        }
        materialize_result_database(
            output_path=output,
            records=records,
            input_record_count=len(records),
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id={
                record.identity.sequence_id: EvidenceSource.COMPUTED
                for record in records
            },
            evidence_schema=adapter.evidence_schema,
            adapter_contract=adapter.contract,
            input_digest="a" * 64,
            metadata={
                "ResultFormatVersion": RESULT_FORMAT_VERSION,
                "ResultSchemaID": "fixture/1",
                "SeqEviVersion": "0.2.0-dev",
                "Adapter": "fixture",
                "AdapterContractVersion": adapter.contract.version,
                "UpstreamTool": "fixture",
                "UpstreamToolVersion": "1",
                "ToolRuntimeDigest": adapter.contract.tool_runtime_digest,
                "ResourceID": adapter.contract.resource_id,
                "InputDigest": "a" * 64,
                "CreatedAt": "2026-08-04T00:00:00Z",
            },
            run_metrics={"input_records": len(records)},
        )

    assert output.is_file()
    assert not list(tmp_path.glob(f".{output.name}.*"))
    read_only = duckdb.connect(str(output), read_only=True)
    try:
        assert read_only.sql("SELECT count(*) FROM main.annotations").fetchone() == (2,)
        assert read_only.sql("SELECT count(*) FROM main.no_hits").fetchone() == (1,)
        metadata_row = read_only.table("_seqevi.metadata").fetchone()
        assert metadata_row is not None
        assert metadata_row[0] == RESULT_FORMAT_VERSION
    finally:
        read_only.close()


def test_result_writer_failure_does_not_publish_partial_file(tmp_path: Path) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    record = read_fasta(fasta)[0]
    output = tmp_path / "failed.duckdb"
    metadata = {
        "ResultFormatVersion": RESULT_FORMAT_VERSION,
        "ResultSchemaID": "fixture/1",
        "SeqEviVersion": "0.2.0-dev",
        "Adapter": "fixture",
        "AdapterContractVersion": adapter.contract.version,
        "UpstreamTool": "fixture",
        "UpstreamToolVersion": "1",
        "ToolRuntimeDigest": adapter.contract.tool_runtime_digest,
        "ResourceID": adapter.contract.resource_id,
        "InputDigest": "a" * 64,
        "CreatedAt": "2026-08-04T00:00:00Z",
    }
    with pytest.raises(OutputPackageError, match="missing terminal evidence"):
        materialize_result_database(
            output_path=output,
            records=(record,),
            input_record_count=1,
            fetched_by_sequence_id={},
            source_by_sequence_id={},
            evidence_schema=adapter.evidence_schema,
            adapter_contract=adapter.contract,
            input_digest="a" * 64,
            metadata=metadata,
            run_metrics={},
        )
    assert not output.exists()
    assert not list(tmp_path.glob(f".{output.name}.*"))
