"""Measure the Slice A DuckDB relation prototype at fixture scale."""

from __future__ import annotations

import argparse
import json
import platform
import resource
import time
from pathlib import Path

import polars as pl

from seqevi.result import RESULT_FORMAT_VERSION, build_result_prototype
from seqevi.sequence import InputSequence, identify_protein_sequence


def _frames(
    count: int,
) -> tuple[pl.DataFrame, pl.DataFrame, tuple[InputSequence, ...]]:
    alphabet = "ACDEFGHIKLMNPQRSTVWY"
    identities = []
    records = []
    for index in range(count):
        value = index
        suffix = []
        for _position in range(6):
            suffix.append(alphabet[value % len(alphabet)])
            value //= len(alphabet)
        # Six base-20 residues cover the 100k fixture while keeping every
        # generated canonical sequence at exactly 240 residues.
        sequence = "M" + (alphabet * 12)[:233] + "".join(suffix)
        identity = identify_protein_sequence(sequence)
        identities.append(identity)
        records.append(
            InputSequence(
                input_order=index + 1,
                input_id=f"protein-{index:09d}",
                input_header=f"protein-{index:09d} fixture",
                identity=identity,
            )
        )
    sequence_ids = [identity.sequence_id for identity in identities]
    md5_values = [identity.md5 for identity in identities]
    statuses = ["no_hit" if index % 10 == 0 else "hit" for index in range(count)]
    sequence_map = pl.DataFrame(
        {
            "InputOrder": list(range(1, count + 1)),
            "InputID": [f"protein-{index:09d}" for index in range(count)],
            "InputHeader": [f"protein-{index:09d} fixture" for index in range(count)],
            "SequenceID": sequence_ids,
            "MD5": md5_values,
            "Length": [240] * count,
            "EvidenceStatus": statuses,
            "EvidenceSource": ["computed"] * count,
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
            "SequenceID": [
                sequence_ids[index] for index in range(count) if index % 10 != 0
            ],
            "SignatureAccession": [
                f"PF{index % 100000:05d}" for index in range(count) if index % 10 != 0
            ],
            "SignatureDescription": [
                "fixture domain" for index in range(count) if index % 10 != 0
            ],
        },
        schema={
            "SequenceID": pl.String,
            "SignatureAccession": pl.String,
            "SignatureDescription": pl.String,
        },
    )
    return sequence_map, evidence, tuple(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequences", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.sequences < 1:
        parser.error("--sequences must be positive")
    root = args.output.resolve()
    if root.exists():
        raise SystemExit(f"benchmark output already exists: {root}")
    root.mkdir(parents=True)
    result_path = root / "result.duckdb"
    started = time.perf_counter()
    sequence_map, evidence, records = _frames(args.sequences)
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
    duckdb_started = time.perf_counter()
    connection, relation = build_result_prototype(
        sequence_map=sequence_map,
        evidence=evidence,
        metadata=metadata,
        semantic_parameters={"application": "Pfam"},
        run_metrics={"input_records": args.sequences},
        database_path=result_path,
    )
    try:
        annotation_rows = relation.count("*").fetchone()
        no_hit_rows = connection.sql("SELECT count(*) FROM main.no_hits").fetchone()
        assert annotation_rows is not None
        assert no_hit_rows is not None
    finally:
        connection.close()
    duckdb_elapsed = time.perf_counter() - duckdb_started
    report = {
        "schema_version": 1,
        "result_format_version": RESULT_FORMAT_VERSION,
        "sequences": args.sequences,
        "evidence_rows": evidence.height,
        "annotation_rows": int(annotation_rows[0]),
        "no_hit_rows": int(no_hit_rows[0]),
        "duckdb_bytes": result_path.stat().st_size,
        "duckdb_elapsed_seconds": duckdb_elapsed,
        "elapsed_seconds": time.perf_counter() - started,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
    (root / "benchmark.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
