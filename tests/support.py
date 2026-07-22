from __future__ import annotations

import gzip
import json
import shutil
import stat
import sys
from collections.abc import Mapping
from pathlib import Path

import polars as pl

from seqevi.adapters import (
    AdapterBatchResult,
    AdapterContract,
    AdapterSequenceResult,
)
from seqevi.errors import AdapterError
from seqevi.evidence import ArtifactFile, EvidenceStatus, sha256_digest
from seqevi.runner import ToolCommand, ToolRunner
from seqevi.sequence import SequenceIdentity

_EVIDENCE_SCHEMA: Mapping[str, pl.DataType] = {
    "SequenceID": pl.String(),
    "Annotation": pl.String(),
}


class FixtureAdapter:
    def __init__(self, *, executable: Path, database: Path) -> None:
        self.executable = executable
        self.database = database
        self._contract = AdapterContract.from_parameters(
            name="fixture",
            version="fixture/1",
            tool_runtime_digest=f"sha256:{sha256_digest(executable.read_bytes())}",
            resource_id=(
                "fixture-db:sha256:"
                f"{sha256_digest((database / 'mode.txt').read_bytes())}"
            ),
            semantic_parameters={"mode": "deterministic"},
        )

    @property
    def contract(self) -> AdapterContract:
        return self._contract

    @property
    def evidence_schema(self) -> Mapping[str, pl.DataType]:
        return _EVIDENCE_SCHEMA

    def run_batch(
        self,
        *,
        identities: tuple[SequenceIdentity, ...],
        input_fasta: Path,
        work_dir: Path,
        runner: ToolRunner,
        timeout_seconds: float | None,
        threads: int,
    ) -> AdapterBatchResult:
        if threads < 1:
            raise AdapterError("fixture threads must be positive")
        raw_path = work_dir / "fixture-output.tsv"
        result = runner.run(
            ToolCommand(
                arguments=(
                    str(self.executable),
                    "--input",
                    str(input_fasta),
                    "--output",
                    str(raw_path),
                    "--database",
                    str(self.database),
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "stdout.log",
                stderr_path=work_dir / "stderr.log",
            ),
            timeout_seconds=timeout_seconds,
        )
        if result.return_code != 0:
            raise AdapterError(
                f"fixture executable exited with {result.return_code}; "
                f"stderr: {result.stderr_path}"
            )
        if not raw_path.is_file():
            raise AdapterError("fixture executable did not create its primary output")

        try:
            frame = pl.read_csv(raw_path, separator="\t")
        except Exception as error:
            raise AdapterError(f"fixture output is malformed: {error}") from error

        required = {"SequenceID", "Status", "Annotation"}
        if set(frame.columns) != required:
            raise AdapterError("fixture output has an unexpected schema")
        expected_ids = {identity.sequence_id for identity in identities}
        observed_ids = set(frame.get_column("SequenceID").to_list())
        if expected_ids != observed_ids or frame.height != len(expected_ids):
            raise AdapterError("fixture output does not account for every sequence")

        statuses = set(frame.get_column("Status").to_list())
        if not statuses.issubset({"hit", "no_hit"}):
            raise AdapterError("fixture output contains an invalid status")

        df_hits = frame.filter(pl.col("Status") == "hit").select(
            "SequenceID", "Annotation"
        )
        normalized: ArtifactFile | None = None
        if df_hits.height:
            normalized_path = work_dir / "fixture.normalized.parquet"
            df_hits.write_parquet(normalized_path, compression="zstd")
            normalized = ArtifactFile.from_path(
                normalized_path,
                "application/vnd.apache.parquet",
            )

        sequence_results = []
        for row in frame.sort("SequenceID").iter_rows(named=True):
            status = EvidenceStatus(row["Status"])
            payload = (
                json.dumps(
                    {
                        "Annotation": row["Annotation"],
                        "SequenceID": row["SequenceID"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                if status is EvidenceStatus.HIT
                else f"no_hit:{row['SequenceID']}".encode()
            )
            sequence_results.append(
                AdapterSequenceResult(
                    sequence_id=row["SequenceID"],
                    status=status,
                    payload_digest=sha256_digest(payload),
                )
            )

        compressed_path = work_dir / "fixture-output.tsv.gz"
        with (
            raw_path.open("rb") as source_handle,
            compressed_path.open("wb") as target_handle,
            gzip.GzipFile(fileobj=target_handle, mode="wb", mtime=0) as compressed,
        ):
            shutil.copyfileobj(source_handle, compressed)
        return AdapterBatchResult(
            sequences=tuple(sequence_results),
            raw_artifact=ArtifactFile.from_path(compressed_path, "application/gzip"),
            normalized_artifact=normalized,
        )


class NeverRunAdapter:
    def __init__(self, delegate: FixtureAdapter) -> None:
        self.contract = delegate.contract
        self.evidence_schema = delegate.evidence_schema

    def run_batch(self, **_kwargs: object) -> AdapterBatchResult:
        raise AssertionError("adapter must not run when all evidence is cached")


def write_fixture_tool(path: Path) -> Path:
    script = f"""#!{sys.executable}
import argparse
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--database", required=True)
args = parser.parse_args()
mode = (Path(args.database) / "mode.txt").read_text().strip()
if mode == "fail":
    print("fixture failure", file=sys.stderr)
    raise SystemExit(7)
if mode == "sleep":
    time.sleep(10)

records = []
header = None
sequence = []
for line in Path(args.input).read_text().splitlines():
    if line.startswith(">"):
        if header is not None:
            records.append((header, "".join(sequence)))
        header = line[1:]
        sequence = []
    else:
        sequence.append(line.strip())
if header is not None:
    records.append((header, "".join(sequence)))

if mode == "malformed":
    Path(args.output).write_text("not-a-valid-table\\n")
    raise SystemExit(0)
if mode == "missing-output":
    raise SystemExit(0)

lines = ["SequenceID\\tStatus\\tAnnotation"]
for sequence_id, sequence in records:
    if sequence.endswith("X"):
        lines.append(f"{{sequence_id}}\\tno_hit\\t")
    else:
        lines.append(f"{{sequence_id}}\\thit\\t{{sequence[:3]}}")
Path(args.output).write_text("\\n".join(lines) + "\\n")
print(f"processed {{len(records)}} sequences")
"""
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def write_fixture_database(path: Path, *, mode: str = "success") -> Path:
    path.mkdir()
    (path / "mode.txt").write_text(mode, encoding="utf-8")
    return path


def write_artifact_file(path: Path, data: bytes, media_type: str) -> ArtifactFile:
    """Write a caller-owned artifact fixture without retaining payload bytes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return ArtifactFile.from_path(path, media_type)
