"""eggNOG-mapper protein annotation adapter."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import re
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl

from seqevi.errors import AdapterError
from seqevi.evidence import ArtifactPayload, EvidenceStatus, sha256_digest
from seqevi.runner import ToolCommand, ToolRunner, ToolTimeoutError
from seqevi.sequence import SequenceIdentity

from .base import AdapterBatchResult, AdapterContract, AdapterSequenceResult

ADAPTER_CONTRACT_VERSION = "eggnog/1"

_NATIVE_COLUMNS = (
    "query",
    "seed_ortholog",
    "evalue",
    "score",
    "eggNOG_OGs",
    "max_annot_lvl",
    "COG_category",
    "Description",
    "Preferred_name",
    "GOs",
    "EC",
    "KEGG_ko",
    "KEGG_Pathway",
    "KEGG_Module",
    "KEGG_Reaction",
    "KEGG_rclass",
    "BRITE",
    "KEGG_TC",
    "CAZy",
    "BiGG_Reaction",
    "PFAMs",
)

EGGNOG_EVIDENCE_SCHEMA: Mapping[str, pl.DataType] = {
    "SequenceID": pl.String(),
    **{
        column: pl.Float64() if column in {"evalue", "score"} else pl.String()
        for column in _NATIVE_COLUMNS
    },
}

_VERSION_PATTERN = re.compile(r"\bemapper-(2\.\d+\.\d+)\b")
_EXPECTED_DB_PATTERN = re.compile(r"Expected eggNOG DB version:\s*([^\s/]+)")
_INSTALLED_DB_PATTERN = re.compile(r"Installed eggNOG DB version:\s*([^\s/]+)")
_PROBE_TIMEOUT_SECONDS = 120.0
_REQUIRED_DATABASE_FILES = (
    "eggnog.db",
    "eggnog.taxa.db",
    "eggnog_proteins.dmnd",
)


@dataclass(frozen=True, slots=True)
class EggnogParameters:
    """Fixed scientific parameters for the v1 eggNOG protein contract."""

    search_mode: str = "diamond"
    input_type: str = "proteins"
    seed_ortholog_evalue: float = 0.001
    tax_scope: str = "auto"
    target_orthologs: str = "all"
    go_evidence: str = "non-electronic"
    pfam_realign: str = "none"

    def __post_init__(self) -> None:
        values = tuple(asdict(self).values())
        if values != (
            "diamond",
            "proteins",
            0.001,
            "auto",
            "all",
            "non-electronic",
            "none",
        ):
            raise ValueError("eggnog/1 uses one fixed protein annotation contract")

    def as_semantic_parameters(self) -> dict[str, object]:
        """Return every result-affecting parameter with explicit defaults."""

        return asdict(self)


class EggnogAdapter:
    """Run eggNOG-mapper 2.x and validate its native annotations table."""

    def __init__(
        self,
        *,
        executable: Path,
        database: Path,
        parameters: EggnogParameters | None = None,
    ) -> None:
        self.executable = executable.resolve()
        self.database = database.resolve()
        self.parameters = parameters or EggnogParameters()
        if not self.executable.is_file():
            raise AdapterError(f"eggNOG-mapper executable is not a file: {executable}")
        if not self.database.is_dir():
            raise AdapterError(f"eggNOG database is not a directory: {database}")

        version_output = _probe_version(self.executable)
        tool_version, database_version = _parse_version_output(version_output)
        runtime_digest = _runtime_digest(self.executable, version_output)
        resource_id = _resource_id(self.database, database_version)
        self._contract = AdapterContract.from_parameters(
            name="eggnog",
            version=ADAPTER_CONTRACT_VERSION,
            tool_runtime_digest=f"sha256:{runtime_digest}",
            resource_id=resource_id,
            semantic_parameters=self.parameters.as_semantic_parameters(),
        )
        self.tool_version = tool_version
        self.database_version = database_version

    @property
    def contract(self) -> AdapterContract:
        return self._contract

    @property
    def evidence_schema(self) -> Mapping[str, pl.DataType]:
        return EGGNOG_EVIDENCE_SCHEMA

    def run_batch(
        self,
        *,
        identities: tuple[SequenceIdentity, ...],
        input_fasta: Path,
        work_dir: Path,
        runner: ToolRunner,
        timeout_seconds: float | None,
    ) -> AdapterBatchResult:
        """Run one deterministic cache-miss batch and validate every row."""

        if not identities:
            raise AdapterError("eggnog batch must not be empty")
        output_name = "seqevi"
        raw_path = work_dir / f"{output_name}.emapper.annotations"
        parameters = self.parameters
        result = runner.run(
            ToolCommand(
                arguments=(
                    str(self.executable),
                    "--input",
                    str(input_fasta),
                    "--itype",
                    parameters.input_type,
                    "--output",
                    output_name,
                    "--output_dir",
                    str(work_dir),
                    "--data_dir",
                    str(self.database),
                    "--cpu",
                    "1",
                    "--override",
                    "--mode",
                    parameters.search_mode,
                    "--seed_ortholog_evalue",
                    str(parameters.seed_ortholog_evalue),
                    "--tax_scope",
                    parameters.tax_scope,
                    "--target_orthologs",
                    parameters.target_orthologs,
                    "--go_evidence",
                    parameters.go_evidence,
                    "--pfam_realign",
                    parameters.pfam_realign,
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "eggnog.stdout.log",
                stderr_path=work_dir / "eggnog.stderr.log",
            ),
            timeout_seconds=timeout_seconds,
        )
        if result.return_code != 0:
            raise AdapterError(
                f"eggNOG-mapper exited with {result.return_code}; "
                f"stderr: {result.stderr_path}"
            )
        if not raw_path.is_file():
            raise AdapterError("eggNOG-mapper did not create its annotations output")

        raw = raw_path.read_bytes()
        frame = _parse_annotations(raw, identities=identities)
        normalized = _normalized_artifact(frame)
        hit_ids = set(frame.get_column("SequenceID"))
        sequence_results = tuple(
            _sequence_result(identity, frame=frame, hit_ids=hit_ids)
            for identity in sorted(identities, key=lambda item: item.sequence_id)
        )
        return AdapterBatchResult(
            sequences=sequence_results,
            raw_artifact=ArtifactPayload(
                gzip.compress(raw, mtime=0), "application/gzip"
            ),
            normalized_artifact=normalized,
        )


def _probe_version(executable: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="seqevi-eggnog-probe-") as raw_dir:
        root = Path(raw_dir)
        stdout_path = root / "stdout.log"
        stderr_path = root / "stderr.log"
        try:
            result = ToolRunner().run(
                ToolCommand(
                    arguments=(str(executable), "--version"),
                    working_dir=executable.parent,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                ),
                timeout_seconds=_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, ToolTimeoutError) as error:
            raise AdapterError(
                f"eggNOG-mapper version probe failed: {error}"
            ) from error
        output = "\n".join(
            (
                stdout_path.read_text(encoding="utf-8", errors="replace"),
                stderr_path.read_text(encoding="utf-8", errors="replace"),
            )
        ).strip()
        if result.return_code != 0:
            raise AdapterError(
                f"eggNOG-mapper version probe exited with {result.return_code}: {output}"
            )
        return output


def _parse_version_output(output: str) -> tuple[str, str]:
    tool_versions = sorted(set(_VERSION_PATTERN.findall(output)))
    expected = sorted(set(_EXPECTED_DB_PATTERN.findall(output)))
    installed = sorted(set(_INSTALLED_DB_PATTERN.findall(output)))
    if len(tool_versions) != 1:
        raise AdapterError(
            "eggnog/1 requires exactly one eggNOG-mapper 2.x release in --version"
        )
    if len(expected) != 1 or len(installed) != 1 or expected != installed:
        raise AdapterError(
            "eggNOG-mapper must report one matching expected and installed DB version"
        )
    return tool_versions[0], installed[0]


def _runtime_digest(executable: Path, version_output: str) -> str:
    identity = {
        "executable_sha256": _file_sha256(executable),
        "version_output": version_output,
    }
    return sha256_digest(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _resource_id(database: Path, version: str) -> str:
    components = []
    for name in _REQUIRED_DATABASE_FILES:
        path = database / name
        if not path.is_file():
            raise AdapterError(f"eggNOG database file does not exist: {path}")
        components.append((name, _file_sha256(path)))
    digest = sha256_digest(
        json.dumps(components, separators=(",", ":")).encode("utf-8")
    )
    return f"eggnog/{version}/sha256:{digest}"


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _parse_annotations(
    raw: bytes, *, identities: tuple[SequenceIdentity, ...]
) -> pl.DataFrame:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AdapterError(
            f"eggNOG annotations are not valid UTF-8: {error}"
        ) from error

    expected = {identity.sequence_id: identity for identity in identities}
    header: tuple[str, ...] | None = None
    rows: list[dict[str, object]] = []
    seen_queries: set[str] = set()
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line:
            raise AdapterError(
                f"eggNOG annotations contain a blank line at {line_number}"
            )
        if line.startswith("##"):
            continue
        if line.startswith("#"):
            candidate = tuple(line.removeprefix("#").split("\t"))
            if candidate[0] == "query":
                if header is not None:
                    raise AdapterError("eggNOG annotations contain duplicate headers")
                header = candidate
            continue
        if header is None:
            raise AdapterError("eggNOG annotations data appeared before its header")
        fields = next(csv.reader((line,), delimiter="\t"))
        if len(fields) != len(header):
            raise AdapterError(
                f"eggNOG annotations line {line_number} has {len(fields)} columns; "
                f"expected {len(header)}"
            )
        if header != _NATIVE_COLUMNS:
            raise AdapterError(
                "eggnog/1 requires the canonical eggNOG-mapper 2.x annotations schema"
            )
        row = _parse_row(header, fields, expected=expected, line_number=line_number)
        query = str(row["SequenceID"])
        if query in seen_queries:
            raise AdapterError(f"eggNOG annotations contain duplicate query: {query}")
        seen_queries.add(query)
        rows.append(row)

    if header is None:
        raise AdapterError("eggNOG annotations are missing the #query header")
    if header != _NATIVE_COLUMNS:
        raise AdapterError(
            "eggnog/1 requires the canonical eggNOG-mapper 2.x annotations schema"
        )
    frame = pl.DataFrame(rows, schema=EGGNOG_EVIDENCE_SCHEMA)
    return frame.sort("SequenceID") if not frame.is_empty() else frame


def _parse_row(
    header: tuple[str, ...],
    fields: list[str],
    *,
    expected: Mapping[str, SequenceIdentity],
    line_number: int,
) -> dict[str, object]:
    native = dict(zip(header, fields, strict=True))
    query = native["query"]
    if query not in expected:
        raise AdapterError(
            f"eggNOG annotations line {line_number} has unknown SequenceID: {query}"
        )
    row: dict[str, object] = {"SequenceID": query}
    for column in _NATIVE_COLUMNS:
        value = native[column]
        if column in {"evalue", "score"}:
            try:
                parsed = float(value)
            except ValueError as error:
                raise AdapterError(
                    f"eggNOG annotations line {line_number} has invalid {column}: {value}"
                ) from error
            if not math.isfinite(parsed):
                raise AdapterError(
                    f"eggNOG annotations line {line_number} has non-finite {column}"
                )
            row[column] = parsed
        else:
            row[column] = None if value == "-" else value
    return row


def _normalized_artifact(frame: pl.DataFrame) -> ArtifactPayload | None:
    if frame.is_empty():
        return None
    buffer = io.BytesIO()
    frame.write_parquet(buffer, compression="zstd")
    return ArtifactPayload(buffer.getvalue(), "application/vnd.apache.parquet")


def _sequence_result(
    identity: SequenceIdentity,
    *,
    frame: pl.DataFrame,
    hit_ids: set[str],
) -> AdapterSequenceResult:
    if identity.sequence_id not in hit_ids:
        payload = json.dumps(
            {"SequenceID": identity.sequence_id, "Status": "no_hit"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return AdapterSequenceResult(
            sequence_id=identity.sequence_id,
            status=EvidenceStatus.NO_HIT,
            payload_digest=sha256_digest(payload),
        )
    row = frame.filter(pl.col("SequenceID") == identity.sequence_id).row(0, named=True)
    payload = json.dumps(
        row,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return AdapterSequenceResult(
        sequence_id=identity.sequence_id,
        status=EvidenceStatus.HIT,
        payload_digest=sha256_digest(payload),
    )
