"""Direct InterProScan/Pfam annotation adapter."""

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
from datetime import datetime
from pathlib import Path

import polars as pl

from seqevi.errors import AdapterError
from seqevi.evidence import ArtifactPayload, EvidenceStatus, sha256_digest
from seqevi.runner import ToolCommand, ToolRunner, ToolTimeoutError
from seqevi.sequence import SequenceIdentity

from .base import AdapterBatchResult, AdapterContract, AdapterSequenceResult

ADAPTER_CONTRACT_VERSION = "interpro-pfam/1"

INTERPRO_PFAM_EVIDENCE_SCHEMA: Mapping[str, pl.DataType] = {
    "SequenceID": pl.String(),
    "ProteinAccession": pl.String(),
    "SequenceMD5": pl.String(),
    "SequenceLength": pl.Int64(),
    "Analysis": pl.String(),
    "SignatureAccession": pl.String(),
    "SignatureDescription": pl.String(),
    "Start": pl.Int64(),
    "Stop": pl.Int64(),
    "Score": pl.Float64(),
    "Status": pl.String(),
    "InterProAccession": pl.String(),
    "InterProDescription": pl.String(),
}

_VERSION_PATTERN = re.compile(r"\b5\.\d+-\d+\.\d+\b")
_PFAM_VERSION_PATTERN = re.compile(r"\d+(?:\.\d+)+\Z")
_PFAM_ACCESSION_PATTERN = re.compile(r"PF\d{5}\Z")
_INTERPRO_ACCESSION_PATTERN = re.compile(r"IPR\d{6}\Z")
_TSV_COLUMN_COUNT = 13
_PROBE_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class InterProPfamParameters:
    """Fixed scientific parameters for the v1 direct Pfam contract."""

    application: str = "Pfam"
    output_format: str = "TSV"
    disable_precalculated_lookup: bool = True
    include_go_terms: bool = False
    include_pathways: bool = False
    sequence_type: str = "protein"

    def __post_init__(self) -> None:
        values = (
            self.application,
            self.output_format,
            self.disable_precalculated_lookup,
            self.include_go_terms,
            self.include_pathways,
            self.sequence_type,
        )
        if values != ("Pfam", "TSV", True, False, False, "protein"):
            raise ValueError(
                "interpro-pfam/1 uses one fixed direct-scan scientific contract"
            )

    def as_semantic_parameters(self) -> dict[str, object]:
        """Return every result-affecting parameter with explicit defaults."""

        return asdict(self)


class InterProPfamAdapter:
    """Run local InterProScan Pfam and validate its native TSV output."""

    def __init__(
        self,
        *,
        executable: Path,
        database: Path,
        parameters: InterProPfamParameters | None = None,
    ) -> None:
        self.executable = executable.resolve()
        self.database = database.resolve()
        self.parameters = parameters or InterProPfamParameters()
        self.install_dir = self.executable.parent
        self.properties_path = self.install_dir / "interproscan.properties"
        self.jar_path = self.install_dir / "interproscan-5.jar"

        self._validate_installation_paths()
        self.properties = _read_properties(self.properties_path)
        self.interproscan_version = _probe_interproscan_version(self.executable)
        pfam_version, model_path = _resolve_pfam_model(
            database=self.database,
            properties=self.properties,
        )
        runtime_digest = _calculate_runtime_digest(
            install_dir=self.install_dir,
            executable=self.executable,
            jar_path=self.jar_path,
            properties=self.properties,
            version=self.interproscan_version,
        )
        resource_id = _calculate_resource_id(
            interproscan_version=self.interproscan_version,
            pfam_version=pfam_version,
            model_path=model_path,
        )
        self._contract = AdapterContract.from_parameters(
            name="interpro-pfam",
            version=ADAPTER_CONTRACT_VERSION,
            tool_runtime_digest=f"sha256:{runtime_digest}",
            resource_id=resource_id,
            semantic_parameters=self.parameters.as_semantic_parameters(),
        )

    @property
    def contract(self) -> AdapterContract:
        return self._contract

    @property
    def evidence_schema(self) -> Mapping[str, pl.DataType]:
        return INTERPRO_PFAM_EVIDENCE_SCHEMA

    def run_batch(
        self,
        *,
        identities: tuple[SequenceIdentity, ...],
        input_fasta: Path,
        work_dir: Path,
        runner: ToolRunner,
        timeout_seconds: float | None,
    ) -> AdapterBatchResult:
        """Run one deterministic cache-miss batch and validate every result."""

        if not identities:
            raise AdapterError("interpro-pfam batch must not be empty")
        ids = [identity.sequence_id for identity in identities]
        if len(ids) != len(set(ids)):
            raise AdapterError("interpro-pfam batch contains duplicate SequenceIDs")

        raw_path = work_dir / "interpro-pfam.tsv"
        properties_path = work_dir / "interproscan.properties"
        temporary_dir = work_dir / "interproscan-temp"
        _write_runtime_properties(
            source=self.properties_path,
            target=properties_path,
            database=self.database,
        )
        result = runner.run(
            ToolCommand(
                arguments=(
                    str(self.executable),
                    "--input",
                    str(input_fasta),
                    "--applications",
                    self.parameters.application,
                    "--formats",
                    self.parameters.output_format,
                    "--disable-precalc",
                    "--outfile",
                    str(raw_path),
                    "--tempdir",
                    str(temporary_dir),
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "interproscan.stdout.log",
                stderr_path=work_dir / "interproscan.stderr.log",
                environment={"INTERPROSCAN_CONF": str(properties_path)},
            ),
            timeout_seconds=timeout_seconds,
        )
        if result.return_code != 0:
            raise AdapterError(
                f"InterProScan exited with {result.return_code}; "
                f"stderr: {result.stderr_path}"
            )
        if not raw_path.is_file():
            raise AdapterError("InterProScan did not create its TSV output")

        raw = raw_path.read_bytes()
        frame = _parse_tsv(raw, identities=identities)
        normalized = _write_normalized_artifact(frame)
        hit_ids = set(frame.get_column("SequenceID"))
        sequence_results = tuple(
            _sequence_result(identity, frame=frame, hit_ids=hit_ids)
            for identity in sorted(identities, key=lambda item: item.sequence_id)
        )
        return AdapterBatchResult(
            sequences=sequence_results,
            raw_artifact=ArtifactPayload(
                gzip.compress(raw, mtime=0),
                "application/gzip",
            ),
            normalized_artifact=normalized,
        )

    def _validate_installation_paths(self) -> None:
        if not self.executable.is_file():
            raise AdapterError(
                f"InterProScan executable is not a file: {self.executable}"
            )
        if not self.database.is_dir():
            raise AdapterError(
                f"InterProScan data directory does not exist: {self.database}"
            )
        if not self.properties_path.is_file():
            raise AdapterError(
                "InterProScan launcher must be beside interproscan.properties: "
                f"{self.properties_path}"
            )
        if not self.jar_path.is_file():
            raise AdapterError(
                f"InterProScan runtime jar does not exist: {self.jar_path}"
            )


def _probe_interproscan_version(executable: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="seqevi-interpro-probe-") as raw_dir:
        probe_dir = Path(raw_dir)
        stdout_path = probe_dir / "stdout.log"
        stderr_path = probe_dir / "stderr.log"
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
            raise AdapterError(f"InterProScan version probe failed: {error}") from error

        output = "\n".join(
            (
                stdout_path.read_text(encoding="utf-8", errors="replace"),
                stderr_path.read_text(encoding="utf-8", errors="replace"),
            )
        )
        if result.return_code != 0:
            raise AdapterError(
                "InterProScan version probe exited with "
                f"{result.return_code}: {output.strip()}"
            )
        versions = sorted(set(_VERSION_PATTERN.findall(output)))
        if len(versions) != 1:
            raise AdapterError(
                "InterProScan version probe must report exactly one release: "
                f"{versions}"
            )
        return versions[0]


def _read_properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "!")):
            continue
        if "=" not in stripped:
            raise AdapterError(f"invalid InterProScan property at {path}:{line_number}")
        key, value = (part.strip() for part in stripped.split("=", maxsplit=1))
        if not key or key in properties:
            raise AdapterError(
                f"duplicate or empty InterProScan property at {path}:{line_number}"
            )
        properties[key] = value

    required = {
        "bin.directory",
        "binary.hmmer3.path",
        "data.directory",
        "pfam-a.hmm.path",
    }
    missing = sorted(required - properties.keys())
    if missing:
        raise AdapterError(
            f"InterProScan properties are missing required keys: {missing}"
        )
    return properties


def _resolve_pfam_model(
    *, database: Path, properties: Mapping[str, str]
) -> tuple[str, Path]:
    configured = properties["pfam-a.hmm.path"]
    prefix = "${data.directory}/"
    if not configured.startswith(prefix):
        raise AdapterError("pfam-a.hmm.path must be relative to ${data.directory}")
    relative = Path(configured.removeprefix(prefix))
    if relative.is_absolute() or ".." in relative.parts:
        raise AdapterError("pfam-a.hmm.path escapes the InterProScan data directory")
    model_path = (database / relative).resolve()
    if not model_path.is_relative_to(database) or not model_path.is_file():
        raise AdapterError(f"Pfam model file does not exist: {model_path}")
    if len(relative.parts) < 3 or relative.parts[0].lower() != "pfam":
        raise AdapterError(f"unexpected Pfam model path: {relative}")
    pfam_version = relative.parts[1]
    if not _PFAM_VERSION_PATTERN.fullmatch(pfam_version):
        raise AdapterError(f"invalid Pfam release in model path: {pfam_version}")
    return pfam_version, model_path


def _calculate_runtime_digest(
    *,
    install_dir: Path,
    executable: Path,
    jar_path: Path,
    properties: Mapping[str, str],
    version: str,
) -> str:
    bin_dir = _resolve_install_path(
        install_dir,
        properties["bin.directory"],
        variables={},
    )
    hmmer_dir = _resolve_install_path(
        install_dir,
        properties["binary.hmmer3.path"],
        variables={"bin.directory": bin_dir},
    )
    if not hmmer_dir.is_dir():
        raise AdapterError(f"InterProScan HMMER directory does not exist: {hmmer_dir}")
    hmmer_files = sorted(path for path in hmmer_dir.rglob("*") if path.is_file())
    if not hmmer_files:
        raise AdapterError(f"InterProScan HMMER directory is empty: {hmmer_dir}")

    components = [
        ("interproscan.sh", _file_sha256(executable)),
        ("interproscan-5.jar", _file_sha256(jar_path)),
    ]
    components.extend(
        (
            f"hmmer/{path.relative_to(hmmer_dir).as_posix()}",
            _file_sha256(path),
        )
        for path in hmmer_files
    )
    identity = {
        "components": components,
        "interproscan_version": version,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256_digest(encoded)


def _resolve_install_path(
    install_dir: Path,
    raw_value: str,
    *,
    variables: Mapping[str, Path],
) -> Path:
    expanded = raw_value
    for name, path in variables.items():
        expanded = expanded.replace(f"${{{name}}}", str(path))
    if "${" in expanded:
        raise AdapterError(f"unsupported InterProScan property expression: {raw_value}")
    candidate = Path(expanded)
    resolved = (
        candidate.resolve()
        if candidate.is_absolute()
        else (install_dir / candidate).resolve()
    )
    if not resolved.is_relative_to(install_dir):
        raise AdapterError(f"InterProScan runtime path escapes its install: {resolved}")
    return resolved


def _calculate_resource_id(
    *, interproscan_version: str, pfam_version: str, model_path: Path
) -> str:
    interpro_release = interproscan_version.split("-", maxsplit=1)[1]
    return (
        f"interpro/{interpro_release}/pfam/{pfam_version}/"
        f"sha256:{_file_sha256(model_path)}"
    )


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _write_runtime_properties(*, source: Path, target: Path, database: Path) -> None:
    lines = source.read_text(encoding="utf-8").splitlines()
    replaced = False
    output = []
    for line in lines:
        stripped = line.strip()
        if (
            stripped
            and not stripped.startswith(("#", "!"))
            and stripped.split("=", maxsplit=1)[0].strip() == "data.directory"
        ):
            output.append(f"data.directory={database}")
            replaced = True
        else:
            output.append(line)
    if not replaced:
        raise AdapterError("InterProScan properties have no data.directory entry")
    target.write_text("\n".join(output) + "\n", encoding="utf-8")


def _parse_tsv(raw: bytes, *, identities: tuple[SequenceIdentity, ...]) -> pl.DataFrame:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AdapterError(f"InterProScan TSV is not valid UTF-8: {error}") from error

    expected = {identity.sequence_id: identity for identity in identities}
    rows: list[dict[str, object]] = []
    seen_rows: set[tuple[object, ...]] = set()
    reader = csv.reader(io.StringIO(text), delimiter="\t")
    for line_number, fields in enumerate(reader, start=1):
        if not fields or fields == [""]:
            raise AdapterError(
                f"InterProScan TSV contains a blank row at line {line_number}"
            )
        if len(fields) != _TSV_COLUMN_COUNT:
            raise AdapterError(
                f"InterProScan TSV line {line_number} has {len(fields)} columns; "
                f"expected {_TSV_COLUMN_COUNT}"
            )
        row = _parse_tsv_row(fields, line_number=line_number, expected=expected)
        row_key = tuple(row[column] for column in INTERPRO_PFAM_EVIDENCE_SCHEMA)
        if row_key in seen_rows:
            raise AdapterError(
                f"InterProScan TSV contains a duplicate match at line {line_number}"
            )
        seen_rows.add(row_key)
        rows.append(row)

    frame = pl.DataFrame(rows, schema=INTERPRO_PFAM_EVIDENCE_SCHEMA)
    if frame.is_empty():
        return frame
    return frame.sort(
        "SequenceID",
        "Start",
        "Stop",
        "SignatureAccession",
        "Score",
        nulls_last=True,
    )


def _parse_tsv_row(
    fields: list[str],
    *,
    line_number: int,
    expected: Mapping[str, SequenceIdentity],
) -> dict[str, object]:
    (
        accession,
        sequence_md5,
        raw_length,
        analysis,
        signature_accession,
        signature_description,
        raw_start,
        raw_stop,
        raw_score,
        status,
        run_date,
        interpro_accession,
        interpro_description,
    ) = fields
    identity = expected.get(accession)
    if identity is None:
        raise AdapterError(
            f"InterProScan TSV line {line_number} has an unknown SequenceID: "
            f"{accession}"
        )
    if sequence_md5.lower() != identity.md5:
        raise AdapterError(
            f"InterProScan TSV line {line_number} MD5 does not match {accession}"
        )
    sequence_length = _parse_int(raw_length, field="sequence length", line=line_number)
    start = _parse_int(raw_start, field="start", line=line_number)
    stop = _parse_int(raw_stop, field="stop", line=line_number)
    if sequence_length != identity.length:
        raise AdapterError(
            f"InterProScan TSV line {line_number} length does not match {accession}"
        )
    if not 1 <= start <= stop <= sequence_length:
        raise AdapterError(
            f"InterProScan TSV line {line_number} has invalid match coordinates"
        )
    if analysis != "Pfam":
        raise AdapterError(
            f"InterProScan TSV line {line_number} is not a Pfam match: {analysis}"
        )
    if not _PFAM_ACCESSION_PATTERN.fullmatch(signature_accession):
        raise AdapterError(
            f"InterProScan TSV line {line_number} has invalid Pfam accession: "
            f"{signature_accession}"
        )
    score = _parse_float(raw_score, field="score", line=line_number)
    if status != "T":
        raise AdapterError(
            f"InterProScan TSV line {line_number} has invalid status: {status}"
        )
    try:
        datetime.strptime(run_date, "%d-%m-%Y")
    except ValueError as error:
        raise AdapterError(
            f"InterProScan TSV line {line_number} has invalid run date: {run_date}"
        ) from error
    normalized_interpro = _optional_text(interpro_accession)
    if normalized_interpro is not None and not _INTERPRO_ACCESSION_PATTERN.fullmatch(
        normalized_interpro
    ):
        raise AdapterError(
            f"InterProScan TSV line {line_number} has invalid InterPro accession: "
            f"{normalized_interpro}"
        )

    return {
        "SequenceID": identity.sequence_id,
        "ProteinAccession": accession,
        "SequenceMD5": identity.md5,
        "SequenceLength": sequence_length,
        "Analysis": analysis,
        "SignatureAccession": signature_accession,
        "SignatureDescription": _optional_text(signature_description),
        "Start": start,
        "Stop": stop,
        "Score": score,
        "Status": status,
        "InterProAccession": normalized_interpro,
        "InterProDescription": _optional_text(interpro_description),
    }


def _parse_int(value: str, *, field: str, line: int) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise AdapterError(
            f"InterProScan TSV line {line} has invalid {field}: {value}"
        ) from error


def _parse_float(value: str, *, field: str, line: int) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise AdapterError(
            f"InterProScan TSV line {line} has invalid {field}: {value}"
        ) from error
    if not math.isfinite(parsed):
        raise AdapterError(
            f"InterProScan TSV line {line} has non-finite {field}: {value}"
        )
    return parsed


def _optional_text(value: str) -> str | None:
    return None if value == "-" else value


def _write_normalized_artifact(frame: pl.DataFrame) -> ArtifactPayload | None:
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
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return AdapterSequenceResult(
            sequence_id=identity.sequence_id,
            status=EvidenceStatus.NO_HIT,
            payload_digest=sha256_digest(payload),
        )

    rows = frame.filter(pl.col("SequenceID") == identity.sequence_id).to_dicts()
    payload = json.dumps(
        rows,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return AdapterSequenceResult(
        sequence_id=identity.sequence_id,
        status=EvidenceStatus.HIT,
        payload_digest=sha256_digest(payload),
    )
