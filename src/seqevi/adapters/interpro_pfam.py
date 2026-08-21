"""Direct InterProScan/Pfam annotation adapter."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from seqevi.errors import AdapterError
from seqevi.evidence import ArtifactFile, EvidenceStatus, sha256_digest
from seqevi.resource_lock import ResourceComponent, resolve_resource_lock
from seqevi.runner import ToolCommand, ToolRunner, ToolTimeoutError
from seqevi.runtime_identity import RuntimeComponent, calculate_runtime_digest
from seqevi.sequence import SequenceIdentity

from .base import AdapterBatchResult, AdapterContract, AdapterSequenceResult

ADAPTER_CONTRACT_VERSION = "interpro-pfam/2"

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
_TSV_COLUMN_COUNT = 15
_PROBE_TIMEOUT_SECONDS = 120.0
_NORMALIZED_ROW_BATCH_SIZE = 1000


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
                "interpro-pfam/2 uses one fixed direct-scan scientific contract"
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
        verify_resource: bool = False,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self.executable = executable.resolve()
        self.database = database.resolve()
        self.parameters = parameters or InterProPfamParameters()
        self.environment = dict(environment or {})
        self.install_dir = self.executable.parent
        self.properties_path = self.install_dir / "interproscan.properties"
        self.jar_path = self.install_dir / "interproscan-5.jar"

        self._validate_installation_paths()
        self.properties = _read_properties(self.properties_path)
        self.interproscan_version = _probe_interproscan_version(
            self.executable,
            environment=self.environment,
        )
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
            environment=self.environment,
        )
        resource_id = _calculate_resource_id(
            database=self.database,
            interproscan_version=self.interproscan_version,
            pfam_version=pfam_version,
            model_path=model_path,
            verify=verify_resource,
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
        threads: int,
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
                    "--cpu",
                    str(threads),
                    "--outfile",
                    str(raw_path),
                    "--tempdir",
                    str(temporary_dir),
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "interproscan.stdout.log",
                stderr_path=work_dir / "interproscan.stderr.log",
                environment={
                    **self.environment,
                    "INTERPROSCAN_CONF": str(properties_path),
                },
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

        normalized, payload_digest_by_id = _parse_tsv(
            raw_path,
            identities=identities,
            normalized_path=work_dir / "interpro-pfam.normalized.parquet",
        )
        sequence_results = tuple(
            _sequence_result(
                identity,
                payload_digest=payload_digest_by_id.get(identity.sequence_id),
            )
            for identity in sorted(identities, key=lambda item: item.sequence_id)
        )
        return AdapterBatchResult(
            sequences=sequence_results,
            raw_artifact=_gzip_artifact(
                raw_path,
                work_dir / "interpro-pfam.tsv.gz",
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


def _probe_interproscan_version(
    executable: Path,
    *,
    environment: Mapping[str, str],
) -> str:
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
                    environment=environment,
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
    environment: Mapping[str, str],
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

    java = shutil.which(
        "java",
        path=environment.get("PATH", os.environ.get("PATH")),
    )
    if java is None:
        raise AdapterError("InterProScan runtime has no Java executable")
    java_path = Path(java).resolve()
    if java_path.name != "java" or java_path.parent.name != "bin":
        raise AdapterError(
            f"resolved Java executable must be JAVA_HOME/bin/java: {java_path}"
        )
    java_home = java_path.parent.parent
    normalized_properties = {
        **properties,
        "data.directory": "${data.directory}",
    }
    properties_digest = sha256_digest(
        json.dumps(
            normalized_properties,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    try:
        jdk_components, jdk_snapshot = _jdk_runtime_components(java_home)
        components = (
            RuntimeComponent("launcher", executable),
            RuntimeComponent("interproscan-5.jar", jar_path),
            *jdk_components,
            *(
                RuntimeComponent(
                    f"hmmer/{path.relative_to(hmmer_dir).as_posix()}",
                    path,
                )
                for path in hmmer_files
            ),
        )
        digest = calculate_runtime_digest(
            runtime_name="interproscan-pfam",
            versions={
                "interproscan": version,
                "properties": f"sha256:{properties_digest}",
            },
            components=components,
        )
        _, after_snapshot = _jdk_runtime_components(java_home)
    except (OSError, UnicodeError) as error:
        raise AdapterError(
            f"cannot read selected JAVA_HOME runtime: {error}"
        ) from error
    if after_snapshot != jdk_snapshot:
        raise AdapterError("selected JAVA_HOME changed while hashing")
    return digest


def _jdk_runtime_components(
    java_home: Path,
) -> tuple[tuple[RuntimeComponent, ...], tuple[tuple[object, ...], ...]]:
    release_path = java_home / "release"
    roots = (java_home / "bin", java_home / "conf", java_home / "lib")
    entries: list[tuple[str, Path]] = [("jdk/release", release_path)]
    snapshot: list[tuple[object, ...]] = []

    def fail(error: OSError) -> None:
        raise error

    for root in roots:
        if not root.is_dir():
            raise AdapterError(
                f"selected JAVA_HOME runtime directory is missing: {root}"
            )
        root_file_count = 0
        for directory, directory_names, file_names in os.walk(
            root, followlinks=False, onerror=fail
        ):
            directory_path = Path(directory)
            relative_directory = directory_path.relative_to(java_home).as_posix()
            directory_stat = directory_path.stat(follow_symlinks=False)
            snapshot.append(
                (
                    relative_directory,
                    "directory",
                    directory_stat.st_dev,
                    directory_stat.st_ino,
                    directory_stat.st_size,
                    directory_stat.st_mtime_ns,
                    directory_stat.st_ctime_ns,
                )
            )
            for name in sorted((*directory_names, *file_names)):
                path = directory_path / name
                if path.is_symlink():
                    raise AdapterError(
                        f"selected JAVA_HOME runtime contains a symbolic link: {path}"
                    )
            for name in sorted(file_names):
                path = directory_path / name
                relative = path.relative_to(java_home).as_posix()
                file_stat = path.stat(follow_symlinks=False)
                if not path.is_file():
                    raise AdapterError(
                        "selected JAVA_HOME runtime entry is not a regular file: "
                        f"{path}"
                    )
                snapshot.append(
                    (
                        relative,
                        "file",
                        file_stat.st_dev,
                        file_stat.st_ino,
                        file_stat.st_size,
                        file_stat.st_mtime_ns,
                        file_stat.st_ctime_ns,
                    )
                )
                entries.append((f"jdk/{relative}", path))
                root_file_count += 1
        if root_file_count == 0:
            raise AdapterError(
                f"selected JAVA_HOME runtime directory has no regular files: {root}"
            )

    if not release_path.is_file() or release_path.is_symlink():
        raise AdapterError(
            f"selected JAVA_HOME release file is missing or invalid: {release_path}"
        )
    release_stat = release_path.stat(follow_symlinks=False)
    release_text = release_path.read_text(encoding="utf-8")
    version_match = re.search(r'^JAVA_VERSION="?(\d+)', release_text, re.MULTILINE)
    if version_match is None:
        raise AdapterError(
            f"selected JAVA_HOME release file has no JAVA_VERSION: {release_path}"
        )
    if int(version_match.group(1)) >= 11:
        modules_path = java_home / "lib" / "modules"
        if not modules_path.is_file() or modules_path.is_symlink():
            raise AdapterError(
                "selected Java 11+ runtime has no regular lib/modules image: "
                f"{modules_path}"
            )
    snapshot.append(
        (
            "release",
            "file",
            release_stat.st_dev,
            release_stat.st_ino,
            release_stat.st_size,
            release_stat.st_mtime_ns,
            release_stat.st_ctime_ns,
        )
    )
    return (
        tuple(RuntimeComponent(name, path) for name, path in sorted(entries)),
        tuple(sorted(snapshot)),
    )


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
    *,
    database: Path,
    interproscan_version: str,
    pfam_version: str,
    model_path: Path,
    verify: bool = False,
) -> str:
    interpro_release = interproscan_version.split("-", maxsplit=1)[1]
    metadata_path = model_path.with_name("pfam_a.dat")
    if not metadata_path.is_file():
        raise AdapterError(f"Pfam metadata file does not exist: {metadata_path}")
    declarations = (
        ResourceComponent(
            "pfam-model",
            model_path.relative_to(database).as_posix(),
        ),
        ResourceComponent(
            "pfam-metadata",
            metadata_path.relative_to(database).as_posix(),
        ),
    )
    locked = resolve_resource_lock(
        database=database,
        resource_name="interpro",
        resource_version=interpro_release,
        components=declarations,
        verify=verify,
    )
    digest = sha256_digest(
        json.dumps(
            [
                (component.name, locked.hash_for(component.name))
                for component in declarations
            ],
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"interpro/{interpro_release}/pfam/{pfam_version}/sha256:{digest}"


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


def _parse_tsv(
    path: Path,
    *,
    identities: tuple[SequenceIdentity, ...],
    normalized_path: Path,
) -> tuple[ArtifactFile | None, dict[str, str]]:
    expected = {identity.sequence_id: identity for identity in identities}
    rows: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(
        prefix=".interpro-pfam-normalized-", dir=normalized_path.parent
    ) as raw_parts_dir:
        parts_dir = Path(raw_parts_dir)
        part_paths: list[Path] = []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle, delimiter="\t")
                for line_number, fields in enumerate(reader, start=1):
                    if not fields or fields == [""]:
                        raise AdapterError(
                            "InterProScan TSV contains a blank row at line "
                            f"{line_number}"
                        )
                    if len(fields) != _TSV_COLUMN_COUNT:
                        raise AdapterError(
                            f"InterProScan TSV line {line_number} has "
                            f"{len(fields)} columns; expected {_TSV_COLUMN_COUNT}"
                        )
                    row = _parse_tsv_row(
                        fields, line_number=line_number, expected=expected
                    )
                    rows.append(row)
                    if len(rows) >= _NORMALIZED_ROW_BATCH_SIZE:
                        part_paths.append(_write_normalized_part(rows, parts_dir))
                        rows.clear()
        except UnicodeDecodeError as error:
            raise AdapterError(
                f"InterProScan TSV is not valid UTF-8: {error}"
            ) from error

        if rows:
            part_paths.append(_write_normalized_part(rows, parts_dir))
        if not part_paths:
            return None, {}
        normalized = pl.concat([pl.scan_parquet(part) for part in part_paths]).sort(
            *INTERPRO_PFAM_EVIDENCE_SCHEMA,
            nulls_last=True,
        )
        payload_digest_by_id = _payload_digests(normalized)
        normalized.sink_parquet(
            normalized_path, compression="zstd", maintain_order=True
        )
    return (
        ArtifactFile.from_path(normalized_path, "application/vnd.apache.parquet"),
        payload_digest_by_id,
    )


def _write_normalized_part(rows: list[dict[str, object]], directory: Path) -> Path:
    path = directory / f"part-{len(tuple(directory.iterdir())):06d}.parquet"
    pl.DataFrame(rows, schema=INTERPRO_PFAM_EVIDENCE_SCHEMA).write_parquet(path)
    return path


def _payload_digests(frame: pl.LazyFrame) -> dict[str, str]:
    digests: dict[str, str] = {}
    current_sequence_id: str | None = None
    current_hasher: Any | None = None
    previous_row: tuple[object, ...] | None = None
    row_index = 0
    for batch in frame.collect_batches(
        chunk_size=_NORMALIZED_ROW_BATCH_SIZE,
        engine="streaming",
    ):
        for row in batch.iter_rows(named=True):
            row_key = tuple(row[column] for column in INTERPRO_PFAM_EVIDENCE_SCHEMA)
            if row_key == previous_row:
                raise AdapterError("InterProScan TSV contains a duplicate match")
            previous_row = row_key
            sequence_id = str(row["SequenceID"])
            if sequence_id != current_sequence_id:
                if current_sequence_id is not None and current_hasher is not None:
                    current_hasher.update(b"]")
                    digests[current_sequence_id] = current_hasher.hexdigest()
                current_sequence_id = sequence_id
                current_hasher = hashlib.sha256()
                current_hasher.update(b"[")
                row_index = 0
            if current_hasher is None:
                raise AssertionError("payload digest state was not initialized")
            if row_index:
                current_hasher.update(b",")
            current_hasher.update(
                json.dumps(
                    row,
                    ensure_ascii=True,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            row_index += 1
    if current_sequence_id is not None and current_hasher is not None:
        current_hasher.update(b"]")
        digests[current_sequence_id] = current_hasher.hexdigest()
    return digests


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
        go_annotations,
        pathway_annotations,
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
    if go_annotations != "-" or pathway_annotations != "-":
        raise AdapterError(
            f"InterProScan TSV line {line_number} contains GO or pathway annotations "
            "outside the interpro-pfam/2 contract"
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


def _gzip_artifact(source: Path, target: Path) -> ArtifactFile:
    with (
        source.open("rb") as source_handle,
        target.open("wb") as target_handle,
        gzip.GzipFile(fileobj=target_handle, mode="wb", mtime=0) as compressed,
    ):
        shutil.copyfileobj(source_handle, compressed)
    return ArtifactFile.from_path(target, "application/gzip")


def _sequence_result(
    identity: SequenceIdentity,
    *,
    payload_digest: str | None,
) -> AdapterSequenceResult:
    if payload_digest is None:
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

    return AdapterSequenceResult(
        sequence_id=identity.sequence_id,
        status=EvidenceStatus.HIT,
        payload_digest=payload_digest,
    )
