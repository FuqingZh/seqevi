"""Direct dbCAN v5 protein CAZyme annotation adapter."""

from __future__ import annotations

import csv
import gzip
import json
import os
import re
import shlex
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl

from seqevi.errors import AdapterError
from seqevi.evidence import ArtifactFile, EvidenceStatus, sha256_digest
from seqevi.resource_lock import ResourceComponent, resolve_resource_lock
from seqevi.runner import ToolCommand, ToolRunner, ToolTimeoutError
from seqevi.runtime_identity import RuntimeComponent, calculate_runtime_digest
from seqevi.sequence import SequenceIdentity

from .base import AdapterBatchResult, AdapterContract, AdapterSequenceResult

ADAPTER_CONTRACT_VERSION = "dbcan-cazyme/1"
RESOURCE_RELEASE = "db_v5-2-9_5-5-2026"

_REQUIRED_RESOURCE_COMPONENTS = (
    ResourceComponent("CAZy-diamond", "CAZy.dmnd"),
    ResourceComponent("dbCAN-HMM", "dbCAN.hmm"),
    ResourceComponent("dbCAN-sub-HMM", "dbCAN-sub.hmm"),
    ResourceComponent("fam-substrate-mapping", "fam-substrate-mapping.tsv"),
)
_OVERVIEW_COLUMNS = (
    "Gene ID",
    "EC#",
    "dbCAN_hmm",
    "dbCAN_sub",
    "DIAMOND",
    "#ofTools",
    "Recommend Results",
    "Substrate",
)
DBCAN_EVIDENCE_SCHEMA: Mapping[str, pl.DataType] = {
    "SequenceID": pl.String(),
    "Gene ID": pl.String(),
    "EC#": pl.String(),
    "dbCAN_hmm": pl.String(),
    "dbCAN_sub": pl.String(),
    "DIAMOND": pl.String(),
    "#ofTools": pl.Int64(),
    "Recommend Results": pl.String(),
    "Substrate": pl.String(),
}
_VERSION_PATTERN = (
    r"(?:dbcan|run_dbcan)\s*(?:version\s*)?:?\s*"
    r"v?([0-9]+\.[0-9]+\.[0-9]+)"
)
_DIAMOND_VERSION_PATTERN = r"\bdiamond version\s+([^\s]+)"
_PROBE_TIMEOUT_SECONDS = 120.0
_NORMALIZED_ROW_BATCH_SIZE = 1000


@dataclass(frozen=True, slots=True)
class DBCanParameters:
    """Fixed scientific parameters for the dbCAN protein contract."""

    mode: str = "protein"
    methods: str = "diamond,hmm,dbCANsub"
    diamond_evalue: float = 1e-102
    dbcan_coverage: float = 0.35
    dbcan_evalue: float = 1e-15
    dbcan_sub_coverage: float = 0.35
    dbcan_sub_evalue: float = 1e-15

    def __post_init__(self) -> None:
        values = tuple(asdict(self).values())
        if values != (
            "protein",
            "diamond,hmm,dbCANsub",
            1e-102,
            0.35,
            1e-15,
            0.35,
            1e-15,
        ):
            raise ValueError("dbcan-cazyme/1 uses one fixed protein CAZyme contract")

    def as_semantic_parameters(self) -> dict[str, object]:
        """Return every result-affecting dbCAN parameter."""

        return asdict(self)


class DBCanCazymeAdapter:
    """Run ``run_dbcan CAZyme_annotation --mode protein`` and validate overview."""

    def __init__(
        self,
        *,
        executable: Path,
        database: Path,
        parameters: DBCanParameters | None = None,
        verify_resource: bool = False,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self.executable = executable.resolve()
        self.database = database.resolve()
        self.parameters = parameters or DBCanParameters()
        self.environment = dict(environment or {})
        self._validate_installation()
        self.dbcan_version = _probe_dbcan_version(
            self.executable, environment=self.environment
        )
        diamond = _resolve_diamond(self.executable, environment=self.environment)
        self.diamond_version = _probe_diamond_version(
            diamond, environment=self.environment
        )
        runtime_digest = _runtime_digest(
            self.executable,
            dbcan_version=self.dbcan_version,
            diamond=diamond,
            diamond_version=self.diamond_version,
            environment=self.environment,
        )
        resource_id = _resource_id(
            self.database,
            verify=verify_resource,
        )
        self._contract = AdapterContract.from_parameters(
            name="dbcan-cazyme",
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
        return DBCAN_EVIDENCE_SCHEMA

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
        """Run one cache-miss batch and normalize its protein overview."""

        if not identities:
            raise AdapterError("dbcan-cazyme batch must not be empty")
        output_dir = work_dir / "dbcan-output"
        output_dir.mkdir()
        parameters = self.parameters
        result = runner.run(
            ToolCommand(
                arguments=(
                    str(self.executable),
                    "CAZyme_annotation",
                    "--input_raw_data",
                    str(input_fasta),
                    "--mode",
                    parameters.mode,
                    "--output_dir",
                    str(output_dir),
                    "--db_dir",
                    str(self.database),
                    "--methods",
                    parameters.methods,
                    "--threads",
                    str(threads),
                    "--e_value_threshold",
                    str(parameters.diamond_evalue),
                    "--coverage_threshold_dbcan",
                    str(parameters.dbcan_coverage),
                    "--e_value_threshold_dbcan",
                    str(parameters.dbcan_evalue),
                    "--coverage_threshold_dbsub",
                    str(parameters.dbcan_sub_coverage),
                    "--e_value_threshold_dbsub",
                    str(parameters.dbcan_sub_evalue),
                ),
                working_dir=work_dir,
                stdout_path=work_dir / "dbcan.stdout.log",
                stderr_path=work_dir / "dbcan.stderr.log",
                environment=_runtime_environment(
                    self.executable, overlay=self.environment
                ),
            ),
            timeout_seconds=timeout_seconds,
        )
        if result.return_code != 0:
            raise AdapterError(
                f"dbCAN exited with {result.return_code}; stderr: {result.stderr_path}"
            )
        overview = output_dir / "overview.tsv"
        if not overview.is_file():
            raise AdapterError("dbCAN did not create overview.tsv")
        normalized, payloads = _parse_overview(
            overview,
            identities=identities,
            normalized_path=work_dir / "dbcan.normalized.parquet",
        )
        sequence_results = tuple(
            _sequence_result(
                identity,
                payload_digest=payloads.get(identity.sequence_id),
            )
            for identity in sorted(identities, key=lambda item: item.sequence_id)
        )
        return AdapterBatchResult(
            sequences=sequence_results,
            raw_artifact=_gzip_artifact(
                overview,
                work_dir / "dbcan.overview.tsv.gz",
            ),
            normalized_artifact=normalized,
        )

    def _validate_installation(self) -> None:
        if not self.executable.is_file() or not os.access(self.executable, os.X_OK):
            raise AdapterError(
                f"dbCAN executable is not an executable file: {self.executable}"
            )
        if not self.database.is_dir():
            raise AdapterError(f"dbCAN resource is not a directory: {self.database}")
        for component in _REQUIRED_RESOURCE_COMPONENTS:
            path = self.database / component.relative_path
            if not path.is_file():
                raise AdapterError(
                    f"dbCAN resource is missing {component.relative_path}: {path}"
                )


def _runtime_environment(
    executable: Path,
    *,
    overlay: Mapping[str, str] | None = None,
) -> dict[str, str]:
    environment = dict(overlay or {})
    inherited = environment.get("PATH", os.environ.get("PATH", ""))
    environment["PATH"] = (
        str(executable.parent)
        if not inherited
        else os.pathsep.join((str(executable.parent), inherited))
    )
    return environment


def _probe_dbcan_version(
    executable: Path,
    *,
    environment: Mapping[str, str],
) -> str:
    output = _probe_command(executable, ("version",), environment=environment)
    versions = sorted(set(re.findall(_VERSION_PATTERN, output, flags=re.IGNORECASE)))
    if len(versions) != 1:
        raise AdapterError(
            "dbcan-cazyme/1 requires exactly one dbCAN 5.2.9 version in "
            "run_dbcan version output"
        )
    if versions[0] != "5.2.9":
        raise AdapterError(f"unsupported dbCAN release: {versions[0]}")
    return versions[0]


def _probe_command(
    executable: Path,
    arguments: tuple[str, ...],
    *,
    environment: Mapping[str, str],
) -> str:
    with tempfile.TemporaryDirectory(prefix="seqevi-dbcan-probe-") as raw_dir:
        root = Path(raw_dir)
        stdout_path = root / "stdout.log"
        stderr_path = root / "stderr.log"
        try:
            result = ToolRunner().run(
                ToolCommand(
                    arguments=(str(executable), *arguments),
                    working_dir=executable.parent,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    environment=_runtime_environment(executable, overlay=environment),
                ),
                timeout_seconds=_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, ToolTimeoutError) as error:
            raise AdapterError(f"dbCAN version probe failed: {error}") from error
        output = "\n".join(
            (
                stdout_path.read_text(encoding="utf-8", errors="replace"),
                stderr_path.read_text(encoding="utf-8", errors="replace"),
            )
        )
        if result.return_code != 0:
            raise AdapterError(
                f"dbCAN version probe exited with {result.return_code}: "
                f"{output.strip()}"
            )
        return output


def _resolve_diamond(
    executable: Path,
    *,
    environment: Mapping[str, str],
) -> Path:
    resolved = shutil.which(
        "diamond",
        path=_runtime_environment(executable, overlay=environment)["PATH"],
    )
    if resolved is None:
        raise AdapterError("dbCAN runtime has no DIAMOND executable")
    return Path(resolved).resolve()


def _probe_diamond_version(
    executable: Path,
    *,
    environment: Mapping[str, str],
) -> str:
    output = _probe_command(executable, ("version",), environment=environment)
    versions = sorted(
        set(re.findall(_DIAMOND_VERSION_PATTERN, output, flags=re.IGNORECASE))
    )
    if len(versions) != 1:
        raise AdapterError("dbcan-cazyme/1 requires exactly one DIAMOND release")
    if versions[0] != "2.1.15":
        raise AdapterError(f"unsupported DIAMOND release: {versions[0]}")
    return versions[0]


def _runtime_digest(
    executable: Path,
    *,
    dbcan_version: str,
    diamond: Path,
    diamond_version: str,
    environment: Mapping[str, str],
) -> str:
    components = [
        RuntimeComponent("launcher", executable),
        RuntimeComponent("diamond", diamond),
    ]
    interpreter = _resolve_python_interpreter(executable, environment=environment)
    components.append(RuntimeComponent("python", interpreter))
    package_root = _resolve_dbcan_package(executable)
    components.extend(
        RuntimeComponent(f"dbcan/{path.relative_to(package_root).as_posix()}", path)
        for path in sorted(package_root.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    )
    components.extend(
        RuntimeComponent(f"python-distributions/{path.parent.name}/RECORD", path)
        for path in sorted(package_root.parent.glob("*.dist-info/RECORD"))
    )
    return calculate_runtime_digest(
        runtime_name="dbcan-cazyme",
        versions={"dbcan": dbcan_version, "diamond": diamond_version},
        components=tuple(components),
    )


def _resolve_python_interpreter(
    executable: Path,
    *,
    environment: Mapping[str, str],
) -> Path:
    try:
        with executable.open("rb") as handle:
            first_line = handle.readline(4096).decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise AdapterError(f"dbCAN launcher cannot be read: {error}") from error
    if not first_line.startswith("#!"):
        raise AdapterError("dbCAN launcher has no Python shebang")
    command = shlex.split(first_line[2:].strip())
    if not command:
        raise AdapterError("dbCAN launcher has an empty shebang")
    name = command[0]
    if Path(name).name == "env":
        candidates = [item for item in command[1:] if not item.startswith("-")]
        if len(candidates) != 1:
            raise AdapterError("unsupported dbCAN env shebang")
        name = candidates[0]
    resolved = shutil.which(
        name,
        path=_runtime_environment(executable, overlay=environment)["PATH"],
    )
    if resolved is None:
        raise AdapterError("dbCAN Python interpreter cannot be resolved")
    return Path(resolved).resolve()


def _resolve_dbcan_package(executable: Path) -> Path:
    runtime_root = executable.parent.parent
    candidates = []
    for python_dir in sorted((runtime_root / "lib").glob("python*")):
        for package_dir_name in ("site-packages", "dist-packages"):
            candidate = python_dir / package_dir_name / "dbcan"
            if candidate.is_dir():
                candidates.append(candidate.resolve())
    unique = sorted(set(candidates))
    if len(unique) != 1:
        raise AdapterError(
            "dbCAN runtime must contain exactly one installed dbcan package"
        )
    return unique[0]


def _resource_id(database: Path, *, verify: bool) -> str:
    lock = resolve_resource_lock(
        database=database,
        resource_name="dbcan",
        resource_version=RESOURCE_RELEASE,
        components=_REQUIRED_RESOURCE_COMPONENTS,
        verify=verify,
    )
    values = [
        (component.name, lock.hash_for(component.name))
        for component in _REQUIRED_RESOURCE_COMPONENTS
    ]
    digest = sha256_digest(
        json.dumps(values, separators=(",", ":"), ensure_ascii=True).encode()
    )
    return f"dbcan/{RESOURCE_RELEASE}/sha256:{digest}"


def _parse_overview(
    path: Path,
    *,
    identities: tuple[SequenceIdentity, ...],
    normalized_path: Path,
) -> tuple[ArtifactFile | None, dict[str, str]]:
    expected = {identity.sequence_id: identity for identity in identities}
    rows: list[dict[str, object]] = []
    payloads: dict[str, str] = {}
    seen: set[str] = set()
    with tempfile.TemporaryDirectory(
        prefix=".dbcan-normalized-", dir=normalized_path.parent
    ) as raw_parts_dir:
        parts_dir = Path(raw_parts_dir)
        part_paths: list[Path] = []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle, delimiter="\t")
                try:
                    header = tuple(next(reader))
                except StopIteration as error:
                    raise AdapterError("dbCAN overview.tsv is empty") from error
                if header != _OVERVIEW_COLUMNS:
                    raise AdapterError(
                        "dbCAN overview.tsv has an unexpected header; expected "
                        + "\\t".join(_OVERVIEW_COLUMNS)
                    )
                for line_number, fields in enumerate(reader, start=2):
                    if not fields or all(not field.strip() for field in fields):
                        raise AdapterError(
                            f"dbCAN overview line {line_number} is blank"
                        )
                    if len(fields) != len(_OVERVIEW_COLUMNS):
                        raise AdapterError(
                            f"dbCAN overview line {line_number} has {len(fields)} "
                            f"columns; expected {len(_OVERVIEW_COLUMNS)}"
                        )
                    row = _parse_overview_row(
                        fields, expected=expected, line_number=line_number
                    )
                    sequence_id = str(row["SequenceID"])
                    if sequence_id in seen:
                        raise AdapterError(
                            f"dbCAN overview contains duplicate Gene ID: {sequence_id}"
                        )
                    seen.add(sequence_id)
                    payloads[sequence_id] = sha256_digest(
                        json.dumps(
                            row,
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    )
                    rows.append(row)
                    if len(rows) >= _NORMALIZED_ROW_BATCH_SIZE:
                        part_paths.append(_write_normalized_part(rows, parts_dir))
                        rows.clear()
        except UnicodeDecodeError as error:
            raise AdapterError(f"dbCAN overview is not valid UTF-8: {error}") from error
        if rows:
            part_paths.append(_write_normalized_part(rows, parts_dir))
        if not part_paths:
            return None, payloads
        pl.concat([pl.scan_parquet(part) for part in part_paths]).sort(
            "SequenceID"
        ).sink_parquet(normalized_path, compression="zstd", maintain_order=True)
    return ArtifactFile.from_path(
        normalized_path, "application/vnd.apache.parquet"
    ), payloads


def _write_normalized_part(rows: list[dict[str, object]], directory: Path) -> Path:
    path = directory / f"part-{len(tuple(directory.iterdir())):06d}.parquet"
    pl.DataFrame(rows, schema=DBCAN_EVIDENCE_SCHEMA).write_parquet(path)
    return path


def _parse_overview_row(
    fields: list[str],
    *,
    expected: Mapping[str, SequenceIdentity],
    line_number: int,
) -> dict[str, object]:
    native = dict(zip(_OVERVIEW_COLUMNS, fields, strict=True))
    sequence_id = native["Gene ID"]
    if sequence_id not in expected:
        raise AdapterError(
            f"dbCAN overview line {line_number} has unknown SequenceID: {sequence_id}"
        )
    raw_tools = native["#ofTools"]
    try:
        tools = int(raw_tools)
    except ValueError as error:
        raise AdapterError(
            f"dbCAN overview line {line_number} has invalid #ofTools: {raw_tools}"
        ) from error
    if tools < 0 or tools > 3:
        raise AdapterError(
            f"dbCAN overview line {line_number} has invalid #ofTools: {raw_tools}"
        )
    return {
        "SequenceID": sequence_id,
        "Gene ID": sequence_id,
        "EC#": _optional_text(native["EC#"]),
        "dbCAN_hmm": _optional_text(native["dbCAN_hmm"]),
        "dbCAN_sub": _optional_text(native["dbCAN_sub"]),
        "DIAMOND": _optional_text(native["DIAMOND"]),
        "#ofTools": tools,
        "Recommend Results": _optional_text(native["Recommend Results"]),
        "Substrate": _optional_text(native["Substrate"]),
    }


def _optional_text(value: str) -> str | None:
    return None if value.strip() in {"", "-", "NA", "None"} else value


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
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
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
