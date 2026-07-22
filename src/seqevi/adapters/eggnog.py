"""eggNOG-mapper protein annotation adapter."""

from __future__ import annotations

import csv
import gzip
import json
import math
import os
import re
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
_DIAMOND_VERSION_PATTERN = re.compile(r"\bdiamond version\s+([^\s/]+)")
_PROBE_TIMEOUT_SECONDS = 120.0
_REQUIRED_DATABASE_FILES = (
    "eggnog.db",
    "eggnog.taxa.db",
    "eggnog_proteins.dmnd",
)
_OPTIONAL_DATABASE_FILES = ("eggnog.taxa.db.traverse.pkl",)
_NORMALIZED_ROW_BATCH_SIZE = 1000


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
        verify_resource: bool = False,
    ) -> None:
        self.executable = executable.resolve()
        self.database = database.resolve()
        self.parameters = parameters or EggnogParameters()
        if not self.executable.is_file():
            raise AdapterError(f"eggNOG-mapper executable is not a file: {executable}")
        if not self.database.is_dir():
            raise AdapterError(f"eggNOG database is not a directory: {database}")

        version_output = _probe_version(self.executable, self.database)
        tool_version, database_version, reported_diamond_version = (
            _parse_version_output(version_output)
        )
        diamond = _resolve_diamond(self.executable)
        diamond_version = _probe_diamond_version(diamond)
        if diamond_version != reported_diamond_version:
            raise AdapterError(
                "eggNOG-mapper and the selected DIAMOND executable report "
                "different versions"
            )
        runtime_digest = _runtime_digest(
            self.executable,
            tool_version=tool_version,
            diamond=diamond,
            diamond_version=diamond_version,
        )
        resource_id = _resource_id(
            self.database,
            database_version,
            verify=verify_resource,
        )
        self._contract = AdapterContract.from_parameters(
            name="eggnog",
            version=ADAPTER_CONTRACT_VERSION,
            tool_runtime_digest=f"sha256:{runtime_digest}",
            resource_id=resource_id,
            semantic_parameters=self.parameters.as_semantic_parameters(),
        )
        self.tool_version = tool_version
        self.diamond_version = diamond_version
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
        threads: int,
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
                    "-i",
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
                    str(threads),
                    "--override",
                    "-m",
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
                environment=_runtime_environment(self.executable),
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

        normalized, payload_digest_by_id = _parse_annotations(
            raw_path,
            identities=identities,
            normalized_path=work_dir / "eggnog.normalized.parquet",
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
                work_dir / "eggnog.annotations.tsv.gz",
            ),
            normalized_artifact=normalized,
        )


def _probe_version(executable: Path, database: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="seqevi-eggnog-probe-") as raw_dir:
        root = Path(raw_dir)
        stdout_path = root / "stdout.log"
        stderr_path = root / "stderr.log"
        try:
            result = ToolRunner().run(
                ToolCommand(
                    arguments=(
                        str(executable),
                        "--version",
                        "--data_dir",
                        str(database),
                    ),
                    working_dir=executable.parent,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    environment=_runtime_environment(executable),
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


def _runtime_environment(executable: Path) -> dict[str, str]:
    runtime_bin = str(executable.parent)
    inherited_path = os.environ.get("PATH")
    return {
        "PATH": (
            runtime_bin
            if not inherited_path
            else os.pathsep.join((runtime_bin, inherited_path))
        )
    }


def _parse_version_output(output: str) -> tuple[str, str, str]:
    tool_versions = sorted(set(_VERSION_PATTERN.findall(output)))
    expected = sorted(set(_EXPECTED_DB_PATTERN.findall(output)))
    installed = sorted(set(_INSTALLED_DB_PATTERN.findall(output)))
    diamond_versions = sorted(set(_DIAMOND_VERSION_PATTERN.findall(output)))
    if len(tool_versions) != 1:
        raise AdapterError(
            "eggnog/1 requires exactly one eggNOG-mapper 2.x release in --version"
        )
    if len(expected) != 1 or len(installed) != 1 or expected != installed:
        raise AdapterError(
            "eggNOG-mapper must report one matching expected and installed DB version"
        )
    if len(diamond_versions) != 1:
        raise AdapterError("eggnog/1 requires exactly one DIAMOND release in --version")
    return tool_versions[0], installed[0], diamond_versions[0]


def _runtime_digest(
    executable: Path,
    *,
    tool_version: str,
    diamond: Path,
    diamond_version: str,
) -> str:
    package_root = _resolve_eggnog_package(executable)
    package_components = tuple(
        RuntimeComponent(
            name=f"eggnogmapper/{path.relative_to(package_root).as_posix()}",
            path=path,
        )
        for path in sorted(package_root.rglob("*"))
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )
    return calculate_runtime_digest(
        runtime_name="eggnog-mapper",
        versions={"diamond": diamond_version, "eggnog-mapper": tool_version},
        components=(
            RuntimeComponent("launcher", executable),
            RuntimeComponent("diamond", diamond),
            *package_components,
        ),
    )


def _resolve_eggnog_package(executable: Path) -> Path:
    runtime_root = executable.parent.parent
    candidates = []
    for python_dir in sorted((runtime_root / "lib").glob("python*")):
        for package_dir_name in ("site-packages", "dist-packages"):
            candidate = python_dir / package_dir_name / "eggnogmapper"
            if candidate.is_dir():
                candidates.append(candidate.resolve())
    unique = sorted(set(candidates))
    if len(unique) != 1:
        raise AdapterError(
            "eggNOG-mapper runtime must contain exactly one installed "
            "eggnogmapper package directory"
        )
    return unique[0]


def _resolve_diamond(executable: Path) -> Path:
    resolved = shutil.which("diamond", path=_runtime_environment(executable)["PATH"])
    if resolved is None:
        raise AdapterError("eggNOG-mapper runtime has no DIAMOND executable")
    return Path(resolved).resolve()


def _probe_diamond_version(executable: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="seqevi-diamond-probe-") as raw_dir:
        root = Path(raw_dir)
        stdout_path = root / "stdout.log"
        stderr_path = root / "stderr.log"
        try:
            result = ToolRunner().run(
                ToolCommand(
                    arguments=(str(executable), "version"),
                    working_dir=executable.parent,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                ),
                timeout_seconds=_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, ToolTimeoutError) as error:
            raise AdapterError(f"DIAMOND version probe failed: {error}") from error
        output = "\n".join(
            (
                stdout_path.read_text(encoding="utf-8", errors="replace"),
                stderr_path.read_text(encoding="utf-8", errors="replace"),
            )
        ).strip()
        if result.return_code != 0:
            raise AdapterError(
                f"DIAMOND version probe exited with {result.return_code}: {output}"
            )
        versions = sorted(set(_DIAMOND_VERSION_PATTERN.findall(output)))
        if len(versions) != 1:
            raise AdapterError("DIAMOND executable did not report exactly one version")
        return versions[0]


def _resource_id(database: Path, version: str, *, verify: bool = False) -> str:
    declarations = tuple(
        ResourceComponent(name=name, relative_path=name)
        for name in _REQUIRED_DATABASE_FILES
    ) + tuple(
        ResourceComponent(name=name, relative_path=name)
        for name in _OPTIONAL_DATABASE_FILES
        if (database / name).is_file()
    )
    locked = resolve_resource_lock(
        database=database,
        resource_name="eggnog",
        resource_version=version,
        components=declarations,
        verify=verify,
    )
    components = [(name, locked.hash_for(name)) for name in _REQUIRED_DATABASE_FILES]
    digest = sha256_digest(
        json.dumps(components, separators=(",", ":")).encode("utf-8")
    )
    return f"eggnog/{version}/sha256:{digest}"


def _parse_annotations(
    path: Path,
    *,
    identities: tuple[SequenceIdentity, ...],
    normalized_path: Path,
) -> tuple[ArtifactFile | None, dict[str, str]]:
    expected = {identity.sequence_id: identity for identity in identities}
    header: tuple[str, ...] | None = None
    rows: list[dict[str, object]] = []
    seen_queries: set[str] = set()
    payload_digest_by_id: dict[str, str] = {}
    with tempfile.TemporaryDirectory(
        prefix=".eggnog-normalized-", dir=normalized_path.parent
    ) as raw_parts_dir:
        parts_dir = Path(raw_parts_dir)
        part_paths: list[Path] = []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    line = raw_line.removesuffix("\n").removesuffix("\r")
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
                                raise AdapterError(
                                    "eggNOG annotations contain duplicate headers"
                                )
                            header = candidate
                        continue
                    if header is None:
                        raise AdapterError(
                            "eggNOG annotations data appeared before its header"
                        )
                    fields = next(csv.reader((line,), delimiter="\t"))
                    if len(fields) != len(header):
                        raise AdapterError(
                            f"eggNOG annotations line {line_number} has "
                            f"{len(fields)} columns; expected {len(header)}"
                        )
                    if header != _NATIVE_COLUMNS:
                        raise AdapterError(
                            "eggnog/1 requires the canonical eggNOG-mapper 2.x "
                            "annotations schema"
                        )
                    row = _parse_row(
                        header, fields, expected=expected, line_number=line_number
                    )
                    query = str(row["SequenceID"])
                    if query in seen_queries:
                        raise AdapterError(
                            f"eggNOG annotations contain duplicate query: {query}"
                        )
                    seen_queries.add(query)
                    payload_digest_by_id[query] = sha256_digest(
                        json.dumps(
                            row,
                            allow_nan=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    )
                    rows.append(row)
                    if len(rows) >= _NORMALIZED_ROW_BATCH_SIZE:
                        part_paths.append(_write_normalized_part(rows, parts_dir))
                        rows.clear()
        except UnicodeDecodeError as error:
            raise AdapterError(
                f"eggNOG annotations are not valid UTF-8: {error}"
            ) from error

        if header is None:
            raise AdapterError("eggNOG annotations are missing the #query header")
        if header != _NATIVE_COLUMNS:
            raise AdapterError(
                "eggnog/1 requires the canonical eggNOG-mapper 2.x annotations schema"
            )
        if rows:
            part_paths.append(_write_normalized_part(rows, parts_dir))
        if not part_paths:
            return None, payload_digest_by_id
        pl.concat([pl.scan_parquet(part) for part in part_paths]).sort(
            "SequenceID"
        ).sink_parquet(normalized_path, compression="zstd", maintain_order=True)
    return (
        ArtifactFile.from_path(normalized_path, "application/vnd.apache.parquet"),
        payload_digest_by_id,
    )


def _write_normalized_part(rows: list[dict[str, object]], directory: Path) -> Path:
    path = directory / f"part-{len(tuple(directory.iterdir())):06d}.parquet"
    pl.DataFrame(rows, schema=EGGNOG_EVIDENCE_SCHEMA).write_parquet(path)
    return path


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
