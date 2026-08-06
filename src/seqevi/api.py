"""Public Python application boundary for SeqEvi annotation results."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from . import __version__
from .adapters import (
    AdapterConfiguration,
    AdapterName,
    AnnotationAdapter,
    create_adapter,
)
from .annotate import AnnotationSummary, run_annotation
from .distribution.oci import run_oci_annotation
from .errors import AnnotationError
from .execution_profile import (
    ExecutionProfile,
    load_execution_profile,
    load_named_profile,
)
from .result import RESULT_FORMAT_VERSION, scan_annotations as _scan_annotations
from .store import open_evidence_store


@dataclass(frozen=True, slots=True)
class ResolvedAnnotationInputs:
    """Concrete inputs after profile/config resolution.

    For a managed v2 profile, ``executable`` is ``None`` and ``profile`` holds
    the immutable OCI runtime. The application boundary dispatches that case
    without exposing Docker branches to scientific adapters.
    """

    adapter: AdapterName
    executable: Path | None
    resource: Path
    store: str | Path | None
    threads: int
    timeout_seconds: float | None
    environment: tuple[tuple[str, str], ...] = ()
    profile: ExecutionProfile | None = None


@dataclass(frozen=True, slots=True)
class AnnotationInvocation:
    """Internal shared application result used by API and CLI presenters."""

    relation: duckdb.DuckDBPyRelation
    summary: AnnotationSummary
    adapter: str
    result_schema_id: str


def annotate(
    fasta: str | Path,
    *,
    output: str | Path,
    profile: str | None = None,
    config: str | Path | None = None,
    adapter: AdapterName | str | None = None,
    executable: str | Path | None = None,
    resource: str | Path | None = None,
    store: str | Path | None = None,
    threads: int | None = None,
    timeout_seconds: float | None = None,
) -> duckdb.DuckDBPyRelation:
    """Annotate a protein FASTA and return its native DuckDB relation.

    Args:
        fasta: Input protein FASTA path.
        output: New `.duckdb` result path. Existing paths are rejected.
        profile: Named profile under the user's SeqEvi configuration directory.
        config: Explicit execution-profile TOML path.
        adapter: Official adapter name for complete explicit mode.
        executable: External adapter launcher path or command name.
        resource: Native upstream annotation resource directory.
        store: Local Store path or shared Store URL.
        threads: Operational worker-thread override.
        timeout_seconds: Optional external-tool timeout.

    Returns:
        The `main.annotations` DuckDB relation. It keeps the read-only result
        connection alive after this function returns.

    Raises:
        AnnotationError: If configuration, annotation, or result publication
            fails. The function does not translate errors into CLI exit codes.

    Examples:
        >>> relation = annotate(
        ...     "proteins.faa",
        ...     output="annotations.duckdb",
        ...     profile="interpro-pfam",
        ... )
        >>> "InputID" in relation.columns
        True

    Notes:
        The Store remains the incremental evidence cache. The returned file is
        a complete immutable snapshot for this invocation, including cached
        sequences and newly computed misses.
    """

    invocation = _run_annotation_application(
        fasta=Path(fasta),
        output=Path(output),
        profile=profile,
        config=config,
        adapter=adapter,
        executable=executable,
        resource=resource,
        store=store,
        threads=threads,
        timeout_seconds=timeout_seconds,
    )
    return invocation.relation


def scan_annotations(path: str | Path) -> duckdb.DuckDBPyRelation:
    """Open an existing SeqEvi result read-only as a native relation.

    Args:
        path: Published `.duckdb` result path.

    Returns:
        The validated `main.annotations` relation. The relation remains usable
        after this function returns and exposes DuckDB's native `select`,
        `filter`, Polars, and Arrow reader methods.

    Raises:
        AnnotationError: If the path is missing or fails the result contract.

    Examples:
        >>> relation = scan_annotations("annotations.duckdb")
        >>> relation.columns[:2]
        ['InputOrder', 'InputID']

    Notes:
        This function opens the database read-only and never upgrades storage,
        writes a WAL, or changes the `_seqevi` catalog.
    """

    return _scan_annotations(Path(path))


def _run_annotation_application(
    *,
    fasta: Path,
    output: Path,
    profile: str | None,
    config: str | Path | None,
    adapter: AdapterName | str | None,
    executable: str | Path | None,
    resource: str | Path | None,
    store: str | Path | None,
    threads: int | None,
    timeout_seconds: float | None,
    adapter_factory: Callable[[AdapterConfiguration], AnnotationAdapter] | None = None,
) -> AnnotationInvocation:
    inputs = resolve_annotation_inputs(
        profile=profile,
        config=config,
        adapter=adapter,
        executable=executable,
        resource=resource,
        store=store,
        threads=threads,
        timeout_seconds=timeout_seconds,
    )
    if inputs.profile is not None and inputs.profile.version == 2:
        managed = run_oci_annotation(
            fasta=fasta.expanduser().resolve(),
            output=output.expanduser().resolve(),
            profile=inputs.profile,
            store=inputs.store,
            threads=inputs.threads,
            timeout_seconds=inputs.timeout_seconds,
        )
        return AnnotationInvocation(
            relation=_scan_annotations(managed.output),
            summary=managed.summary,
            adapter=managed.adapter,
            result_schema_id=managed.result_schema_id,
        )
    if inputs.executable is None:
        raise AnnotationError("local annotation requires an executable")
    factory = create_adapter if adapter_factory is None else adapter_factory
    configured_adapter = factory(
        AdapterConfiguration(
            name=inputs.adapter,
            executable=inputs.executable,
            database=inputs.resource,
            environment=inputs.environment,
        )
    )
    metadata = _result_metadata_template(configured_adapter)
    with open_evidence_store(inputs.store) as evidence_store:
        summary = run_annotation(
            fasta_path=fasta.expanduser().resolve(),
            output_dir=output.expanduser().resolve(),
            adapter=configured_adapter,
            store=evidence_store,
            timeout_seconds=inputs.timeout_seconds,
            threads=inputs.threads,
            output_format="duckdb",
            result_metadata=metadata,
        )
    return AnnotationInvocation(
        relation=_scan_annotations(summary.output_dir),
        summary=summary,
        adapter=metadata["Adapter"],
        result_schema_id=metadata["ResultSchemaID"],
    )


def resolve_annotation_inputs(
    *,
    profile: str | None,
    config: str | Path | None,
    adapter: AdapterName | str | None,
    executable: str | Path | None,
    resource: str | Path | None,
    store: str | Path | None,
    threads: int | None,
    timeout_seconds: float | None,
) -> ResolvedAnnotationInputs:
    """Resolve one profile, config file, or complete explicit identity set."""

    if profile is not None and config is not None:
        raise AnnotationError("--profile and --config cannot be used together")
    selected_profile: ExecutionProfile | None = None
    if profile is not None:
        selected_profile = load_named_profile(profile)
    elif config is not None:
        selected_profile = load_execution_profile(Path(config))

    explicit_identity = (adapter, executable, resource)
    if selected_profile is not None:
        if any(value is not None for value in explicit_identity):
            raise AnnotationError(
                "--adapter, --executable and --resource cannot be combined with "
                "--profile or --config"
            )
        return _resolved_from_profile(
            selected_profile,
            store=store,
            threads=threads,
            timeout_seconds=timeout_seconds,
        )

    missing = [
        option
        for option, value in (
            ("--adapter", adapter),
            ("--executable", executable),
            ("--resource", resource),
        )
        if value is None
    ]
    if missing:
        raise AnnotationError(
            "explicit mode requires "
            + ", ".join(missing)
            + "; alternatively select --profile or --config"
        )
    assert adapter is not None
    assert executable is not None
    assert resource is not None
    return ResolvedAnnotationInputs(
        adapter=_coerce_adapter_name(adapter),
        executable=_resolve_executable(executable),
        resource=_resolve_resource(resource),
        store=store,
        threads=threads if threads is not None else 1,
        timeout_seconds=timeout_seconds,
        profile=None,
    )


def _resolved_from_profile(
    profile: ExecutionProfile,
    *,
    store: str | Path | None,
    threads: int | None,
    timeout_seconds: float | None,
) -> ResolvedAnnotationInputs:
    if profile.version == 2:
        return ResolvedAnnotationInputs(
            adapter=profile.adapter,
            executable=None,
            resource=profile.resource,
            store=store if store is not None else profile.store,
            threads=(
                threads
                if threads is not None
                else profile.threads
                if profile.threads is not None
                else 1
            ),
            timeout_seconds=(
                timeout_seconds
                if timeout_seconds is not None
                else profile.timeout_seconds
            ),
            profile=profile,
        )
    if profile.executable is None:
        raise AnnotationError("local execution profile has no executable")
    return ResolvedAnnotationInputs(
        adapter=profile.adapter,
        executable=profile.executable,
        resource=profile.resource,
        store=store if store is not None else profile.store,
        threads=(
            threads
            if threads is not None
            else profile.threads
            if profile.threads is not None
            else 1
        ),
        timeout_seconds=(
            timeout_seconds if timeout_seconds is not None else profile.timeout_seconds
        ),
        environment=profile.environment,
        profile=profile,
    )


def _coerce_adapter_name(value: AdapterName | str) -> AdapterName:
    try:
        return value if isinstance(value, AdapterName) else AdapterName(value)
    except ValueError as error:
        raise AnnotationError(f"unknown adapter: {value}") from error


def _resolve_executable(value: str | Path) -> Path:
    value_text = str(value)
    candidate = (
        value_text.replace("~", str(Path.home()), 1)
        if value_text.startswith("~")
        else value_text
    )
    resolved = shutil.which(candidate)
    if resolved is None:
        raise AnnotationError(f"executable not found or not executable: {value}")
    return Path(resolved).resolve()


def _resolve_resource(value: str | Path) -> Path:
    resolved = Path(value).expanduser().resolve()
    if not resolved.is_dir():
        raise AnnotationError(f"annotation resource is not a directory: {resolved}")
    return resolved


def _result_metadata_template(adapter: AnnotationAdapter) -> dict[str, str]:
    name = adapter.contract.name
    if name == "eggnog":
        upstream_tool = "eggNOG-mapper"
        upstream_version = str(getattr(adapter, "tool_version", "unknown"))
        schema_id = "eggnog-mapper/2"
    elif name == "interpro-pfam":
        upstream_tool = "InterProScan"
        upstream_version = str(getattr(adapter, "interproscan_version", "unknown"))
        schema_id = "interproscan-pfam/5"
    elif name == "dbcan-cazyme":
        upstream_tool = "dbCAN"
        upstream_version = str(getattr(adapter, "dbcan_version", "unknown"))
        schema_id = "dbcan-cazyme/5"
    else:
        upstream_tool = name
        upstream_version = "unknown"
        schema_id = f"{name}/1"
    return {
        "ResultFormatVersion": RESULT_FORMAT_VERSION,
        "ResultSchemaID": schema_id,
        "SeqEviVersion": __version__,
        "Adapter": adapter.contract.name,
        "AdapterContractVersion": adapter.contract.version,
        "UpstreamTool": upstream_tool,
        "UpstreamToolVersion": upstream_version,
        "ToolRuntimeDigest": adapter.contract.tool_runtime_digest,
        "ResourceID": adapter.contract.resource_id,
    }
