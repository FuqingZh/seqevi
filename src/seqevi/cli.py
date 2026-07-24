"""SeqEvi command-line entrypoint."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from . import __version__
from .adapters import (
    AdapterConfiguration,
    AdapterName,
    create_adapter,
)
from .annotate import run_annotation
from .errors import AnnotationError, FastaValidationError, StoreError
from .execution_profile import (
    ExecutionProfile,
    initialize_named_profile,
    list_named_profiles,
    load_execution_profile,
    load_named_profile,
    profile_example,
    redacted_effective_configuration,
)
from .resource_lock import resource_lock_path
from .store import open_evidence_store

app = typer.Typer(
    name="seqevi",
    help="Content-addressed protein sequence annotation evidence.",
    invoke_without_command=True,
    no_args_is_help=False,
)
resource_app = typer.Typer(
    name="resource",
    help="Initialize and verify immutable database resource locks.",
    no_args_is_help=True,
)
profile_app = typer.Typer(
    name="profile",
    help="Inspect and validate reusable external-tool execution profiles.",
    no_args_is_help=True,
)
app.add_typer(resource_app, name="resource")
app.add_typer(profile_app, name="profile")


@dataclass(frozen=True, slots=True)
class _ResolvedAnnotationInputs:
    adapter: AdapterName
    executable: Path
    resource: Path
    store: str | None
    threads: int
    timeout_seconds: float | None
    environment: tuple[tuple[str, str], ...] = ()


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


def _resolve_executable(value: str) -> Path:
    candidate = str(Path(value).expanduser()) if value.startswith("~") else value
    resolved = shutil.which(candidate)
    if resolved is None:
        raise typer.BadParameter(f"Executable not found or not executable: {value}")
    return Path(resolved).resolve()


@app.callback()
def root(
    context: typer.Context,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the SeqEvi version and exit.",
        ),
    ] = False,
) -> None:
    """Run SeqEvi commands or show top-level help."""

    del version
    if context.invoked_subcommand is None:
        typer.echo(context.get_help())


@app.command("annotate")
def annotate_command(
    fasta: Annotated[
        Path,
        typer.Option(
            "--fasta",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Protein FASTA to annotate.",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "-o",
            "--output",
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
            help="New directory for the invocation package.",
        ),
    ],
    adapter: Annotated[
        AdapterName | None,
        typer.Option("--adapter", help="Official annotation adapter to run."),
    ] = None,
    executable: Annotated[
        Path | None,
        typer.Option(
            "--executable",
            parser=_resolve_executable,
            metavar="EXECUTABLE",
            help="Adapter tool executable or command name.",
        ),
    ] = None,
    resource: Annotated[
        Path | None,
        typer.Option(
            "--resource",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
            help="Native upstream annotation resource directory.",
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            help="Named profile under the SeqEvi user configuration directory.",
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Explicit execution-profile TOML file.",
        ),
    ] = None,
    store: Annotated[
        str | None,
        typer.Option(
            "--store",
            envvar="SEQEVI_STORE",
            help="Local Store path or shared Store HTTP(S) URL.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(
            "--timeout-seconds",
            min=0.001,
            help="Optional external tool timeout in seconds.",
        ),
    ] = None,
    threads: Annotated[
        int | None,
        typer.Option(
            "--threads",
            min=1,
            help="Worker threads; overrides the profile default.",
        ),
    ] = None,
) -> None:
    """Reuse exact evidence and annotate only cache-miss sequences."""

    try:
        inputs = _resolve_annotation_inputs(
            profile_name=profile,
            config_path=config,
            adapter=adapter,
            executable=executable,
            resource=resource,
            store=store,
            threads=threads,
            timeout_seconds=timeout_seconds,
        )
        configured_adapter = create_adapter(
            AdapterConfiguration(
                name=inputs.adapter,
                executable=inputs.executable,
                database=inputs.resource,
                environment=inputs.environment,
            )
        )
        with open_evidence_store(inputs.store) as evidence_store:
            summary = run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=configured_adapter,
                store=evidence_store,
                timeout_seconds=inputs.timeout_seconds,
                threads=inputs.threads,
            )
    except (AnnotationError, FastaValidationError, StoreError) as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error

    typer.echo(
        f"Annotated {summary.unique_sequences} unique sequences "
        f"({summary.cache_hits} cached, {summary.computed} computed); "
        f"output: {summary.output_dir}"
    )


def _resolve_annotation_inputs(
    *,
    profile_name: str | None,
    config_path: Path | None,
    adapter: AdapterName | None,
    executable: Path | None,
    resource: Path | None,
    store: str | None,
    threads: int | None,
    timeout_seconds: float | None,
) -> _ResolvedAnnotationInputs:
    if profile_name is not None and config_path is not None:
        raise AnnotationError("--profile and --config cannot be used together")
    selected_profile: ExecutionProfile | None = None
    if profile_name is not None:
        selected_profile = load_named_profile(profile_name)
    elif config_path is not None:
        selected_profile = load_execution_profile(config_path)

    explicit_identity = (adapter, executable, resource)
    if selected_profile is not None:
        if any(value is not None for value in explicit_identity):
            raise AnnotationError(
                "--adapter, --executable and --resource cannot be combined with "
                "--profile or --config"
            )
        return _ResolvedAnnotationInputs(
            adapter=selected_profile.adapter,
            executable=selected_profile.executable,
            resource=selected_profile.resource,
            store=store if store is not None else selected_profile.store,
            threads=(
                threads
                if threads is not None
                else selected_profile.threads
                if selected_profile.threads is not None
                else 1
            ),
            timeout_seconds=(
                timeout_seconds
                if timeout_seconds is not None
                else selected_profile.timeout_seconds
            ),
            environment=selected_profile.environment,
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
            "explicit mode requires " + ", ".join(missing) + "; "
            "alternatively select --profile or --config"
        )
    assert adapter is not None
    assert executable is not None
    assert resource is not None
    return _ResolvedAnnotationInputs(
        adapter=adapter,
        executable=executable,
        resource=resource,
        store=store,
        threads=threads if threads is not None else 1,
        timeout_seconds=timeout_seconds,
    )


@profile_app.command("example")
def profile_example_command(
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official adapter for the example profile."),
    ],
) -> None:
    """Print a complete execution-profile TOML example."""

    typer.echo(profile_example(adapter), nl=False)


@profile_app.command("validate")
def profile_validate_command(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Execution-profile TOML file to validate.",
        ),
    ],
) -> None:
    """Validate one profile without launching its annotation tool."""

    try:
        validated = load_execution_profile(config)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(
        f"Valid {validated.adapter.value} profile: {validated.source}; "
        f"resource: {validated.resource}"
    )


@profile_app.command("init")
def profile_init_command(
    name: Annotated[str, typer.Argument(help="Name for the new user profile.")],
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official adapter for the new profile."),
    ],
) -> None:
    """Create a complete named profile without overwriting an existing file."""

    try:
        destination = initialize_named_profile(name, adapter)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(f"Created {adapter.value} profile: {destination}")


@profile_app.command("list")
def profile_list_command() -> None:
    """List named profiles in deterministic order."""

    try:
        names = list_named_profiles()
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    for name in names:
        typer.echo(name)


@profile_app.command("show")
def profile_show_command(
    name: Annotated[str, typer.Argument(help="Named user profile to inspect.")],
) -> None:
    """Show resolved profile configuration with environment values redacted."""

    try:
        selected = load_named_profile(name)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(redacted_effective_configuration(selected), nl=False)


@resource_app.command("verify")
def verify_resource_command(
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official annotation adapter to verify."),
    ],
    executable: Annotated[
        Path,
        typer.Option(
            "--executable",
            parser=_resolve_executable,
            metavar="EXECUTABLE",
            help="Adapter tool executable or command name.",
        ),
    ],
    resource: Annotated[
        Path,
        typer.Option(
            "--resource",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
            help="Native upstream annotation resource directory.",
        ),
    ],
) -> None:
    """Fully hash a database and verify its SeqEvi resource lock."""

    try:
        configured_adapter = create_adapter(
            AdapterConfiguration(
                name=adapter,
                executable=executable,
                database=resource,
                verify_resource=True,
            )
        )
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error

    lock_path = resource_lock_path(resource)
    lock_status = str(lock_path) if lock_path.is_file() else "not persisted (read-only)"
    typer.echo(
        f"Verified resource {configured_adapter.contract.resource_id}; "
        f"lock: {lock_status}"
    )


@app.command("serve")
def serve_command(
    database_url: Annotated[
        str,
        typer.Option(
            "--database-url",
            envvar="SEQEVI_DATABASE_URL",
            help="PostgreSQL SQLAlchemy URL for shared metadata.",
        ),
    ],
    artifacts_dir: Annotated[
        Path,
        typer.Option(
            "--artifacts-dir",
            envvar="SEQEVI_ARTIFACTS_DIR",
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
            help="Writable POSIX directory for content-addressed artifacts.",
        ),
    ],
    host: Annotated[
        str,
        typer.Option("--host", help="HTTP bind address."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option("--port", min=1, max=65535, help="HTTP bind port."),
    ] = 8000,
    maximum_batch_size: Annotated[
        int,
        typer.Option("--maximum-batch-size", min=1, max=10000),
    ] = 1000,
    maximum_artifact_bytes: Annotated[
        int,
        typer.Option("--maximum-artifact-bytes", min=1),
    ] = 512 * 1024 * 1024,
) -> None:
    """Run the passive PostgreSQL/POSIX shared Store service."""

    try:
        import uvicorn

        from .service import ServiceSettings, create_service_app

        settings = ServiceSettings(
            database_url=database_url,
            artifacts_dir=artifacts_dir,
            maximum_batch_size=maximum_batch_size,
            maximum_artifact_bytes=maximum_artifact_bytes,
        )
        uvicorn.run(create_service_app(settings), host=host, port=port)
    except ImportError as error:
        typer.echo(
            "Error: shared Store dependencies are missing; install seqevi[server]",
            err=True,
        )
        raise typer.Exit(code=1) from error


def main() -> None:
    """Run the SeqEvi CLI."""

    app()
