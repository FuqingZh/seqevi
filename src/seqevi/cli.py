"""SeqEvi command-line entrypoint."""

from __future__ import annotations

import shutil
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
from .store import open_evidence_store

app = typer.Typer(
    name="seqevi",
    help="Content-addressed protein sequence annotation evidence.",
    invoke_without_command=True,
    no_args_is_help=False,
)


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
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official annotation adapter to run."),
    ],
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
    executable: Annotated[
        Path,
        typer.Option(
            "--executable",
            parser=_resolve_executable,
            metavar="EXECUTABLE",
            help="Adapter tool executable or command name.",
        ),
    ],
    database: Annotated[
        Path,
        typer.Option(
            "--database",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
            help="Native upstream annotation database directory.",
        ),
    ],
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
) -> None:
    """Reuse exact evidence and annotate only cache-miss sequences."""

    try:
        configured_adapter = create_adapter(
            AdapterConfiguration(
                name=adapter,
                executable=executable,
                database=database,
            )
        )
        with open_evidence_store(store) as evidence_store:
            summary = run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=configured_adapter,
                store=evidence_store,
                timeout_seconds=timeout_seconds,
            )
    except (AnnotationError, FastaValidationError, StoreError) as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error

    typer.echo(
        f"Annotated {summary.unique_sequences} unique sequences "
        f"({summary.cache_hits} cached, {summary.computed} computed); "
        f"output: {summary.output_dir}"
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
