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
from .store import LocalStore

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
            help="Local evidence Store path.",
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
        with LocalStore.open(store) as local_store:
            summary = run_annotation(
                fasta_path=fasta,
                output_dir=output,
                adapter=configured_adapter,
                store=local_store,
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


def main() -> None:
    """Run the SeqEvi CLI."""

    app()
