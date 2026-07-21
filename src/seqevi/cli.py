"""SeqEvi command-line entrypoint."""

from __future__ import annotations

from typing import Annotated

import typer

from . import __version__

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
    """Show the current foundation CLI until v1 commands are implemented."""

    del version
    if context.invoked_subcommand is None:
        typer.echo(context.get_help())


def main() -> None:
    """Run the SeqEvi CLI."""

    app()
