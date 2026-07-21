from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

import seqevi.cli
from seqevi.cli import app

from .support import FixtureAdapter, write_fixture_database, write_fixture_tool

runner = CliRunner()


def test_cli_reports_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"


def test_cli_without_command_describes_current_surface() -> None:
    result = runner.invoke(app)

    assert result.exit_code == 0
    assert "Content-addressed protein sequence annotation evidence" in result.stdout
    assert "annotate" in result.stdout


def test_annotate_help_uses_concrete_external_input_names() -> None:
    result = runner.invoke(app, ["annotate", "--help"])

    assert result.exit_code == 0
    assert "--fasta" in result.stdout
    assert "--executable" in result.stdout
    assert "--database" in result.stdout
    assert "--output" in result.stdout
    assert "--runtime" not in result.stdout
    assert "--resource" not in result.stdout
    assert "_resolve_executable" not in result.stdout


def test_annotate_cli_runs_injected_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    output = tmp_path / "output"
    monkeypatch.setenv(
        "PATH",
        os.pathsep.join((str(executable.parent), os.environ.get("PATH", ""))),
    )

    monkeypatch.setattr(
        seqevi.cli,
        "create_adapter",
        lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
    )
    result = runner.invoke(
        app,
        [
            "annotate",
            "--adapter",
            "interpro-pfam",
            "--fasta",
            str(fasta),
            "--store",
            str(tmp_path / "store"),
            "--output",
            str(output),
            "--executable",
            executable.name,
            "--database",
            str(database),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 unique sequences (0 cached, 1 computed)" in result.stdout
    assert (output / "datapackage.json").is_file()
