from __future__ import annotations

import os
from pathlib import Path

import pytest
from rich.text import Text
from typer.testing import CliRunner

import seqevi.cli
from seqevi.cli import app

from .support import FixtureAdapter, write_fixture_database, write_fixture_tool

runner = CliRunner()


def _compact_help(text: str) -> str:
    return "".join(Text.from_ansi(text).plain.split())


def test_cli_reports_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"


def test_cli_without_command_describes_current_surface() -> None:
    result = runner.invoke(app)

    assert result.exit_code == 0
    assert "Content-addressed protein sequence annotation evidence" in result.stdout
    assert "annotate" in result.stdout
    assert "resource" in result.stdout


def test_annotate_help_uses_concrete_external_input_names() -> None:
    result = runner.invoke(
        app,
        ["annotate", "--help"],
        env={"COLUMNS": "240"},
        terminal_width=240,
    )
    help_text = _compact_help(result.stdout)

    assert result.exit_code == 0
    assert "--fasta" in help_text
    assert "--executable" in help_text
    assert "--resource" in help_text
    assert "--output" in help_text
    assert "--threads" in help_text
    assert "--runtime" not in help_text
    assert "--database" not in help_text
    assert "--profile" in help_text
    assert "--config" in help_text
    assert "_resolve_executable" not in help_text


def test_serve_help_exposes_only_deployment_inputs() -> None:
    result = runner.invoke(
        app,
        ["serve", "--help"],
        env={"COLUMNS": "200"},
        terminal_width=200,
    )
    help_text = _compact_help(result.stdout)

    assert result.exit_code == 0
    assert "--database-url" in help_text
    assert "--artifacts-dir" in help_text
    assert "--maximum-batch-size" in help_text
    assert "--maximum-artifact-bytes" in help_text
    assert "--adapter" not in help_text
    assert "--fasta" not in help_text


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
            "--resource",
            str(database),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 unique sequences (0 cached, 1 computed)" in result.stdout
    assert (output / "datapackage.json").is_file()


def test_annotate_cli_loads_explicit_execution_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    config = tmp_path / "interpro.toml"
    config.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "interpro-pfam"',
                f'executable = "{executable}"',
                f'resource = "{database}"',
                f'store = "{tmp_path / "profile-store"}"',
                "threads = 3",
                "",
                "[environment]",
                'JAVA_HOME = "/opt/jdk-17"',
                "",
            )
        ),
        encoding="utf-8",
    )
    captured: list[seqevi.cli.AdapterConfiguration] = []

    def create(configuration: seqevi.cli.AdapterConfiguration) -> FixtureAdapter:
        captured.append(configuration)
        return FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        )

    monkeypatch.setattr(seqevi.cli, "create_adapter", create)
    result = runner.invoke(
        app,
        [
            "annotate",
            "--config",
            str(config),
            "--fasta",
            str(fasta),
            "--output",
            str(tmp_path / "output"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured[0].name.value == "interpro-pfam"
    assert captured[0].database == database.resolve()
    assert captured[0].environment == (("JAVA_HOME", "/opt/jdk-17"),)


def test_annotate_cli_rejects_mixed_profile_and_explicit_identity(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    config = tmp_path / "profile.toml"
    config.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "eggnog"',
                f'executable = "{executable}"',
                f'resource = "{database}"',
            )
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "annotate",
            "--config",
            str(config),
            "--adapter",
            "eggnog",
            "--fasta",
            str(fasta),
            "--output",
            str(tmp_path / "output"),
        ],
    )

    assert result.exit_code == 1
    assert "cannot be combined" in result.stderr


def test_profile_example_is_complete_toml() -> None:
    result = runner.invoke(
        app,
        ["profile", "example", "--adapter", "interpro-pfam"],
    )

    assert result.exit_code == 0
    assert "version = 1" in result.stdout
    assert 'adapter = "interpro-pfam"' in result.stdout
    assert 'resource = "/opt/interproscan/data"' in result.stdout
    assert 'path_prepend = ["/opt/jdk-17/bin"]' in result.stdout
