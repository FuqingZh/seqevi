from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import sqlalchemy
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
    assert result.stdout.strip() == "0.3.0"


def test_cli_without_command_describes_current_surface() -> None:
    result = runner.invoke(app)

    assert result.exit_code == 0
    assert "Content-addressed protein sequence annotation evidence" in result.stdout
    assert "annotate" in result.stdout
    assert "resource" in result.stdout


@pytest.mark.parametrize(
    ("command", "maintenance_name", "revision"),
    [
        (
            "store-maintenance-prepare",
            "maintenance_prepare_database",
            "0002_artifact_byte_size_bigint",
        ),
        (
            "store-maintenance-upgrade",
            "maintenance_upgrade_database",
            "0003_evidence_claim_leases",
        ),
        (
            "store-maintenance-downgrade",
            "maintenance_downgrade_database",
            "0004_claim_sessions",
        ),
        (
            "store-maintenance-prepare-rollback",
            "maintenance_prepare_database",
            "0003_evidence_claim_leases",
        ),
    ],
)
def test_maintenance_commands_normalize_postgresql_url_for_psycopg3(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    maintenance_name: str,
    revision: str,
) -> None:
    urls: list[str] = []

    class FakeEngine:
        def dispose(self) -> None:
            pass

    def fake_create_engine(url: str) -> FakeEngine:
        urls.append(url)
        return FakeEngine()

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    monkeypatch.setattr(
        f"seqevi.store.migration.{maintenance_name}", lambda *_args, **_kwargs: None
    )
    result = runner.invoke(
        app,
        [
            command,
            "--database-url",
            "postgresql://seqevi@postgres/seqevi",
            "--acknowledge-database",
            "postgresql:seqevi@postgres:5432/seqevi",
            "--acknowledge-revision",
            revision,
        ],
    )

    assert result.exit_code == 0, result.output
    assert urls == ["postgresql+psycopg://seqevi@postgres/seqevi"]


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
    output = tmp_path / "output.duckdb"
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
    assert output.is_file()


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
            str(tmp_path / "output.duckdb"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured[0].name.value == "interpro-pfam"
    assert captured[0].database == database.resolve()
    assert captured[0].environment == (("JAVA_HOME", "/opt/jdk-17"),)


def test_annotate_cli_json_reports_result_and_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
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
            "--json",
            "--adapter",
            "interpro-pfam",
            "--fasta",
            str(fasta),
            "--store",
            str(tmp_path / "store"),
            "--output",
            str(tmp_path / "output.duckdb"),
            "--executable",
            str(executable),
            "--resource",
            str(database),
        ],
    )

    payload = json.loads(result.stdout)
    assert result.exit_code == 0, result.output
    assert payload["status"] == "ok"
    assert payload["adapter"] == "fixture"
    assert payload["result_schema"] == "fixture/1"
    assert payload["counts"] == {
        "input_records": 1,
        "unique_sequences": 1,
        "cache_hits": 0,
        "computed": 1,
        "hits": 1,
        "no_hits": 0,
    }
    assert payload["metrics"]["existing_finalizations"] == 0
    assert payload["output"].endswith("output.duckdb")


def test_annotate_cli_json_reports_typed_error(tmp_path: Path) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "annotate",
            "--json",
            "--fasta",
            str(fasta),
            "--output",
            str(tmp_path / "output.duckdb"),
        ],
    )

    payload = json.loads(result.stderr)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert payload["error_type"] == "AnnotationError"


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

    dbcan = runner.invoke(
        app,
        ["profile", "example", "--adapter", "dbcan-cazyme"],
    )
    assert dbcan.exit_code == 0
    assert 'adapter = "dbcan-cazyme"' in dbcan.stdout
    assert 'executable = "/opt/dbcan-5.2.9/bin/run_dbcan"' in dbcan.stdout
    assert 'resource = "/data/dbcan/db_v5-2-9_5-5-2026/raw"' in dbcan.stdout


def test_profile_init_list_and_show_use_isolated_xdg_home(tmp_path: Path) -> None:
    config_home = tmp_path / "config"
    environment = {"XDG_CONFIG_HOME": str(config_home)}

    initialized = runner.invoke(
        app,
        ["profile", "init", "zeta", "--adapter", "eggnog"],
        env=environment,
    )
    duplicate = runner.invoke(
        app,
        ["profile", "init", "zeta", "--adapter", "interpro-pfam"],
        env=environment,
    )
    runner.invoke(
        app,
        ["profile", "init", "Alpha", "--adapter", "interpro-pfam"],
        env=environment,
    )
    listed = runner.invoke(app, ["profile", "list"], env=environment)

    assert initialized.exit_code == 0
    assert duplicate.exit_code == 1
    assert "already exists" in duplicate.stderr
    assert listed.stdout.splitlines() == ["Alpha", "zeta"]

    profile_path = config_home / "seqevi" / "profiles" / "zeta.toml"
    executable = write_fixture_tool(tmp_path / "tool")
    resource = write_fixture_database(tmp_path / "resource")
    profile_path.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "eggnog"',
                f'executable = "{executable}"',
                f'resource = "{resource}"',
                "",
                "[environment]",
                'API_TOKEN = "secret-value"',
            )
        ),
        encoding="utf-8",
    )
    shown = runner.invoke(app, ["profile", "show", "zeta"], env=environment)

    assert shown.exit_code == 0, shown.output
    assert "adapter: eggnog" in shown.stdout
    assert "environment_names: API_TOKEN" in shown.stdout
    assert "secret-value" not in shown.stdout
