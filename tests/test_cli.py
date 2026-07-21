from typer.testing import CliRunner

from seqevi.cli import app

runner = CliRunner()


def test_cli_reports_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"


def test_cli_without_command_describes_current_surface() -> None:
    result = runner.invoke(app)

    assert result.exit_code == 0
    assert "Content-addressed protein sequence annotation evidence" in result.stdout
