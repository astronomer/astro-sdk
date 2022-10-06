from typer.testing import CliRunner

from sql_cli.__main__ import app

runner = CliRunner()


def test_about():
    result = runner.invoke(app, ["about"])
    assert result.exit_code == 0
    assert "Find out more" in result.stdout


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "Astro SQL CLI 0.0.0" in result.stdout
