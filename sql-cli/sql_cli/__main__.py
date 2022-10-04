import typer

import sql_cli

app = typer.Typer(add_completion=False)


@app.command()
def version() -> None:
    """
    Print the SQL CLI version.
    """
    typer.echo(f"Astro SQL CLI {sql_cli.__version__}")


@app.command()
def about() -> None:
    """
    Print additional information about the project.
    """
    typer.echo("Find out more: https://github.com/astronomer/astro-sdk/sql-cli")


if __name__ == "__main__":  # pragma: no cover
    app()
