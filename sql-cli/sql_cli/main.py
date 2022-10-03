import typer

import sql_cli

app = typer.Typer(add_completion=False)


@app.command()
def version():
    """
    Empower analysts to build workflows to transform data using SQL.
    """
    typer.echo(f"Astro SQL CLI {sql_cli.__version__}")


@app.command()
def about():
    """
    About the project.
    """
    typer.echo("Find out more: https://github.com/astronomer/astro-sdk/sql-cli")
