import typer

import sql_cli

app = typer.Typer(add_completion=False)


@app.command()
def main(version: bool = False):
    """
    Empower analysts to build workflows to transform data using SQL.
    """
    if version:
        typer.echo(f"Astro SQL CLI {sql_cli.__version__}")
