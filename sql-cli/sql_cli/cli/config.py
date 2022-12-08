from __future__ import annotations

from pathlib import Path

import typer

from sql_cli.astro.command import AstroCommand
from sql_cli.cli.utils import resolve_project_dir

app = typer.Typer()


class InvalidGetConfigArg(Exception):
    pass


def _get(key: str, project_dir: Path, env: str, as_json: bool) -> str | dict:
    """
    Typer-agnostic implementation of the `config get` command.

    :param key: (optional) Key to be fetched from the configuration file. If none is specified, return all.
    :param project_dir: (optional) Path to the project directory
    :param env: Environment configuration (default, dev)
    :param as_json: If the output should be displayed as JSON.
    :returns: Either the string containing the key value or a JSON containing desired the key-value pair(s)
    """
    from sql_cli.configuration import Config

    project_dir_absolute = resolve_project_dir(project_dir)
    project_config = Config(environment=env, project_dir=project_dir_absolute).from_yaml_to_config()
    if key:
        return getattr(project_config, key)
    elif as_json:
        return project_config.as_json()
    else:
        raise InvalidGetConfigArg("Please, either define a key or use the --as-json flag")


@app.command(
    cls=AstroCommand,
    help="""
    Gets the project configuration. There are two was of using this:
         1) astro config get airflow_home
         2) astro config get --as-json

    The first returns a key from the config whereas the second returns all the configuration as JSON.
    """,
)
def get(
    key: str = typer.Argument(
        default="",
        show_default=False,
        help="Key from the configuration whose value needs to be fetched.",
    ),
    project_dir: Path = typer.Option(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    env: str = typer.Option(
        default="default",
        help="(Optional) Environment used to fetch the configuration key from.",
    ),
    as_json: bool = typer.Option(False, help="If the response should be in JSON format", show_default=True),
) -> None:
    value = _get(key, project_dir, env, as_json)
    print(value)
