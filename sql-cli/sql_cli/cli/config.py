from __future__ import annotations

import json
from pathlib import Path

import typer

from sql_cli.astro.command import AstroCommand
from sql_cli.cli.utils import resolve_project_dir
from sql_cli.constants import DEFAULT_ENVIRONMENT

app = typer.Typer()


class InvalidConfigException(Exception):
    pass


def _get(key: str, project_dir: Path, env: str, as_json: bool) -> str:
    """
    Typer-agnostic implementation of the `config get` command.

    :param key: Key to be fetched from the configuration file.
    :param project_dir: Path to the project directory
    :param env: SQL CLI environment (default, dev)
    :param as_json: If the output should be displayed as JSON.
    :returns: Either the string containing the key value or a JSON containing desired the key-value pair(s)
    """
    from sql_cli.configuration import Config

    project_dir_absolute = resolve_project_dir(project_dir)
    project_config = Config(environment=env, project_dir=project_dir_absolute).from_yaml_to_config()
    if key and as_json:
        raise InvalidConfigException("Sorry, key and --json are mutually exclusive. Give only one of them.")
    elif key:
        return getattr(project_config, key)
    elif as_json:
        return json.dumps(project_config.to_dict())
    else:
        raise InvalidConfigException(
            "Please, either give a key or use the --json flag. It is mandatory to give one of them."
        )


def _set(key: str, project_dir: Path, env: str, astro_deployment_id: str, astro_workspace_id: str) -> None:
    """
    Set deployment configuration associated to a SQL CLI environment.

    :param key: Configuration property (key) to be set. At the mmoment only "deploy" is accepted.
    :param project_dir: Path to the project directory.
    :param env: SQL CLI environment (default, dev).
    :param astro_deployment_id: Astronomer Cloud deployment ID.
    :param astro_workspace_id: Astronomer Cloud deployment workspace ID.
    """
    from sql_cli.configuration import Config

    if key != "deploy":
        raise InvalidConfigException(
            f"The key {key} is not supported yet. Only deploy is currently supported."
        )

    project_dir_absolute = resolve_project_dir(project_dir)
    project_config = Config(environment=env, project_dir=project_dir_absolute).from_yaml_to_config()
    config_filepath = project_config.get_env_config_filepath()

    project_config.write_value_to_yaml(
        "deployment", "astro_deployment_id", astro_deployment_id, config_filepath
    )
    project_config.write_value_to_yaml(
        "deployment", "astro_workspace_id", astro_workspace_id, config_filepath
    )


@app.command(
    "get",
    cls=AstroCommand,
    help="""
    Get the project configuration.

    Example of usages:
    $ flow config get airflow_home
    $ flow config get --json

    The first returns a key from the config whereas the second returns all the configuration as JSON.
    """,
)
def get_config(
    key: str = typer.Argument(
        default="",
        show_default=False,
        help="Configuration key which value needs to be fetched.",
    ),
    project_dir: Path = typer.Option(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    env: str = typer.Option(
        default=DEFAULT_ENVIRONMENT,
        help="(Optional) Environment used to fetch the configuration key from.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="If the response should be in JSON format", show_default=True
    ),
) -> None:
    value = _get(key, project_dir, env, as_json)
    print(value)


@app.command(
    "set",
    cls=AstroCommand,
    help="""
    Set the project configuration.

    Example:
    $ flow config set deploy --env=dev --astro-workspace-id=cl123 --astro-deployment-id=cl345
    """,
)
def set_config(
    key: str = typer.Argument(
        default="",
        show_default=False,
        help="Key from the configuration whose value needs to be fetched.",
    ),
    project_dir: Path = typer.Option(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    astro_deployment_id: str = typer.Option(
        ...,
        help="Astro deployment deployment ID (e.g. cl8bqua474573873jwenjhb6bbo)",
    ),
    astro_workspace_id: str = typer.Option(
        ...,
        help="Astro deployment workspace ID (e.g. cl6geh889308371i01vscssm4q)",
    ),
    env: str = typer.Option(
        default=DEFAULT_ENVIRONMENT,
        help="(Optional) Environment used to fetch the configuration key from.",
    ),
) -> None:
    _set(
        key, project_dir, env, astro_workspace_id=astro_workspace_id, astro_deployment_id=astro_deployment_id
    )
