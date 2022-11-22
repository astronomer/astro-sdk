from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2.environment import Environment
from jinja2.loaders import FileSystemLoader
from jinja2.meta import find_undeclared_variables
from jinja2.runtime import StrictUndefined


def find_template_variables(file_path: Path) -> set[str]:
    """
    Find template variables in given file path which needed to be declared when rendering.

    :param file_path: The file path to check for variables.

    :returns: all undeclared variables.
    """
    env = Environment(
        loader=FileSystemLoader(file_path.parent),
        undefined=StrictUndefined,
        autoescape=True,
    )
    template_source = env.loader.get_source(env, file_path.name)
    parsed_content = env.parse(template_source)
    return find_undeclared_variables(parsed_content)  # type: ignore


def render(
    template_file: Path,
    context: dict[str, Any],
    output_file: Path,
    searchpath: Path = Path(__file__).parent.parent,
) -> None:
    """
    Render the template_file with context to a file.

    :param template_file: The path to the template file.
    :param context: The context to pass to the template file.
    :param output_file: The file to output the rendered version.
    :param searchpath: The default is the project directory.
    """
    (
        Environment(
            loader=FileSystemLoader(searchpath),
            undefined=StrictUndefined,
            autoescape=True,
            keep_trailing_newline=True,
        )
        .get_template(template_file.as_posix())
        .stream(**context)
        .dump(output_file.as_posix())
    )
