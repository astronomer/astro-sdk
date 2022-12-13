from __future__ import annotations

from pathlib import Path

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
