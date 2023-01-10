from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2.environment import Environment
from jinja2.loaders import FileSystemLoader
from jinja2.runtime import StrictUndefined


def render(template_file: Path, context: dict[str, Any], output_file: Path) -> None:
    """
    Render the template_file with context to a file.

    :param template_file: The path to the template file.
    :param context: The context to pass to the template file.
    :param output_file: The file to output the rendered version.
    """
    (
        Environment(
            loader=FileSystemLoader(Path(__file__).parent),
            undefined=StrictUndefined,
            autoescape=True,
            keep_trailing_newline=True,
        )
        .get_template(template_file.as_posix())
        .stream(**context)
        .dump(output_file.as_posix())
    )
