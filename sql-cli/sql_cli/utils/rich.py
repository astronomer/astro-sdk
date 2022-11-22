from typing import Union

import click
from rich.panel import Panel
from typer.rich_utils import (
    ALIGN_ERRORS_PANEL,
    ERRORS_PANEL_TITLE,
    STYLE_ERRORS_PANEL_BORDER,
    STYLE_ERRORS_SUGGESTION,
    _get_rich_console,
    highlighter,
)

from sql_cli.astro.utils import resolve_command_path


def rich_format_error(self: click.ClickException) -> None:
    """Print richly formatted click errors.

    Called by custom exception handler to print richly formatted click errors.
    Mimics original click.ClickException.echo() function but with rich formatting.
    """
    console = _get_rich_console(stderr=True)
    ctx: Union[click.Context, None] = getattr(self, "ctx", None)
    if ctx is not None:
        console.print(ctx.get_usage())

    if ctx is not None and ctx.command.get_help_option(ctx) is not None:
        console.print(
            f"Try [blue]'{resolve_command_path(ctx.command_path)} {ctx.help_option_names[0]}'[/] for help.",
            style=STYLE_ERRORS_SUGGESTION,
        )

    console.print(
        Panel(
            highlighter(self.format_message()),
            border_style=STYLE_ERRORS_PANEL_BORDER,
            title=ERRORS_PANEL_TITLE,
            title_align=ALIGN_ERRORS_PANEL,
        )
    )
