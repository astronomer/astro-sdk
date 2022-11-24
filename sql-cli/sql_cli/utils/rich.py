from typing import IO, Any, Optional, Union

import click
from rich.highlighter import ReprHighlighter
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


def rprint(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    file: Optional[IO[str]] = None,
    flush: bool = False,  # skipcq: PYL-W0613
) -> None:
    r"""Print object(s) supplied via positional arguments.
    This function has an identical signature to the built-in print.
    For more advanced features, see the :class:`~rich.console.Console` class.

    Args:
        sep (str, optional): Separator between printed objects. Defaults to " ".
        end (str, optional): Character to write at end of output. Defaults to "\\n".
        file (IO[str], optional): File to write to, or None for stdout. Defaults to None.
        flush (bool, optional): Has no effect as Rich always flushes output. Defaults to False.

    """
    from rich.console import Console

    # We use the rich console from typer which allows us to set FORCE_TERMINAL & MAX_WIDTH
    # both we use for consistent output locally (via Makefile) and in CI
    console = _get_rich_console()
    # And we reset the hightlighter to the default highlighter,
    # because the OptionHighlighter only makes sense for options e.g in help output
    console.highlighter = ReprHighlighter()

    write_console = console if file is None else Console(file=file)
    return write_console.print(*objects, sep=sep, end=end)
