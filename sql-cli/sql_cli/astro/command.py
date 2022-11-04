import click
import typer
from typer.core import TyperCommand

from sql_cli.astro.utils import resolve_command_path


class AstroCommand(TyperCommand):
    def format_usage(self, ctx: typer.Context, formatter: click.HelpFormatter) -> None:
        """Writes the usage line into the formatter.

        This is a low-level method called by :meth:`get_usage`.
        """
        pieces = self.collect_usage_pieces(ctx)
        formatter.write_usage(resolve_command_path(ctx.command_path), " ".join(pieces))
