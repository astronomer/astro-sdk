import os

import click
import typer
from typer.core import TyperCommand


class AstroCommand(TyperCommand):
    def format_usage(self, ctx: typer.Context, formatter: click.HelpFormatter) -> None:
        """Writes the usage line into the formatter.

        This is a low-level method called by :meth:`get_usage`.
        """
        pieces = self.collect_usage_pieces(ctx)
        command_path = ctx.command_path
        if os.getenv("ASTRO_CLI"):
            command_path = " ".join(["astro", ctx.command_path])
        formatter.write_usage(command_path, " ".join(pieces))
