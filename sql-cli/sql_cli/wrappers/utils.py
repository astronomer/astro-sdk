import os


def resolve_command_path(command_path: str) -> str:
    """
    Resolve the command path using the current environment.

    :param command_path: The command path to resolve.

    :returns: the resolved command path.
    """
    if os.getenv("ASTRO_CLI"):  # being set in astro-cli dockerfile for sql-cli
        return " ".join(["astro", command_path])
    return command_path
