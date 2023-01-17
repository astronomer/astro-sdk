from __future__ import annotations

from pathlib import Path


def resolve_project_dir(project_dir: Path | None) -> Path:
    """Resolve project directory to be used by the corresponding command for its functionality."""
    # If the caller has not supplied a `project_dir`, we assume that the user is calling the command from the project
    # directory itself and hence we resolve the current working directory as the project directory.
    return project_dir.resolve() if project_dir else Path.cwd()
