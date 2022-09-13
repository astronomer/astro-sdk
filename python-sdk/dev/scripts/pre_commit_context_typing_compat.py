#!/usr/bin/env python
"""
Pre-commit hook to verify ``airflow.utils.context.Context`` is not imported in provider modules.

# TODO: This pre-commit hook can be removed once the repo has a minimum Apache Airflow requirement of 2.3.3+.
"""
from __future__ import annotations

import os
from ast import ImportFrom, NodeVisitor, parse
from pathlib import Path, PosixPath

SOURCES_ROOT = Path(__file__).parents[2]
ASTRO_ROOT = SOURCES_ROOT / "src" / "astro"
TYPING_COMPAT_PATH = "python-sdk/src/astro/utils/typing_compat.py"


class ImportCrawler(NodeVisitor):
    """AST crawler to determine if a module has an incompatible `airflow.utils.context.Context` import."""

    def __init__(self) -> None:
        self.has_incompatible_context_imports = False

    def visit_ImportFrom(self, node: ImportFrom) -> None:
        """Visit an ImportFrom node to determine if `airflow.utils.context.Context` is imported directly."""
        if self.has_incompatible_context_imports:
            return

        for alias in node.names:
            if f"{node.module}.{alias.name}" == "airflow.utils.context.Context":
                if not self.has_incompatible_context_imports:
                    self.has_incompatible_context_imports = True


def get_all_provider_files() -> list[PosixPath]:
    """Retrieve all eligible provider module files."""
    provider_files = []
    for (root, _, file_names) in os.walk(ASTRO_ROOT):
        for file_name in file_names:
            file_path = Path(root, file_name)
            if (
                file_path.is_file()
                and file_path.name.endswith(".py")
                and TYPING_COMPAT_PATH not in str(file_path)
            ):
                provider_files.append(file_path)

    return provider_files


def find_incompatible_context_imports(file_paths: list[PosixPath]) -> list[str]:
    """Retrieve any provider files that import `airflow.utils.context.Context` directly."""
    incompatible_context_imports = []
    for file_path in file_paths:
        file_ast = parse(file_path.read_text(), filename=file_path.name)
        crawler = ImportCrawler()
        crawler.visit(file_ast)
        if crawler.has_incompatible_context_imports:
            incompatible_context_imports.append(str(file_path))

    return incompatible_context_imports


if __name__ == "__main__":
    provider_files = get_all_provider_files()
    files_needing_typing_compat = find_incompatible_context_imports(provider_files)

    if len(files_needing_typing_compat) > 0:
        error_message = (
            "The following files are importing `airflow.utils.context.Context`. "
            "This is not compatible with the minimum `apache-airflow` requirement of this repository. "
            "Please use `astro.utils.typing_compat.Context` instead.\n\n\t{}".format(
                "\n".join(files_needing_typing_compat)
            )
        )

        raise SystemExit(error_message)
