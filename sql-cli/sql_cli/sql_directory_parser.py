from __future__ import annotations

from pathlib import Path
from typing import Iterable

import frontmatter

from sql_cli.utils.jinja import find_template_variables


class SqlFile:
    """
    A SqlFile is equivalent to a transform step in the Astro SDK.

    :param root_directory: The root directory path of the project.
    :param path: The path to the sql file.
    :param target_directory: The target directory path for the executable sql.
    """

    def __init__(self, root_directory: Path, path: Path, target_directory: Path) -> None:
        self.root_directory = root_directory
        self.path = path
        self.target_directory = target_directory

        post = frontmatter.load(self.path)
        self.content = post.content
        self.metadata = post.metadata

    def __eq__(self, other: object) -> bool:
        """
        Check if this sql file equals the given object.

        :param other: The object to compare to.

        :returns: True if this sql file is equal to the other one.
        """
        if isinstance(other, SqlFile):
            return self.root_directory == other.root_directory and self.path == other.path
        return False

    def __gt__(self, other: SqlFile) -> bool:
        """
        Check if this sql file path comes before the other sql file path in alphabetic order.

        :param other: The other sql file to compare to.

        :returns: True if this sql file path comes before the other one in alphabetic order.
        """
        return self.root_directory / self.path > self.root_directory / other.path

    def __hash__(self) -> int:
        """
        Defines the hash for the sql file.

        :returns: the hash of the sql file.
        """
        return hash(self.root_directory) ^ hash(self.path)

    def get_parameters(self) -> list[str]:
        """
        Get all parameters used for parameterized sql queries.

        :returns: declared parameters for the sql query.
        """
        return sorted(find_template_variables(self.path))

    def has_sub_directory(self) -> bool:
        """
        Check if sql file is in a sub directory.

        :returns: True if it's not in the parent root directory.
        """
        return self.path.parent != self.root_directory

    def get_sub_directories(self) -> Iterable[str]:
        """
        Get the directory names between root and sql file path.

        :yields: a sub directory name.
        """
        for parent in self.path.parents:
            if parent == self.root_directory:
                break
            yield parent.name

    def get_variable_name(self) -> str:
        """
        Get the variable name used as a unique identifier as a python variable.

        :returns: the file name without suffix.
        """
        if self.has_sub_directory():
            return f"{'__'.join(self.get_sub_directories())}__{self.path.stem}"
        return self.path.stem

    def get_relative_target_path(self) -> Path:
        """
        Get the relative path to the executable sql file within the DAGs folder.

        :returns: the path where SQL files without any headers are being placed.
        """
        target_full_directory = (
            self.target_directory / "sql" / self.root_directory.name / "/".join(self.get_sub_directories())
        )
        target_full_directory.mkdir(parents=True, exist_ok=True)

        target_path = target_full_directory / self.path.name

        target_path.write_text(self.content)

        return target_path.relative_to(self.target_directory)


def get_sql_files(directory: Path, target_directory: Path) -> set[SqlFile]:
    """
    Get all sql files within a directory.

    :param directory: The directory look in for sql files.
    :param target_directory: The target directory path for the executable sql.

    :returns: the sql files found in the directory.
    """
    return {
        SqlFile(root_directory=directory, path=child, target_directory=target_directory)
        for child in directory.rglob("*.sql")
        if child.is_file() and not child.is_symlink()
    }
