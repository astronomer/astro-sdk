from __future__ import annotations
from __future__ import annotations

from pathlib import Path

import frontmatter
from astro.sql.operators.transform import TransformOperator
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

    def to_transform_operator(self):
        return TransformOperator(
            conn_id=self.metadata.get("conn_id"),
            parameters=None,
            handler=None,
            database=self.metadata.get("database"),
            schema=self.metadata.get("schema"),
            python_callable=lambda: (str(self.path), None),
            sql=self.content,
        )


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
