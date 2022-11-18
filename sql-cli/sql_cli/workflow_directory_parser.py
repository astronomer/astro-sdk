from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Iterable

import airflow
import frontmatter
import yaml
from airflow.models import BaseOperator
from yaml.scanner import ScannerError

from astro.sql import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from sql_cli.constants import GENERATED_WORKFLOW_INCLUDE_DIRECTORY, LOAD_FILE_OPERATOR
from sql_cli.operators.load_file import get_load_file_instance
from sql_cli.utils.jinja import find_template_variables


class WorkflowFile:
    """
    A Workflow is equivalent to a chain of corresponding operators in Astro SDK.

    :param root_directory: The root directory path of the project.
    :param path: The path to the workflow file.
    :param target_directory: The target directory path for the executable workflow file.
    """

    def __init__(self, root_directory: Path, path: Path, target_directory: Path) -> None:
        self.root_directory = root_directory
        self.path = path
        self.target_directory = target_directory
        self.raw_content = self.path.read_text()

        post = frontmatter.load(self.path)
        self.content = post.content
        self.metadata = post.metadata

    def __eq__(self, other: object) -> bool:
        """
        Check if this workflow file equals the given object.

        :param other: The object to compare to.

        :returns: True if this workflow file is equal to the other one.
        """
        if isinstance(other, WorkflowFile):
            return self.root_directory == other.root_directory and self.path == other.path
        return False

    def __gt__(self, other: WorkflowFile) -> bool:
        """
        Check if this workflow file path comes before the other workflow file path in alphabetic order.

        :param other: The other workflow file to compare to.

        :returns: True if this workflow file path comes before the other one in alphabetic order.
        """
        return self.root_directory / self.path > self.root_directory / other.path

    def __hash__(self) -> int:
        """
        Defines the hash for the workflow file.

        :returns: the hash of the workflow file.
        """
        return hash(self.root_directory) ^ hash(self.path)

    def get_parameters(self) -> list[str]:
        """
        Get all parameters used for parameterized workflow files.

        :returns: declared parameters for the workflow file.
        """
        return sorted(find_template_variables(self.path))

    def has_sub_directory(self) -> bool:
        """
        Check if workflow file is in a subdirectory.

        :returns: True if it's not in the parent root directory.
        """
        return self.path.parent != self.root_directory

    def get_sub_directories(self) -> Iterable[str]:
        """
        Get the directory names between root and workflow file path.

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
        Get the relative path to the executable workflow file within the DAGs folder.

        :returns: the path where workflow files without any headers are being placed.
        """
        target_full_directory = (
            self.target_directory
            / GENERATED_WORKFLOW_INCLUDE_DIRECTORY
            / self.root_directory.name
            / "/".join(self.get_sub_directories())
        )
        target_full_directory.mkdir(parents=True, exist_ok=True)

        target_path = target_full_directory / self.path.name

        target_path.write_text(self.content)

        return target_path.relative_to(self.target_directory)

    def write_raw_content_to_target_path(self) -> None:
        """
        Writes both content and headers to the target directory.
        This is because with the "render" function, we will still need
        the headers for creating proper TransformOperators
        """
        target_full_directory = (
            self.target_directory
            / GENERATED_WORKFLOW_INCLUDE_DIRECTORY
            / self.root_directory.name
            / "/".join(self.get_sub_directories())
        )
        target_full_directory.mkdir(parents=True, exist_ok=True)

        target_path = target_full_directory / self.path.name

        target_path.write_text(self.raw_content)

    @abstractmethod
    def to_operator(self) -> BaseOperator:
        raise NotImplementedError()


class SqlFile(WorkflowFile):
    """A SqlFile is equivalent to a transform step in the Astro SDK."""

    def __init__(self, root_directory: Path, path: Path, target_directory: Path) -> None:
        super().__init__(root_directory, path, target_directory)

    def to_operator(self) -> TransformOperator:
        """
        Converts SQLFile into a TransformOperator that can be added to a DAG.
        Any relevant metadata from the file frontmatter will be passed to the TransformOperator,
        though we do not pass parameter dependencies at this stage.

        :return: a TransformOperator
        """
        kwargs = {
            "conn_id": self.metadata.get("conn_id"),
            "parameters": None,
            "handler": None,
            "database": self.metadata.get("database"),
            "schema": self.metadata.get("schema"),
            "python_callable": lambda: (str(self.path), None),
            "sql": self.content,
        }
        if airflow.__version__.startswith("2.2."):
            kwargs["op_args"] = []
        return TransformOperator(**kwargs)


class YamlFile(WorkflowFile):
    operator_instance_builder_callable_map = {LOAD_FILE_OPERATOR: get_load_file_instance}

    def __init__(self, root_directory: Path, path: Path, target_directory: Path) -> None:
        super().__init__(root_directory, path, target_directory)
        with path.open() as yaml_file:
            self.yaml_content = yaml.safe_load(yaml_file)
        self.operator = self._get_operator()
        self.yaml_file_name = self.path.stem

    def _get_operator(self) -> str:
        top_level_keys = list(self.yaml_content.keys())
        top_level_keys_count = len(top_level_keys)
        if top_level_keys_count > 1:
            raise ScannerError(
                f"Only one top level operator expected. Got {top_level_keys_count} operators: {top_level_keys}"
            )
        operator = top_level_keys[0]
        if operator not in YamlFile.operator_instance_builder_callable_map:
            raise NotImplementedError(f"Operator support for {operator} not available")
        return operator

    def get_yaml_content(self):
        return self.yaml_content[self.operator]

    def to_operator(self) -> LoadFileOperator:
        return self.operator_instance_builder_callable_map[self.operator](
            self.get_yaml_content(), self.yaml_file_name
        )


SUPPORTED_FILES = {"*.sql": SqlFile, "*.yaml": YamlFile, "*.yml": YamlFile}


def get_files_by_type(
    directory: Path, file_type: str, target_directory: Path | None
) -> set[SqlFile] | set[YamlFile]:
    """
    Get all files of the given file type within a directory.

    :param directory: The directory look in for files.
    :param file_type: The type of the files to look for.
    :param target_directory: The target directory path for the executable workflow.

    :returns: the workflow files found in the directory.
    """
    return {
        SUPPORTED_FILES[file_type](
            root_directory=directory, path=child, target_directory=target_directory  # type: ignore
        )
        for child in directory.rglob(file_type)
        if child.is_file() and not child.is_symlink()
    }


def get_workflow_files(directory: Path, target_directory: Path | None) -> set[WorkflowFile]:
    workflow_files: set[WorkflowFile] = set()
    for file_type in SUPPORTED_FILES:
        files = get_files_by_type(directory, file_type, target_directory=target_directory)
        workflow_files = workflow_files.union(files)
    return workflow_files
