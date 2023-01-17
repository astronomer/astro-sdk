from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Any, Iterable

import airflow
import frontmatter
import yaml
from airflow.models import BaseOperator
from yaml.scanner import ScannerError

from astro.sql import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from sql_cli.constants import GENERATED_WORKFLOW_INCLUDE_DIRECTORY
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
        self.metadata, self.content = frontmatter.parse(self.raw_content)

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

    @property
    def name(self) -> str:
        """
        The file name of the workflow file.

        :returns the file name without last suffix.
        """
        return self.path.stem

    @property
    def operator_name(self) -> str:
        """
        The name of the aql function to run.

        :returns: the function name of the operator in aql.
        """
        raise NotImplementedError("Operator name is not defined.")

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
            return f"{'__'.join(self.get_sub_directories())}__{self.name}"
        return self.name

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
        """
        Deserialize the operator to an actual aql operator.

        :returns: the aql operator.
        """
        raise NotImplementedError("aql operator not implemented!")


class SqlFile(WorkflowFile):
    """A SqlFile is equivalent to a transform step in the Astro SDK."""

    @property
    def operator_name(self) -> str:
        """
        The name of the aql function to run.

        :returns: the function name of the operator in aql.
        """
        return "transform_file"

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
    """
    A YamlFile, for example, could be a load_file step in the Astro SDK.

    :param root_directory: The root directory path of the project.
    :param path: The path to the yaml file.
    :param target_directory: The target directory path for the executable yaml file.
    """

    SUPPORTED_OPERATORS = {"load_file": get_load_file_instance}

    def __init__(self, root_directory: Path, path: Path, target_directory: Path) -> None:
        super().__init__(root_directory, path, target_directory)
        self.yaml_content = yaml.safe_load(self.raw_content)

    @property
    def operator_name(self) -> str:
        """
        The name of the aql function to run.

        :returns: the function name of the operator in aql.
        """
        operators = list(self.yaml_content.keys())
        operators_count = len(operators)

        if operators_count != 1:
            raise ScannerError(f"Exactly one operator expected. Got {operators_count} operators: {operators}")

        operator = operators[0]

        if operator not in self.SUPPORTED_OPERATORS:
            raise NotImplementedError(f"Operator support for {operator} not available!")

        return operator

    def get_yaml_content(self) -> dict[str, Any]:
        """
        Get the yaml content for the operator.

        :returns: the operator kwargs.
        """
        return self.yaml_content[self.operator_name]

    def to_operator(self) -> LoadFileOperator:
        """
        Deserialize the operator to an actual aql operator.

        :returns: the aql operator.
        """
        return self.SUPPORTED_OPERATORS[self.operator_name](self.get_yaml_content(), self.name)


SUPPORTED_FILES = {"*.sql": SqlFile, "*.yaml": YamlFile, "*.yml": YamlFile}


def get_workflow_files(directory: Path, target_directory: Path | None) -> set[WorkflowFile]:
    """
    Get all workflow files within a directory.

    :param directory: The directory look in for files.
    :param target_directory: The target directory path for the executable workflow.

    :returns: a set of all the workflow files.
    """
    return {
        SUPPORTED_FILES[file_type](
            root_directory=directory,
            path=child,
            target_directory=target_directory,  # type: ignore
        )
        for file_type in SUPPORTED_FILES
        for child in directory.rglob(file_type)
        if child.is_file() and not child.is_symlink()
    }
