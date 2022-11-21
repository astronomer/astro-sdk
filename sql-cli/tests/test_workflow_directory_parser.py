from pathlib import Path

import pytest
from yaml.scanner import ScannerError

from sql_cli.workflow_directory_parser import SqlFile, WorkflowFile, YamlFile, get_workflow_files


class WorkflowFileSubClass(WorkflowFile):
    pass


def test_workflow_file_get_parameters(sql_file_with_parameters):
    """Test that the parameters order doesn't change randomly."""
    assert sql_file_with_parameters.get_parameters() == ["a", "b"]


def test_workflow_file_has_sub_directory(sql_file, sql_file_in_sub_directory):
    """Test that subdirectories are being detected."""
    assert sql_file_in_sub_directory.has_sub_directory()
    assert not sql_file.has_sub_directory()


def test_workflow_file_get_sub_directories(sql_file, sql_file_in_sub_directory):
    """Test that subdirectories are being returned."""
    assert next(sql_file_in_sub_directory.get_sub_directories(), None) == "sub_dir"
    assert not next(sql_file.get_sub_directories(), None)


def test_sql_file_get_variable_name(sql_file, sql_file_in_sub_directory):
    """Test that variable name is being defined based on the path."""
    assert sql_file.get_variable_name() == "a"
    assert sql_file_in_sub_directory.get_variable_name() == "sub_dir__a"


def test_sql_file_get_relative_target_path(sql_file, sql_file_in_sub_directory):
    """Test that relative target path can be retrieved."""
    assert sql_file.get_relative_target_path() == Path("include/basic/a.sql")
    assert sql_file_in_sub_directory.get_relative_target_path() == Path("include/basic/sub_dir/a.sql")


def test_get_workflow_files(root_directory, dags_directory):
    """Test that get_workflow_files gets all workflow files within a directory."""
    kwargs = {
        "root_directory": root_directory,
        "target_directory": dags_directory,
    }
    assert get_workflow_files(directory=root_directory, target_directory=dags_directory) == {
        SqlFile(path=root_directory / path, **kwargs) for path in {"a.sql", "b.sql", "c.sql", "sub_dir/a.sql"}
    }.union({YamlFile(path=root_directory / path, **kwargs) for path in {"a.yaml"}})


def test_get_workflow_files_with_symlink(root_directory_symlink, dags_directory):
    """Test that get_sql_files ignores symlinks."""
    assert not get_workflow_files(directory=root_directory_symlink, target_directory=dags_directory)


def test_get_workflow_files_with_multiple_operators(root_directory_multiple_operators, dags_directory):
    """Tests that a workflow containing a yaml file having multiple operators raises exception"""
    workflow_files = get_workflow_files(
        directory=root_directory_multiple_operators, target_directory=dags_directory
    )
    with pytest.raises(ScannerError):
        for file in workflow_files:
            _ = file.operator_name


def test_get_workflows_files_with_unsupported_operator(root_directory_unsupported_operator, dags_directory):
    """Tests that a workflow containing a yaml file having multiple operators raises exception"""
    workflow_files = get_workflow_files(
        directory=root_directory_unsupported_operator, target_directory=dags_directory
    )
    with pytest.raises(NotImplementedError):
        for file in workflow_files:
            _ = file.operator_name


def test_subclass_missing_not_implemented_methods_raise_exception(root_directory, dags_directory):
    workflow_file = WorkflowFileSubClass(
        root_directory=root_directory, path=root_directory / "a.sql", target_directory=dags_directory
    )

    with pytest.raises(NotImplementedError):
        _ = workflow_file.operator_name

    with pytest.raises(NotImplementedError):
        _ = workflow_file.to_operator()


def test_workflow_file_equality_check(root_directory, dags_directory):
    """Tests that the equality check operator verifies whether two WorkflowFile instances refer to the same file."""
    workflow_file1 = WorkflowFile(
        root_directory=root_directory, path=root_directory / "a.sql", target_directory=dags_directory
    )
    workflow_file2 = WorkflowFile(
        root_directory=root_directory, path=root_directory / "a.sql", target_directory=dags_directory
    )
    workflow_file3 = WorkflowFile(
        root_directory=root_directory, path=root_directory / "b.sql", target_directory=dags_directory
    )

    assert workflow_file1 == workflow_file2

    assert workflow_file1 != workflow_file3
