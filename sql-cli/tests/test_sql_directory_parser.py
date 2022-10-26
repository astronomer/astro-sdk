from pathlib import Path

from sql_cli.sql_directory_parser import SqlFile, get_sql_files


def test_sql_file_get_parameters(sql_file_with_parameters):
    """Test that the parameters order doesn't change randomly."""
    assert sql_file_with_parameters.get_parameters() == ["a", "b"]


def test_get_sql_files(root_directory, dags_directory):
    """Test that get_sql_files gets all sql files within a directory."""
    assert get_sql_files(directory=root_directory, target_directory=dags_directory) == {
        SqlFile(
            root_directory=root_directory,
            path=root_directory / path,
            target_directory=dags_directory,
        )
        for path in {"a.sql", "b.sql", "c.sql", "sub_dir/a.sql"}
    }


def test_get_sql_files_with_symlink(root_directory_symlink, dags_directory):
    """Test that get_sql_files ignores symlinks."""
    assert not get_sql_files(directory=root_directory_symlink, target_directory=dags_directory)
