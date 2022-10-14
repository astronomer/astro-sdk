from sql_cli.sql_directory_parser import SqlFile, get_sql_files


def test_sql_file_get_parameters(sql_file_with_parameters):
    """Test that the parameters order doesn't change randomly."""
    assert sql_file_with_parameters.get_parameters() == ["a", "b"]


def test_sql_file_has_sub_directory(sql_file, sql_file_in_sub_directory):
    """Test that sub directories are being detected."""
    assert sql_file_in_sub_directory.has_sub_directory()
    assert not sql_file.has_sub_directory()


def test_sql_file_get_sub_directories(sql_file, sql_file_in_sub_directory):
    """Test that sub directories are being returned."""
    assert next(sql_file_in_sub_directory.get_sub_directories(), None) == "sub_dir"
    assert not next(sql_file.get_sub_directories(), None)


def test_sql_file_get_variable_name(sql_file, sql_file_in_sub_directory):
    """Test that variable name is being defined based on the path."""
    assert sql_file.get_variable_name() == "a"
    assert sql_file_in_sub_directory.get_variable_name() == "sub_dir__a"


def test_sql_file_get_relative_target_path(sql_file, sql_file_in_sub_directory):
    """Test that relative target path can be retrieved."""
    assert sql_file.get_relative_target_path() == ".airflow/dags/sql/basic/a.sql"
    assert sql_file_in_sub_directory.get_relative_target_path() == ".airflow/dags/sql/basic/sub_dir/a.sql"


def test_get_sql_files(root_directory, dags_directory):
    """Test that get_sql_files gets all sql files within a directory."""
    assert get_sql_files(directory=root_directory, target_directory=dags_directory / "sql") == {
        SqlFile(
            root_directory=root_directory,
            path=root_directory / path,
            target_directory=dags_directory,
        )
        for path in {"a.sql", "b.sql", "c.sql", "sub_dir/a.sql"}
    }


def test_get_sql_files_with_symlink(root_directory_symlink, dags_directory):
    """Test that get_sql_files ignores symlinks."""
    assert not get_sql_files(directory=root_directory_symlink, target_directory=dags_directory / "sql")
