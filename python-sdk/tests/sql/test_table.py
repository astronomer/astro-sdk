import pytest

from astro.sql.table import Metadata, Table


def test_table_with_explicit_name():
    """Check that we respect the name of table when it's named an we set it to non-temp"""
    table = Table(conn_id="some_connection", name="some_name")
    assert table.conn_id == "some_connection"
    assert table.name == "some_name"
    assert not table.temp


def test_table_without_name():
    """Check that we create a name, that it is always the same name, and that we set temp name to True"""
    table = Table(conn_id="some_connection")
    assert table.conn_id == "some_connection"
    name1 = table.name
    name2 = table.name
    assert name1
    assert isinstance(name1, str)
    assert len(table.name) == 62
    assert name1 == name2
    assert table.temp


def test_table_without_name_and_schema():
    """Check that the table name is smaller when there is metadata associated to the table."""
    table = Table(conn_id="some_connection")
    table.metadata.schema = "abc"
    assert isinstance(table.name, str)
    assert len(table.name) == 59  # max length limit - len("abc.")
    assert table.temp


def test_table_name_set_after_initialization():
    """Check that the table is no longer considered temp when the name is set after initialization."""
    table = Table(conn_id="some_connection")
    assert table.temp
    table.name = "something"
    assert not table.temp


def test_table_name_with_temp_prefix():
    """Check that the table is no longer considered temp when the name is set after initialization."""
    table = Table(conn_id="some_connection")
    assert table.name.startswith("_tmp_")


@pytest.mark.parametrize(
    "metadata,expected_is_empty",
    [
        (Metadata(), True),
        (Metadata(schema="test"), False),
    ],
)
def test_is_empty_metadata(metadata, expected_is_empty):
    """Check that is_empty returns"""
    assert metadata.is_empty() == expected_is_empty


@pytest.mark.parametrize(
    "metadata,expected_metadata",
    [
        (
            {"schema": "test", "database": "db1"},
            Metadata(schema="test", database="db1"),
        ),
        (Metadata(schema="test"), Metadata(schema="test")),
    ],
)
def test_metadata_converter(metadata, expected_metadata):
    """Test you can pass a dict to metadata param"""
    table = Table(metadata=metadata)
    assert table.metadata == expected_metadata
