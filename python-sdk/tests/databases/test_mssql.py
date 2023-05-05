from unittest import mock

from astro.databases.mssql import MssqlDatabase


@mock.patch("astro.databases.mssql.MssqlDatabase.schema_exists", return_value=False)
@mock.patch("astro.databases.mssql.MssqlDatabase.run_sql")
def test_create_schema_if_needed(mock_run_sql, mock_schema_exists):
    """
    Test that run_sql is called with expected arguments when
    create_schema_if_needed method is called when the schema is not available
    """
    db = MssqlDatabase(conn_id="fake_conn_id")
    db.create_schema_if_applicable("non-existing-schema")
    mock_run_sql.assert_called_once_with(
        """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'non-existing-schema')
    BEGIN
        EXEC( 'CREATE SCHEMA non-existing-schema' );
    END
    """,
        autocommit=True,
    )
