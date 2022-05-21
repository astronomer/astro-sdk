from astro.databases import create_database
from astro.sql.table import Table


def handle_schema(conn_id: str, output_table: Table) -> None:
    """
    In this function we ensure that both the temporary schema and the explicitly stated schema exist.
    We also ensure that if there is a table conflict, the conflicting table is dropped so the new data
    can be added.
    :return:
    """
    database_impl = create_database(conn_id)
    database_impl.create_schema_if_needed(output_table.metadata.schema)
    database_impl.drop_table(output_table)
