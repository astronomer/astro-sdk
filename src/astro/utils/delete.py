from astro.utils.load import load_dataframe_into_sql_table


def delete_dataframe_rows_from_table(pandas_dataframe, target_table, hook, engine):
    """
    Deletes all SQL table records which match the dataframe values.

    :param pandas_dataframe: Dataframe containing records to be deleted
    :param filetype: pandas.Dataframe
    :param table_name: Table that will have rows deleted
    :param engine: SQLAlchemy engine referencing target database
    :type filetype: str
    :type table_name: str
    :type engine: SQLAlchemy engine
    """
    # TempTable: self.output_table = self.output_table.to_table(create_table_name(context=context), SCHEMA)
    conn = engine.connect()
    tmp_table_name = load_dataframe_into_sql_table(pandas_dataframe, target_table, hook)
    delete_rows_statement = f"DELETE FROM {target_table.table_name} WHERE Id IN (SELECT Id FROM {tmp_table_name}"
    conn.execute(delete_rows_statement)
    delete_temp_table_statement = f"DROP TABLE {tmp_table_name};"
    conn.execute(delete_temp_table_statement)
