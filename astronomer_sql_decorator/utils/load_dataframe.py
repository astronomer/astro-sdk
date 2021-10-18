from snowflake.connector.pandas_tools import pd_writer

from astronomer_sql_decorator.operators.temp_hooks import (
    TempPostgresHook,
    TempSnowflakeHook,
)


def move_dataframe_to_sql(
    output_table_name, conn_id, database, schema, warehouse, conn_type, df
):
    # Select database Hook based on `conn` type
    hook = {
        "postgres": TempPostgresHook(postgres_conn_id=conn_id, schema=database),
        "snowflake": TempSnowflakeHook(
            snowflake_conn_id=conn_id,
            database=database,
            schema=schema,
            warehouse=warehouse,
        ),
    }.get(conn_type, None)
    if database:
        hook.database = database
    # Write df to target db
    # Note: the `method` argument changes when writing to Snowflake
    write_method = None
    if conn_type == "snowflake":
        meth = pd_writer
    df.to_sql(
        output_table_name,
        con=hook.get_sqlalchemy_engine(),
        schema=None,
        if_exists="replace",
        method=write_method,
        index=False,
    )
