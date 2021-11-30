from pandas.io.sql import SQLDatabase
from snowflake.connector.pandas_tools import write_pandas

from astro.sql.operators.temp_hooks import TempPostgresHook, TempSnowflakeHook


def move_dataframe_to_sql(
    output_table_name,
    conn_id,
    database,
    schema,
    warehouse,
    conn_type,
    df,
    chunksize=None,
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
    if conn_type == "snowflake":

        db = SQLDatabase(engine=hook.get_sqlalchemy_engine())
        db.prep_table(
            df,
            output_table_name.lower(),
            if_exists="replace",
            index=False,
        )
        write_pandas(
            hook.get_conn(),
            df,
            output_table_name,
            chunk_size=chunksize,
            quote_identifiers=False,
        )
    else:
        df.to_sql(
            output_table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=None,
            if_exists="replace",
            chunksize=chunksize,
            method="multi",
            index=False,
        )
