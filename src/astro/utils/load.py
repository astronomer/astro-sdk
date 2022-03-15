import tempfile

import pandas as pd
import pyarrow as pa
import smart_open
from pandas.io.sql import SQLDatabase
from pyarrow.parquet import ParquetFile
from sqlalchemy import create_engine

from astro.constants import DEFAULT_CHUNK_SIZE, LOAD_COLUMN_AUTO_DETECT_ROWS
from astro.utils.dependencies import (
    BigQueryHook,
    PostgresHook,
    SnowflakeHook,
    pandas_tools,
)
from astro.utils.schema_util import create_schema_query, schema_exists


def load_file_into_dataframe(filepath, filetype, transport_params=None, **kwargs):
    """
    Load the contents of a file into a Pandas dataframe.

    :param filepath: File system path to a single file
    :param filetype: One of the supported filetypes ("csv", "json", "ndjson", "parquet")
    :param transport_params: Necessary parameters to connect to object store, in case the file is in (S3, GCS)
    :param kwargs: Additional parameters to be used to load the data into a dataframe
    :type filepath: str
    :type filetype: str
    :type transport_params: dict
    :type kwargs: dict
    :return: return dataframe containing the loaded data
    :rtype: `pandas.DataFrame`
    """
    mode = {"parquet": "rb"}.get(filetype, "r")
    with smart_open.open(
        filepath, mode=mode, transport_params=transport_params
    ) as stream:
        if filetype == "csv":
            dataframe = pd.read_csv(stream, **kwargs)
        elif filetype == "json":
            dataframe = pd.read_json(stream, **kwargs)
        elif filetype == "ndjson":
            dataframe = pd.read_json(stream, lines=True, **kwargs)
        elif filetype == "parquet":
            dataframe = pd.read_parquet(stream, **kwargs)
        else:
            raise ValueError(f"Unable to load file {stream} of type {filetype}")
        return dataframe


def load_file_rows_into_dataframe(
    filepath, filetype, rows_count=LOAD_COLUMN_AUTO_DETECT_ROWS
):
    """
    Load the first rows of a file available in the filesystem into a Pandas dataframe.

    :param filepath: File system path to a single file
    :param filetype: One of the supported filetypes ("csv", "json", "ndjson", "parquet")
    :param rows_count: Total rows of the file to be loaded into the dataframe
    :type filepath: str
    :type filetype: str
    :type rows_count: int
    :return: return dataframe containing the loaded data
    :rtype: `pandas.DataFrame`
    """
    if filetype == "parquet":
        parquet_file = ParquetFile(filepath)
        first_rows = next(parquet_file.iter_batches(batch_size=rows_count))
        dataframe = pa.Table.from_batches([first_rows]).to_pandas()
    else:
        dataframe = load_file_into_dataframe(filepath, filetype, nrows=rows_count)
    return dataframe


def load_file_into_sql_table(filepath, filetype, table_name, engine):
    """
    Efficiently save the contents of a file into a SQL table.

    :param filepath: File system path to a single file
    :param filetype: One of the supported filetypes ("csv", "json", "ndjson", "parquet")
    :param table_name: Qualified table name (including schema, if relevant)
    :param engine: SQLAlchemy engine referencing target database
    :type filepath: str
    :type filetype: str
    :type table_name: str
    :type engine: SQLAlchemy engine
    """
    database = engine.url.database
    if database != "postgres":
        raise ValueError(f"Feature not available at {database}")
    csv_sep = ","
    conn = engine.connect()
    with tempfile.NamedTemporaryFile() as tmp_file:
        csv_filepath = tmp_file.name
        df = load_file_into_dataframe(filepath, filetype)
        df.to_csv(csv_filepath, index=None)
        copy_csv_statement = (
            f"COPY {table_name} from {csv_filepath} delimiter {csv_sep} csv header"
        )
        conn.execute(copy_csv_statement)


def load_dataframe_into_sql_table(
    pandas_dataframe,
    output_table,
    hook,
    chunksize=DEFAULT_CHUNK_SIZE,
    if_exists="replace",
):
    """
    Save the contents of a Pandas dataframe into a SQL table. Create the schema if it doesn't exist.

    :param pandas_dataframe: Data intended to be transferred to a SQL table
    :param output_table: Details of the destination SQL table
    :param hook: Details of the hook to be used to do the transfer. Should be compatible with the `output_table.conn_id`
    :param chunksize: Size of the chunks to be used to load the data.
    :param if_exists: One of ("replace", "append"). If the table already exists, replaces by default.
    :type pandas_dataframe: `pandas.DataFrame`
    :type output_table: type `astro.table.Table`
    :type hook: type (BigQueryHook, PostgresHook, SnowflakeHook, SqliteHook)
    :type chunksize: type int
    :type if_exists: type str
    :return: return output table name
    :rtype: str
    """
    conn = hook.get_connection(output_table.conn_id)
    conn_type = conn.conn_type
    user = conn.login
    output_table_name = output_table.table_name  # qualified?
    schema = output_table.schema

    # Crate schema if it doesn't exist
    if conn_type != "sqlite" and not schema_exists(hook, schema, conn_type):
        schema_query = create_schema_query(conn_type, hook, schema, user)
        hook.run(schema_query)

    if conn_type == "snowflake":
        db = SQLDatabase(engine=hook.get_sqlalchemy_engine())
        # make columns uppercase to prevent weird errors in snowflake
        pandas_dataframe.columns = pandas_dataframe.columns.str.upper()
        db.prep_table(
            pandas_dataframe,
            output_table_name.lower(),
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
        pandas_tools.write_pandas(
            hook.get_conn(),
            pandas_dataframe,
            output_table_name,
            chunk_size=chunksize,
            quote_identifiers=False,
        )
    elif conn_type == "bigquery":
        pandas_dataframe.to_gbq(
            f"{schema}.{output_table_name}",
            if_exists=if_exists,
            chunksize=chunksize,
            project_id=hook.project_id,
        )
    elif conn_type == "sqlite":
        uri = hook.get_uri().replace("///", "////")
        engine = create_engine(uri)
        pandas_dataframe.to_sql(
            output_table_name,
            con=engine,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )
    else:
        pandas_dataframe.to_sql(
            output_table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )
    return output_table.table_name
