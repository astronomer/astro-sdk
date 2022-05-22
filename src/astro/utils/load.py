"""
Functions for loading data from a source location to a destination location.
"""
import io
import json
import tempfile
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import smart_open
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pandas.io.sql import SQLDatabase
from pyarrow.parquet import ParquetFile
from sqlalchemy.engine import Engine

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    LOAD_COLUMN_AUTO_DETECT_ROWS,
    Database,
    FileType,
)
from astro.sql.table import Table
from astro.utils.database import get_database_name, get_sqlalchemy_engine
from astro.utils.dependencies import pandas_tools
from astro.utils.file import get_filetype, get_size
from astro.utils.schema_util import create_schema_query, schema_exists


def load_file_into_dataframe(
    filepath: str,
    filetype: Optional[FileType] = None,
    transport_params: Optional[dict] = None,
    normalize_config: Optional[dict] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load the contents of a file into a Pandas dataframe.
    :param filepath: File system path to a single file
    :param filetype: One of the supported filetypes ("csv", "json", "ndjson", "parquet")
    :param transport_params: Necessary parameters to connect to object store, in case the file is in (S3, GCS)
    :param normalize_config: parameters to pandas json_normalize function
    :param kwargs: Additional parameters to be used to load the data into a dataframe
    :type filepath: str
    :type filetype: str
    :type normalize_config: dict
    :type transport_params: dict
    :type kwargs: dict
    :return: return dataframe containing the loaded data
    :rtype: `pandas.DataFrame`
    """
    if filetype is None:
        filetype = get_filetype(filepath)

    mode = {FileType.PARQUET: "rb"}.get(filetype, "r")
    with smart_open.open(
        filepath, mode=mode, transport_params=transport_params
    ) as stream:
        if filetype == FileType.CSV:
            dataframe = pd.read_csv(stream, **kwargs)
        elif filetype == FileType.JSON:
            dataframe = pd.read_json(stream, **kwargs)
        elif filetype == FileType.NDJSON:
            dataframe = flatten_ndjson(normalize_config, stream)
        elif filetype == FileType.PARQUET:
            dataframe = pd.read_parquet(stream, **kwargs)
        else:
            raise ValueError(f"Unable to load file '{filepath}' of type '{filetype}'")
        return dataframe


def flatten_ndjson(
    normalize_config: Union[None, dict], stream: io.TextIOWrapper
) -> pd.DataFrame:
    """
    Flatten the nested ndjson/json.

    :param normalize_config: parameters in dict format of pandas json_normalize() function.
        https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
    :param stream: io.TextIOWrapper object for the file
    :type normalize_config: dict
    :type stream: io.TextIOWrapper
    :return: return dataframe containing the loaded data
    :rtype: `pandas.DataFrame`
    """
    normalize_config = normalize_config or {}

    df = None
    rows = stream.readlines(DEFAULT_CHUNK_SIZE)
    while len(rows) > 0:
        if df is None:
            df = pd.DataFrame(
                pd.json_normalize([json.loads(row) for row in rows], **normalize_config)
            )
        rows = stream.readlines(DEFAULT_CHUNK_SIZE)
    return df


def load_file_rows_into_dataframe(
    filepath: str,
    filetype: Optional[FileType] = None,
    rows_count: int = LOAD_COLUMN_AUTO_DETECT_ROWS,
) -> pd.DataFrame:
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
    if filetype is None:
        filetype = get_filetype(filepath)
    if filetype in [FileType.JSON, FileType.NDJSON]:
        dataframe = load_file_into_dataframe(filepath, filetype)
        dataframe = dataframe.iloc[0:rows_count]
    elif filetype == FileType.PARQUET:
        parquet_file = ParquetFile(filepath)
        first_rows = next(parquet_file.iter_batches(batch_size=rows_count))
        dataframe = pa.Table.from_batches([first_rows]).to_pandas()
    else:
        dataframe = load_file_into_dataframe(filepath, filetype, nrows=rows_count)
    return dataframe


def load_file_into_sql_table(
    filepath: str, filetype: FileType, table_name: str, engine: Engine
) -> None:
    """
    Efficiently save the contents of a file into a SQL table, by using the COPY command.
    Two caveats with this approach:
    - we are copying files to the worker node
    - we are using pandas dataframe to convert multiple file formats to CSV

    :param filepath: File system path to a single file
    :param filetype: One of the supported filetypes ("csv", "json", "ndjson", "parquet")
    :param table_name: Qualified table name (including schema, if relevant)
    :param engine: SQLAlchemy engine referencing target database
    :type filepath: str
    :type filetype: str
    :type table_name: str
    :type engine: SQLAlchemy engine
    """
    database_name = get_database_name(engine)
    if database_name not in [Database.POSTGRES, Database.POSTGRESQL]:
        raise ValueError(f"Function not available for {database_name.value}")
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        csv_filepath = tmp_file.name
        # At the moment we are using dataframes to convert among filetypes
        # since, among the file formats we support, Postgres only accepts CSV
        # TODO: chunk the files so we don't need to load huge files in memory
        df = load_file_into_dataframe(filepath, filetype)
        df.to_csv(csv_filepath, index=None, header=False)
        tmp_file.flush()

        tmp_file.seek(0)
        psycopg_conn = engine.raw_connection()
        # The COPY statement only works if we run it from within the server,
        # unless we use psql '\copy' or psycopg `cursor.copy_from`
        with psycopg_conn.cursor() as cursor:
            cursor.copy_expert(
                file=tmp_file,
                sql=f"COPY {table_name} FROM STDIN WITH CSV",
                size=get_size(csv_filepath),
            )
            psycopg_conn.commit()


def load_dataframe_into_sql_table(
    pandas_dataframe: pd.DataFrame,
    output_table: Table,
    hook: Union[BaseHook, SqliteHook],
    chunksize: int = DEFAULT_CHUNK_SIZE,
    if_exists: str = "replace",
) -> None:
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
    """
    database = get_database_name(hook)
    engine = get_sqlalchemy_engine(hook)

    output_table_name = output_table.name
    schema = output_table.metadata.schema
    conn = hook.get_connection(output_table.conn_id)
    user = conn.login

    if (
        database != Database.SQLITE
        and schema
        and not schema_exists(hook, schema, database.value)
    ):
        schema_query = create_schema_query(database.value, hook, schema, user)
        hook.run(schema_query)

    if database == Database.SNOWFLAKE:
        db = SQLDatabase(engine=engine)
        # Make columns uppercase to prevent weird errors in snowflake
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
    elif database == Database.BIGQUERY:
        pandas_dataframe.to_gbq(
            f"{schema}.{output_table_name}",
            if_exists=if_exists,
            chunksize=chunksize,
            project_id=hook.project_id,
        )
    elif database == Database.SQLITE:
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
            con=engine,
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )


def copy_remote_file_to_local(
    source_filepath: str,
    target_filepath: Optional[str] = None,
    is_binary: bool = False,
    transport_params: Optional[dict] = None,
) -> str:
    """
    Copy the contents of a file (which may be available locally or remotely) to a local file.
    If no target_filepath is specified, creates one, and returns it.

    :param source_filepath: Local filepath or remote URI of the source file
    :param target_filepath: (optional) Destination filepath in the local filesystem
    :param is_binary: If the given file is binary or not
    :param transport_params: Necessary parameters to connect to object store, in case the file is in (S3, GCS)
    :type source_filepath: str
    :type target_filepath: str
    :type is_binary: bool
    :type transport_params: dict
    :return: Target file path
    :rtype: str
    """
    # TODO: if the file is too big (e.g. larger than the available disk) we should change this to be a generator and
    # chunk the original file into smaller pieces

    read_mode = "rb" if is_binary else "r"
    write_mode = "wb" if is_binary else "w"
    if target_filepath is None:
        tmp_file = tempfile.NamedTemporaryFile(mode=write_mode, delete=False)
        target_filepath = tmp_file.name

    with open(target_filepath, write_mode) as fp_out:
        with smart_open.open(
            source_filepath, mode=read_mode, transport_params=transport_params
        ) as fp_in:
            content = fp_in.read()
            fp_out.write(content)

    return target_filepath
