import os
from typing import Union

import sqlalchemy
from airflow.models import BaseOperator

from astro.constants import DEFAULT_CHUNK_SIZE, FileType
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils import get_hook
from astro.utils.delete import delete_dataframe_rows_from_table
from astro.utils.file import get_filetype, get_size, is_binary, is_small
from astro.utils.load import (
    copy_remote_file_to_local,
    load_dataframe_into_sql_table,
    load_file_into_dataframe,
    load_file_into_sql_table,
    load_file_rows_into_dataframe,
)
from astro.utils.path import (
    get_location,
    get_paths,
    get_transport_params,
    is_local,
    validate_path,
)
from astro.utils.task_id_helper import get_task_id


class AgnosticLoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database.

    :param path: File path.
    :type path: str
    :param output_table_name: Name of table to create.
    :type output_table_name: str
    :param file_conn_id: Airflow connection id of input file (optional)
    :type file_conn_id: str
    :param output_conn_id: Database connection id.
    :type output_conn_id: str
    """

    template_fields = (
        "output_table",
        "file_conn_id",
        "path",
    )

    def __init__(
        self,
        path,
        output_table: Union[TempTable, Table],
        file_conn_id="",
        chunksize=DEFAULT_CHUNK_SIZE,
        if_exists="replace",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Union[TempTable, Table] = output_table
        self.path = path
        self.chunksize = chunksize
        self.file_conn_id = file_conn_id
        self.kwargs = kwargs
        self.output_table = output_table
        self.if_exists = if_exists

    def _configure_output_table(self, context):
        if type(self.output_table) == TempTable:
            self.output_table = self.output_table.to_table(
                create_table_name(context=context), SCHEMA
            )
        else:
            self.output_table.schema = self.output_table.schema or SCHEMA
        if not self.output_table.table_name:
            self.output_table.table_name = create_table_name(context=context)

    def execute(self, context):
        """
        Load an existing dataset from a supported file into a SQL table.
        """
        hook = get_hook(
            conn_id=self.output_table.conn_id,
            database=self.output_table.database,
            schema=self.output_table.schema,
            warehouse=self.output_table.warehouse,
        )
        paths = get_paths(self.path, self.file_conn_id)
        transport_params = get_transport_params(paths[0], self.file_conn_id)

        if self.output_table.database == "postgres":
            return self.load_to_postgres(context, paths, hook, transport_params)
        else:
            return self.load_using_pandas(context, paths, hook, transport_params)

    def _create_an_empty_table_using_pandas(self, filepath, filetype, hook):
        """
        Create an empty SQL table. Uses Pandas to identify the types of the columns.
        In order to identify the column type, load the first rows of content, based on the value of
        `astro.constants.LOAD_COLUMN_AUTO_DETECT_ROWS`, and delete them after.
        """
        pandas_dataframe = load_file_rows_into_dataframe(filepath, filetype)
        load_dataframe_into_sql_table(
            pandas_dataframe,
            self.output_table,
            hook,
            self.chunksize,
            self.if_exists,
        )
        engine = self.get_sql_alchemy_engine()
        delete_dataframe_rows_from_table(
            pandas_dataframe, self.output_table, hook, engine
        )

    def load_to_postgres(self, context, paths, hook, transport_params):
        """
        Use Postgres COPY to load files into a table. Makes a local copy of the dataset files if they were originally
        available remotely (GCS, S3, HTTP).

        This implementation assumes that, if we have multiple files, that all the files have a similer size as the first
        one. We may have to review this in the future.
        """
        first_filepath = paths[0]
        file_type = get_filetype(first_filepath)

        credentials = transport_params

        # Identify if the files are remote. If they are, make a local copy of the first one.
        remote_source = False
        if not is_local(first_filepath):
            remote_source = True
            is_bin = is_binary(file_type)
            local_filepath = copy_remote_file_to_local(
                first_filepath, is_binary=is_bin, transport_params=credentials
            )

        # Performance tests (inside tests/benchmark) have shown that we can load small files efficiently using pandas
        # across all the supported databases. Therefore, if the files are small, we default to this strategy.
        if is_small(local_filepath):
            return self.load_using_pandas(context, paths, hook, credentials)
        else:
            engine = self.get_sql_alchemy_engine()
            if not sqlalchemy.inspect(engine).has_table(
                self.output_table.table_name, schema=self.output_table.schema
            ):
                self._create_an_empty_table_using_pandas(
                    local_filepath, file_type, hook
                )
            for path in paths:
                if remote_source and path != local_filepath:
                    local_filepath = copy_remote_file_to_local(
                        path, is_binary=is_bin, transport_params=credentials
                    )
                load_file_into_sql_table(
                    local_filepath, file_type, self.output_table.table_name, engine
                )
                if remote_source:
                    os.remove(local_filepath)
            return self.output_table

    def load_using_pandas(self, context, paths, hook, transport_params):
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """
        self._configure_output_table(context)
        self.log.info(f"Loading {self.path} into {self.output_table}...")
        for path in paths:
            pandas_dataframe = self._load_file_into_dataframe(path, transport_params)
            load_dataframe_into_sql_table(
                pandas_dataframe,
                self.output_table,
                hook,
                self.chunksize,
                self.if_exists,
            )
        self.log.info(f"Completed loading the data into {self.output_table}.")
        return self.output_table

    def _load_file_into_dataframe(self, filepath, transport_params):
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """
        validate_path(filepath)
        filetype = get_filetype(filepath)
        return load_file_into_dataframe(filepath, filetype, transport_params)


def load_file(
    path,
    output_table=None,
    file_conn_id=None,
    task_id=None,
    if_exists="replace",
    **kwargs,
):
    """Convert AgnosticLoadFile into a function.

    Returns an XComArg object.

    :param path: File path.
    :type path: str
    :param output_table: Table to create
    :type output_table: Table
    :param file_conn_id: Airflow connection id of input file (optional)
    :type file_conn_id: str
    :param task_id: task id, optional.
    :type task_id: str
    """

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_task_id("load_file", "")

    return AgnosticLoadFile(
        task_id=task_id,
        path=path,
        output_table=output_table,
        file_conn_id=file_conn_id,
        if_exists=if_exists,
        **kwargs,
    ).output
