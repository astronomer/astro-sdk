from typing import Union

from airflow.models import BaseOperator

from astro.constants import DEFAULT_CHUNK_SIZE
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils import get_hook
from astro.utils.load import load_dataframe_into_sql_table, load_file_into_dataframe
from astro.utils.path import get_paths, get_transport_params, validate_path
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

    def execute_postgres(self, context):
        # 1. download file(s), if not local
        # 2. check size
        # 3. load a subset of rows - specific per format
        # 3. convert the remaining to csv
        # 4. get first N lines of the csv file
        # 5. create the table using the dataframe operation & N lines to automatically indentify columns/types
        # 6. copy the remaining data using the CP
        # copy usa from '/Users/EDB1/Downloads/usa.csv' delimiter ',' csv header;
        pass

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
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """
        self._configure_output_table(context)
        self.log.info(f"Loading {self.path} into {self.output_table}...")

        hook = get_hook(
            conn_id=self.output_table.conn_id,
            database=self.output_table.database,
            schema=self.output_table.schema,
            warehouse=self.output_table.warehouse,
        )

        paths = get_paths(self.path, self.file_conn_id)
        for path in paths:
            pandas_dataframe = self._load_file_into_dataframe(path)
            load_dataframe_into_sql_table(
                pandas_dataframe,
                self.output_table,
                hook,
                self.chunksize,
                self.if_exists,
            )

        self.log.info(f"Completed loading the data into {self.output_table}.")
        return self.output_table

    def _load_file_into_dataframe(self, filepath):
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """
        validate_path(filepath)
        filetype = filepath.split(".")[-1]
        transport_params = get_transport_params(filepath, self.file_conn_id)
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
