from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases import BaseDatabase, create_database
from astro.files import File, get_files
from astro.sql.table import Table
from astro.utils.load import populate_normalize_config

from astro.utils.task_id_helper import get_task_id


class AgnosticLoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    :param chunk_size: Specify the number of records in each batch to be written at a time.
    :param if_exists: Overwrite file if exists. Default False.
    """

    template_fields = ("output_table", "input_file")

    def __init__(
        self,
        input_file: File,
        output_table: Table,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_exists: LoadExistStrategy = "replace",
        ndjson_normalize_sep: str = "_",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Table = output_table
        self.input_file = input_file
        self.chunk_size = chunk_size
        self.kwargs = kwargs
        self.if_exists = if_exists
        self.ndjson_normalize_sep = ndjson_normalize_sep
        self.normalize_config: Dict[str, str] = {}

    def execute(self, context: Any) -> Table:
        """
        Load an existing dataset from a supported file into a SQL table.
        """
        if self.input_file.conn_id:
            BaseHook.get_connection(self.input_file.conn_id)

        database = create_database(self.output_table.conn_id)
        self.normalize_config = populate_normalize_config(
            ndjson_normalize_sep=self.ndjson_normalize_sep,
            database=database,
        )
        return self.load_data(database=database, input_file=self.input_file)

    def load_data(self, input_file: File, database: BaseDatabase) -> Table:
        """Loads csv/parquet table from local/S3/GCS with Pandas.
        Infers SQL database type based on connection then loads table to db.
        """
        self.log.info("Loading %s into %s ...", self.input_file.path, self.output_table)
        if_exists = self.if_exists
        for file in get_files(
            input_file.path, input_file.conn_id, normalize_config=self.normalize_config
        ):
            database.load_pandas_dataframe_to_table(
                source_dataframe=file.export_to_dataframe(),
                target_table=self.output_table,
                if_exists=if_exists,
                chunk_size=self.chunk_size,
            )
            if_exists = "append"

        self.log.info(f"Completed loading the data into {self.output_table}.")

        return self.output_table


def load_file(
    input_file: File,
    output_table: Table,
    task_id: Optional[str] = None,
    if_exists: LoadExistStrategy = "replace",
    ndjson_normalize_sep: str = "_",
    **kwargs,
) -> XComArg:
    """Convert AgnosticLoadFile into a function that Returns an XComArg object
    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param task_id: task id, optional
    :param if_exists: default override an existing Table. Options: fail, replace, append
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
        ex - {"a": {"b":"c"}} will result in
            column - "a_b"
            where ndjson_normalize_sep = "_"
    """

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_task_id("load_file", "")

    return AgnosticLoadFile(
        task_id=task_id,
        input_file=input_file,
        output_table=output_table,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        **kwargs,
    ).output
