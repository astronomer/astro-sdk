from typing import Any, Dict, Optional, Union

import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases import BaseDatabase, create_database
from astro.files import File, get_files
from astro.sql.table import Table
from astro.utils.task_id_helper import get_task_id


class LoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    :param chunk_size: Specify the number of records in each batch to be written at a time.
    :param if_exists: Overwrite file if exists. Default False.

    :return: If ``output_table`` is passed this operator returns a Table object. If not
        passed, returns a dataframe.
    """

    template_fields = ("output_table", "input_file")

    def __init__(
        self,
        input_file: File,
        output_table: Optional[Table] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_exists: LoadExistStrategy = "replace",
        ndjson_normalize_sep: str = "_",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table = output_table
        self.input_file = input_file
        self.chunk_size = chunk_size
        self.kwargs = kwargs
        self.if_exists = if_exists
        self.ndjson_normalize_sep = ndjson_normalize_sep
        self.normalize_config: Dict[str, str] = {}

    def execute(self, context: Any) -> Union[Table, pd.DataFrame]:
        """
        Load an existing dataset from a supported file into a SQL table or a Dataframe.
        """
        if self.input_file.conn_id:
            # Verify a Connection with self.input_file.conn_id actually exists in Airflow
            # Raises a AirflowNotFoundException exception otherwise
            BaseHook.get_connection(self.input_file.conn_id)

        return self.load_data(input_file=self.input_file)

    def load_data(self, input_file: File) -> Union[Table, pd.DataFrame]:

        self.log.info("Loading %s into %s ...", self.input_file.path, self.output_table)
        if self.output_table:
            return self.load_data_to_table(input_file)
        else:
            return self.load_data_to_dataframe(input_file)

    def load_data_to_table(self, input_file: File) -> Table:
        """
        Loads csv/parquet table from local/S3/GCS with Pandas.
        Infers SQL database type based on connection then loads table to db.
        """
        if not isinstance(self.output_table, Table):
            raise ValueError(
                "Please pass a valid Table instance in 'output_table' parameter"
            )
        database = create_database(self.output_table.conn_id)
        self.output_table = database.populate_table_metadata(self.output_table)
        self.normalize_config = self._populate_normalize_config(
            ndjson_normalize_sep=self.ndjson_normalize_sep,
            database=database,
        )
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
        self.log.info("Completed loading the data into %s.", self.output_table)
        return self.output_table

    def load_data_to_dataframe(self, input_file: File) -> Optional[pd.DataFrame]:
        """
        Loads csv/parquet file from local/S3/GCS with Pandas. Returns dataframe as no
        SQL table was specified
        """
        df = None
        for file in get_files(input_file.path, input_file.conn_id):
            if isinstance(df, pd.DataFrame):
                df = pd.concat([df, file.export_to_dataframe()])
            else:
                df = file.export_to_dataframe()
        self.log.info("Completed loading the data into dataframe.")
        return df

    @staticmethod
    def _populate_normalize_config(
        database: BaseDatabase,
        ndjson_normalize_sep: str = "_",
    ) -> Dict[str, str]:
        """
        Validate pandas json_normalize() parameter for databases, since default params result in
        invalid column name. Default parameter result in the columns name containing '.' char.

        :param ndjson_normalize_sep: separator used to normalize nested ndjson.
            https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
        :param database: supported database
        """

        def replace_illegal_columns_chars(char: str, database: BaseDatabase) -> str:
            index = (
                database.illegal_column_name_chars.index(char)
                if char in database.illegal_column_name_chars
                else None
            )
            if index is not None:
                return str(database.illegal_column_name_chars_replacement[index])
            else:
                return str(char)

        normalize_config: Dict[str, Any] = {
            "meta_prefix": ndjson_normalize_sep,
            "record_prefix": ndjson_normalize_sep,
            "sep": ndjson_normalize_sep,
        }
        normalize_config["meta_prefix"] = replace_illegal_columns_chars(
            normalize_config["meta_prefix"], database
        )
        normalize_config["record_prefix"] = replace_illegal_columns_chars(
            normalize_config["record_prefix"], database
        )
        normalize_config["sep"] = replace_illegal_columns_chars(
            normalize_config["sep"], database
        )

        return normalize_config


def load_file(
    input_file: File,
    output_table: Optional[Table] = None,
    task_id: Optional[str] = None,
    if_exists: LoadExistStrategy = "replace",
    ndjson_normalize_sep: str = "_",
    **kwargs,
) -> XComArg:
    """Convert LoadFile into a function that Returns an XComArg object
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

    return LoadFile(
        task_id=task_id,
        input_file=input_file,
        output_table=output_table,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        **kwargs,
    ).output
