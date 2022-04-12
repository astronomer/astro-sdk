from typing import Any, Dict, List, Optional, Union

import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import DEFAULT_CHUNK_SIZE
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils import get_hook
from astro.utils.database import get_database_from_conn_id
from astro.utils.file import get_filetype
from astro.utils.load import (
    load_dataframe_into_sql_table,
    load_file_into_dataframe,
    populate_normalize_config,
)
from astro.utils.path import get_paths, get_transport_params, validate_path
from astro.utils.task_id_helper import get_task_id


class AgnosticLoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database.

    :param path: File path.
    :param output_table_name: Name of table to create.
    :param file_conn_id: Airflow connection id of input file (optional)
    :param output_conn_id: Database connection id.
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    """

    template_fields = (
        "output_table",
        "file_conn_id",
        "path",
    )

    def __init__(
        self,
        path: str,
        output_table: Union[TempTable, Table],
        file_conn_id: Optional[str] = None,
        chunksize: int = DEFAULT_CHUNK_SIZE,
        if_exists: str = "replace",
        ndjson_normalize_sep: str = "_",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Union[TempTable, Table] = output_table
        self.path = path
        self.chunksize = chunksize
        self.file_conn_id = file_conn_id
        self.kwargs = kwargs
        self.if_exists = if_exists
        self.ndjson_normalize_sep = ndjson_normalize_sep
        self.normalize_config: Dict[str, str] = {}

    def execute(self, context: Any) -> Union[TempTable, Table]:
        """
        Load an existing dataset from a supported file into a SQL table.
        """
        if self.file_conn_id:
            BaseHook.get_connection(self.file_conn_id)

        self.normalize_config = populate_normalize_config(
            ndjson_normalize_sep=self.ndjson_normalize_sep,
            database=get_database_from_conn_id(self.output_table.conn_id),
        )

        hook = get_hook(
            conn_id=self.output_table.conn_id,
            database=self.output_table.database,
            schema=self.output_table.schema,
            warehouse=self.output_table.warehouse,
        )

        paths = get_paths(self.path, self.file_conn_id)
        transport_params = get_transport_params(paths[0], self.file_conn_id)
        return self.load_using_pandas(context, paths, hook, transport_params)

    def load_using_pandas(
        self, context: Any, paths: List[str], hook: BaseHook, transport_params: dict
    ) -> Union[TempTable, Table]:
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """
        self._configure_output_table(context)
        self.log.info(f"Loading {self.path} into {self.output_table}...")
        if_exists = self.if_exists
        for path in paths:
            pandas_dataframe = self._load_file_into_dataframe(path, transport_params)
            load_dataframe_into_sql_table(
                pandas_dataframe,
                self.output_table,
                hook,
                self.chunksize,
                if_exists=if_exists,
            )
            if_exists = "append"

        self.log.info(f"Completed loading the data into {self.output_table}.")

        return self.output_table

    def _configure_output_table(self, context: Any) -> None:
        # TODO: Move this function to the SQLDecorator, so it can be reused across operators
        if isinstance(self.output_table, TempTable):
            self.output_table = self.output_table.to_table(
                create_table_name(context=context)
            )
        if not self.output_table.table_name:
            self.output_table.table_name = create_table_name(context=context)

    def _load_file_into_dataframe(
        self, filepath: str, transport_params: dict
    ) -> pd.DataFrame:
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """
        validate_path(filepath)
        filetype = get_filetype(filepath)
        return load_file_into_dataframe(
            filepath, filetype, transport_params, normalize_config=self.normalize_config
        )


def load_file(
    path: str,
    output_table: Union[TempTable, Table],
    file_conn_id: Optional[str] = None,
    task_id: Optional[str] = None,
    if_exists: str = "replace",
    ndjson_normalize_sep: str = "_",
    **kwargs,
) -> XComArg:
    """Convert AgnosticLoadFile into a function.

    Returns an XComArg object.

    :param path: File path.
    :param output_table: Table to create
    :param file_conn_id: Airflow connection id of input file (optional)
    :param task_id: task id, optional.
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
        path=path,
        output_table=output_table,
        file_conn_id=file_conn_id,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        **kwargs,
    ).output
