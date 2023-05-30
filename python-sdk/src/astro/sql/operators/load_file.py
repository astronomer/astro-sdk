from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
from airflow.decorators.base import get_unique_task_id
from airflow.hooks.base import BaseHook
from airflow.models.xcom_arg import XComArg

from astro import settings
from astro.airflow.datasets import kwargs_with_datasets
from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy
from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.dataframes.load_options import (
    PandasCsvLoadOptions,
    PandasJsonLoadOptions,
    PandasNdjsonLoadOptions,
    PandasParquetLoadOptions,
)
from astro.dataframes.pandas import PandasDataframe
from astro.files import File, resolve_file_path_pattern
from astro.options import LoadOptions, LoadOptionsList
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.table import BaseTable
from astro.utils.compat.typing import Context

DEPRECATED_LOAD_OPTIONS_CLASSES = [
    PandasCsvLoadOptions,
    PandasJsonLoadOptions,
    PandasNdjsonLoadOptions,
    PandasParquetLoadOptions,
]


class LoadFileOperator(AstroSQLBaseOperator):
    """Load S3/local file into either a database or a pandas dataframe

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    :param chunk_size: Specify the number of records in each batch to be written at a time.
    :param if_exists: Overwrite file if exists. Default False.
    :param use_native_support: Use native support for data transfer if available on the destination.
    :param native_support_kwargs: kwargs to be used by method involved in native support flow
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
    :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
    :param assume_schema_exists: If True, skips check to see if output_table schema exists

    :return: If ``output_table`` is passed this operator returns a Table object. If not
        passed, returns a dataframe.
    """

    template_fields = ("output_table", "input_file")

    def __init__(
        self,
        input_file: File,
        output_table: BaseTable | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_exists: LoadExistStrategy = "replace",
        ndjson_normalize_sep: str = "_",
        use_native_support: bool = True,
        native_support_kwargs: dict | None = None,
        load_options: LoadOptions | list[LoadOptions] | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        enable_native_fallback: bool | None = settings.LOAD_FILE_ENABLE_NATIVE_FALLBACK,
        assume_schema_exists: bool = settings.ASSUME_SCHEMA_EXISTS,
        **kwargs,
    ) -> None:
        kwargs.setdefault("task_id", get_unique_task_id("load_file"))
        super().__init__(
            **kwargs_with_datasets(
                kwargs=kwargs,
                input_datasets=input_file,
                output_datasets=output_table,
            )
        )
        if native_support_kwargs:
            warnings.warn(
                """`load_options` will replace `native_support_kwargs` parameter in astro-sdk-python>=1.5.0. Please
                use `load_options` parameter instead.""",
                DeprecationWarning,
                stacklevel=2,
            )

        if load_options is not None:
            if not isinstance(load_options, list):
                load_options = [load_options]

            deprecated_classes = {type(cls).__name__ for cls in load_options}.intersection(
                [cls.__name__ for cls in DEPRECATED_LOAD_OPTIONS_CLASSES]
            )
            if deprecated_classes:
                warnings.warn(
                    f'`{", ".join(deprecated_classes)}` will be replaced by'
                    f" `astro.dataframes.load_options.PandasLoadOptions` in astro-sdk-python>=2.0.0."
                    f" Please use `astro.dataframes.load_options.PandasLoadOptions` class instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )

        self.output_table = output_table
        self.input_file = input_file
        self.input_file.load_options = load_options
        self.chunk_size = chunk_size
        self.kwargs = kwargs
        self.if_exists = if_exists
        self.ndjson_normalize_sep = ndjson_normalize_sep
        self.normalize_config: dict[str, str] = {}
        self.use_native_support = use_native_support
        self.native_support_kwargs: dict[str, Any] = native_support_kwargs or {}
        self.columns_names_capitalization = columns_names_capitalization
        self.enable_native_fallback = enable_native_fallback
        self.assume_schema_exists = assume_schema_exists
        self.load_options_list = LoadOptionsList(load_options)

    def execute(self, context: Context) -> BaseTable | File:  # skipcq: PYL-W0613
        """
        Load an existing dataset from a supported file into a SQL table or a Dataframe.
        """
        if self.input_file.conn_id:
            check_if_connection_exists(self.input_file.conn_id)

        # TODO: remove pushing to XCom once we update the airflow version.
        if self.output_table:
            context["ti"].xcom_push(key="output_table_conn_id", value=str(self.output_table.conn_id))
            context["ti"].xcom_push(key="output_table_name", value=str(self.output_table.name))
        return self.load_data(input_file=self.input_file, context=context)

    def load_data(self, input_file: File, context: Context) -> BaseTable | pd.DataFrame:
        self.log.info("Loading %s into %s ...", self.input_file.path, self.output_table)
        if self.output_table:
            return self.load_data_to_table(input_file, context)
        else:
            return self.load_data_to_dataframe(input_file)

    def load_data_to_table(self, input_file: File, context: Context) -> BaseTable:
        """
        Loads csv/parquet table from local/S3/GCS with Pandas.
        Infers SQL database type based on connection then loads table to db.
        """
        if not isinstance(self.output_table, BaseTable):
            raise ValueError("Please pass a valid Table instance in 'output_table' parameter")
        database = create_database(
            self.output_table.conn_id, self.output_table, load_options_list=self.load_options_list
        )
        self.output_table = database.populate_table_metadata(self.output_table)
        normalize_config = self._populate_normalize_config(
            ndjson_normalize_sep=self.ndjson_normalize_sep,
            database=database,
        )
        database.load_file_to_table(
            input_file=input_file,
            normalize_config=normalize_config,
            output_table=self.output_table,
            if_exists=self.if_exists,
            chunk_size=self.chunk_size,
            use_native_support=self.use_native_support,
            native_support_kwargs=self.native_support_kwargs,
            columns_names_capitalization=self.columns_names_capitalization,
            enable_native_fallback=self.enable_native_fallback,
            assume_schema_exists=self.assume_schema_exists,
            databricks_job_name=f"Load data {self.dag_id}_{self.task_id}",
        )
        self.log.info("Completed loading the data into %s.", self.output_table)
        return self.output_table

    def load_data_to_dataframe(self, input_file: File) -> pd.DataFrame | None:
        """
        Loads csv/parquet file from local/S3/GCS with Pandas. Returns dataframe as no
        SQL table was specified
        """
        df = None
        for file in resolve_file_path_pattern(
            input_file.path,
            input_file.conn_id,
            normalize_config=self.normalize_config,
            filetype=input_file.type.name,
            load_options=input_file.load_options,
        ):
            if isinstance(df, pd.DataFrame):
                df = pd.concat(
                    [
                        df,
                        file.export_to_dataframe(
                            columns_names_capitalization=self.columns_names_capitalization
                        ),
                    ],
                    ignore_index=True,
                )
            else:
                df = file.export_to_dataframe(
                    columns_names_capitalization=self.columns_names_capitalization,
                )

        if not isinstance(df, PandasDataframe):
            df = PandasDataframe.from_pandas_df(df)

        self.log.info("Completed loading the data into dataframe.")
        return df

    @staticmethod
    def _populate_normalize_config(
        database: BaseDatabase,
        ndjson_normalize_sep: str = "_",
    ) -> dict[str, str]:
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

        normalize_config: dict[str, Any] = {
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
        normalize_config["sep"] = replace_illegal_columns_chars(normalize_config["sep"], database)

        return normalize_config

    def get_openlineage_facets_on_complete(self, task_instance):  # skipcq: PYL-W0613
        """
        Returns the lineage data
        """
        from astro.lineage import (
            BaseFacet,
            DataSourceDatasetFacet,
            OpenlineageDataset,
            OperatorLineage,
            SchemaDatasetFacet,
            SchemaField,
        )
        from astro.lineage.facets import InputFileDatasetFacet, InputFileFacet, OutputDatabaseDatasetFacet

        # if the input_file is a folder or pattern, it needs to be resolved to
        # list the files
        input_files = resolve_file_path_pattern(
            self.input_file.path,
            self.input_file.conn_id,
            normalize_config={},
            filetype=self.input_file.type.name,
            load_options=self.input_file.load_options,
        )

        input_uri = (
            f"{self.input_file.openlineage_dataset_namespace}{self.input_file.openlineage_dataset_name}"
        )
        input_dataset: list[OpenlineageDataset] = [
            OpenlineageDataset(
                namespace=self.input_file.openlineage_dataset_namespace,
                name=self.input_file.openlineage_dataset_name,
                facets={
                    "input_file_facet": InputFileDatasetFacet(
                        is_pattern=self.input_file.is_pattern(),
                        number_of_files=len(input_files),
                        files=[
                            InputFileFacet(
                                filepath=file.path,
                                file_size=file.size,
                                file_type=file.type.name,
                            )
                            for file in input_files
                        ],
                    ),
                    "dataSource": DataSourceDatasetFacet(
                        name=self.input_file.openlineage_dataset_name, uri=input_uri
                    ),
                },
            )
        ]

        output_dataset: list[OpenlineageDataset] = []
        if self.output_table is not None and self.output_table.openlineage_emit_temp_table_event():
            # TODO: remove pushing to XCom once we update the airflow version.
            self.output_table.conn_id = task_instance.xcom_pull(
                task_ids=task_instance.task_id, key="output_table_conn_id"
            )
            self.output_table.name = task_instance.xcom_pull(
                task_ids=task_instance.task_id, key="output_table_name"
            )
            output_dataset = [
                OpenlineageDataset(
                    namespace=self.output_table.openlineage_dataset_namespace(),
                    name=self.output_table.openlineage_dataset_name(),
                    facets={
                        "output_database_facet": OutputDatabaseDatasetFacet(
                            metadata=self.output_table.metadata,
                            columns=self.output_table.columns,
                            schema=self.output_table.sqlalchemy_metadata.schema,
                            used_native_path=self.use_native_support,
                            enabled_native_fallback=self.enable_native_fallback,
                            native_support_arguments=self.native_support_kwargs,
                        ),
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaField(
                                    name=self.output_table.metadata.schema,
                                    type=self.output_table.metadata.database,
                                )
                            ]
                        ),
                        "dataSource": DataSourceDatasetFacet(
                            name=self.output_table.name, uri=self.output_table.openlineage_dataset_uri()
                        ),
                    },
                )
            ]

        run_facets: dict[str, BaseFacet] = {}
        job_facets: dict[str, BaseFacet] = {}

        return OperatorLineage(
            inputs=input_dataset, outputs=output_dataset, run_facets=run_facets, job_facets=job_facets
        )


def load_file(
    input_file: File,
    output_table: BaseTable | None = None,
    task_id: str | None = None,
    if_exists: LoadExistStrategy = "replace",
    ndjson_normalize_sep: str = "_",
    use_native_support: bool = True,
    native_support_kwargs: dict | None = None,
    columns_names_capitalization: ColumnCapitalization = "original",
    enable_native_fallback: bool | None = True,
    load_options: LoadOptions | list[LoadOptions] | None = None,
    **kwargs: Any,
) -> XComArg:
    """Load a file or bucket into either a SQL table or a pandas dataframe.

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param task_id: task id, optional
    :param if_exists: default override an existing Table. Options: fail, replace, append
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
        ex - ``{"a": {"b":"c"}}`` will result in: ``column - "a_b"`` where ``ndjson_normalize_sep = "_"``
    :param use_native_support: Use native support for data transfer if available on the destination.
    :param native_support_kwargs: kwargs to be used by method involved in native support flow
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
        in the resulting dataframe
    :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
    :param load_options: load options while reading and loading file
    """

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_unique_task_id("load_file")

    if native_support_kwargs:
        warnings.warn(
            """`load_options` will replace `native_support_kwargs` parameter in astro-sdk-python>=1.5.0. Please use
            `load_options` parameter instead.""",
            DeprecationWarning,
            stacklevel=2,
        )

    return LoadFileOperator(
        task_id=task_id,
        input_file=input_file,
        output_table=output_table,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        use_native_support=use_native_support,
        native_support_kwargs=native_support_kwargs,
        columns_names_capitalization=columns_names_capitalization,
        enable_native_fallback=enable_native_fallback,
        load_options=load_options,
        **kwargs,
    ).output


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True
