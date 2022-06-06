import pathlib
from typing import List

import pandas as pd
import pytest
from airflow.decorators import task, task_group
from airflow.utils import timezone

from astro import sql as aql
from astro.databases import create_database
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from tests.sql.operators import utils as test_utils

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

CWD = pathlib.Path(__file__).parent

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@aql.transform
def apply_transform(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@aql.dataframe
def do_a_dataframe_thing(df: pd.DataFrame):
    return df


@aql.dataframe
def count_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    return len(df1) + len(df2)


@task
def compare(a, b):
    assert a == b


@task_group
def run_dataframe_funcs(input_table: Table):
    table_counts = count_dataframes(df1=input_table, df2=input_table)

    df1 = do_a_dataframe_thing(input_table)
    df2 = do_a_dataframe_thing(input_table)
    df_counts = count_dataframes(df1, df2)
    compare(table_counts, df_counts)


@aql.run_raw_sql
def add_constraint(table: Table):
    db = create_database(table.conn_id)
    constraints = ("list", "sell")
    return db.setup_merge(parameters=constraints)


@task_group
def run_append(output_specs: List):
    load_main = aql.load_file(
        input_file=File(path=str(CWD) + "/data/homes_main.csv"),
        output_table=output_specs[0],
    )
    load_append = aql.load_file(
        input_file=File(path=str(CWD) + "/data/homes_append.csv"),
        output_table=output_specs[1],
    )

    aql.append(
        columns=["sell", "living"],
        main_table=load_main,
        append_table=load_append,
    )


@task_group
def run_merge(output_specs: List):
    main_table = aql.load_file(
        input_file=File(path=str(CWD) + "/data/homes_merge_1.csv"),
        output_table=output_specs[0],
    )
    merge_table = aql.load_file(
        input_file=File(path=str(CWD) + "/data/homes_merge_2.csv"),
        output_table=output_specs[1],
    )

    con1 = add_constraint(main_table)

    merged_table = aql.merge(
        target_table=main_table,
        source_table=merge_table,
        target_conflict_columns=["list", "sell"],
        source_to_target_columns_map={"list": "list", "sell": "sell"},
        if_conflicts="ignore",
    )
    con1 >> merged_table


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {"param": {"metadata": Metadata(schema=SCHEMA)}},
            {
                "param": {"metadata": Metadata(schema=SCHEMA)},
            },
        ],
    ],
    indirect=True,
    ids=["table"],
)
def test_full_dag(sql_server, sample_dag, test_table):
    with sample_dag:
        output_table = test_table
        loaded_table = aql.load_file(
            input_file=File(path=str(CWD) + "/data/homes.csv"),
            output_table=output_table[0],
        )
        tranformed_table = apply_transform(loaded_table)
        run_dataframe_funcs(tranformed_table)
        run_append(output_table)
        run_merge(output_table)
        aql.export_file(
            input_data=tranformed_table,
            output_file=File(path="/tmp/out_agg.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)
