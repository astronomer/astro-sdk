import pathlib

import pandas as pd
import pytest
from airflow.decorators import task, task_group
from airflow.utils import timezone

from astro import sql as aql
from astro.databases.sqlite import SqliteDatabase
from astro.dataframe import dataframe as adf
from astro.sql.tables import Table
from tests.operators import utils as test_utils

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

CWD = pathlib.Path(__file__).parent

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


def merge_keys(sql_server):
    """
    To match with their respective API's, we have a slightly different "merge_keys" value
    when a user is using snowflake.
    :param sql_server:
    :return:
    """
    sql_name, _ = sql_server

    if sql_name == "snowflake":
        return {"list": "list", "sell": "sell"}
    else:
        return ["list", "sell"]


@aql.transform
def apply_transform(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@adf
def do_a_dataframe_thing(df: pd.DataFrame):
    return df


@adf
def count_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    return len(df1) + len(df2)


@task
def compare(a, b):
    assert a == b


@task_group
def run_validation(input_table: Table):
    agg_validated_table = aql.aggregate_check(
        input_table,
        check="select count(*) FROM {{table}}",
        equal_to=47,
    )
    aql.save_file(
        input_data=agg_validated_table.output,
        output_file_path="/tmp/out_agg.csv",
        overwrite=True,
    )

    # TODO add boolean and stats checks here


@task_group
def run_dataframe_funcs(input_table: Table):
    table_counts = count_dataframes(df1=input_table, df2=input_table)

    df1 = do_a_dataframe_thing(input_table)
    df2 = do_a_dataframe_thing(input_table)
    df_counts = count_dataframes(df1, df2)
    compare(table_counts, df_counts)


@aql.run_raw_sql
def add_constraint(table: Table):
    if isinstance(table.db, SqliteDatabase):
        return "CREATE UNIQUE INDEX unique_index ON {{table}}(list,sell)"
    return "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE (list,sell)"


@task_group
def run_append(output_specs: Table):
    load_main = aql.load_file(
        path=str(CWD) + "/data/homes_main.csv",
        output_table=output_specs,
    )
    load_append = aql.load_file(
        path=str(CWD) + "/data/homes_append.csv",
        output_table=output_specs,
    )

    aql.append(
        columns=["sell", "living"],
        main_table=load_main,
        append_table=load_append,
    )


@task_group
def run_merge(output_specs: Table, merge_keys):
    main_table = aql.load_file(
        path=str(CWD) + "/data/homes_merge_1.csv",
        output_table=output_specs,
    )
    merge_table = aql.load_file(
        path=str(CWD) + "/data/homes_merge_2.csv",
        output_table=output_specs,
    )

    con1 = add_constraint(main_table)

    merged_table = aql.merge(
        target_table=main_table,
        merge_table=merge_table,
        merge_keys=merge_keys,
        target_columns=["list", "sell"],
        merge_columns=["list", "sell"],
        conflict_strategy="ignore",
    )
    con1 >> merged_table


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
        # pytest.param("bigquery", marks=pytest.mark.xfail(reason="some bug")),
        "sqlite",
    ],
    indirect=True,
)
def test_full_dag(sql_server, sample_dag, test_table):
    with sample_dag:
        output_table = test_table
        loaded_table = aql.load_file(
            str(CWD) + "/data/homes.csv", output_table=output_table
        )
        tranformed_table = apply_transform(loaded_table)
        run_dataframe_funcs(tranformed_table)
        run_append(output_table)
        run_merge(output_table, merge_keys(sql_server))
        run_validation(tranformed_table)
    test_utils.run_dag(sample_dag)
