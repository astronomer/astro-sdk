import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

query = f"""SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE
TABLE_SCHEMA = '{os.environ.get("SNOWFLAKE_SCHEMA", "ASTROFLOW_CI")}' and
TABLE_OWNER = '{os.environ.get("SNOWFLAKE_TABLE_OWNER","AIRFLOW_TEST_USER")}' and
 TABLE_NAME LIKE '_TMP_%' and CREATED < '{date.today() - timedelta(days = 1)}'"""


def make_drop_statements(task_instance: Any):
    temp_tables = task_instance.xcom_pull(key="return_value", task_ids=["snowflake_op_sql_str"])[0]
    delete_temp_tables = ""
    # converting list of dictornary to sql statements
    for temp_table in temp_tables:
        temp_table = "DROP TABLE IF EXISTS " + temp_table["TABLE_NAME"] + ";"
        delete_temp_tables += temp_table
    print(len(delete_temp_tables))
    if len(delete_temp_tables) == 0:
        delete_temp_tables = "Select 1"
    return delete_temp_tables


def handle_result(result: Any):
    return result.fetchall()


with DAG(
    dag_id="example_snowflake_cleanup",
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": "snowflake_conn"},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    snowflake_op_sql_str = SnowflakeOperator(task_id="snowflake_op_sql_str", sql=query, handler=handle_result)

    create_drop_statement = PythonOperator(
        task_id="create_drop_statement", python_callable=make_drop_statements
    )

    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id="snowflake_op_sql_multiple_stmts",
        sql="{{ task_instance.xcom_pull(task_ids='create_drop_statement', dag_id='example_snowflake_cleanup', key='return_value') }}",  # noqa: E501
    )

    snowflake_op_sql_str >> create_drop_statement >> snowflake_op_sql_multiple_stmts  # skipcq PYL-W0104
