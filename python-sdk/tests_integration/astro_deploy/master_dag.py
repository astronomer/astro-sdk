import logging
import os
import time
from datetime import datetime
from typing import Any, List

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session

SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#provider-alert")
SLACK_WEBHOOK_CONN = os.getenv("SLACK_WEBHOOK_CONN", "http_slack")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow_app")


def get_report(dag_run_ids: List[str], **context: Any) -> None:
    """Fetch dags run details and generate report"""
    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        message_list: List[str] = []

        airflow_version = context["ti"].xcom_pull(task_ids="get_airflow_version")
        airflow_version_message = f"Airflow version for the below astro-sdk run is `{airflow_version}` \n\n"
        message_list.append(airflow_version_message)

        for dr in last_dags_runs:
            dr_status = f" *{dr.dag_id} : {dr.get_state()}* \n"
            message_list.append(dr_status)
            for ti in dr.get_task_instances():
                task_code = ":black_circle: "
                if ti.task_id not in ["end", "get_report"]:
                    if ti.state == "success":
                        task_code = ":large_green_circle: "
                    elif ti.state == "failed":
                        task_code = ":red_circle: "
                    elif ti.state == "upstream_failed":
                        task_code = ":large_orange_circle: "
                    task_message_str = f"{task_code} {ti.task_id} : {ti.state} \n"
                    message_list.append(task_message_str)

        logging.info("%s", "".join(message_list))
        # Send dag run report on Slack
        try:
            SlackWebhookOperator(
                task_id="slack_alert",
                http_conn_id=SLACK_WEBHOOK_CONN,
                message="".join(message_list),
                channel=SLACK_CHANNEL,
                username=SLACK_USERNAME,
            ).execute(context=None)
        except Exception as exception:
            logging.exception("Error occur while sending slack alert.")
            raise exception


def prepare_dag_dependency(task_info, execution_time):
    """Prepare list of TriggerDagRunOperator task and dags run ids for dags of same providers"""
    _dag_run_ids = []
    _task_list = []
    for _example_dag in task_info:
        _task_id = list(_example_dag.keys())[0]

        _run_id = f"{_task_id}_{_example_dag.get(_task_id)}_" + execution_time
        _dag_run_ids.append(_run_id)
        _task_list.append(
            TriggerDagRunOperator(
                task_id=_task_id,
                trigger_dag_id=_example_dag.get(_task_id),
                trigger_run_id=_run_id,
                wait_for_completion=True,
                reset_dag_run=True,
                execution_date=execution_time,
                allowed_states=["success", "failed", "skipped"],
            )
        )
    return _task_list, _dag_run_ids


with DAG(
    dag_id="example_master_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["master_dag"],
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: time.sleep(30),
    )

    list_installed_pip_packages = BashOperator(
        task_id="list_installed_pip_packages", bash_command="pip freeze"
    )

    get_airflow_version = BashOperator(
        task_id="get_airflow_version", bash_command="airflow version", do_xcom_push=True
    )

    dag_run_ids = []

    load_file_task_info = [
        {"example_load_file": "example_load_file"},
        {"example_google_bigquery_gcs_load_and_save": "example_google_bigquery_gcs_load_and_save"},
        {"example_amazon_s3_postgres_load_and_save": "example_amazon_s3_postgres_load_and_save"},
        {"example_amazon_s3_postgres": "example_amazon_s3_postgres"},
    ]

    load_file_trigger_tasks, ids = prepare_dag_dependency(load_file_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*load_file_trigger_tasks)

    transform_task_info = [
        {"example_transform": "example_transform"},
        {"example_transform_file": "example_transform_file"},
        {"example_sqlite_load_transform": "example_sqlite_load_transform"},
        {"example_amazon_s3_snowflake_transform": "example_amazon_s3_snowflake_transform"},
        {"calculate_popular_movies": "calculate_popular_movies"},
    ]

    transform_trigger_tasks, ids = prepare_dag_dependency(transform_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*transform_trigger_tasks)

    dataframe_task_info = [
        {"example_dataframe": "example_dataframe"},
        {"calculate_top_2_movies_using_dataframe": "calculate_top_2_movies_using_dataframe"},
    ]

    dataframe_trigger_tasks, ids = prepare_dag_dependency(dataframe_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*dataframe_trigger_tasks)

    append_task_info = [
        {"example_append": "example_append"},
        {"example_snowflake_partial_table_with_append": "example_snowflake_partial_table_with_append"},
    ]

    append_trigger_tasks, ids = prepare_dag_dependency(append_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*append_trigger_tasks)

    merge_trigger_tasks = [{"example_merge_bigquery": "example_merge_bigquery"}]

    merge_trigger_tasks, ids = prepare_dag_dependency(merge_trigger_tasks, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*merge_trigger_tasks)

    dynamic_task_info = [
        {"example_dynamic_map_task": "example_dynamic_map_task"},
        {"example_dynamic_task_template": "example_dynamic_task_template"},
    ]

    dynamic_task_trigger_tasks, ids = prepare_dag_dependency(dynamic_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*dynamic_task_trigger_tasks)

    # example_dataset_producer [TODO]

    report = PythonOperator(
        task_id="get_report",
        python_callable=get_report,
        op_kwargs={"dag_run_ids": dag_run_ids},
        trigger_rule="all_done",
        provide_context=True,
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

    start >> [  # skipcq PYL-W0104
        list_installed_pip_packages,
        get_airflow_version,
        load_file_trigger_tasks[0],
        transform_trigger_tasks[0],
        dataframe_trigger_tasks[0],
        append_trigger_tasks[0],
        merge_trigger_tasks[0],
        dynamic_task_trigger_tasks[0],
    ]

    last_task = [
        list_installed_pip_packages,
        get_airflow_version,
        load_file_trigger_tasks[-1],
        transform_trigger_tasks[-1],
        dataframe_trigger_tasks[-1],
        append_trigger_tasks[-1],
        merge_trigger_tasks[-1],
        dynamic_task_trigger_tasks[-1],
    ]

    last_task >> end  # skipcq PYL-W0104
    last_task >> report  # skipcq PYL-W0104
