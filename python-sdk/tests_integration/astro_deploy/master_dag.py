import logging
import os
import time
from datetime import datetime
from typing import Any, List

from airflow import DAG, settings
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Connection, DagRun
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session

SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#provider-alert")
SLACK_WEBHOOK_CONN = os.getenv("SLACK_WEBHOOK_CONN", "http_slack")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow_app")
IS_RUNTIME_RELEASE = os.getenv("IS_RUNTIME_RELEASE", default="False")
IS_RUNTIME_RELEASE = bool(IS_RUNTIME_RELEASE)
AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "not_set"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "not_set"),
    "region_name": "us-east-1",
}
AMI_ID = os.getenv("AWS_AMI_ID", "not_set")
INBOUND_SECURITY_GROUP_ID = os.getenv("AWS_INBOUND_SECURITY_GROUP_ID", "not_set")
EC2_INSTANCE_ID_KEY = "ec2_instance_id"
INSTANCE_PUBLIC_IP = "instance_public_ip"
SFTP_USERNAME = os.getenv("SFTP_USERNAME", "not_set")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "not_set")
FTP_USERNAME = os.getenv("FTP_USERNAME", "not_set")
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "not_set")


def get_report(dag_run_ids: List[str], **context: Any) -> None:  # noqa: C901
    """Fetch dags run details and generate report"""

    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        message_list: List[str] = []

        airflow_version = context["ti"].xcom_pull(task_ids="get_airflow_version")
        if IS_RUNTIME_RELEASE:
            airflow_version_message = (
                f"Results generated for latest Runtime version {os.environ['ASTRONOMER_RUNTIME_VERSION']} "
                f"with {os.environ['AIRFLOW__CORE__EXECUTOR']}  \n\n"
            )
        else:
            airflow_version_message = (
                f"Airflow version for the below astro-sdk run is `{airflow_version}` "
                f"with {os.environ['AIRFLOW__CORE__EXECUTOR']} \n\n"
            )
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


def start_sftp_ftp_services_method():
    import boto3

    ec2 = boto3.resource("ec2", **AWS_S3_CREDS)
    instance = ec2.create_instances(
        ImageId=AMI_ID,
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        SecurityGroupIds=[INBOUND_SECURITY_GROUP_ID],
    )
    print("instance : ", instance)
    instance_id = instance[0].instance_id
    ti = get_current_context()["ti"]
    ti.xcom_push(key=EC2_INSTANCE_ID_KEY, value=instance_id)
    while get_instances_status(instance_id) != "running":
        logging.info("Waiting for Instance to be available in running state. Sleeping for 30 seconds.")
        time.sleep(30)


def get_instances_status(instance_id: str) -> str:
    """Get the instance status by id"""
    import boto3

    client = boto3.client("ec2", **AWS_S3_CREDS)
    response = client.describe_instances(
        InstanceIds=[instance_id],
    )
    print("response : ", response)
    instance_details = response["Reservations"][0]["Instances"][0]
    instance_state: str = instance_details["State"]["Name"]
    print("instance_state : ", instance_state)
    if instance_state == "running":
        ti = get_current_context()["ti"]
        ti.xcom_push(key=INSTANCE_PUBLIC_IP, value=instance_details["PublicIpAddress"])
    return instance_state


def create_sftp_ftp_airflow_connection(task_instance: Any) -> None:
    """
    Checks if airflow connection exists, if yes then deletes it.
    Then, create a new sftp_default, ftp_default connection.
    """
    sftp_conn = Connection(
        conn_id="sftp_conn",
        conn_type="sftp",
        host=task_instance.xcom_pull(key=INSTANCE_PUBLIC_IP, task_ids=["start_sftp_ftp_services"])[0],
        login=SFTP_USERNAME,
        password=SFTP_PASSWORD,
    )  # create a connection object

    ftp_conn = Connection(
        conn_id="ftp_conn",
        conn_type="ftp",
        host=task_instance.xcom_pull(key=INSTANCE_PUBLIC_IP, task_ids=["start_sftp_ftp_services"])[0],
        login=FTP_USERNAME,
        password=FTP_PASSWORD,
    )  # create a connection object

    session = settings.Session()
    for conn in [sftp_conn, ftp_conn]:
        connection = session.query(Connection).filter_by(conn_id=conn.conn_id).one_or_none()
        if connection is None:
            logging.info("Connection %s doesn't exist.", str(conn.conn_id))
        else:
            session.delete(connection)
            session.commit()
            logging.info("Connection %s deleted.", str(conn.conn_id))

        session.add(conn)
        session.commit()  # it will insert the connection object programmatically.
        logging.info("Connection %s is created", conn.conn_id)


def terminate_instance(task_instance: "TaskInstance") -> None:  # noqa: F821
    """Terminate ec2 instance by instance id"""
    import boto3

    ec2 = boto3.client("ec2", **AWS_S3_CREDS)
    ec2_instance_id_xcom = task_instance.xcom_pull(
        key=EC2_INSTANCE_ID_KEY, task_ids=["start_sftp_ftp_services"]
    )[0]
    ec2.terminate_instances(
        InstanceIds=[
            ec2_instance_id_xcom,
        ],
    )


if IS_RUNTIME_RELEASE:
    schedule_interval = None
else:
    schedule_interval = "@daily"
with DAG(
    dag_id="example_master_dag",
    schedule_interval=schedule_interval,
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

    start_sftp_ftp_services = PythonOperator(
        task_id="start_sftp_ftp_services",
        python_callable=start_sftp_ftp_services_method,
    )

    create_sftp_ftp_default_airflow_connection = PythonOperator(
        task_id="create_sftp_ftp_default_airflow_connection",
        python_callable=create_sftp_ftp_airflow_connection,
    )

    terminate_ec2_instance = PythonOperator(
        task_id="terminate_instance", trigger_rule="all_done", python_callable=terminate_instance
    )

    dag_run_ids = []

    load_file_task_info = [
        {"example_google_bigquery_gcs_load_and_save": "example_google_bigquery_gcs_load_and_save"},
        {"example_amazon_s3_postgres_load_and_save": "example_amazon_s3_postgres_load_and_save"},
        {"example_amazon_s3_postgres": "example_amazon_s3_postgres"},
        {"example_load_file": "example_load_file"},
    ]

    load_file_trigger_tasks, ids = prepare_dag_dependency(load_file_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*load_file_trigger_tasks)

    transform_task_info = [
        {"example_amazon_s3_snowflake_transform": "example_amazon_s3_snowflake_transform"},
        {"example_transform_mssql": "example_transform_mssql"},
    ]

    transform_trigger_tasks, ids = prepare_dag_dependency(transform_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*transform_trigger_tasks)

    dataframe_task_info = [
        {"example_dataframe": "example_dataframe"},
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

    data_validation_dags_info = [
        {"data_validation_check_column": "data_validation_check_column"},
    ]

    data_validation_trigger_tasks, ids = prepare_dag_dependency(data_validation_dags_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*data_validation_trigger_tasks)

    dataset_dags_info = [
        {"example_dataset_producer": "example_dataset_producer"},
    ]

    dataset_trigger_tasks, ids = prepare_dag_dependency(dataset_dags_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*dataset_trigger_tasks)

    cleanup_snowflake_task_info = [{"example_snowflake_cleanup": "example_snowflake_cleanup"}]

    cleanup_snowflake_trigger_tasks, ids = prepare_dag_dependency(cleanup_snowflake_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*cleanup_snowflake_trigger_tasks)

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

    (  # skipcq PYL-W0104
        start
        >> start_sftp_ftp_services
        >> create_sftp_ftp_default_airflow_connection
        >> [  # skipcq PYL-W0104
            list_installed_pip_packages,
            get_airflow_version,
            load_file_trigger_tasks[0],
            transform_trigger_tasks[0],
            dataframe_trigger_tasks[0],
            append_trigger_tasks[0],
            merge_trigger_tasks[0],
            dynamic_task_trigger_tasks[0],
            data_validation_trigger_tasks[0],
            dataset_trigger_tasks[0],
            cleanup_snowflake_trigger_tasks[0],
        ]
    )

    last_task = [
        list_installed_pip_packages,
        get_airflow_version,
        load_file_trigger_tasks[-1],
        transform_trigger_tasks[-1],
        dataframe_trigger_tasks[-1],
        append_trigger_tasks[-1],
        merge_trigger_tasks[-1],
        dynamic_task_trigger_tasks[-1],
        data_validation_trigger_tasks[-1],
        dataset_trigger_tasks[-1],
        cleanup_snowflake_trigger_tasks[-1],
    ]

    last_task >> end  # skipcq PYL-W0104
    last_task >> report  # skipcq PYL-W0104
    last_task >> terminate_ec2_instance  # skipcq PYL-W0104
