from datetime import datetime
from textwrap import dedent
from random import randint
import os
from airflow import settings
from airflow.decorators import dag, task, task_group
from airflow.models import Connection
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskmixin import TaskMixin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

postgres_conn_id = "postgres_conn"
gcp_conn_id = "google_cloud_platform"
snowflake_conn_id = "snowflake_conn"
aws_conn_id = "aws_conn"
bigquery_conn_id = "bigquery"
redshift_conn_id = "redshift_conn"
mysql_conn_id = "mysql_conn"


@task
def create_defaults():
    with create_session() as session:
        create_default_connections(session)

@task
def create_postgres_connection():
    conn_config = Connection(
        conn_id=postgres_conn_id,
        conn_type="postgres",
        host="database-1.cyoubjkhnirn.us-east-1.rds.amazonaws.com",
        login="postgres",
        password=os.environ['POSTGRES_PASSWORD'],
        port=5432,
    )
    try:
        conn = BaseHook.get_connection(postgres_conn_id)
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(conn_config)
        session.commit()

@task
def create_gdrive_connection():
    gdrive_connection = Connection(
        conn_id="gdrive_conn",
        conn_type="google_cloud_platform",
        extra={"extra__google_cloud_platform__scope":"https://www.googleapis.com/auth/drive.readonly"}
    )
    try:
        conn = BaseHook.get_connection("gdrive_conn")
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(gdrive_connection)
        session.commit()
@task
def create_snowflake_connection():
    snowflake_conn_config = Connection(
        conn_id=snowflake_conn_id,
        conn_type="snowflake",
        schema="UTKARSHSHARMA",
        host="https://gp21411.us-east-1.snowflakecomputing.com/",
        login="UTKARSHSHARMA",
        password=os.environ['SNOWFLAKE_PASSWORD'],
        extra={"account":"gp21411","database":"SANDBOX","region":"us-east-1","extra__snowflake__insecure_mode":"false"}
    )

    try:
        conn = BaseHook.get_connection(snowflake_conn_id)
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(snowflake_conn_config)
        session.commit()


@task
def create_aws_connection():
    aws_conn_config = Connection(
        conn_id=aws_conn_id,
        conn_type="aws",
        login="AKIAZG42HVH6TCD2Y7QJ",
        password=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    try:
        conn = BaseHook.get_connection(aws_conn_id)
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(aws_conn_config)
        session.commit()

@task
def create_redshift_connection():
    redshift_conn_config = Connection(
        conn_id=redshift_conn_id,
        conn_type="redshift",
        host="utkarsh-cluster.cdru7mxqmtyx.us-east-2.redshift.amazonaws.com",
        login="awsuser",
        password=os.environ['REDSHIFT_PASSWORD'],
        schema="dev",
    )
    try:
        conn = BaseHook.get_connection(redshift_conn_id)
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(redshift_conn_config)
        session.commit()
@task
def create_gcp_connection():
    gcp_conn_config = Connection(
        conn_id=gcp_conn_id,
        conn_type="google_cloud_platform",
        schema="default",
        extra={"extra__google_cloud_platform__project": "astronomer-dag-authoring",
               "extra__google_cloud_platform__keyfile_dict": os.environ["GCP_PLATFORM_KEYFILE_SECRET"],
               "extra__google_cloud_platform__num_retries": 5}
    )

    try:
        conn = BaseHook.get_connection(gcp_conn_id)
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(gcp_conn_config)
        session.commit()

@task
def create_mssql_connection():
    mysql_conn = Connection(
        conn_id=mysql_conn_id,
        conn_type="mssql",
        host="astroserver.database.windows.net",
        login="astrouser",
        password=os.environ['MYSQL_PASSWORD'],
        schema="astrodb",
        port=1433
    )
    try:
        conn = BaseHook.get_connection(mysql_conn_id)
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(mysql_conn)
        session.commit()

@task
def create_sftp_connection():
    sftp_conn_config = Connection(
        conn_id="sftp_conn",
        conn_type="sftp",
        host="localhost:2222",
        login="foo",
        password=os.environ['SFTP_PASSWORD'],
        port=2222
    )
    try:
        conn = BaseHook.get_connection("sftp_conn")
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(sftp_conn_config)
        session.commit()

@task
def create_ftp_connection():
    ftp_conn_config = Connection(
        conn_id="ftp_conn",
        conn_type="ftp",
        host="localhost:2222",
        login="foo",
        password=os.environ['SFTP_PASSWORD'],
        port=21
    )
    try:
        conn = BaseHook.get_connection("ftp_conn")
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(ftp_conn_config)
        session.commit()

@task
def create_databricks_connection():
    databricks_conn_config = Connection(
        conn_id="databricks_conn",
        conn_type="delta",
        host="https://dbc-9c390870-65ef.cloud.databricks.com/",
        password=os.environ['DATABRICKS_PASSWORD'],
    )
    try:
        conn = BaseHook.get_connection("databricks_conn")
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(databricks_conn_config)
        session.commit()

@task
def create_duckdb_connection():
    duckdb_conn_config = Connection(
        conn_id="duckdb_conn",
        conn_type="duckdb",
        host="/tmp/ciduckdb.duckdb",
    )
    try:
        conn = BaseHook.get_connection("duckdb_conn")
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(duckdb_conn_config)
        session.commit()

@task
def create_slack_connection():
    slack_conn = Connection(
        conn_id="http_slack",
        conn_type="slackwebhook",
        password=os.environ['SLACK_WEBHOOK_PASSWORD'],
        extra={
            # Specify extra parameters here
            "timeout": "42",
        },
    )
    try:
        conn = BaseHook.get_connection("http_slack")
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(slack_conn)
        session.commit()


@task_group
def test_connection(prev_task: TaskMixin) -> TaskMixin:
    "Make sure we can talk to the DB before expecting subsequent DAGs to do so"

    test_table = "test_kashin"

    # a random number
    @task
    def pick_test_val():
        return randint(1, 99)

    test_val = pick_test_val()

    # put it in an empty database
    place_val = PostgresOperator(
        task_id="place_value",
        postgres_conn_id=postgres_conn_id,
        sql=dedent(
            f"""
                DROP TABLE IF EXISTS {test_table};
                CREATE TABLE {test_table}(num integer);
                INSERT INTO {test_table}(num) VALUES ({test_val});
                """
        ),
    )

    @task
    def check_val(expected_val):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        num = pg_hook.get_records(sql=f"SELECT * FROM {test_table};")[0][0]
        print(f"expecting {expected_val} to be {num}")
        assert expected_val == num

    # do it in this order
    checked = check_val(test_val)
    test_val >> place_val
    prev_task >> place_val >> checked

    return checked


@dag(
    dag_id="create_sdk_required_connections",
    start_date=datetime(1970, 1, 1),
    schedule_interval=None,
    tags=["setup"],
    doc_md=dedent(
        f"""
        This DAG creates connections required by astro sdk dags connection 
        Then it makes sure that we can talk to that DB (it will fail if we can't)

        Run this before you run any dags that depend on that connection
        """
    ),
)
def setup():
    t1 = create_postgres_connection()
    test_connection(t1)
    create_gcp_connection()
    create_snowflake_connection()
    create_aws_connection()
    create_redshift_connection()
    create_mssql_connection()
    create_sftp_connection()
    create_ftp_connection()
    create_databricks_connection()
    create_duckdb_connection()
    create_slack_connection()
    create_gdrive_connection()
    create_defaults()
    print_sdk_version = BashOperator(
        task_id="print_sdk_version",
        bash_command='echo ASTRO_SDK_VER="$(pip freeze | grep astro-sdk-python)" ',
    )

the_dag = setup()
