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


postgres_conn_id = "postgres_conn"
gcp_conn_id = "google_cloud_platform"
snowflake_conn_id = "snowflake_conn"
aws_conn_id = "aws_conn"
bigquery_conn_id = "bigquery"
redshift_conn_id = "redshift_conn"
mysql_conn_id = "mysql_conn"



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
        login="AKIAZG42HVH6QSYQ4PRF",
        password=os.environ["ASTROSDK_AWS_SECRET_ACCESS_KEY"],
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
               "extra__google_cloud_platform__keyfile_dict": "{   \"type\": \"service_account\",   \"project_id\": \"astronomer-dag-authoring\",   \"private_key_id\": \"c4affe8b62724ed2f5c6ab434b6f309db0d4a7f0\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQDYt6VkNavEnhMg\\nEfqtX2XthUm8O25WBObfbyHRxmWBoGuKyhr7hm5/OVKL1pCbcPodrrJwpnwAzASH\\nkvffaTMsSyQhgSvZh5rYIu2XPYtT7lpqWZnJ6SmkRGnDqsRHsq47dqCSGTo0ch0A\\nwF20wYSKUc0WpiJ7Lrqht0pdux5fUNkRy+wO88p9XSS+ErRVixNua52kky4B+nja\\nFQkoSf127kOEHaSul/G/aL5Tm9dPQvPNyQzgaILBrsA0vyuL6N5v7L5jDValsbLb\\np22j+bqbF3iVNdEjqI+MabcPu7qeYhgU4uK+q+V3laEHWX2tdqSdU8efXDU6rkKL\\nhdfn8NbBAgMBAAECgf9c+CuTmb1eU7yQqCXfSmryQkPSObdTtOFLk46b7NM5kMA8\\nggdL7+0R1wglGp8JOqY8eufskP88DZ8ZKgtRZFKiUHnnhapyCffQ0OsWqT2kJpR9\\nxdfGWVLr4rgcoJ14xJvKFqc7sGbafjnC9hiST+e0DvOUdnZEXKo5GzsOir9fflz3\\nkMwgvEZOeP315l2Wy/ZtV51mmCZ9S03AQq9ZHAgo0BAJ+E0AB4EC6nDw3TY/xK/1\\ntnDLKyhSt8FJ97QP3GsnlghItvJjOwHj/7SAIjhAitl4T228iedhgyjGwwBDu8MM\\nyQ2vxlDYl+IhPxI0B6vFUy9S847ObTMquwji2gECgYEA+uDkP8dr750g+4wO9aIE\\nJd88KLj35AY8Kifqevw6T5IJTW+ZC6n4PveMzhSb427OleIqCrx6lP8u2Mchv+kV\\nL6+NEdKl+2qo4D9pRdYo2tmKjNa4IijRbJLAcp4KccRB8LC3JA5UWNNtvGQQHcME\\nvvmEA3jUAf5Ev7YJuJ2bFwECgYEA3SQ54X2T4hAAzQIwcJCe0mzL2m7xfZrYBKER\\nbXRjroemvoeiLpbUkvMrkix3LtXjv6fqH+tLEKvw/r89hsMwlJzEJvRODRKnW89X\\nQg3Cw1JQGgUqj7e6w7AQqVcw9zRwwVr6uOb8zb/5zmjzlLlhEl6YMSrNufS+xfLb\\nijibf8ECgYEAtniqKyVuASrlGXDGVKxRngYfKHNYgbZzPiooJumbs9JBB9x6kXw/\\nvfSpqh5dP++RPHzyHTr3YbW1KiC0EHSEX1mtHmB7L0FlsS0N8aiRsJP3bPajmNzd\\nbb3TUcpXSGX5nAm8OvJdMPoe355cBnmY0xqChU/1y7lX4aSSr3+alQECgYASKRVv\\nxSZN3E6Uh2d+9wFz+mdLSu0eCYdP6gJKYNKG1LEG2JCjDtPqNXoQiQJIoFwRszJw\\n6JvDhLtH0GC3HnRw3+bj5BA81plT2tUpoYOrEqltWZtDLi1yQJU9suFci7vuxQ/t\\n+1orc2aKYugOAKNOJOKOtxsp+EVqcCVDVp6+gQKBgBz21Sqz78xxPiSX2r29V52z\\n6TV9oOOwL2DJvRWe2wBZf754xjKBXdwq2rY4WI2UwgE48muYG1U8Nvv7sFzIzdnZ\\nwS9Vq0GWYSoBUdkhdnkYd9wa2V48KQbKn8VQ7tzrg/X4JjJrngoRzwW19fIzXsBR\\n93yPG/qn0emjGPm5gueG\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"astro-qa@astronomer-dag-authoring.iam.gserviceaccount.com\",   \"client_id\": \"117195956945128506357\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/astro-qa%40astronomer-dag-authoring.iam.gserviceaccount.com\" }",
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
        conn = BaseHook.get_connection(mysql_conn)
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
        conn = BaseHook.get_connection(sftp_conn_config)
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
        conn = BaseHook.get_connection(ftp_conn_config)
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
        conn = BaseHook.get_connection(databricks_conn_config)
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
        conn = BaseHook.get_connection(duckdb_conn_config)
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(duckdb_conn_config)
        session.commit()

@task
def create_slack_connection():
    slack_conn = Connection(
        conn_id="http_slack",
        conn_type="slack",
        password="",
    )
    try:
        conn = BaseHook.get_connection(slack_conn)
        print(f"Found: {conn}")
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(slack_conn)
        session.commit()
@task
def create_bigquery_connection():
    bigquery_conn_config = Connection(
        conn_id=bigquery_conn_id,
        conn_type="gcpbigquery",
        extra={"extra__google_cloud_platform__project": "astronomer-dag-authoring",
               "extra__google_cloud_platform__keyfile_dict": "{   \"type\": \"service_account\",   \"project_id\": \"astronomer-dag-authoring\",   \"private_key_id\": \"c4affe8b62724ed2f5c6ab434b6f309db0d4a7f0\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQDYt6VkNavEnhMg\\nEfqtX2XthUm8O25WBObfbyHRxmWBoGuKyhr7hm5/OVKL1pCbcPodrrJwpnwAzASH\\nkvffaTMsSyQhgSvZh5rYIu2XPYtT7lpqWZnJ6SmkRGnDqsRHsq47dqCSGTo0ch0A\\nwF20wYSKUc0WpiJ7Lrqht0pdux5fUNkRy+wO88p9XSS+ErRVixNua52kky4B+nja\\nFQkoSf127kOEHaSul/G/aL5Tm9dPQvPNyQzgaILBrsA0vyuL6N5v7L5jDValsbLb\\np22j+bqbF3iVNdEjqI+MabcPu7qeYhgU4uK+q+V3laEHWX2tdqSdU8efXDU6rkKL\\nhdfn8NbBAgMBAAECgf9c+CuTmb1eU7yQqCXfSmryQkPSObdTtOFLk46b7NM5kMA8\\nggdL7+0R1wglGp8JOqY8eufskP88DZ8ZKgtRZFKiUHnnhapyCffQ0OsWqT2kJpR9\\nxdfGWVLr4rgcoJ14xJvKFqc7sGbafjnC9hiST+e0DvOUdnZEXKo5GzsOir9fflz3\\nkMwgvEZOeP315l2Wy/ZtV51mmCZ9S03AQq9ZHAgo0BAJ+E0AB4EC6nDw3TY/xK/1\\ntnDLKyhSt8FJ97QP3GsnlghItvJjOwHj/7SAIjhAitl4T228iedhgyjGwwBDu8MM\\nyQ2vxlDYl+IhPxI0B6vFUy9S847ObTMquwji2gECgYEA+uDkP8dr750g+4wO9aIE\\nJd88KLj35AY8Kifqevw6T5IJTW+ZC6n4PveMzhSb427OleIqCrx6lP8u2Mchv+kV\\nL6+NEdKl+2qo4D9pRdYo2tmKjNa4IijRbJLAcp4KccRB8LC3JA5UWNNtvGQQHcME\\nvvmEA3jUAf5Ev7YJuJ2bFwECgYEA3SQ54X2T4hAAzQIwcJCe0mzL2m7xfZrYBKER\\nbXRjroemvoeiLpbUkvMrkix3LtXjv6fqH+tLEKvw/r89hsMwlJzEJvRODRKnW89X\\nQg3Cw1JQGgUqj7e6w7AQqVcw9zRwwVr6uOb8zb/5zmjzlLlhEl6YMSrNufS+xfLb\\nijibf8ECgYEAtniqKyVuASrlGXDGVKxRngYfKHNYgbZzPiooJumbs9JBB9x6kXw/\\nvfSpqh5dP++RPHzyHTr3YbW1KiC0EHSEX1mtHmB7L0FlsS0N8aiRsJP3bPajmNzd\\nbb3TUcpXSGX5nAm8OvJdMPoe355cBnmY0xqChU/1y7lX4aSSr3+alQECgYASKRVv\\nxSZN3E6Uh2d+9wFz+mdLSu0eCYdP6gJKYNKG1LEG2JCjDtPqNXoQiQJIoFwRszJw\\n6JvDhLtH0GC3HnRw3+bj5BA81plT2tUpoYOrEqltWZtDLi1yQJU9suFci7vuxQ/t\\n+1orc2aKYugOAKNOJOKOtxsp+EVqcCVDVp6+gQKBgBz21Sqz78xxPiSX2r29V52z\\n6TV9oOOwL2DJvRWe2wBZf754xjKBXdwq2rY4WI2UwgE48muYG1U8Nvv7sFzIzdnZ\\nwS9Vq0GWYSoBUdkhdnkYd9wa2V48KQbKn8VQ7tzrg/X4JjJrngoRzwW19fIzXsBR\\n93yPG/qn0emjGPm5gueG\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"astro-qa@astronomer-dag-authoring.iam.gserviceaccount.com\",   \"client_id\": \"117195956945128506357\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/astro-qa%40astronomer-dag-authoring.iam.gserviceaccount.com\" }",
               "extra__google_cloud_platform__num_retries": 5}
    )

    try:
        conn = BaseHook.get_connection(bigquery_conn_id)
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except AirflowNotFoundException:
        session = settings.Session()
        session.add(bigquery_conn_config)
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

    # get it back, is it the same?
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
    create_bigquery_connection()
    create_redshift_connection()
    create_mssql_connection()
    create_sftp_connection()
    create_ftp_connection()
    create_databricks_connection()
    create_duckdb_connection()
   # create_slack_connection()
    print_sdk_version = BashOperator(
        task_id="print_sdk_version",
        bash_command='echo ASTRO_SDK_VER="$(pip freeze | grep astro-sdk-python)" ',
    )

the_dag = setup()
