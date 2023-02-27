import os
from datetime import datetime

from airflow.models import DAG

from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.table import Metadata, Table

# To create IAM role with needed permissions,
# refer: https://www.dataliftoff.com/iam-roles-for-loading-data-from-s3-into-redshift/
REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN = os.getenv("REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN")
AWS_CONN_ID = "aws_conn"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_load_file_redshift",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


with dag:
    # [START load_file_redshift_example_1]
    aql.load_file(
        input_file=File(
            "s3://astro-sdk/sample_pattern",
            conn_id=AWS_CONN_ID,
            filetype=FileType.CSV,
        ),
        output_table=Table(conn_id="redshift_conn", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_redshift_example_1]

    # [START load_file_redshift_example_2]
    aql.load_file(
        input_file=File(
            "gs://astro-sdk/workspace/sample_pattern.csv",
            conn_id="bigquery",
            filetype=FileType.CSV,
        ),
        output_table=Table(conn_id="redshift_conn", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_redshift_example_2]

    # [START load_file_redshift_example_3]
    aql.load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(conn_id="redshift_conn", metadata=Metadata(schema="astro")),
        use_native_support=True,
        native_support_kwargs={
            "IGNOREHEADER": 1,
            "REGION": "us-west-2",
            "IAM_ROLE": REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN,
        },
    )
    # [END load_file_redshift_example_3]

    aql.cleanup()
