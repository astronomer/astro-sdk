"""
Pre-requisites for load_file_example_19:
 - Install dependencies for Astro Python SDK with Google, refer to README.md
 - You can either specify a service account key file and set `GOOGLE_APPLICATION_CREDENTIALS`
    with the file path to the service account.
 - In the connection we need to specify the scopes.
    Connection variable is ``extra__google_cloud_default__scope``
    or in Airflow Connections UI ``Scopes (comma separated)``
    For ex:- https://www.googleapis.com/auth/drive.readonly
    Please refer to https://developers.google.com/identity/protocols/oauth2/scopes#drive for more details.
 - In Google Cloud, for the project we need to enable the Google Drive API.
    To enable the API please refer https://developers.google.com/drive/api/guides/enable-drive-api
 - Create a Google Drive folder (or) use an existing folder with a file inside it,
    and share the file with service account email id in order for it to be able to access those
    folders/files. In this example DAG, we will load this file into Snowflake table.
    For sharing a file/folder
    please refer https://www.labnol.org/google-api-service-account-220404#4-share-a-drive-folder
"""

import os
import pathlib
from datetime import datetime

import sqlalchemy
from airflow.models import DAG

from astro import sql as aql
from astro.constants import FileType
from astro.databases.databricks.load_options import DeltaLoadOptions
from astro.dataframes.load_options import PandasLoadOptions
from astro.files import File
from astro.options import SnowflakeLoadOptions
from astro.table import Metadata, Table

# To create IAM role with needed permissions,
# refer: https://www.dataliftoff.com/iam-roles-for-loading-data-from-s3-into-redshift/
REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN = os.getenv("REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN")
SNOWFLAKE_CONN_ID = "snowflake_conn"
DATABRICKS_CONN_ID = "databricks_conn"
MSSQL_CONN_ID = "mssql_conn"
DUCKDB_CONN_ID = "duckdb_conn"
AWS_CONN_ID = "aws_conn"
MYSQL_CONN_ID = "mysql_conn"

CWD = pathlib.Path(__file__).parent

ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_POSTGRESS_CONN_ID = os.getenv("ASTRO_POSTGRESS_CONN_ID", "postgres_conn")
ASTRO_REDSHIFT_CONN_ID = os.getenv("ASTRO_REDSHIFT_CONN_ID", "redshift_conn")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_load_file",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


with dag:
    # [START load_file_example_1]
    my_homes_table = aql.load_file(
        task_id="s3_to_postgress",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.csv"),
        output_table=Table(
            conn_id=ASTRO_POSTGRESS_CONN_ID,
        ),
    )
    # [END load_file_example_1]

    # [START load_file_example_2]
    dataframe = aql.load_file(
        task_id="s3_to_dataframe",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.csv"),
    )
    # [END load_file_example_2]

    # [START load_file_example_3]
    sample_table = aql.load_file(
        task_id="s3_to_postgress_ndjson",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.ndjson"),
        output_table=Table(
            conn_id=ASTRO_POSTGRESS_CONN_ID,
        ),
        ndjson_normalize_sep="__",
    )
    # [END load_file_example_3]

    # [START load_file_example_4]
    new_table = aql.load_file(
        task_id="s3_to_postgress_replace",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.csv"),
        output_table=Table(
            conn_id=ASTRO_POSTGRESS_CONN_ID,
        ),
        if_exists="replace",
    )
    # [END load_file_example_4]

    # [START load_file_example_5]
    custom_schema_table = aql.load_file(
        task_id="s3_to_postgress_specify_schema",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.csv"),
        output_table=Table(
            conn_id=ASTRO_POSTGRESS_CONN_ID,
            columns=[
                sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
            ],
        ),
    )
    # [END load_file_example_5]

    # [START load_file_example_6]
    dataframe = aql.load_file(
        task_id="s3_to_dataframe_capitalization_upper",
        input_file=File(path="s3://astro-sdk/python_sdk/example_dags/data/sample.csv"),
        columns_names_capitalization="upper",
    )
    # [END load_file_example_6]

    # [START load_file_example_7]
    aql.load_file(
        task_id="s3_to_bigquery",
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(conn_id=ASTRO_GCP_CONN_ID, metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_7]

    # [START load_file_example_8]
    aql.load_file(
        task_id="s3_to_bigquery_native_support",
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(conn_id=ASTRO_GCP_CONN_ID, metadata=Metadata(schema="astro")),
        use_native_support=True,
        native_support_kwargs={
            "ignore_unknown_values": True,
            "allow_jagged_rows": True,
            "skip_leading_rows": "1",
        },
    )
    # [END load_file_example_8]

    # [START load_file_example_10]
    my_homes_table = aql.load_file(
        task_id="local_to_postgress_glob",
        input_file=File(path=str(CWD.parent) + "/tests/data/homes*", filetype=FileType.CSV),
        output_table=Table(
            conn_id=ASTRO_POSTGRESS_CONN_ID,
        ),
    )
    # [END load_file_example_10]

    # [START load_file_example_11]
    aql.load_file(
        task_id="s3_to_bigquery_pattern",
        input_file=File("s3://astro-sdk/sample_pattern", conn_id=AWS_CONN_ID, filetype=FileType.CSV),
        output_table=Table(conn_id=ASTRO_GCP_CONN_ID, metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_11]

    # [START load_file_example_12]
    aql.load_file(
        task_id="gs_to_bigquery_pattern",
        input_file=File(
            "gs://astro-sdk/workspace/sample_pattern",
            conn_id=ASTRO_GCP_CONN_ID,
            filetype=FileType.CSV,
        ),
        output_table=Table(conn_id=ASTRO_GCP_CONN_ID, metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_12]

    # [START load_file_example_13]
    # Redshift tests are broken due to AWS issue.
    # aql.load_file(
    #     task_id="s3_to_redshift_pattern",
    #     input_file=File(
    #         "s3://astro-sdk/sample_pattern",
    #         conn_id=AWS_CONN_ID,
    #         filetype=FileType.CSV,
    #     ),
    #     output_table=Table(conn_id=ASTRO_REDSHIFT_CONN_ID, metadata=Metadata(schema="astro")),
    #     use_native_support=False,
    # )
    # [END load_file_example_13]

    # [START load_file_example_14]
    # Redshift tests are broken due to AWS issue.
    # aql.load_file(
    #     task_id="gs_to_redshift",
    #     input_file=File(
    #         "gs://astro-sdk/workspace/sample_pattern.csv",
    #         conn_id=ASTRO_GCP_CONN_ID,
    #         filetype=FileType.CSV,
    #     ),
    #     output_table=Table(conn_id=ASTRO_REDSHIFT_CONN_ID, metadata=Metadata(schema="astro")),
    #     use_native_support=False,
    # )
    # [END load_file_example_14]

    # [START load_file_example_16]
    # Redshift tests are broken due to AWS issue.
    # aql.load_file(
    #     task_id="s3_to_redshift_native",
    #     input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
    #     output_table=Table(conn_id=ASTRO_REDSHIFT_CONN_ID, metadata=Metadata(schema="astro")),
    #     use_native_support=True,
    #     native_support_kwargs={
    #         "IGNOREHEADER": 1,
    #         "REGION": "us-west-2",
    #         "IAM_ROLE": REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN,
    #     },
    # )
    # [END load_file_example_16]

    # [START load_file_example_17]
    aql.load_file(
        task_id="gs_to_bigquery_native",
        input_file=File(
            "gs://astro-sdk/workspace/sample_pattern.csv",
            conn_id=ASTRO_GCP_CONN_ID,
            filetype=FileType.CSV,
        ),
        output_table=Table(conn_id=ASTRO_GCP_CONN_ID, metadata=Metadata(schema="astro")),
        use_native_support=True,
        native_support_kwargs={
            "ignore_unknown_values": True,
            "allow_jagged_rows": True,
            "skip_leading_rows": "1",
        },
        enable_native_fallback=True,
    )
    # [END load_file_example_17]

    # [START load_file_example_18]
    dataframe = aql.load_file(
        task_id="s3_to_dataframe_filetype",
        input_file=File(
            path="s3://astro-sdk/python_sdk/example_dags/data/sample_csv.data", filetype=FileType.CSV
        ),
    )
    # [END load_file_example_18]

    # [START load_file_example_19]
    aql.load_file(
        task_id="gdrive_to_snowflake",
        input_file=File(path="gdrive://test-google-drive-support/sample.csv", conn_id="gdrive_conn"),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
            ),
        ),
    )
    # [END load_file_example_19]

    # [START load_file_example_20]
    aql.load_file(
        task_id="sftp_to_snowflake",
        input_file=File(
            path="sftp://upload/ADOPTION_CENTER_1_unquoted.csv", conn_id="sftp_conn", filetype=FileType.CSV
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
            ),
        ),
    )
    # [END load_file_example_20]

    # [START load_file_example_21]
    aql.load_file(
        task_id="ftp_to_snowflake",
        input_file=File(
            path="ftp://upload/ADOPTION_CENTER_1_unquoted.csv",
            conn_id="ftp_conn",
            filetype=FileType.CSV,
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
            ),
        ),
    )
    # [END load_file_example_21]

    # [START load_file_example_22]
    aql.load_file(
        task_id="s3_to_snowflake_delimiter",
        input_file=File("s3://tmp9/delimiter_dollar.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        use_native_support=False,
        load_options=[PandasLoadOptions(delimiter="$")],
    )
    # [END load_file_example_22]

    # [START load_file_example_23]
    aql.load_file(
        task_id="s3_to_snowflake_native",
        input_file=File("s3://astro-sdk/python_sdk/example_dags/data/sample.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        load_options=[
            SnowflakeLoadOptions(
                file_options={"SKIP_HEADER": 1, "SKIP_BLANK_LINES": True},
                copy_options={"ON_ERROR": "CONTINUE"},
            )
        ],
    )
    # [END load_file_example_23]

    # [START load_file_example_24]
    aql.load_file(
        task_id="s3_to_databricks",
        input_file=File("s3://astro-sdk/python_sdk/example_dags/data/sample.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=DATABRICKS_CONN_ID,
        ),
        load_options=[
            DeltaLoadOptions(
                copy_into_format_options={"header": "true", "inferSchema": "true"},
                copy_into_copy_options={"mergeSchema": "true"},
                load_secrets=True,
            )
        ],
    )
    # [END load_file_example_24]

    # [START load_file_example_25]
    aql.load_file(
        task_id="azure_to_snowflake",
        input_file=File("wasb://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
            ),
        ),
    )
    # [END load_file_example_25]

    # [START load_file_example_26]
    aql.load_file(
        task_id="s3_to_mssql",
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=MSSQL_CONN_ID,
        ),
    )
    # [END load_file_example_26]

    # [START load_file_example_27]
    aql.load_file(
        task_id="s3_to_duckdb",
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=DUCKDB_CONN_ID,
        ),
    )
    # [END load_file_example_27]

    # [START load_file_example_28]
    aql.load_file(
        task_id="s3_to_mysql",
        input_file=File("s3://tmp9/homes_main.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=MYSQL_CONN_ID,
        ),
    )
    # [END load_file_example_28]

    # [START load_file_example_29]
    aql.load_file(
        task_id="s3_to_snowflake_native_with_metadata_columns",
        input_file=File("s3://astro-sdk/python_sdk/example_dags/data/sample.csv", conn_id=AWS_CONN_ID),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        load_options=[
            SnowflakeLoadOptions(
                file_options={"SKIP_HEADER": 1, "SKIP_BLANK_LINES": True},
                copy_options={"ON_ERROR": "CONTINUE"},
                metadata_columns=[
                    "METADATA$FILENAME",
                    "METADATA$FILE_ROW_NUMBER",
                    "METADATA$FILE_CONTENT_KEY",
                    "METADATA$FILE_LAST_MODIFIED",
                    "METADATA$START_SCAN_TIME",
                ],
            )
        ],
    )
    # [END load_file_example_29]

    aql.cleanup()
