import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_amazon_s3_postgres",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


with dag:
    # Loading data from S3 to Bigquery
    s3_to_gcs_op = S3ToGCSOperator(
        aws_conn_id="awd_default",
        task_id="s3_to_gcs_example",
        bucket="my-s3-bucket",
        prefix="data.csv",
        gcp_conn_id="google_cloud_default",
        dest_gcs="gs://my-gcs-bucket/some/customers/data.csv",
        replace=False,
        gzip=True,
        dag=dag,
    )
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example",
        bucket="my-gcs-bucket",
        source_objects=["gs://my-gcs-bucket/some/customers/data.csv"],
        destination_project_dataset_table="dummy_project.customers.info",
        write_disposition="WRITE_TRUNCATE",
    )

    # Clean up
    gcs_delete_object = GCSDeleteObjectsOperator(
        bucket_name="my-gcs-bucket",
        objects=["gs://my-gcs-bucket/some/customers/data.csv"],
    )

    s3_to_gcs_op >> gcs_to_bigquery >> gcs_delete_object

    # Transform
    create_empty_table = BigQueryCreateEmptyTableOperator(
        table_id="temp_customer_data",
        dataset_id="customer",
        project_id="dummy_project",
        schema_fields=["id", "name", "onboardDate"],
    )
    transform_data = BigQueryExecuteQueryOperator(
        sql="SELECT * FROM dummy_project.customer.info where onboardDate > 2020-1-1",
        destination_dataset_table="dummy_project.customer.temp_customer_data",
    )

    # Clean up
    bigquery_delete_table = BigQueryDeleteTableOperator(
        gcp_conn_id="google_cloud_default",
        deletion_dataset_table="dummy_project.customers.info",
    )

    create_empty_table >> transform_data >> bigquery_delete_table

    # Export transformed data to S3
    bq_to_gcs = BigQueryToGCSOperator(
        source_project_dataset_table="dummy_project.customer.temp_customer_data",
        destination_cloud_storage_uris=[
            "gs://my-gcs-bucket/some/customers/transformed_data.csv"
        ],
    )

    # Clean up
    gcs_delete_transformed_table = BigQueryDeleteTableOperator(
        gcp_conn_id="google_cloud_default",
        deletion_dataset_table="dummy_project.customer.temp_customer_data",
    )

    bq_to_gcs >> gcs_delete_transformed_table
