import pandas
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

from astro import sql as aql
from astro.files import File
from astro.table import Metadata, Table
from tests.sql.operators.utils import run_dag


def test_load():
    from astro.spark.autoloader.autoloader_job import load_file_to_delta

    file = File(path="s3://tmp9/databricks-test/", conn_id="s3_conn_benchmark")
    delta_table = Table(conn_id="my_databricks_conn")
    load_file_to_delta(file, delta_table)
    hook = DatabricksSqlHook("my_databricks_conn")
    _, res = hook.run(f"SELECT * FROM {delta_table.name}", handler=lambda x: x.fetchall())
    print(res)


def test_aql_load(sample_dag):
    file = File(path="s3://tmp9/databricks-test/", conn_id="s3_conn_benchmark")
    delta_table = Table(conn_id="my_databricks_conn", metadata=Metadata(schema="default"))

    @aql.dataframe
    def validate(df: pandas.DataFrame):
        assert len(df) == 26

    with sample_dag:
        delta_t = aql.load_file(input_file=file, output_table=delta_table)
        validate(delta_t)
    run_dag(sample_dag)
    hook = DatabricksSqlHook("my_databricks_conn")
    _, res = hook.run(
        f"SELECT * FROM {delta_table.name}", handler=lambda cur: cur.fetchall_arrow().to_pandas()
    )
    print(res)
    assert len(res) == 26


def test_aql_transform(sample_dag):
    file = File(path="s3://tmp9/databricks-test/", conn_id="s3_conn_benchmark")
    delta_table = Table(conn_id="my_databricks_conn", metadata=Metadata(schema="default", database="default"))

    @aql.dataframe
    def validate(df: pandas.DataFrame):
        print(df)
        assert len(df) == 26

    @aql.transform
    def get_all(table: Table):
        return "SELECT * FROM {{table}}"

    with sample_dag:
        delta_t = aql.load_file(input_file=file, output_table=delta_table)
        transformed_table = get_all(delta_t)
        validate(transformed_table)
        aql.cleanup()
    run_dag(sample_dag)
