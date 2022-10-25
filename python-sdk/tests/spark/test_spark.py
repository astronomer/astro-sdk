import pandas
import pyspark.sql.dataframe

from astro.spark.load_to_delta import load_to_spark_df
from astro.files import File
from astro.table import Table
from astro import sql as aql
from astro.spark.table import DeltaTable
from astro.spark.delta import DeltaDatabase
import tests.sql.operators.utils as test_utils
from astro.spark.table import DeltaTable as AstroDeltaTable
from pyspark.sql.dataframe import DataFrame as SparkDataframe
# from delta.tables import *


@aql.transform()
def combine_data(center_1: Table):
    return """SELECT * FROM {{center_1}}"""


@aql.dataframe()
def spark_df_func(df: pandas.DataFrame):
    df.show()
    return df


def test_aql_load(sample_dag):
    from astro import sql as aql
    file = File(path="s3a://tmp9/ADOPTION_CENTER_2_unquoted.csv", conn_id="s3_conn_benchmark")
    delta_table = AstroDeltaTable(path="/tmp/delta-table-airflow", conn_id="my_delta_conn")
    with sample_dag:
        a = aql.load_file(input_file=file, output_table=delta_table)
        b = spark_df_func(a)
        c = spark_df_func(b)
    test_utils.run_dag(dag=sample_dag)


def test_file():
    file = File(path="s3a://tmp9/ADOPTION_CENTER_2_unquoted.csv", conn_id="s3_conn_benchmark")
    df = load_to_spark_df(file)
    df.show()
    df.agg({'age': 'sum'}).show()


def test_load():
    file = File(path="s3a://tmp9/ADOPTION_CENTER_2_unquoted.csv", conn_id="s3_conn_benchmark")
    delta_table = DeltaTable(path="/tmp/delta-table-airflow")
    db = DeltaDatabase(conn_id="foo")
    db.load_file_to_table(input_file=file, output_table=delta_table)


