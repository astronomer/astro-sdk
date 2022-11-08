from astro.files import File
from astro.spark.table import DeltaTable


def test_load():
    from astro.spark.autoloader.autoloader_job import load_file_to_delta

    file = File(path="s3a://tmp9/databricks-test/", conn_id="s3_conn_benchmark")
    load_file_to_delta(file, DeltaTable(conn_id="my_delta_conn"))
