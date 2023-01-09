import tempfile
from unittest import mock

from astro.constants import DatabricksLoadMode
from astro.databases.databricks.api_utils import generate_file
from astro.databases.databricks.load_file.load_file_job import _create_load_file_pyspark_file
from astro.databases.databricks.load_options import DeltaLoadOptions
from astro.files import File
from astro.table import Table

expected_basic_file_copy_into = '''from pyspark.sql.functions import input_file_name, current_timestamp

src_data_path = "foobar"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"baz" # This can be generated based on task ID and dag ID or just entirely random
checkpoint_path = f"/tmp/{username}/_checkpoint/{table_name}"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name}")
spark.sql(
    f"""COPY INTO {table_name} FROM '{src_data_path}' FILEFORMAT=CSV
    FORMAT_OPTIONS ('header' = 'true','inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
    """
)
'''

expected_basic_file_autoloader = """from pyspark.sql.functions import input_file_name, current_timestamp

src_data_path = "foobar"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"baz" # This can be generated based on task ID and dag ID or just entirely random
checkpoint_path = f"/tmp/{username}/_checkpoint/{table_name}"
load_options = {
    "cloudFiles.schemaLocation": checkpoint_path,
    "cloudFiles.format": "CSV"
}

write_options = {
    "checkpointLocation": checkpoint_path
}

spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)
(spark.readStream
  .format("cloudFiles")
  .options(**load_options)
  .load(src_data_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .options(**write_options)
  .trigger(once=True)
  .toTable(table_name))
"""


def test_generate_file_autoloader(tmp_path):
    delta_options = DeltaLoadOptions.get_default_delta_options()
    delta_options.load_secrets = False
    output_file = generate_file(
        data_source_path="foobar",
        table_name="baz",
        source_type="s3",
        output_file_path=tmp_path / "example.txt",
        load_options=delta_options,
        file_type="CSV",
    )
    assert output_file.read_text() == expected_basic_file_autoloader


def test_generate_file_copy_into(tmp_path):
    delta_options = DeltaLoadOptions.get_default_delta_options()
    delta_options.load_secrets = False
    delta_options.load_mode = DatabricksLoadMode.COPY_INTO
    output_file = generate_file(
        data_source_path="foobar",
        table_name="baz",
        source_type="s3",
        output_file_path=tmp_path / "example.txt",
        load_options=delta_options,
        file_type="CSV",
    )
    assert output_file.read_text() == expected_basic_file_copy_into


secret_gen_string = """secret_key_list = dbutils.secrets.list("astro-sdk-secrets")
for secret_key in secret_key_list:
    if 'astro_sdk_' in secret_key.key:
        key_name = secret_key.key.replace("astro_sdk_","")
        # We are using print here as we have yet to find a way to make logs surface in the databricks UI
        print(f"setting {key_name}")
        sc._jsc.hadoopConfiguration().set(key_name, dbutils.secrets.get("astro-sdk-secrets", secret_key.key))"""


def test_generate_file_with_secrets(tmp_path):
    options = DeltaLoadOptions.get_default_delta_options()
    output_file = generate_file(
        data_source_path="foobar",
        table_name="baz",
        source_type="s3",
        output_file_path=tmp_path / "example.txt",
        load_options=options,
    )
    assert secret_gen_string in output_file.read_text()


def test_generate_file_without_secrets(tmp_path):
    options = DeltaLoadOptions.get_default_delta_options()
    options.load_secrets = False
    output_file = generate_file(
        data_source_path="foobar",
        table_name="baz",
        source_type="s3",
        output_file_path=tmp_path / "example.txt",
        load_options=options,
    )
    assert secret_gen_string not in output_file.read_text()


def test_generate_file_append_copy_into(tmp_path):
    options = DeltaLoadOptions.get_default_delta_options()
    options.load_secrets = False
    options.if_exists = "append"
    options.load_mode = DatabricksLoadMode.COPY_INTO
    output_file = generate_file(
        data_source_path="foobar",
        table_name="baz",
        source_type="s3",
        output_file_path=tmp_path / "example.txt",
        load_options=options,
    )
    assert "DROP TABLE" not in output_file.read_text()


@mock.patch("astro.databricks.load_file.load_file_job.load_file_to_dbfs", autospec=True)
@mock.patch("databricks_cli.sdk.api_client.ApiClient", autospec=True)
def test_generate_file_overwrite_autoloader(mock_api_client, mock_load_to_dbfs):
    options = DeltaLoadOptions.get_default_delta_options()
    options.load_secrets = False
    options.if_exists = "replace"
    options.load_mode = DatabricksLoadMode.AUTOLOADER
    with tempfile.NamedTemporaryFile(suffix=".py") as tfile:
        _create_load_file_pyspark_file(
            api_client=mock_api_client,
            databricks_options=options,
            dbfs_file_path=None,
            delta_table=Table(),
            input_file=File(path="foo.csv"),
            output_file=tfile,
        )

        output_file_path = mock_load_to_dbfs.mock_calls[0].args[0]
        assert "cloudFiles.allowOverwrites" in output_file_path.read_text()
