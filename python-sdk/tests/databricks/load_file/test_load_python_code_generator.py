import tempfile
from pathlib import Path

from astro.databricks.api_utils import generate_file
from astro.databricks.load_options import DeltaLoadOptions

expected_basic_file = '''import logging
from pyspark.sql.functions import input_file_name, current_timestamp
logger = logging.getLogger("broadcast")

src_data_path = "foobar"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"baz" # This can be generated based on task ID and dag ID or just entirely random
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_3_quickstart"



spark.sql(f"DROP TABLE IF EXISTS {table_name}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name}")
spark.sql(
    f"""COPY INTO {table_name} FROM '{src_data_path}' FILEFORMAT=CSV
    FORMAT_OPTIONS ('header' = 'true','inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
    """
)
'''


def test_generate_file_basic():
    with tempfile.NamedTemporaryFile() as tfile:
        delta_options = DeltaLoadOptions.get_default_delta_options()
        delta_options.load_secrets = False
        output_file = generate_file(
            data_source_path="foobar",
            table_name="baz",
            source_type="s3",
            output_file_path=Path(tfile.name),
            load_options=delta_options,
            file_type="CSV",
        )
        with open(output_file) as file:
            file_string = file.read()
            assert file_string == expected_basic_file


secret_gen_string = """secret_key_list = dbutils.secrets.list("astro-sdk-secrets")
for secret_key in secret_key_list:
    if 'astro_sdk_' in secret_key.key:
        key_name = secret_key.key.replace("astro_sdk_","")
        logger.info(f"setting {key_name}")
        sc._jsc.hadoopConfiguration().set(key_name, dbutils.secrets.get("astro-sdk-secrets", secret_key.key))"""


def test_generate_file_with_secrets():
    with tempfile.NamedTemporaryFile() as tfile:
        options = DeltaLoadOptions.get_default_delta_options()
        output_file = generate_file(
            data_source_path="foobar",
            table_name="baz",
            source_type="s3",
            output_file_path=Path(tfile.name),
            load_options=options,
        )
        with open(output_file) as file:
            file_string = file.read()
            assert secret_gen_string in file_string


def test_generate_file_without_secrets():
    with tempfile.NamedTemporaryFile() as tfile:
        options = DeltaLoadOptions.get_default_delta_options()
        options.load_secrets = False
        output_file = generate_file(
            data_source_path="foobar",
            table_name="baz",
            source_type="s3",
            output_file_path=Path(tfile.name),
            load_options=options,
        )
        with open(output_file) as file:
            file_string = file.read()
            assert secret_gen_string not in file_string
