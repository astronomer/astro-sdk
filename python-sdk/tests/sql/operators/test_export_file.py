"""
Unittest module to test Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=KEY \
    AWS_SECRET_ACCESS_KEY=SECRET \
    python3 -m unittest tests.operators.test_export_file.TestSaveFile.test_save_postgres_table_to_local

"""
import os
import pathlib
import tempfile
from pathlib import Path

import astro.sql as aql
import boto3
import pandas as pd
import pytest
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import SUPPORTED_FILE_TYPES, Database
from astro.files import File
from astro.settings import SCHEMA

# Import Operator
from astro.sql.operators.export_file import export_file
from astro.sql.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


def s3fs_creds():
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    # To-do: clean-up how S3 creds are passed to s3fs

    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }


def test_save_dataframe_to_local(sample_dag):
    @aql.dataframe
    def make_df():
        return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    with sample_dag:
        df = make_df()
        aql.export_file(
            input_data=df,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = pd.read_csv("/tmp/saved_df.csv")
    assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))


@pytest.mark.parametrize(
    "database_table_fixture", [{"database": Database.SQLITE}], indirect=True
)
def test_save_temp_table_to_local(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture
    data_path = str(CWD) + "/../../data/homes.csv"
    with sample_dag:
        table = aql.load_file(input_file=File(path=data_path), output_table=test_table)
        aql.export_file(
            input_data=table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize(
    "database_table_fixture", [{"database": Database.SQLITE}], indirect=True
)
def test_save_returns_output_file(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert not df.empty

    data_path = str(CWD) + "/../../data/homes.csv"
    with sample_dag:
        table = aql.load_file(input_file=File(path=data_path), output_table=test_table)
        file = aql.export_file(
            input_data=table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
        res_df = aql.load_file(input_file=file)
        validate(res_df)
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_save_all_db_tables_to_s3(sample_dag, database_table_fixture):
    _creds = s3fs_creds()
    file_name = f"{test_utils.get_table_name('test_save')}.csv"

    output_file_path = f"s3://tmp9/{file_name}"

    db, test_table = database_table_fixture
    with sample_dag:
        export_file(
            input_data=test_table,
            output_file=File(path=output_file_path, conn_id="aws_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    # # Read output CSV
    df_file = pd.read_csv(output_file_path, storage_options=s3fs_creds())

    assert len(df_file) == 47
    assert (df["sell"] == df_file["sell"]).all()

    # Delete object from S3
    s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
    s3.Object("tmp9", file_name).delete()


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_save_all_db_tables_to_gcs(sample_dag, database_table_fixture):
    database, test_table = database_table_fixture
    file_name = f"{test_utils.get_table_name('test_save')}.csv"
    bucket = "dag-authoring"

    output_file_path = f"gs://{bucket}/test/{file_name}"

    with sample_dag:
        export_file(
            input_data=test_table,
            output_file=File(path=output_file_path, conn_id="google_cloud_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)
    df = database.export_table_to_pandas_dataframe(source_table=test_table)

    assert (df["sell"].sort_values() == [129, 138, 142, 175, 232]).all()

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.delete(bucket, f"test/{file_name}")


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_save_all_db_tables_to_local_file_exists_overwrite_false(
    sample_dag, database_table_fixture
):
    _, test_table = database_table_fixture
    with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file, pytest.raises(
        FileExistsError
    ):
        with sample_dag:
            export_file(
                input_data=test_table,
                output_file=File(path=temp_file.name),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google", "amazon"],
)
def test_save_table_remote_file_exists_overwrite_false(
    sample_dag, database_table_fixture, remote_files_fixture
):
    _, test_table = database_table_fixture
    with pytest.raises(FileExistsError):
        with sample_dag:
            export_file(
                input_data=test_table,
                output_file=File(path=remote_files_fixture[0], conn_id="aws_default"),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
    ],
    indirect=True,
)
def test_unique_task_id_for_same_path(
    sample_dag,
    database_table_fixture,
):
    _, test_table = database_table_fixture
    file_name = f"{test_utils.get_table_name('output')}.csv"
    OUTPUT_FILE_PATH = str(CWD) + f"/../../data/{file_name}"

    tasks = []
    with sample_dag:
        for i in range(4):
            params = {
                "input_data": test_table,
                "output_file": File(path=OUTPUT_FILE_PATH),
                "if_exists": "replace",
            }

            if i == 3:
                params["task_id"] = "task_id"
            task = export_file(**params)
            tasks.append(task)
    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[0].operator.task_id == "export_file"
    assert tasks[1].operator.task_id == "export_file__1"
    assert tasks[2].operator.task_id == "export_file__2"
    assert tasks[3].operator.task_id == "task_id"

    os.remove(OUTPUT_FILE_PATH)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
def test_export_file(sample_dag, database_table_fixture, file_type):
    _, test_table = database_table_fixture
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = Path(tmp_dir, f"sample.{file_type}")
        with sample_dag:
            export_file(
                input_data=test_table,
                output_file=File(path=str(filepath)),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)

        df = test_utils.load_to_dataframe(filepath, file_type)
        assert len(df) == 3
        expected = pd.DataFrame(
            [
                {"id": 1, "name": "First"},
                {"id": 2, "name": "Second"},
                {"id": 3, "name": "Third with unicode पांचाल"},
            ]
        )
        assert df.rename(columns=str.lower).equals(expected)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["postgresql", "redshift"],
)
def test_populate_table_metadata(sample_dag, database_table_fixture):
    """
    Test default populating of table fields in export_file op.
    """
    _, test_table = database_table_fixture
    test_table.metadata.schema = None

    @aql.dataframe
    def validate(table: Table):
        assert table.metadata.schema == SCHEMA

    with sample_dag:
        aql.export_file(
            input_data=test_table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
        validate(test_table)

    test_utils.run_dag(sample_dag)


@pytest.mark.skipif(
    not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    input_data = Table("test_name")
    output_file = File("gs://bucket/object.csv")
    task = aql.export_file(
        input_data=input_data,
        output_file=output_file,
    )
    assert task.operator.inlets == [input_data]
    assert task.operator.outlets == [output_file]


@pytest.mark.skipif(
    DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    input_data = Table("test_name")
    output_file = File("gs://bucket/object.csv")
    task = aql.export_file(
        input_data=input_data,
        output_file=output_file,
    )
    assert task.operator.inlets == []
    assert task.operator.outlets == []
