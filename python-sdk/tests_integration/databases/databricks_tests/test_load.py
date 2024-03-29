import pathlib

import pytest

from astro.constants import Database, FileType
from astro.files import File

CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_autoloader_load_file_local(database_table_fixture):
    filepath = str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=File(filepath),
        output_table=table,
        databricks_job_name="test_local_local",
    )
    assert database.table_exists(table)
    df = database.export_table_to_pandas_dataframe(table)
    assert not df.empty
    assert len(df) == 3
    assert df.columns.to_list() == ["id", "name"]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_autoloader_load_file_s3(database_table_fixture):
    file = File("s3://tmp9/databricks-test/", conn_id="aws_default", filetype=FileType.CSV)
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=file,
        output_table=table,
    )
    assert database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_delta_load_file_gcs(database_table_fixture):
    file = File(
        "gs://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.csv",
        conn_id="google_cloud_default",
        filetype=FileType.CSV,
    )
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=file,
        output_table=table,
    )
    assert database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [
        {"provider": "azure", "file_create": True},
    ],
    indirect=True,
    ids=["azure_blob_storage"],
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_delta_load_file_azure_wasb(database_table_fixture, remote_files_fixture):
    file_ = File(path=remote_files_fixture[0], conn_id="wasb_conn_with_access_key")
    database, table = database_table_fixture
    assert file_.exists()
    assert not database.table_exists(table)
    database.load_file_to_table(
        input_file=file_,
        output_table=table,
    )
    assert database.table_exists(table)
    database.drop_table(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_delta_load_file_gcs_autoloader(database_table_fixture):
    file = File(
        "gs://astro-sdk/benchmark/trimmed/covid_overview/",
        conn_id="google_cloud_default",
        filetype=FileType.CSV,
    )
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=file,
        output_table=table,
    )
    assert database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_delta_load_file_gcs_default_connection(database_table_fixture):
    file = File(
        "gs://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.csv",
        conn_id="google_cloud_default",
        filetype=FileType.CSV,
    )
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=file,
        output_table=table,
    )
    assert database.table_exists(table)
