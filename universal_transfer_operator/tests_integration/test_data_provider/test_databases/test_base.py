import pathlib
from urllib.parse import urlparse, urlunparse

import pandas as pd
import pytest
import smart_open
from pyarrow.lib import ArrowInvalid
from utils.test_utils import create_unique_str

from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {"name": "SqliteDataProvider", "local_file_path": f"{str(CWD)}/../../data/sample.csv"},
        {
            "name": "SnowflakeDataProvider",
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "BigqueryDataProvider",
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "S3DataProvider",
            "object": File(path=f"s3://tmp9/{create_unique_str(10)}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "GCSDataProvider",
            "object": File(path=f"gs://uto-test/{create_unique_str(10)}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "LocalDataProvider",
            "object": File(path=f"/tmp/{create_unique_str(10)}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "SFTPDataProvider",
            "object": File(path=f"sftp://upload/{create_unique_str(10)}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {
            "name": "SqliteDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
        {
            "name": "SnowflakeDataProvider",
        },
        {"name": "S3DataProvider", "object": File(path=f"s3://tmp9/{create_unique_str(10)}.csv")},
        {
            "name": "GCSDataProvider",
            "object": File(path=f"gs://uto-test/{create_unique_str(10)}.csv"),
        },
        {"name": "LocalDataProvider", "object": File(path=f"/tmp/{create_unique_str(10)}.csv")},
        {"name": "SFTPDataProvider", "object": File(path=f"sftp://upload/{create_unique_str(10)}.csv")},
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
def test_read_write_methods_of_datasets(src_dataset_fixture, dst_dataset_fixture):
    """
    Test datasets read and write methods of all datasets
    """
    src_dp, _ = src_dataset_fixture
    dst_dp, _ = dst_dataset_fixture
    for source_data in src_dp.read():
        dst_dp.write(source_data)
    output_df = export_to_dataframe(dst_dp)
    input_df = pd.read_csv(f"{str(CWD)}/../../data/sample.csv")

    assert output_df.equals(input_df)


def export_to_dataframe(data_provider) -> pd.DataFrame:
    """Read file from all supported location and convert them into dataframes."""
    if isinstance(data_provider.dataset, File):
        path = data_provider.dataset.path
        # Currently, we are passing the credentials to sftp server via URL - sftp://username:password@localhost, we are
        # populating the credentials in the URL if the server destination is SFTP.
        if data_provider.dataset.path.startswith("sftp://"):
            path = get_complete_url(data_provider)
        try:
            # Currently, there is a limitation, when we are saving data of a table in a file, we choose rhe parquet
            # format, when moving this saved file to another filetype location(like s3/gcs/local) we are not able to
            # change the data format, because of this case when validating if the source is a database and
            # destination is a filetype, we need to check for parquet format, for other cases like -
            # database -> database, filesystem -> database and filesystem -> filesystem it works as expected.
            with smart_open.open(path, mode="rb", transport_params=data_provider.transport_params) as stream:
                return pd.read_parquet(stream)
        except ArrowInvalid:
            with smart_open.open(path, mode="r", transport_params=data_provider.transport_params) as stream:
                return pd.read_csv(stream)
    elif isinstance(data_provider.dataset, Table):
        return data_provider.export_table_to_pandas_dataframe()


def get_complete_url(dataset_provider):
    """
    Add sftp credential to url
    """
    path = dataset_provider.dataset.path
    original_url = urlparse(path)
    cred_url = urlparse(dataset_provider.get_uri())
    url_netloc = f"{cred_url.netloc}/{original_url.netloc}"
    url_path = original_url.path
    cred_url = cred_url._replace(netloc=url_netloc, path=url_path)
    path = urlunparse(cred_url)
    return path


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {"name": "SnowflakeDataProvider"},
        {"name": "BigqueryDataProvider"},
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_load_pandas_dataframe_to_table_with_replace(dst_dataset_fixture):
    """Load Pandas Dataframe to a SQL table with replace strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2, 3]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 3
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    pandas_dataframe = pd.DataFrame(data={"id": [3, 4]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (3,)
    assert rows[1] == (4,)

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_load_pandas_dataframe_to_table_with_append(dst_dataset_fixture):
    """Load Pandas Dataframe to a SQL table with append strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
        if_exists="append",
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
        if_exists="append",
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 4
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.drop_table(dataset)
