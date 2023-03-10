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
        {"name": "S3DataProvider", "object": File(path=f"s3://tmp9/{create_unique_str(10)}")},
        {
            "name": "GCSDataProvider",
            "object": File(path=f"gs://uto-test/{create_unique_str(10)}"),
        },
        {"name": "LocalDataProvider", "object": File(path=f"/tmp/{create_unique_str(10)}")},
        {"name": "SFTPDataProvider", "object": File(path=f"sftp://upload/{create_unique_str(10)}")},
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
def test_read_write_methods_of_datasets(src_dataset_fixture, dst_dataset_fixture):
    """
    Test datasets read and write methods of all datasets
    """
    src_dp, src_dataset = src_dataset_fixture
    dst_dp, dst_dataset = dst_dataset_fixture
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
