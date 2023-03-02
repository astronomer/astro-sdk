import pytest

from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.data_providers.database.sqlite import SqliteDataProvider
from universal_transfer_operator.data_providers.filesystem.aws.s3 import S3DataProvider
from universal_transfer_operator.data_providers.filesystem.google.cloud.gcs import GCSDataProvider
from universal_transfer_operator.data_providers.filesystem.sftp import SFTPDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table


@pytest.mark.parametrize(
    "datasets",
    [
        {"dataset": File("s3://astro-sdk-test/uto/", conn_id="aws_default"), "expected": S3DataProvider},
        {"dataset": File("gs://uto-test/uto/", conn_id="google_cloud_default"), "expected": GCSDataProvider},
        {"dataset": File("sftp://upload/sample.csv", conn_id="sftp_default"), "expected": SFTPDataProvider},
        {"dataset": Table("some_table", conn_id="sqlite_default"), "expected": SqliteDataProvider},
    ],
    ids=lambda d: d["dataset"].conn_id,
)
def test_create_dataprovider(datasets):
    """Test that the correct data-provider is created for a dataset"""
    data_provider = create_dataprovider(dataset=datasets["dataset"])
    assert isinstance(data_provider, datasets["expected"])
