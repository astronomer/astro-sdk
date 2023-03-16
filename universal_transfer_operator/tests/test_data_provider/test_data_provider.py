import pytest

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.data_providers.database.snowflake import SnowflakeDataProvider
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
        {"dataset": Table("some_table", conn_id="snowflake_conn"), "expected": SnowflakeDataProvider},
    ],
    ids=lambda d: d["dataset"].conn_id,
)
def test_create_dataprovider(datasets):
    """Test that the correct data-provider is created for a dataset"""
    data_provider = create_dataprovider(dataset=datasets["dataset"])
    assert isinstance(data_provider, datasets["expected"])


def test_raising_of_NotImplementedError():
    """
    Test that the class inheriting from DataProviders should implement methods
    """

    class Test(DataProviders):
        pass

    methods = [
        "check_if_exists",
        "read",
        "openlineage_dataset_namespace",
        "openlineage_dataset_name",
        "populate_metadata",
    ]

    test = Test(dataset=File("/tmp/test.csv"), transfer_mode=TransferMode.NONNATIVE)
    for method in methods:
        with pytest.raises(NotImplementedError):
            m = test.__getattribute__(method)
            m()

    with pytest.raises(NotImplementedError):
        test.hook


def test_openlineage_dataset_uri():
    """
    Test openlineage_dataset_uri is creating the correct uri
    """

    class Test(DataProviders):
        @property
        def openlineage_dataset_name(self):
            return "/rest/v1/get_user"

        @property
        def openlineage_dataset_namespace(self):
            return "http://localhost:9900"

    test = Test(dataset=File("/tmp/test.csv"), transfer_mode=TransferMode.NONNATIVE)
    assert test.openlineage_dataset_uri == "http://localhost:9900/rest/v1/get_user"
