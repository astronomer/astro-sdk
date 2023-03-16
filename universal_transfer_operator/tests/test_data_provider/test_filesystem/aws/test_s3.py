from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.filesystem.aws.s3 import S3DataProvider
from universal_transfer_operator.datasets.file.base import File


def test_transfer_config_args():
    """Test transfer_config_args property returns {} and not None, since apache-airflow-providers-amazon==3.2.0
    needs {} and not None"""
    dataset = File(path="s3://tmp/test.csv")
    provider = S3DataProvider(dataset=dataset, transfer_mode=TransferMode.NONNATIVE)
    assert provider.transfer_config_args == {}
