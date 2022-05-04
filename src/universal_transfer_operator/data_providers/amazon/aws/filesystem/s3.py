from pathlib import Path
from typing import Optional, Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from boto3.s3.transfer import S3Transfer

from src.universal_transfer_operator.data_providers.base import DataProviders


class S3DataProvider(DataProviders, S3Hook):
    """
    S3DataProvider class is responsible to create s3 connection, read dataset and write dataset.
    It inherits S3Hook and implements DataProviders.
    """

    def read_dataset(self, key: str, bucket_name: Optional[str] = None) -> S3Transfer:
        """
        Reads the dataset from s3 bucket and key and returns a boto3.s3.Object

        :param key: the path to the key
        :param bucket_name: the name of the bucket
        :return: the key object from the bucket
        :rtype: boto3.s3.Object
        """
        return self.get_key(key, bucket_name)

    def write_dataset(
        self,
        filename: Union[Path, str],
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Loads a local file to S3

        :param filename: path to the file to load.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param gzip: If True, the file will be compressed locally
        :param acl_policy: String specifying the canned ACL policy for the file being
            uploaded to the S3 bucket.
        """
        self.load_file(filename, key, bucket_name, replace, encrypt, gzip, acl_policy)
