import pathlib

import pandas as pd
from airflow.providers.sftp.hooks.sftp import SFTPHook
from utils.test_utils import create_unique_str

from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.data_providers.base import FileStream
from universal_transfer_operator.datasets.file.base import File

CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"


def upload_file_to_sftp_server(conn_id: str, local_path: str, remote_path: str):
    sftp = SFTPHook(ssh_conn_id=conn_id)
    sftp.store_file(remote_full_path=remote_path, local_full_path=local_path)


def test_sftp_read():
    """
    Test to validate working of SFTPDataProvider.read() method
    """
    filepath = DATA_DIR + "sample.csv"
    remote_path = f"/upload/{create_unique_str(10)}.csv"
    upload_file_to_sftp_server(conn_id="sftp_conn", local_path=filepath, remote_path=remote_path)

    dataprovider = create_dataprovider(dataset=File(path=f"sftp:/{remote_path}", conn_id="sftp_conn"))
    iterator_obj = dataprovider.read()
    source_data = iterator_obj.__next__()

    sftp_df = pd.read_csv(source_data.remote_obj_buffer)
    true_df = pd.read_csv(filepath)
    assert sftp_df.equals(true_df)


def download_file_from_sftp(conn_id: str, local_path: str, remote_path: str):
    sftp = SFTPHook(ssh_conn_id=conn_id)
    sftp.retrieve_file(
        local_full_path=local_path,
        remote_full_path=remote_path,
    )


def test_sftp_write():
    """
    Test to validate working of SFTPDataProvider.write() method
    """
    local_filepath = DATA_DIR + "sample.csv"
    file_name = f"{create_unique_str(10)}.csv"
    remote_filepath = f"sftp://upload/{file_name}"

    dataprovider = create_dataprovider(dataset=File(path=remote_filepath, conn_id="sftp_conn"))
    fs = FileStream(
        remote_obj_buffer=open(local_filepath),
        actual_filename=local_filepath,
        actual_file=File(local_filepath),
    )
    dataprovider.write(source_ref=fs)

    downloaded_file = f"/tmp/{file_name}"
    download_file_from_sftp(
        conn_id="sftp_conn", local_path=downloaded_file, remote_path=f"{remote_filepath.split('sftp:/')[1]}"
    )

    sftp_df = pd.read_csv(downloaded_file)
    true_df = pd.read_csv(local_filepath)
    assert sftp_df.equals(true_df)
