from typing import Any

from airflow.providers.ftp.hooks.ftp import FTPHook

from src.universal_transfer_operator.data_providers.base import DataProviders


class FTPDataProvider(DataProviders, FTPHook):
    """
    FTPDataProvider class is responsible to create FTP connection, read dataset and write dataset.
    It inherits FTPHook and implements DataProviders.
    """

    def read_dataset(
        self,
        remote_full_path: str,
        local_full_path_or_buffer: str,
        callback: Any = None,
    ) -> Any:
        """
        Transfers the remote file to a local location.

        If local_full_path_or_buffer is a string path, the file will be put
        at that location; if it is a file-like buffer, the file will
        be written to the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        :param callback: callback which is called each time a block of data
            is read. if you do not use a callback, these blocks will be written
            to the file or buffer passed in. if you do pass in a callback, note
            that writing to a file or buffer will need to be handled inside the
            callback.
            [default: output_handle.write()]

        .. code-block:: python

            hook = FTPHook(ftp_conn_id="my_conn")

            remote_path = "/path/to/remote/file"
            local_path = "/path/to/local/file"

            # with a custom callback (in this case displaying progress on each read)
            def print_progress(percent_progress):
                self.log.info("Percent Downloaded: %s%%" % percent_progress)


            total_downloaded = 0
            total_file_size = hook.get_size(remote_path)
            output_handle = open(local_path, "wb")


            def write_to_file_with_progress(data):
                total_downloaded += len(data)
                output_handle.write(data)
                percent_progress = (total_downloaded / total_file_size) * 100
                print_progress(percent_progress)


            hook.retrieve_file(remote_path, None, callback=write_to_file_with_progress)

            # without a custom callback data is written to the local_path
            hook.retrieve_file(remote_path, local_path)
        """
        return self.retrieve_file(remote_full_path, local_full_path_or_buffer, callback)

    def write_dataset(
        self, remote_full_path: str, local_full_path_or_buffer: Any
    ) -> None:
        """
        Transfers a local file to the remote location.

        If local_full_path_or_buffer is a string path, the file will be read
        from that location; if it is a file-like buffer, the file will
        be read from the buffer but not closed.

        :param remote_full_path: full path to the remote file
        :param local_full_path_or_buffer: full path to the local file or a
            file-like buffer
        """
        self.store_file(remote_full_path, local_full_path_or_buffer)
