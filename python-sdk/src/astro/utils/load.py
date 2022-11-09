import tempfile
from typing import Optional

import smart_open


def copy_remote_file_to_local(
    source_filepath: str,
    target_filepath: Optional[str] = None,
    is_binary: bool = False,
    transport_params: Optional[dict] = None,
) -> str:
    """
    Copy the contents of a file (which may be available locally or remotely) to a local file.
    If no target_filepath is specified, creates one, and returns it.

    :param source_filepath: Local filepath or remote URI of the source file
    :param target_filepath: (optional) Destination filepath in the local filesystem
    :param is_binary: If the given file is binary or not
    :param transport_params: Necessary parameters to connect to object store, in case the file is in (S3, GCS)
    :type source_filepath: str
    :type target_filepath: str
    :type is_binary: bool
    :type transport_params: dict
    :return: Target file path
    :rtype: str
    """
    # TODO: if the file is too big (e.g. larger than the available disk) we should change this to be a generator and
    # chunk the original file into smaller pieces

    read_mode = "rb" if is_binary else "r"
    write_mode = "wb" if is_binary else "w"
    if target_filepath is None:
        tmp_file = tempfile.NamedTemporaryFile(mode=write_mode, delete=False)
        target_filepath = tmp_file.name

    with open(target_filepath, write_mode) as fp_out, smart_open.open(
        source_filepath, mode=read_mode, transport_params=transport_params
    ) as fp_in:
        content = fp_in.read()
        fp_out.write(content)

    return target_filepath
