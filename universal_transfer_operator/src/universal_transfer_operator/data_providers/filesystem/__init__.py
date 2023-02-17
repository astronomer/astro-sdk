from __future__ import annotations

from universal_transfer_operator.constants import FileType, TransferMode
from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.utils import TransferParameters


def resolve_file_path_pattern(
    file: File,
    filetype: FileType | None = None,
    normalize_config: dict | None = None,
    transfer_params: TransferParameters = None,
    transfer_mode: TransferMode = TransferMode.NONNATIVE,
) -> list[File]:
    """get file objects by resolving path_pattern from local/object stores
    path_pattern can be
    1. local location - glob pattern
    2. s3/gcs location - prefix

    :param file: File dataset object
    :param filetype: constant to provide an explicit file type
    :param normalize_config: parameters in dict format of pandas json_normalize() function
    :param transfer_params: kwargs to be used by method involved in transfer flow.
    :param transfer_mode: Use transfer_mode TransferMode; native, non-native or thirdparty.
    """
    location = create_dataprovider(
        dataset=file,
        transfer_params=transfer_params,
        transfer_mode=transfer_mode,
    )
    files = []
    for path in location.paths:
        if not path.endswith("/"):
            file = File(
                path=path,
                conn_id=file.conn_id,
                filetype=filetype,
                normalize_config=normalize_config,
            )
            files.append(file)
    if len(files) == 0:
        raise FileNotFoundError(f"File(s) not found for path/pattern '{file.path}'")
    return files
