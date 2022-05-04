import importlib
from pathlib import Path
from typing import Optional

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation
from astro.utils.path import get_class_name, get_dict_with_module_names_to_dot_notations

DEFAULT_CONN_TYPE_TO_MODULE_PATH = get_dict_with_module_names_to_dot_notations(
    Path(__file__)
)
DEFAULT_CONN_TYPE_TO_MODULE_PATH["https"] = DEFAULT_CONN_TYPE_TO_MODULE_PATH["http"]
DEFAULT_CONN_TYPE_TO_MODULE_PATH["gs"] = DEFAULT_CONN_TYPE_TO_MODULE_PATH["gcs"]


def create_file_location(path: str, conn_id: Optional[str] = None) -> BaseFileLocation:
    """
    Location factory method to generate location class
    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """
    filetype: FileLocation = BaseFileLocation.get_location_type(path)
    module_path = DEFAULT_CONN_TYPE_TO_MODULE_PATH[filetype.value]
    module_ref = importlib.import_module(module_path)
    class_name = get_class_name(module_ref)
    location: BaseFileLocation = getattr(module_ref, class_name)(path, conn_id)
    return location
