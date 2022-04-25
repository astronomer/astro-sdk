import importlib
from pathlib import Path
from typing import Dict, Optional

from astro.constants import FileLocation
from astro.files.locations.base import Location
from astro.utils.path import get_dict_with_module_names_to_dot_notations

DEFAULT_CONN_TYPE_TO_MODULE_PATH = get_dict_with_module_names_to_dot_notations(
    Path(__file__)
)
DEFAULT_CONN_TYPE_TO_MODULE_PATH["https"] = DEFAULT_CONN_TYPE_TO_MODULE_PATH["http"]
DEFAULT_CONN_TYPE_TO_MODULE_PATH["gs"] = DEFAULT_CONN_TYPE_TO_MODULE_PATH["gcs"]


def location_factory(path: str, conn_id: Optional[str] = None) -> Location:
    """Location factory method to generate location class
    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """
    location_to_class: Dict[FileLocation, str] = {
        FileLocation.LOCAL: "Local",
        FileLocation.S3: "S3",
        FileLocation.GS: "GCS",
        FileLocation.HTTP: "Http",
        FileLocation.HTTPS: "Http",
    }
    filetype: FileLocation = Location.get_location_type(path)
    module_path = DEFAULT_CONN_TYPE_TO_MODULE_PATH[filetype.value]
    module_ref = importlib.import_module(module_path)
    return getattr(module_ref, location_to_class[filetype])(path, conn_id)
