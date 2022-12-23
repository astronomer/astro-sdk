from __future__ import annotations

from urllib.parse import urlparse

from airflow.utils.log.logging_mixin import LoggingMixin
from attr import define, field

try:
    # Airflow >= 2.4
    from airflow.datasets import Dataset

    DATASET_SUPPORT = True
except ImportError:
    # Airflow < 2.4
    Dataset = object
    DATASET_SUPPORT = False


@define
class UniversalDataset(LoggingMixin, Dataset):
    """
    Repersents all file dataset, and abstract away the details related to location and file types.
    Intended to be used within library.

    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """

    path: str
    conn_id: str
    uri: str = field(init=False)
    extra: dict = field(init=True, factory=dict)

    template_fields = ("path", "conn_id", "extra")

    def dataset_scheme(self):
        """
        Return the scheme based on path
        """
        parsed = urlparse(self.path)
        return parsed.scheme

    def dataset_namespace(self):
        """
        The namespace of a dataset can be combined to form a URI (scheme:[//authority]path)

        Namespace = scheme:[//authority] (the dataset)
        """
        parsed = urlparse(self.path)
        namespace = f"{self.dataset_scheme()}://{parsed.netloc}"
        return namespace

    def dataset_name(self):
        """
        The name of a dataset can be combined to form a URI (scheme:[//authority]path)

        Name = path (the datasets)
        """
        parsed = urlparse(self.path)
        return parsed.path
