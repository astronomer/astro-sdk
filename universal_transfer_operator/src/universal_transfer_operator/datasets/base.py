from __future__ import annotations

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

    :param conn_id: Airflow connection ID
    """

    uri: str = field(init=False)
    extra: dict = field(init=True, factory=dict)

    template_fields = "extra"
