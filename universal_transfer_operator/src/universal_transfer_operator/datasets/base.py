from __future__ import annotations

try:
    # Airflow >= 2.4
    from airflow.datasets import Dataset

    DATASET_SUPPORT = True
except ImportError:
    # Airflow < 2.4
    Dataset = object  # type: ignore
    DATASET_SUPPORT = False
