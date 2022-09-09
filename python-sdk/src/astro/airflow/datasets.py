try:
    # Airflow >= 2.4
    from airflow.datasets import Dataset

    DATASET_SUPPORT = True
except ImportError:
    # Airflow < 2.4
    Dataset = object
    DATASET_SUPPORT = False
