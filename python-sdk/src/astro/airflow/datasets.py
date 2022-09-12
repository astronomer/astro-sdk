from __future__ import annotations

try:
    # Airflow >= 2.4
    from airflow.datasets import Dataset

    DATASET_SUPPORT = True
except ImportError:
    # Airflow < 2.4
    Dataset = object
    DATASET_SUPPORT = False


def kwargs_with_datasets(
    kwargs: dict | None = None,
    input_datasets: list[Dataset] | Dataset | None = None,
    output_datasets: list[Dataset] | Dataset | None = None,
):
    """
    Extract inlets and outlets from kwargs if users have passed it. If not, set input datasets as inlets and
    set output dataset as outlets
    """
    kwargs = kwargs or {}
    if "inlets" in kwargs or DATASET_SUPPORT and input_datasets:
        inlets = kwargs.get("inlets", input_datasets)
        inlets = inlets if isinstance(inlets, list) else [inlets]
        kwargs.update({"inlets": inlets})

    if "outlets" in kwargs or DATASET_SUPPORT and output_datasets:
        outlets = kwargs.get("outlets", output_datasets)
        outlets = outlets if isinstance(outlets, list) else [outlets]
        kwargs.update({"outlets": outlets})

    return kwargs
