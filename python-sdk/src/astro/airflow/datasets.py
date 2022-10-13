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
    from astro import settings

    kwargs = kwargs or {}
    if "inlets" in kwargs or DATASET_SUPPORT and settings.AUTO_ADD_INLETS_OUTLETS and input_datasets:
        # Remove non-dataset objects like Temp tables (unless passed by user)
        input_datasets = input_datasets if isinstance(input_datasets, list) else [input_datasets]
        input_datasets = [input_ds for input_ds in input_datasets if isinstance(input_ds, Dataset)]

        inlets = kwargs.get("inlets", input_datasets)
        kwargs.update({"inlets": inlets})

    if "outlets" in kwargs or DATASET_SUPPORT and settings.AUTO_ADD_INLETS_OUTLETS and output_datasets:
        # Remove non-dataset objects like Temp tables (unless passed by user)
        output_datasets = output_datasets if isinstance(output_datasets, list) else [output_datasets]
        output_datasets = [output_ds for output_ds in output_datasets if isinstance(output_ds, Dataset)]

        outlets = kwargs.get("outlets", output_datasets)
        kwargs.update({"outlets": outlets})

    return kwargs
