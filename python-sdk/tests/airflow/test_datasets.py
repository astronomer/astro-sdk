from unittest import mock

import airflow
import pytest
from astro.airflow.datasets import kwargs_with_datasets
from astro.sql.table import Table


@pytest.mark.parametrize(
    "kwargs,input_datasets,output_datasets,dataset_support,expected_kwargs",
    [
        (None, None, None, True, {}),
        ({"task_id": "ex1"}, None, None, True, {"task_id": "ex1"}),
        (
            {"task_id": "ex1"},
            Table("inlet", conn_id="con1"),
            Table("outlet", conn_id="con2"),
            True,
            {
                "task_id": "ex1",
                "inlets": [Table("inlet", conn_id="con1")],
                "outlets": [Table("outlet", conn_id="con2")],
            },
        ),
        (
            {
                "task_id": "ex1",
                "inlets": Table("inlet", conn_id="con1"),
                "outlets": Table("outlet"),
            },
            Table("input_dataset"),
            Table("output_dataset"),
            True,
            {
                "task_id": "ex1",
                "inlets": [Table("inlet", conn_id="con1")],
                "outlets": [Table("outlet")],
            },
        ),
        (None, None, None, False, {}),
        ({"task_id": "ex1"}, None, None, False, {"task_id": "ex1"}),
        (
            {"task_id": "ex1"},
            Table("inlet", conn_id="con1"),
            Table("outlet", conn_id="con2"),
            False,
            {"task_id": "ex1"},
        ),
        (
            {
                "task_id": "ex1",
                "inlets": Table("inlet", conn_id="con1"),
                "outlets": Table("outlet", conn_id="con2"),
            },
            Table("input_dataset"),
            Table("output_dataset"),
            False,
            {
                "task_id": "ex1",
                "inlets": [Table("inlet", conn_id="con1")],
                "outlets": [Table("outlet", conn_id="con2")],
            },
        ),
    ],
)
def test_kwargs_with_datasets(
    kwargs, input_datasets, output_datasets, dataset_support, expected_kwargs
):
    """
    Test that:
      1. we can extract inlets and outlets from kwargs if users pass it
      2. passed input_datasets and output_datasets are correctly set as inlets/outlets and passed to kwargs
      3. if dataset is not support (Airflow <2.4), we do not set inlets/outlets unless user specifies it
    """
    with mock.patch("astro.airflow.datasets.DATASET_SUPPORT", new=dataset_support):
        assert (
            kwargs_with_datasets(kwargs, input_datasets, output_datasets)
            == expected_kwargs
        )


@pytest.mark.skipif(
    airflow.__version__ < "2.4.0", reason="Require Airflow version >= 2.4.0"
)
def test_example_dataset_dag():
    import os

    from airflow.models.dagbag import DagBag

    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../../example_dags")

    from airflow.datasets import Dataset

    producer_dag = db.get_dag("example_dataset_producer")
    consumer_dag = db.get_dag("example_dataset_consumer")
    # Test that last task in the producer DAG produces an outlet
    outlets = producer_dag.tasks[-1].outlets
    assert isinstance(outlets[0], Dataset)
    # Test that dataset_triggers is only set if all the instances passed to the DAG object are Datasets
    assert consumer_dag.dataset_triggers == outlets
    assert outlets[0].uri == "astro://sqlite_default@?table=imdb_movies"
