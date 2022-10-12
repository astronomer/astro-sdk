import pathlib
from unittest import mock

from airflow.decorators import task
from airflow.utils.cli import get_dag

from astro import sql as aql
from sql_cli.run_dag import _run_task, run_dag

CWD = pathlib.Path(__file__).parent


@mock.patch("airflow.models.taskinstance.TaskInstance")
@mock.patch("airflow.settings.SASession")
def test_run_task_successfully(mock_session, mock_task_instance):
    mock_task_instance.map_index = 0
    _run_task(mock_task_instance, mock_session)
    mock_task_instance._run_raw_task.assert_called_once()
    mock_session.flush.assert_called_once()


def test_run_task_cleanup_log(sample_dag, caplog):
    @aql.dataframe
    def upstream_task():
        return 5

    @aql.dataframe
    def downstream_task(f):
        return f

    with sample_dag:
        downstream_task(upstream_task())
        aql.cleanup()
    run_dag(sample_dag)
    assert "aql.cleanup async, continuing" in caplog.text


def test_run_dag_dynamic_task(sample_dag, caplog):
    @task
    def get_list():
        return [1, 2, 3]

    @task
    def print_val(v):
        print(v)

    with sample_dag:
        print_val.expand(v=get_list())
    run_dag(sample_dag)
    for i in [1, 2]:
        assert f"Running task print_val index {i}" in caplog.text


def test_run_dag_with_skip(sample_dag, caplog):
    @task.branch
    def who_is_prettiest():
        return "snow_white"

    @task
    def snow_white():
        return "the witch is mad"

    @task
    def witch():
        return "the witch is happy"

    @task
    def movie_ends():
        return "movie ends"

    with sample_dag:
        who_is_prettiest() >> [snow_white(), witch()] >> movie_ends()
    run_dag(sample_dag)
    assert "Task Skipped, continuing" in caplog.text


def test_run_dag(caplog):
    dag = get_dag(dag_id="example_dataframe", subdir=f"{CWD}/test_dag")
    run_dag(dag)
    assert "The worst month was 2020-05" in caplog.text


def test_run_dag_with_conn_id(caplog):
    dag = get_dag(dag_id="example_sqlite_load_transform", subdir=f"{CWD}/test_dag")
    run_dag(dag, conn_file_path=f"{CWD}/test_conn.yaml")
    assert "top movie:" in caplog.text
