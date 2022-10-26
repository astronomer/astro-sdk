from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sql_cli.exceptions import EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.sql_directory_parser import SqlFile, get_sql_files
from airflow.models.dag import DAG


@dataclass(frozen=True)
class SqlFilesDAG:
    """
    A DAG of sql files i.e. used for finding the right order to execute the sql files in.

    :param dag_id: The id of the DAG to generate.
    :param start_date: The start date of the DAG.
    :param sql_files: The sql files to use for DAG generation.
    """

    dag_id: str
    start_date: datetime
    sql_files: list[SqlFile]

    def __post_init__(self) -> None:
        if not self.sql_files:
            raise EmptyDag("Missing SQL files!")

    def to_transform_dag(self):
        param_dict = {s.path.stem: s.to_transform_operator() for s in self.sql_files}
        for s in self.sql_files:
            for p in s.get_parameters():
                param_dict[s.path.stem].parameters[p] = param_dict[p].output
                param_dict[s.path.stem].set_upstream(param_dict[p])
        return list(param_dict.values())


def render_dag(directory: Path, dag_id: str):
    if not directory.exists():
        raise SqlFilesDirectoryNotFound("The directory does not exist!")
    sql_files = sorted(get_sql_files(directory, target_directory=None))
    sql_files_dag = SqlFilesDAG(
        dag_id=directory.name,
        start_date=datetime(2020, 1, 1),
        sql_files=sql_files,
    )
    result_dag = DAG(dag_id=dag_id, start_date=datetime(2020,1,1))
    with result_dag:
        sql_files_dag.to_transform_dag()
    return result_dag
