from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import Tuple

from sql_cli.dag_generator import SqlFilesDAG
from sql_cli.sql_directory_parser import get_sql_files
from sql_cli.utils import render_jinja


def cli() -> Namespace:
    """
    The entrypoint for the sql cli.

    :returns: the command line args passed by the user.
    """
    parser = ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    return args


def init_project(directory: Path) -> Tuple[Path, Path]:
    """
    Initialize the project for a given directory.

    :params directory: The directory to initialize the project in.

    :returns: a tuple containing the target directory and dags directory.
    """
    target_directory = directory.parent / "_target" / directory.name
    target_directory.mkdir(parents=True, exist_ok=True)
    dags_directory = directory.parent / "_dags"
    dags_directory.mkdir(parents=True, exist_ok=True)
    return target_directory, dags_directory


def generate_dag(directory: Path, target_directory: Path, dags_directory: Path) -> None:
    """
    Generate a DAG from SQL files.

    :params directory: The directory containing the raw sql files.
    :params target_directory: The directory containing the executable sql files.
    :params dags_directory: The directory containing the generated DAG.
    """
    sql_files = sorted(get_sql_files(directory, target_directory))
    sql_files_dag = SqlFilesDAG(
        dag_id=directory.name,
        start_date=datetime.utcnow(),
        sql_files=sql_files,
    )
    render_jinja(
        context={"dag": sql_files_dag},
        output_file=dags_directory / f"{sql_files_dag.dag_id}.py",
    )


def main() -> None:
    args = cli()
    directory = args.directory.resolve()
    target_directory, dags_directory = init_project(directory)
    generate_dag(directory, target_directory, dags_directory)


if __name__ == "__main__":
    main()
