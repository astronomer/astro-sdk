from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import Tuple

from sql_cli.dag_generator import SqlFilesDAG
from sql_cli.sql_directory_parser import get_sql_files
from sql_cli.utils import render_jinja


def cli() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    return args


def init_project(directory: Path) -> Tuple[Path, Path]:
    target_directory = directory.parent / "_target" / directory.name
    target_directory.mkdir(parents=True, exist_ok=True)
    dags_directory = directory.parent / "_dags"
    dags_directory.mkdir(parents=True, exist_ok=True)
    return target_directory, dags_directory


def generate_dag(directory: Path, target_directory: Path, dags_directory: Path) -> None:
    sql_files = get_sql_files(directory, target_directory)
    sql_files_dag = SqlFilesDAG(sql_files)
    sql_files_sorted = sql_files_dag.build()
    render_jinja(
        context={
            "dag_id": directory.name,
            "start_date": datetime.utcnow(),
            "sql_files": sql_files_sorted,
            "input_tables": sql_files_dag.nodes,
            "needs_metadata_import": any(
                "database" in sql_file.metadata or "schema" in sql_file.metadata
                for sql_file in sql_files
            ),
        },
        output_file=dags_directory / f"{directory.name}.py",
    )


def main() -> None:
    args = cli()
    directory = args.directory.resolve()
    target_directory, dags_directory = init_project(directory)
    generate_dag(directory, target_directory, dags_directory)


if __name__ == "__main__":
    main()
