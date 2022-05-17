import random
import string

from airflow.models import DagRun, TaskInstance

from astro.constants import UNIQUE_TABLE_NAME_LENGTH

MAX_TABLE_NAME_SIZE = 63


# TODO: deprecate by the end of the refactoring
def create_table_name(context) -> str:
    ti: TaskInstance = context["ti"]
    dag_run: DagRun = ti.get_dagrun()
    table_name = f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}".replace(
        "-", "_"
    ).replace(".", "__")[:MAX_TABLE_NAME_SIZE]
    if not table_name.isidentifier():
        table_name = f'"{table_name}"'
    return table_name


# TODO: deprecate by the end of the refactoring
def create_unique_table_name(length: int = UNIQUE_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id
