from typing import cast

from airflow.decorators.base import get_unique_task_id


def get_task_id(prefix: str, path: str) -> str:
    """Generate unique tasks id based on the path.

    :param prefix: prefix string
    :param path: file path.
    """
    task_id = f"{prefix}_{path.rsplit('/', 1)[-1].replace('.', '_')}"
    return cast(str, get_unique_task_id(task_id))
