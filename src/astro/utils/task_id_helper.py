from airflow.decorators.base import get_unique_task_id


def get_task_id(prefix, path):
    """Generate unique tasks id based on the path.
    :parma prefix: prefix string
    :type prefix: str
    :param path: file path.
    :type path: str
    """
    task_id = "{}_{}".format(prefix, path.rsplit("/", 1)[-1].replace(".", "_"))
    return get_unique_task_id(task_id)
