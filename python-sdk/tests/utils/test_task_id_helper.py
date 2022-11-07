from astro.utils .task_id_helper import get_task_id


def test_get_task_id():
    task_id = get_task_id("_tmp", "s3://tmp/09")
    assert task_id == "_tmp_09"
