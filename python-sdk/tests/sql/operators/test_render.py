import pathlib

from astro.sql.operators.render import render_tasks

CWD = pathlib.Path(__file__).parent


def test_render_directory(sample_dag):
    with sample_dag:
        render_tasks(CWD / "render_files")

    assert len(sample_dag.tasks) == 3
    assert [t.task_id for t in sample_dag.tasks] == ["transform_a", "transform_b", "transform_c"]
    known_deps = {
        "transform_a": [],
        "transform_b": ["transform_a"],
        "transform_c": ["transform_a", "transform_b"],
    }
    for k, v in sample_dag.task_dict.items():
        assert set(known_deps[k]) == v.upstream_task_ids


def test_render_directory_with_load(sample_dag):
    with sample_dag:
        render_tasks(CWD / "render_files_with_load")

    assert len(sample_dag.tasks) == 4
    assert [t.task_id for t in sample_dag.tasks] == [
        "transform_a",
        "transform_b",
        "transform_c",
        "load_file",
    ]
    known_deps = {
        "load_file": [],
        "transform_a": ["load_file"],
        "transform_b": ["transform_a"],
        "transform_c": ["transform_b", "transform_a"],
    }
    for k, v in sample_dag.task_dict.items():
        assert set(known_deps[k]) == v.upstream_task_ids
