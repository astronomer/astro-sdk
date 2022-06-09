from airflow.decorators import task

from tests.sql.operators import utils as test_utils


@task()
def add_one(input):
    print(f"current number is {input}")
    return input + 1


def test_cleanup_dag(sample_astro_dag):
    with sample_astro_dag:
        for i in range(3):
            add_one(i)
    test_utils.run_dag(sample_astro_dag)
