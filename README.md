<h1 align="center">
  astro
</h1>
  <h3 align="center">
  workflows made easy<br><br>
</h3>

[![Python versions](https://img.shields.io/pypi/pyversions/astro-projects.svg)](https://pypi.org/pypi/astro-projects)
[![License](https://img.shields.io/pypi/l/astro-projects.svg)](https://pypi.org/pypi/astro-projects)
[![Development Status](https://img.shields.io/pypi/status/astro-projects.svg)](https://pypi.org/pypi/astro-projects)
[![PyPI downloads](https://img.shields.io/pypi/dm/astro-projects.svg)](https://pypistats.org/packages/astro-projects)
[![Contributors](https://img.shields.io/github/contributors/astro-projects/astro)](https://github.com/astro-projects/astro)
[![Commit activity](https://img.shields.io/github/commit-activity/m/astro-projects/astro)](https://github.com/astro-projects/astro)
[![CI](https://github.com/astro-projects/astro/actions/workflows/ci.yaml/badge.svg)](https://github.com/astro-projects/astro)

**astro** allows rapid and clean development of {Extract, Load, Transform} workflows using Python.
It empowers DAG authors to achieve more with less code. 
It is powered by [Apache Airflow](https://www.airflow.apache.org) and maintained by [Astronomer](https://astronomer.io).

> :warning: **Disclaimer** This project development status is alpha. This means it is not production-ready yet.
The interfaces may change. We welcome alpha users and brave souls to test it - any feedback is very much appreciated.


## Quickstart

### Install

**astro** is available at [PyPI](https://pypi.org/project/astro-projects/) and it can be installed using standard Python
[installation tools](https://packaging.python.org/en/latest/tutorials/installing-packages/).

To install a cloud-agnostic version of **astro**, run

```commandline
pip install astro-projects
```

If using cloud-providers, install using the optional dependencies of interest. Example:

```commandline
pip install astro-projects[google,snowflake,postgres]
```


### Try it out

This is an example is an example Airflow DAG, to a local folder named `dags`:

```python
from datetime import datetime
from airflow import DAG
from astro import sql as aql
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)


@aql.transform
def top_five_scify_movies(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM {{input_table}}
    """


with DAG(
    "calculate_popular_movies", schedule_interval=None, start_date=START_DATE
) as dag:

    imdb_movies = aql.load_file(
        path="https://raw.githubusercontent.com/astro-projects/astro/readme/tests/data/imdb.csv",
        task_id="load_csv",
        output_table=Table(
            table_name="imdb_movies", database="sqlite", conn_id="sqlite_default"
        ),
    )

    top_five_scify_movies(
        input_table=imdb_movies,
        output_table=Table(
            table_name="top_scify", database="sqlite", conn_id="sqlite_default"
        ),
    )
```

Execute the DAG locally:

```commandline
export AIRFLOW_HOME=`pwd`
export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

sqlite3 /tmp/sqlite_default.db "VACUUM;"

airflow db init
airflow dags test calculate_popular_movies `date --iso-8601`
```

## Requirements

Because **astro** is built using the [Task Flow API](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html), 
it depends on Apache Airflow >= 2.0.0.

## Supported technologies


| Databases       | File types | File locations |
|-----------------|------------|----------------|
| Google BigQuery | CSV        | Amazon S3      |
| Postgres        | JSON       | Filesystem     |
| Snowflake       | NDJSON     | Google GCS     |
| SQLite          | Parquet    |                |


## Documentation

The documentation is a work in progress, and we aim to follow the [Diátaxis](https://diataxis.fr/) system:
* **Tutorial**: a hands-on introduction to **astro**
* **How-to guides**: simple step-by-step user guides to accomplish specific tasks
* **[Technical reference](docs/OLD_README.md)**: commands, modules, classes and methods
* **Explanation**: Clarification and discussion of key decisions when designing the project.

## Changelog

We follow Semantic Versioning for releases. Check the [changelog](docs/CHANGELOG.md) for the latest changes.

## Contribution Guidelines

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

A detailed overview on how to contribute can be found in the [Contributing Guide](docs/CONTRIBUTING.md).

As contributors and maintainers to this project, you are expected to abide by the [Contributor Code of Conduct](docs/CODE_OF_CONDUCT.md).

## License

[Apache Licence 2.0](LICENSE)