<h1 align="center">
  astro
</h1>
  <h3 align="center">
  workflows made easy<br><br>
</h3>

[![Python versions](https://img.shields.io/pypi/pyversions/astro-sdk-python.svg)](https://pypi.org/pypi/astro-sdk-python)
[![License](https://img.shields.io/pypi/l/astro-sdk-python.svg)](https://pypi.org/pypi/astro-sdk-python)
[![Development Status](https://img.shields.io/pypi/status/astro-sdk-python.svg)](https://pypi.org/pypi/astro-sdk-python)
[![PyPI downloads](https://img.shields.io/pypi/dm/astro-sdk-python.svg)](https://pypistats.org/packages/astro-sdk-python)
[![Contributors](https://img.shields.io/github/contributors/astronomer/astro-sdk)](https://github.com/astronomer/astro-sdk)
[![Commit activity](https://img.shields.io/github/commit-activity/m/astronomer/astro-sdk)](https://github.com/astronomer/astro-sdk)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/astronomer/astro-sdk/main.svg)](https://results.pre-commit.ci/latest/github/astronomer/astro-sdk/main)
[![CI](https://github.com/astronomer/astro-sdk/actions/workflows/ci.yaml/badge.svg)](https://github.com/astronomer/astro-sdk)
[![codecov](https://codecov.io/gh/astronomer/astro-sdk/branch/main/graph/badge.svg?token=MI4SSE50Q6)](https://codecov.io/gh/astronomer/astro-sdk)

**Astro Python SDK** allows for rapid and clean development of extract, transform, and load (ETL) workflows using Python.

The SDK abstracts the boilerplate code required for communication between datasets and tasks, which helps DAG authors to achieve more with less code.

It is powered by [Apache Airflow](https://airflow.apache.org) and maintained by [Astronomer](https://astronomer.io).

> :warning: **Disclaimer** This project is in a **preview** release state. In other words, it is not production-ready yet.
The interfaces may change. We welcome users to try out the interfaces and provide us with feedback.

## Prerequisites

- Apache Airflow >= 2.1.0.

## Install

The Astro Python SDK is available at [PyPI](https://pypi.org/project/astro-sdk-python/). Use the standard Python
[installation tools](https://packaging.python.org/en/latest/tutorials/installing-packages/).

To install a cloud-agnostic version of the SDK, run:

```shell
pip install astro-sdk-python
```

You can also install dependencies for using the SDK with popular cloud providers:

```shell
pip install astro-sdk-python[amazon,google,snowflake,postgres]
```


## Quickstart

1. Copy the following DAG into a file named `calculate_popular_movies.py` and add it to the `dags` directory of your Airflow project:

    ```Python
    from datetime import datetime
    from airflow import DAG
    from astro import sql as aql
    from astro.files import File
    from astro.sql.table import Table

    @aql.transform()
    def top_five_animations(input_table: Table):
        return """
            SELECT Title, Rating
            FROM {{input_table}}
            WHERE Genre1=='Animation'
            ORDER BY Rating desc
            LIMIT 5;
        """

    with DAG(
        "calculate_popular_movies",
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
    ) as dag:
        imdb_movies = aql.load_file(
            File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"),
            output_table=Table(
                name="imdb_movies", conn_id="sqlite_default"
            ),
        )
        top_five_animations(
            input_table=imdb_movies,
            output_table=Table(
                name="top_animation"
            ),
        )
    ```

2. Ensure that your Airflow environment is set up correctly by running the following commands:

    ```shell
    export AIRFLOW_HOME=`pwd`
    export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
    airflow db init
    ```

3. Create a SQLite database for the example to run with and run the DAG:

    ```shell
    # The sqlite_default connection has different host for MAC vs. Linux
    export SQL_TABLE_NAME=`airflow connections get sqlite_default -o     yaml | grep host | awk '{print $2}'`
    sqlite3 "$SQL_TABLE_NAME" "VACUUM;"
    ```

4. Run the example DAG:

    ```sh
    airflow dags test calculate_popular_movies `date -Iseconds`
    ```

5. Check the result of your DAG by running:

    ```shell
    sqlite3 "$SQL_TABLE_NAME" "select * from top_animation;" ".exit"
    ```

    You should see the following output:

    ```shell
    $ sqlite3 "$SQL_TABLE_NAME" "select * from top_animation;" ".exit"
    Toy Story 3 (2010)|8.3
    Inside Out (2015)|8.2
    How to Train Your Dragon (2010)|8.1
    Zootopia (2016)|8.1
    How to Train Your Dragon 2 (2014)|7.9
    ```

## Supported technologies

| Databases       |
|-----------------|
| Google BigQuery |
| Postgres        |
| Snowflake       |
| SQLite          |

| File types |
|------------|
| CSV        |
| JSON       |
| NDJSON     |
| Parquet    |

| File stores |
|------------ |
| Amazon S3   |
| Filesystem  |
| Google GCS  |

## Available operations

The following are some key functions available in the SDK:

- `load_file`: load a given file into a SQL table
- `transform`: applies a SQL select statement to a source table and saves the result to a destination table
- `drop_table`: Drops a SQL table
- `run_raw_sql`: run any SQL statement without handling its output
- `append`: insert rows from the source SQL table into the destination SQL table, if there are no conflicts
- `merge`: insert rows from the source SQL table into the destination SQL table, depending on conflicts:
  - ignore: do not add rows that already exist
  - update: replace existing rows with new ones
- `export_file`: export SQL table rows into a destination file
- `dataframe`: export given SQL table into in-memory Pandas data-frame

For a full list of available operators, see the [SDK reference documentation](https://astro-sdk-python.readthedocs.io/en/latest/astro/sql/operators/append.html).

## Documentation

The documentation is a work in progress--we aim to follow the [Diátaxis](https://diataxis.fr/) system:

- **[Getting Started](docs/getting-started/GETTING_STARTED.md)**: A hands-on introduction to the Astro Python SDK
- **How-to guides**: Simple step-by-step user guides to accomplish specific tasks
- **[Reference guide](https://astro-sdk-python.readthedocs.io/)**: Commands, modules, classes and methods
- **Explanation**: Clarification and discussion of key decisions when designing the project

## Changelog

The Astro Python SDK follows semantic versioning for releases. Check the [changelog](docs/CHANGELOG.md) for the latest changes.

## Release managements

To learn more about our release philosophy and steps, see [Managing Releases](docs/development/RELEASE.md).

## Contribution guidelines

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

Read the [Contribution Guideline](docs/development/CONTRIBUTING.md) for a detailed overview on how to contribute.

Contributors and maintainers should abide by the [Contributor Code of Conduct](docs/development/CODE_OF_CONDUCT.md).

## License

[Apache Licence 2.0](LICENSE)
