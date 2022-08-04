# Developing the package

Prerequisites

* (Optional but highly recommended) [pipx](https://pypa.github.io/pipx/).
* [Nox](https://nox.thea.codes/) for all automation.

If you use pipx, use the following command to install Nox:

    pipx install nox

Once you have installed, please run the following command to create a `.env` file:

```bash
cat .env-template > .env
```

You will see that there are a series of AWS and Snowflake-based env variables. You really only need these set if you want
to test snowflake or AWS functionality.

Finally, let's set up a toy postgres to run queries against.

We've created a docker image that uses the sample [pagila](https://github.com/devrimgunduz/pagila) database for testing and experimentation.
To use this database please run the following docker image in the background. You'll notice that we are using port `5433` to ensure that
this postgres instance does not interfere with other running postgres instances.

```
docker run --rm -it -p 5433:5432 dimberman/pagila-test &
```


## Setup a development environment

This is useful for IDE and editor support.

    nox -s dev

Once completed, point the Python environment to `.nox/dev` in your IDE or
editor of choice.


## Set up pre-commit hooks

If you do NOT have [pre-commit](https://pre-commit.com/) installed, run the
following command to get a copy:

    nox --install-only lint

and find the `pre-commit` command in `.nox/lint`.

After locating the pre-commit command, run:

    path/to/pre-commit install


## Run linters manually

    nox -s lint


## Run tests

<!-- Tests don't run yet, we're missing `test-connections.yaml`. -->

On all supported Python versions:

    nox -s test

On only 3.9 (for example):

    nox -s test-3.9

Please also note that you can reuse an existing environment if you run nox with the `-r` argument (or even `-R` if you
don't want to attempt to reinstall packages). This can significantly speed up repeat test runs.

## Build documentation

    nox -s build_docs

## Check code coverage

To run code coverage locally, you can either use `pytest` in one of the test environments or
run `nox -s test` with coverage arguments. We use [pytest-cov](https://pypi.org/project/pytest-cov/) for our coverage reporting.

Below is an example of running a coverage report on a single test. In this case the relevant file is `src/astro/sql/operators/sql_decorator.py`
since we are testing the postgres `transform` decorator.

```shell script
nox -R -s test -- --cov-report term --cov-branch --cov=src/astro/sql/operators  tests/operators/test_postgres_decorator.py
===================================================== test session starts =====================================================
platform darwin -- Python 3.9.10, pytest-6.2.5, py-1.11.0, pluggy-1.0.0
rootdir: /Users/dimberman/code/astronomer/astro-project/plugins/astro, configfile: pyproject.toml
plugins: anyio-3.5.0, requests-mock-1.9.3, split-0.6.0, dotenv-0.5.2, cov-3.0.0
collected 12 items

tests/operators/test_postgres_decorator.py ............                                                                 [100%]

====================================================== warnings summary =======================================================

---------- coverage: platform darwin, python 3.9.10-final-0 ----------
Name                                                  Stmts   Miss Branch BrPart  Cover   Missing
-------------------------------------------------------------------------------------------------
src/astro/sql/operators/__init__.py                       0      0      0      0   100%
src/astro/sql/operators/agnostic_aggregate_check.py      46     32     16      0    26%   61-89, 100-138, 162
src/astro/sql/operators/agnostic_boolean_check.py        66     45     16      0    30%   19-21, 24, 27, 51-65, 80-95, 98-105, 109, 115-128, 131, 149
src/astro/sql/operators/agnostic_load_file.py            56     35     10      0    35%   61-67, 76-101, 106-110, 118-140, 166-167
src/astro/sql/operators/export_file.py                   65     43     14      0    30%   64-70, 79-95, 98-109, 112-152, 162-182, 188-190, 220-224
src/astro/sql/operators/agnostic_sql_append.py           50     36     20      0    23%   45-56, 67-85, 90-117
src/astro/sql/operators/agnostic_sql_merge.py            43     28     12      0    31%   48-59, 69-118
src/astro/sql/operators/agnostic_sql_truncate.py         20     11      2      0    50%   32-40, 55-60
src/astro/sql/operators/agnostic_stats_check.py         110     86     32      0    21%   24-27, 32-33, 36-49, 52-73, 76-92, 95-98, 103-119, 122, 125-134, 146-169, 196-216, 231-260, 280
src/astro/sql/operators/sql_dataframe.py                 76     13     22      2    79%   83, 130, 160-174
src/astro/sql/operators/sql_decorator.py                201     45     78     16    72%   107-110, 126->128, 137, 166, 175, 194-196, 206->210, 223-224, 228-243, 247-248, 259, 277, 280, 287, 291-293, 296, 311, 315, 322-327, 330-335, 340, 346-363, 380-392
-------------------------------------------------------------------------------------------------
TOTAL                                                   733    374    222     18    46%
```

## Release a new version

<!-- Not yet verified. -->

Build new release artifacts:

    nox -s build

Publish a release to PyPI:

    nox -s release


## Nox tips

* Pass `-R` to skip environment setup, e.g. `nox -Rs lint`
* Pass `-r` to skip environment creation but re-install packages, e.g. `nox -rs dev`
* Find more automation commands with `nox -l`

## Test DAG in Airflow UI

You can configure the Docker-based testing environment to test your DAG

1. Install the latest versions of the Docker Community Edition and Docker Compose and add them to the PATH.

2. Run ``make -f Makefile.mk build-run``

3. Put the DAGs you want to run in the dev/dags directory:

4. If you want to add Connections, create a connections.yaml file in the dev directory.

   See the `Connections Guide <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>`_ for more information.

   Example:

```
druid_broker_default:
  conn_type: druid
  extra: '{"endpoint": "druid/v2/sql"}'
  host: druid-broker
  login: null
  password: null
  port: 8082
  schema: null
airflow_db:
  conn_type: mysql
  extra: null
  host: mysql
  login: root
  password: plainpassword
  port: null
  schema: airflow
```

5. The following commands are available to run from the root of the repository.

  ``make -f Makefile.mk logs`` - To view the logs of the all the containers
  ``make -f Makefile.mk stop`` - To stop all the containers
  ``make -f Makefile.mk clean`` - To remove all the containers along with volumes
  ``make -f Makefile.mk help`` - To view the available commands
  ``make -f Makefile.mk build-run`` - To build the docker image and then run containers

6. Following ports are accessible from the host machine:

  ``8080 ``- Webserver
  ``5555`` - Flower
  ``5432`` - Postgres

7. Dev Directories:

  ``dev/dags/`` - DAG Files
  ``dev/logs/`` - Logs files of the Airflow containers
