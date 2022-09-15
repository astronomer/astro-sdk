# Developing the package

## Prerequisites

* At least Python 3.7, 3.8 or 3.9
* (Optional but highly recommended) [pyenv](https://github.com/pyenv/pyenv)

_On **Apple M1** it is [currently required](https://github.com/psycopg/psycopg2/issues/1286#issuecomment-914286206) to install `postgresql` package. Once [compatible wheels](https://github.com/psycopg/psycopg2/issues/1482) are released, you can remove it._

## Setup a development environment

To setup your local environment simply run the below statement:

```bash
make local target=setup
```

You will see that there are a series of AWS and Snowflake-based env variables. You really only need these set if you want
to test snowflake or AWS functionality.

Finally, let's set up a toy postgres to run queries against.

We've created a docker image that uses the sample [pagila](https://github.com/devrimgunduz/pagila) database for testing and experimentation.
To use this database please run the following docker image in the background. You'll notice that we are using port `5433` to ensure that
this postgres instance does not interfere with other running postgres instances.

```bash
docker run --rm -it -p 5433:5432 dimberman/pagila-test &
```

## Setup IDE and editor support

```bash
nox -s dev
```

Once completed, point the Python environment to `.nox/dev` in your IDE or
editor of choice.

## Set up pre-commit hooks

If you do NOT have [pre-commit](https://pre-commit.com/) installed, run the
following command to get a copy:

```bash
nox --install-only lint
```

and find the `pre-commit` command in `.nox/lint`.

After locating the pre-commit command, run:

```bash
path/to/pre-commit install
```

## Run linters manually

```bash
nox -s lint
```

## Run tests

<!-- Tests don't run yet, we're missing `test-connections.yaml`. -->

On all supported Python versions:

```bash
nox -s test
```

On only 3.9 (for example):

```bash
nox -s test-3.9
```

Please also note that you can reuse an existing environment if you run nox with the `-r` argument (or even `-R` if you
don't want to attempt to reinstall packages). This can significantly speed up repeat test runs.

## Build documentation

```bash
nox -s build_docs
```

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

```bash
nox -s build
```

Publish a release to PyPI:

```bash
nox -s release
```

## Nox tips

* Pass `-R` to skip environment setup, e.g. `nox -Rs lint`
* Pass `-r` to skip environment creation but re-install packages, e.g. `nox -rs dev`
* Find more automation commands with `nox -l`

## Using a container to run Airflow DAGs

You can configure the Docker-based testing environment to test your DAG

1. Install the latest versions of the Docker Community Edition and Docker Compose and add them to the PATH.

1. Run `make container target=build-run`

1. Put the DAGs you want to run in the dev/dags directory:

1. If you want to add Connections, create a connections.yaml file in the dev directory.

   See the [Connections Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) for more information.

   Example:

    ```yaml
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

1. The following commands are available to run from the root of the repository.

* `make container target=logs` - To view the logs of the all the containers
* `make container target=stop` - To stop all the containers
* `make container target=clean` - To remove all the containers along with volumes
* `make container target=help` - To view the available commands
* `make container target=build-run` - To build the docker image and then run containers
* `make container target=docs` -  To build the docs using Sphinx
* `make container target=restart` - To restart Scheduler & Triggerer containers
* `make container target=restart-all` - To restart all the containers
* `make container target=shell` - To run bash/shell within a container (Allows interactive session)

1. Following ports are accessible from the host machine:

* `8080` - Webserver
* `5555` - Flower
* `5432` - Postgres

1. Dev Directories:

* `dev/dags/` - DAG Files
* `dev/logs/` - Logs files of the Airflow containers


## Adding support for a new database

You can use Test Driven approach for adding support for a new database to the Astro Python SDK. You can fulfil the 
[tests](https://github.com/astronomer/astro-sdk/tree/main/python-sdk/tests) for all the 
Python SDK operators by adding parameters to the existing tests for the database and additionally adding more database specific 
implementation tests. You can take a look at this [PR](https://github.com/astronomer/astro-sdk/pull/639) and 
[PR](https://github.com/astronomer/astro-sdk/pull/753) on how to add parameters for the existing tests for all the 
operators.

To start with you can take the following steps for the initial configuration:
1. Add the database name constant to the [Database](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/src/astro/constants.py#L42) class in the `constants.py` module.
2. Add the database schema constant to the [settings.py](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/src/astro/settings.py#L8) module. 
   The [default schema name](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/src/astro/constants.py#L12) is `tmp_astro`. 
   So in case you do not specify your own schema name, your tests will create tables in that schema in your database.
3. Create a `test-connections.yaml` in the `python-sdk` directory to add connections to the database which will be used by the tests. 
   This file is ignored by git as mentioned in `.gitignore` , so you may not worry that your secrets will get checked in accidentally.
   Sample `test-connections.yaml` file would look like the below:
   ```yaml
   - conn_id: gcp_conn
     conn_type: google_cloud_platform
     description: null
     extra: null
   - conn_id: aws_conn
     conn_type: aws
     description: null
     extra: null
   - conn_id: redshift_conn
     conn_type: redshift
     schema: "dev"
     host: <YOUR_REDSHIFT_CLUSTER_HOST_URL>
     port: 5439
     login: <YOUR_REDSHIFT_CLUSTER_USER>
     password: <YOUR_REDSHIFT_CLUSTER_PASSWORD>
   ```
4. Add a mapping of the database name to the connection ID in [conftest.py](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/conftest.py#L25).
5. Add needed environment variables for the implementation to work by creating a `.env` file and adding those to it.
   The environment variable could be related to cloud access credentials, development specific airflow config variables, etc. <br/>
   `.env` file could look like the below (you can ask your team to share with you team level credentials for accessing specific sources, if any.)
   ```text
   AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
   GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_GOOGLE_SERVICE_ACCOUNT_JSON>
   AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True
   AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
   AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
   ASTRO_CHUNKSIZE=10
   GCP_BUCKET=astro-sdk
   REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN=<REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN>
   ASTRO_PUBLISH_BENCHMARK_DATA=True
   ```
6. For the tests to run in CI, you will need to create appropriate secrets in the Github Actions configuration for the repository. Contact the repository admins
   @kaxil [Kaxil Naik](mailto: kaxil@astronomer.io) or @tatiana [Tatiana Al-Chueyr](mailto: tatiana.alchueyr@astronomer.io)  to add your needed secrets to Github Actions.
   To make the connections available, create them in [.github/ci-test-connection.yaml](https://github.com/astronomer/astro-sdk/blob/main/.github/ci-test-connections.yaml) 
   similar to that in step 3 and refer the secrets from the environment variables which need to be created in 
   [.github/workflows/ci-python-sdk.yaml](https://github.com/astronomer/astro-sdk/blob/main/.github/workflows/ci-python-sdk.yaml).
7. Add the database as Python SDK supported database in the [test_constants.py](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/tests/test_constants.py) module's [test_supported_database()](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/tests/test_constants.py#L23) method implementation.

With the above configuration set, you can now proceed for the implementation of supporting all the SDK operators the new database.
The purpose of each of the operators can be found in the [Astro SDK Python - Operators](https://astro-sdk-python.readthedocs.io/en/stable/operators.html) document.
As described before, you can use test driven development and run the tests for the operators one by one located in the `tests` directory.
By default, the [base class](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/src/astro/databases/base.py) 
implementation methods will be used for the database in the tests. You will need to override some of these base class methods for the database
to make run the tests successfully. You can create a module for your database in the [databases](https://github.com/astronomer/astro-sdk/tree/main/python-sdk/src/astro/databases) directory.
You can start with running the tests for the `load_file` operator. Relevant tests can be found in [tests/sql/operators](https://github.com/astronomer/astro-sdk/tree/main/python-sdk/tests/sql/operators) directory. 
Tests for load_operator are kept in [test_load_file.py](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/tests/sql/operators/test_load_file.py). 
You might to need to override few base class methods to establish connection to the database based on its semantics. 
e.g. `sql_type()`, `hook()`, `sqlalchemy_engine()`, `default_metadata()`, `schema_exists()`, `table_exists()`, `load_pandas_dataframe_to_table()`

Following are important pointers for implementing the operators:
1. Investigate how to write a Pandas dataframe to the database and use that in the `load_pandas_dataframe_to_table` implementation.
2. Check what all file types are supported by the database for load and implement support for those. In general, Astro SDK supported file types and file stores can be found [here](https://github.com/astronomer/astro-sdk#supported-technologies).
3. Check what merge strategies the database supports and use that in the `merge_table` implementation. Example PR: [Merge implementation for Redshift](https://github.com/astronomer/astro-sdk/pull/753).
4. The default approach to load a file to a database table is to load the table into a Pandas dataframe first and then load the Pandas dataframe into the database table. However, this generally is a slower approach. 
   For optimised loads, you need to try to support native loads. Check what all file types and object stores the database supports for native load and provide support for those. Also, handle/retry the native load exceptions thrown by the corresponding connector library.
   Example PR: [Native load support for Redshift](https://github.com/astronomer/astro-sdk/pull/700).

Additionally, once you have implemented support for all the Astro SDK Python Operators, you also need to benchmark the performance of the file loads.
You can refer to the [benchmarking guide](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/tests/benchmark/README.md) to generate results and publish those for the database like in [PR](https://github.com/astronomer/astro-sdk/pull/806).