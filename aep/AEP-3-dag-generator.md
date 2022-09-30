# AEP-3 DAG Generator

Authors:

* Felix Uellendall (felix.uellendall@astronomer.io)

## Status

| <!-- -->       |  <!-- -->                                                 |
| -------------- | --------------------------------------------------------- |
| **State**      | Accepted                                                  |
| **Discussion** | <https://github.com/astronomer/astro-sdk/issues/794>      |
| **Created**    | 2022-09-29                                                |

## Motivation

To create a SQL CLI, we need to offer SQL as a native integration into Airflow. This would mean having a system where a SQL engineer can create a directory of SQL files and have Airflow automatically turn the output into a DAG. We can assume that the SQL tasks are in individual files and use a similar templating scheme to the astro-python-sdk to communicate inheritance.

## Proposal

Our users will interact with the DAG generator via command line. They define the SQL files to execute, containing jinja variables to reference input tables. By doing that, we can determine the dependencies and create a DAG out of it. Furthermore our users can define SQL file headers to configure database settings such as conn_id, output schema and database.

### Scope

The initial scope must contain the following objectives:

* a python function to generate a DAG from a directory of SQL files
* ability to configure conn_id, output schema and database

## Assumptions and Limitations

### Option 1: Static Task generation using `aql.transform_sql` with raw SQL

This method would be exposed as CLI command to generate an Airflow DAG from a directory of SQL files.

Pros:

* **Easy to debug** the final DAG as it shows all tasks
* **Static Tasks** i.e. the task generation does not affect the performance of the Airflow DAG parser

Cons:

* Requires DAG creation via SQL files prior to creating the Airflow DAG to determine the right order of `aql.transform_file` statements
* DAG can become very huge as it contains the raw SQL content to execute

### Option 2: Static Task generation using `aql.transform_sql` with file path

This method would be exposed as CLI command to generate an Airflow DAG from a directory of SQL files.

Draft implementation can be found [here](https://github.com/astronomer/astro-sdk/pull/836).

Pros:

* **Easy to debug** the final DAG as it shows all tasks
* **Static Tasks** i.e. the task generation does not affect the performance of the Airflow DAG parser
* **Compact DAG** as it only contains references to SQL files hence it does not grow as SQL content grows

Cons:

* Requires DAG creation via SQL files prior to creating the Airflow DAG to determine the right order of `aql.transform_file` statements

### Option 3: Dynamic Task generation using `aql.transform_directory` (previously `aql.render`)

This method would be exposed as CLI command to generate an Airflow DAG from a directory of SQL files.

Example implementation can be found [here](https://github.com/astronomer/astro-sdk/blob/0.8.4/src/astro/sql/parsers/sql_directory_parser.py).

### Pros

* **Compact DAG** as it only contains references to SQL files hence it does not grow as SQL content grows

### Cons

* Dynamic Tasks i.e. the task generation does affect the performance of the Airflow DAG parser
* Difficult to debug the DAG as tasks are being generated dynamically i.e. there is no way of looking at the generated tasks
