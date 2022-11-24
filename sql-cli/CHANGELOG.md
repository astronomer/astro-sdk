# Changelog

## 0.2.1

A patch release containing the following change:

* the `load_file` example now uses SQLite, like the other workflows

## 0.2.0

A feature release containing the following major features:

* load files to DB without Python
* dynamic DAG generation

### Features

* Integrate load file operator together with generate and run in SQL CLI
* Expose the dynamic DAG generation in the Astro CLI
* Inform the progress to users when Astro CLI - SQL CLI command takes longer

### Improvements

* Fix error help for astro-cli by adding astro to the "Try 'flow -h' for help.'" output
* Do not process example_dags in sql-cli check for import errors
* Fix Airflow 2.2 issue #1162

### Docs

* Verify docs has correct syntax for specifying connection

### Misc

* Refactor cli tests to rely on large terminal
* Change default to generate tasks in generate and run command
* Use ReprHighlighter instead of OptionHighlighter in rprint messages
* Refactor cli tests to rely on large terminal
* Fix noxfile and pyproject
* Remove duplicated print statements in sql-cli

## 0.1.1

A patch release primarily focused on fixing bugs.

The major change is the integration in the Astro CLI 1.7.

### Features

* Allow installing SQL CLI for Python 3.7
* Expose the SQL CLI in the Astro CLI

### Improvements

* Run should have a clean and direct message if the table the SQL statement is trying to access isn't available
* Improve the experience if we generate an invalid DAG
* SQL CLI commands printing lots of warnings
* Remove SQL CLI help duplication
* Reduce command run times for SQL CLI
* Improve error handling for unknown variables and cycles in Workflows
* SQL CLI 0.1 in Astro CLI 1.7: Unable to use --airflow-dags-folder during initialization
* SQL CLI 0.1 in Astro CLI 1.7: Verbose run command
* Astro CLI shows incorrect usage for flow commands

### Docs

* Make SQL CLI documentation available at astro docs

### Misc

* Enhance coverage for SQL CLI integration in Astro CLI

## 0.1.0

The first alpha release of the sql-cli

### Features

* Project initialisation
* Connection validation
* DAG generation
* Workflow execution
* Version command
* About command
