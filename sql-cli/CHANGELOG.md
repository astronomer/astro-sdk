# Changelog

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
