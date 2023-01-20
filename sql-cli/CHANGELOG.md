# Changelog

## 0.3.0

### Features

For more information on the features, please, check their help or access: https://docs.astronomer.io/astro/cli/sql-cli.html

1. Deployment to Astro Cloud

The main feature of this release is exposed in the Astro CLI 1.10, through the command:

```sh
astro flow deploy
```

Related tickets:
* Allow SQL CLI users to define scheduling and other DAG metadata using YML ([#1614](https://github.com/astronomer/astro-sdk/pull/1614))
* Add flow deploy command ([#1615](https://github.com/astronomer/astro-sdk/pull/1615))

2. Examples runnable from Astro CLI

SQL CLI examples can now run in the Astro CLI when the user uses

```sh
astro dev init
astro flow init --data-dir=./include
```

Related tickets:
* Add configurable data dir to project init ([#1407](https://github.com/astronomer/astro-sdk/pull/1407))

3. Improved troubleshooting

We are exposing additional logs when users use one or both of the following flags:

```sh
astro flow --debug <cmd> --verbose
```

Related tickets:
* Add global debug option and more verbose options to sql-cli ([#1177](https://github.com/astronomer/astro-sdk/pull/1177))

4. Improvements in DAG generation

Allow users to export SQL workflows as DAGs to custom output directories

```sh
astro flow generate <workflow-name> --output-dir <>
```

Related tickets:
* Add output-dir option to flow generate ([#1533](https://github.com/astronomer/astro-sdk/pull/1533))


### Breaking changes

1. The `flow config` was split into two (`get` / `set`). The consequence of this breaking change is that the Astro CLI 1.8 and 1.9 flow commands will not work as expected. Users should upgrade to the latest version of the Astro CLI to be able to use the SQL CLI features:

```
sudo bash < <(curl -sSL https://install.astronomer.io) -s v1.10.0
```

Related ticket:
* Expose get/set deployment config in SQL CLI ([#1479](https://github.com/astronomer/astro-sdk/pull/1479))

2. The global configuration is no longer a directory but a file. Previously created SQL CLI projects will no longer work with this new release unless this change is manually applied:

The `config/global/configuration.yml` should be moved to `config/global.yml`.

Related ticket:
* Move file config/global/configuration.yml -> config/global.yml ([#1409](https://github.com/astronomer/astro-sdk/pull/1409))


### Others

* Add error message for validate connections ([#1554](https://github.com/astronomer/astro-sdk/pull/1554))
* Move "include" directory to project ([#1560](https://github.com/astronomer/astro-sdk/pull/1560))
* Disable logging for non-debug mode ([#1569](https://github.com/astronomer/astro-sdk/pull/1569))
* Add developer docs ([#1577](https://github.com/astronomer/astro-sdk/pull/1577))
* Fix poetry nox integration ([#1570](https://github.com/astronomer/astro-sdk/pull/1570))
* Fix minor consistency issues ([#1505](https://github.com/astronomer/astro-sdk/pull/1505))
* Add temp-flow make command ([#1495](https://github.com/astronomer/astro-sdk/pull/1495))
* Remove stale .airflow/global directory ([#1491](https://github.com/astronomer/astro-sdk/pull/1491))
* Bump sql-cli version to 0.3.0a0 ([#1484](https://github.com/astronomer/astro-sdk/pull/1484))
* Install poetry in nox sessions using session.run using pip instead of session.install ([#1443](https://github.com/astronomer/astro-sdk/pull/1443))
* Bump certifi from 2022.9.24 to 2022.12.7 in /sql-cli ([#1408](https://github.com/astronomer/astro-sdk/pull/1408))
* Make environment optional for Config instance ([#1417](https://github.com/astronomer/astro-sdk/pull/1417))
* Fix elapsed seconds for run command ([#1406](https://github.com/astronomer/astro-sdk/pull/1406v
* [sql-cli] Fix validate logging ([#1398](https://github.com/astronomer/astro-sdk/pull/1398))
* Create .env file on project initialisation + important refactorings (read desc) ([#1391](https://github.com/astronomer/astro-sdk/pull/1391))
* Redirect default airflow_home to tmp directory in tests ([#1384](https://github.com/astronomer/astro-sdk/pull/1384))
* Add working examples and tests for all supported connection types ([#1388](https://github.com/astronomer/astro-sdk/pull/1388))
* Cache poetry installs ([#1392](https://github.com/astronomer/astro-sdk/pull/1392))


## 0.2.2


### Breaking changes

Projects created with previous versions of the SQL CLI will need to be reinitialised, so they respect
the new configuration structure, implemented as part of the bug fix #1230.
It's recommended to back up any previous configuration.

Before the config structure was:
```
config/
├── default
│   └── configuration.yml
└── dev
    └── configuration.yml

```

Now it is:

```
config/
├── default
│   └── configuration.yml
├── dev
│   └── configuration.yml
└── global
    └── configuration.yml

```

The global configuration contains environment-agnostic properties which were previously (incorrectly) set in the environment-specific settings:

```
airflow:
  dags_folder: /home/tati/Code/astro-sdk/sql-cli/test-0.2.2/dags
  home: /home/tati/Code/astro-sdk/sql-cli/test-0.2.2/.airflow/default

```


### Bug fixes

* Fix misplacement of generated DAGs by introducing the concept of global configuration #1230
* Change validate to only check the connection of interest if the flag --connection is given #1370
* Fix connection not found issue #1124
* Improve error message when the remote connection can't be established #1366
* Fix verbose logging #1368
* Fix validate connection ID matching #1361
* Support Airflow 2.2 #1162

### Improvements

* Improve how Airflow configuration is set/retrieved #1219
* Improve generated DAGs (using black) #1362
* Astro CLI end-to-end integration tests #1234
* Run tests against multiple versions of Airflow #1055


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
