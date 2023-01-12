# Contributing

The objective of this document is to serve as guide for developers.

## SQL CLI

See [Getting started](README.md) for setting up the project locally.

### Running tests

We are using _pytest_ to run tests and `nox` to run them across different python and airflow versions.

For running tests it is advisable to use `nox` to be able to test with different python and airflow versions.

For example:

```bash
nox -s "test-3.8(airflow='2.5')"
```

In case you only want to test a specific test, you can append the path to the nox command via `--` and _pytest_ syntax:

```bash
nox -s "test-3.8(airflow='2.5')" -- tests/test___main__.py::test_validate
```

If you only want to test on the current installed version - and want to run all tests you can use the `make test` command.

### Debugging

For development it can be quite useful to leverage the cli `--debug` and per-command `--verbose` flags, which both show additional information about internals.

The `--debug` flag specifically enables debug logs for external modules such as `airflow`.

The `--verbose` flag which you can only set per command, displays debug logs of the `sql_cli` logger.

## Astro CLI Integration

The SQL CLI has a deep integration with the [Astro CLI](https://docs.astronomer.io/astro/cli/overview), as this is what our users should use to interact with Astro.

Therefore, it is very important to read the following sections, even if you only develop on the sql-cli package.

### `astro flow` command

The _cobra_ `flow` command is the **entrypoint to the SQL CLI** within the Astro CLI.

_You can find the code in the [astro-cli repository](https://github.com/astronomer/astro-cli) `cmd/sql` directory._

For example, you can call `astro flow version` to get the version of the sql-cli package. For a full list of commands, run `astro flow --help`.

Internally, what happens is that we build a command that we pass on to a docker container which has the sql-cli installed and executes the command.
Additionally, the "flow" command requires some more functionality like mounting files from the host to the container to be able to use the created sql-cli project outside of the container after the command exits. Furthermore, we resolve directory paths before mounting so that we have the exact same directory structure outside and inside the container.
_You can find the code for that in `sql` directory._

_You can find the user documentation of the "flow" command [here](https://docs.astronomer.io/astro/cli/sql-cli)._

### E2E tests

Currently, our E2E tests live in the astro-cli repository. You can find them in `cmd/sql/e2e/flow_test.go`. However, there is an on-going [discussion](https://docs.google.com/document/d/1sNr2a2cxNY8UWR4c_EvN1ckAPcjcyGaLsqWG6erCnJY) about where to move the E2E tests to.
