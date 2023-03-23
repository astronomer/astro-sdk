# Development

## To build locally:
```make container target=build-run```

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
