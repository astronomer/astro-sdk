<!--
I imagine this file along with INSTALLATION.md will go into documentation
at some point.
-->

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

On all supported Pyhon versions:

    nox -s test

On only 3.9 (for example):

    nox -s test-3.9

Please also note that you can reuse an existing environtment if you run nox with the `-r` argument (or even `-R` if you
don't want to attempt to reinstall packages). This can significantly speed up repeat test runs.

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
