"""Nox automation definitions."""

import pathlib

import nox

nox.options.sessions = ["dev"]
nox.options.reuse_existing_virtualenvs = True


@nox.session(python="3.9")
def dev(session: nox.Session) -> None:
    """Create a dev environment with everything installed.

    This is useful for setting up IDE for autocompletion etc. Point the
    development environment to ``.nox/dev``.
    """
    session.install("nox")
    session.install("poetry")
    session.run("poetry", "install")


@nox.session(python=["3.7", "3.8", "3.9"])
@nox.parametrize("airflow", ["2.1", "2.2.5", "2.3.4", "2.4", "2.5"])
def test(session: nox.Session, airflow: str) -> None:
    """Run both unit and integration tests."""

    env = {
        # Airflow automatically creates the ~/airflow directory on importing airflow
        # but we are not using this directory hence we change the home to a tmp directory to avoid side effects.
        "AIRFLOW_HOME": session.create_tmp(),
    }

    if airflow.startswith("2.2."):
        # We are duplicating the tests dependencies until we find a better solution.
        # The solution might be to move out of poetry.
        dev = ("pytest", "pytest-cov", "mypy", "types-pyyaml")
        session.install("-e", ".", *dev)
        # To install some versions of Airflow, we need constraints, due to issues like:
        # https://github.com/apache/airflow/issues/19804
        constraints_url = (
            "https://raw.githubusercontent.com/apache/airflow/"
            f"constraints-{airflow}/constraints-{session.python}.txt"
        )
        # Poetry does not support constraints:
        # https://github.com/python-poetry/poetry/issues/3225
        session.install(f"apache-airflow=={airflow}", "-c", constraints_url, env=env)

        session.log("Installed Dependencies:")
        session.run("pip3", "freeze")

        session.run("pytest", *session.posargs)
    else:
        session.install("poetry")
        session.run("poetry", "install", "--with", "dev")
        session.run("poetry", "run", "pip", "install", f"apache-airflow=={airflow}", env=env)

        session.log("Installed Dependencies:")
        session.run("poetry", "run", "pip3", "freeze")
        session.run("poetry", "run", "pytest", *session.posargs)


@nox.session(python=["3.8"])
def type_check(session: nox.Session) -> None:
    """Run MyPy checks."""
    session.install("poetry")
    session.run("poetry", "install", "--with", "type_check")
    session.run("mypy", "--version")
    session.run("mypy")


@nox.session()
def lint(session: nox.Session) -> None:
    """Run linters."""
    session.install("pre-commit")
    if session.posargs:
        args = [*session.posargs, "--all-files"]
    else:
        args = ["--all-files", "--show-diff-on-failure"]
    session.run("pre-commit", "run", *args)


@nox.session()
def build(session: nox.Session) -> None:
    """Build release artifacts."""
    session.install("build")

    # TODO: Automate version bumping, Git tagging, and more?

    dist = pathlib.Path("dist")
    if dist.exists() and next(dist.iterdir(), None) is not None:
        session.error(
            "There are files in dist/. Remove them and try again. "
            "You can use `git clean -fxdi -- dist` command to do this."
        )
    dist.mkdir(exist_ok=True)

    session.run("python", "-m", "build", *session.posargs)


@nox.session()
def release(session: nox.Session) -> None:
    """Publish a release."""
    session.install("twine")
    # TODO: Better artifact checking.
    session.run("twine", "check", *session.posargs)
    session.run("twine", "upload", *session.posargs)
