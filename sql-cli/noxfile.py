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
    session.install("-e", "../python-sdk/.[all]")
    session.install("-e", "../python-sdk/.[tests]")
    session.install("poetry")
    session.run("poetry", "install")


@nox.session(python=["3.7", "3.8", "3.9"])
@nox.parametrize("airflow", ["2.2.5", "2.4"])
def test(session: nox.Session, airflow: str) -> None:
    """Run both unit and integration tests."""
    session.install("-e", "../python-sdk/.[all]")
    session.install("-e", "../python-sdk/.[tests]")
    session.install("poetry")
    session.run("poetry", "install")
    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")
    airflow_home = f"~/airflow-{airflow}-{session.python}"
    session.run("airflow", "db", "init", env={"AIRFLOW_HOME": airflow_home})
    session.run(
        "pytest",
        *session.posargs,
        "--cov=sql_cli",
        "--cov-report=xml",
        "--cov-branch",
        env={"AIRFLOW_HOME": airflow_home},
        external=True,
    )


@nox.session(python=["3.8"])
def type_check(session: nox.Session) -> None:
    """Run MyPy checks."""
    session.install("poetry")
    session.run("poetry", "install")
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
