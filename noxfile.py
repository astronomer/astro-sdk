"""Nox automation definitions."""

import pathlib

import nox

nox.options.sessions = ["dev"]


@nox.session(python="3.9")
def dev(session: nox.Session) -> None:
    """Create a dev environment with everything installed.

    This is useful for setting up IDE for autocompletion etc. Point the
    development environment to ``.nox/dev``.
    """
    session.install("nox")
    session.install("-e", ".[all]")
    session.install("-e", ".[tests]")


@nox.session(python=["3.7", "3.8", "3.9"])
def test(session: nox.Session) -> None:
    """Run unit tests."""
    session.install("-e", ".[all]")
    session.install("-e", ".[tests]")
    session.run("airflow", "db", "init")
    session.run("pytest", *session.posargs)


@nox.session()
@nox.parametrize(
    "extras",
    [
        ("postgres-only", {"include": ["postgres"], "exclude": ["amazon"]}),
        ("postgres-amazon", {"include": ["postgres", "amazon"]}),
        ("snowflake-amazon", {"include": ["snowflake", "amazon"]}),
        ("sqlite", {"include": ["sqlite"]}),
    ],
)
def test_examples_by_dependency(session: nox.Session, extras):
    _, extras = extras
    pypi_deps = ",".join(extras["include"])
    pytest_options = " and ".join(extras["include"])
    pytest_options = " and not ".join([pytest_options, *extras.get("exclude", [])])
    pytest_args = ["-k", pytest_options]

    session.install("-e", f".[{pypi_deps}]")
    session.install("-e", f".[tests]")
    session.run("airflow", "db", "init")

    session.run("pytest", "tests/test_example_dags.py", *pytest_args, *session.posargs)


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
