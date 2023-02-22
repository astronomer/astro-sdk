"""Nox automation definitions."""


import nox

nox.options.sessions = ["dev"]
nox.options.reuse_existing_virtualenvs = True


@nox.session(python="3.10")
def dev(session: nox.Session) -> None:
    """Create a dev environment with everything installed.

    This is useful for setting up IDE for autocompletion etc. Point the
    development environment to ``.nox/dev``.
    """
    session.install("nox")
    session.install("-e", ".[all,tests]")


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
@nox.parametrize("airflow", ["2.2.5", "2.4", "2.5.0"])
def test(session: nox.Session, airflow) -> None:
    """Run both unit and integration tests."""
    env = {
        "AIRFLOW_HOME": f"~/airflow-{airflow}-python-{session.python}",
    }

    if airflow == "2.2.5":
        env[
            "AIRFLOW__CORE__XCOM_BACKEND"
        ] = "astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend"
        env["AIRFLOW__ASTRO_SDK__STORE_DATA_LOCAL_DEV"] = "True"

        # If you need a pinned version of a provider to be present in a nox session then
        # update the constraints file used below with that  version of provider
        # For example as part of MSSQL support we need apache-airflow-providers-microsoft-mssql>=3.2 and this
        # has been updated in the below constraint file.
        session.install(f"apache-airflow=={airflow}", "-c", "tests/modified_constraint_file.txt")
        session.install("-e", ".[all,tests]", "-c", "tests/modified_constraint_file.txt")
        session.install("apache-airflow-providers-common-sql==1.2.0")
        # install smart-open 6.3.0 since it has FTP implementation
        session.install("smart-open>=6.3.0")
    else:
        env["AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES"] = "airflow\\.* astro\\.*"

        session.install(f"apache-airflow=={airflow}")
        session.install("-e", ".[all,tests]")

    # Log all the installed dependencies
    session.log("Installed Dependencies:")
    session.run("pip3", "freeze")

    session.run("airflow", "db", "init", env=env)

    # Since pytest is not installed in the nox session directly, we need to set `external=true`.
    session.run(
        "pytest",
        "-vv",
        *session.posargs,
        env=env,
        external=True,
    )


@nox.session(python=["3.8"])
def type_check(session: nox.Session) -> None:
    """Run MyPy checks."""
    session.install("-e", ".[all,tests]")
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
