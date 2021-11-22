"""Setup.py for the Astronomer sample Airflow provider package. Built from datadog provider package for now."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-sample setup."""
setup(
    name="astronomer-sql-decorator",
    version="0.0.1",
    description="A decorator that allows users to run SQL queries natively in Airflow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": ["provider_info=astro.__init__:get_provider_info"]
    },
    license="Apache License 2.0",
    packages=[
        "astro",
        "astro.dataframe",
        "astro.sql.operators",
        "astro.sql",
        "astro.utils",
    ],
    install_requires=[
        "apache-airflow>=2.0",
        "pandas>=1.3.4",
        "s3fs",
        "apache-airflow-providers-snowflake",
        "snowflake-sqlalchemy==1.2.0",
        "apache-airflow-providers-postgres",
        "snowflake-connector-python[pandas]",
        "boto3==1.18.65",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Daniel Imberman, Plinio Guzman",
    author_email="daniel@astronomer.io",
    url="http://astronomer.io/",
    python_requires="~=3.7",
)
