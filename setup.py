"""Setup.py for the Astronomer sample Airflow provider package. Built from datadog provider package for now."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-sample setup."""
setup(
    name='astronomer-sql-decorator',
    version="0.0.1",
    description='A decorator that allows users to run SQL queries natively in Airflow.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=astronomer_sql_decorator.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['astronomer_sql_decorator', 'astronomer_sql_decorator.operators'],
    install_requires=['apache-airflow>=2.0', 'pandas', 'apache-airflow-providers-postgres'],
    setup_requires=['setuptools', 'wheel'],
    author='Daniel Imberman, Plinio Guzman',
    author_email='daniel@astronomer.io',
    url='http://astronomer.io/',
    python_requires='~=3.7',
)
