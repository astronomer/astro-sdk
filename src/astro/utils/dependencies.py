"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from astro import constants


class MissingPackage(object):
    def __init__(self, module_name, related_extras):
        self.module_name = module_name
        self.related_extras = related_extras

    def __getattr__(self, item):
        raise RuntimeError(
            f"Error loading the module {self.module_name},"
            f" please make sure all the dependencies are installed."
            f" try - pip install {constants.PYPI_PROJECT_NAME}[{self.related_extras}]"
        )


try:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except ModuleNotFoundError:
    BigQueryHook = MissingPackage(
        "airflow.providers.google.cloud.hooks.bigquery", "gcp"
    )

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ModuleNotFoundError:
    PostgresHook = MissingPackage(
        "airflow.providers.postgres.hooks.postgres", "postgres"
    )

try:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
except ModuleNotFoundError:
    SnowflakeHook = MissingPackage(
        "airflow.providers.snowflake.hooks.snowflake", "snowflake"
    )
