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

"""A decorator that allows users to run SQL queries natively in Airflow."""

__version__ = "0.7.0"

from astro.dataframe import dataframe


# This is needed to allow Airflow to pick up specific metadata fields it needs
# for certain features. We recognize it's a bit unclean to define these in
# multiple places, but at this point it's the only workaround if you'd like
# your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        # Required.
        "package-name": "astro-projects",
        "name": "Astro SQL Provider",
        "description": __doc__,
        "versions": [__version__],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }
