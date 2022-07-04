"""A decorator that allows users to run SQL queries natively in Airflow."""

__version__ = "0.11.0"


# The following line is an import work-around to avoid raising a circular dependency issue related to `create_database`
# Without this, if we run the following imports, in this specific order:
#   from astro.databases import create_database
#   from astro.sql.table import Metadata, Table, create_unique_table_name
# We face ImportError, as it happened in:
# https://github.com/astronomer/astro-sdk/pull/396/commits/fbe73bdbe46d65777258a5f79f461ef69f08a673
# https://github.com/astronomer/astro-sdk/actions/runs/2378526135
# Although astro.database does not depend on astro.sql, it depends on astro.sql.table - and, unless astro.sql was
# imported beforehand, it will also load astro.sql. In astro.sql we import lots of operators which depend on
# astro.database, and this is what leads to the circular dependency.

from airflow.configuration import conf

import astro.sql  # noqa: F401


# This is needed to allow Airflow to pick up specific metadata fields it needs
# for certain features. We recognize it's a bit unclean to define these in
# multiple places, but at this point it's the only workaround if you'd like
# your custom conn type to show up in the Airflow UI.
def get_provider_info() -> dict:
    return {
        # Required.
        "package-name": "astro-sdk-python",
        "name": "Astro SQL Provider",
        "description": __doc__,
        "versions": [__version__],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }


if not conf.getboolean(section="core", key="enable_xcom_pickling"):
    raise OSError(
        "AIRFLOW__CORE__ENABLE_XCOM_PICKLING environment variable needs to be set to True or enable_xcom_pickling=true "
        "in airflow.cfg before importing astro-sdk-python."
    )
