"""A decorator that allows users to run SQL queries natively in Airflow."""

__version__ = "0.0.1"
from astro.dataframe import from_sql as python


# This is needed to allow Airflow to pick up specific metadata fields it needs
# for certain features. We recognize it's a bit unclean to define these in
# multiple places, but at this point it's the only workaround if you'd like
# your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        # Required.
        "package-name": "astro",
        "name": "Astro SQL Provider",
        "description": __doc__,
        "versions": [__version__],
        # Optional.
        "hook-class-names": [],
        "extra-links": [],
    }
