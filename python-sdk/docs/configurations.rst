Configuration
=============

Configuring the database default schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If users don't define a specific ``Table`` (metadata) ``schema``, the Astro SDK will fall back to the global default schema configuration.

There are two options to define the default schema:
1. At a global level, for all databases
2. At a database level, for each specific database

If the user does not configure the database-specific configuration, the Astro SDK will use the global default schema (which has the value ``tmp_astro`` if undefined). Example:
environment variable :

.. code:: shell

   AIRFLOW__ASTRO_SDK__SCHEMA="tmp"

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   schema = "tmp"

We can also configure the default schema specific to the database type (example: specific to Snowflake, BigQuery, Postgres). If both the default and database-specific schemas are defined, the preference is given to the database-specific value.

.. code:: python

   AIRFLOW__ASTRO_SDK__POSTGRES_DEFAULT_SCHEMA = "tmp"

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   postgres_default_schema = "tmp"
