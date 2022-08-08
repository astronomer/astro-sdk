Configuration
=============

Configuring the database default schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can configure the default schema that will be used for all operation involving database.
environment variable :

.. code:: shell

   AIRFLOW__ASTRO_SDK__SCHEMA="tmp"

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   schema = "tmp"

We can also configure schema on database level.

.. code:: python

   AIRFLOW__ASTRO_SDK__POSTGRES_DEFAULT_SCHEMA = "tmp"

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   postgres_default_schema = "tmp"
