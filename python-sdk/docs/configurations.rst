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

   AIRFLOW__ASTRO_SDK__POSTGRES_DEFAULT_SCHEMA = "postgres_tmp"
   AIRFLOW__ASTRO_SDK__BIGQUERY_DEFAULT_SCHEMA = "bigquery_tmp"
   AIRFLOW__ASTRO_SDK__SNOWFLAKE_DEFAULT_SCHEMA = "snowflake_tmp"
   AIRFLOW__ASTRO_SDK__REDSHIFT_DEFAULT_SCHEMA = "redshift_tmp"

or by updating Airflow's configuration

.. code:: ini

   [astro_sdk]
   postgres_default_schema = "postgres_tmp"
   bigquery_default_schema = "bigquery_tmp"
   snowflake_default_schema = "snowflake_tmp"
   redshift_default_schema = "redshift_tmp"




Configuring the unsafe dataframe storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: shell

   AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE = False

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   dataframe_allow_unsafe_storage = True

Configuring the storage integration for Snowflake
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A storage integration is a Snowflake object that stores a generated identity and access management (IAM) entity for your external cloud storage, along with an optional set of allowed or blocked storage locations (Amazon S3, Google Cloud Storage, or Microsoft Azure). Cloud provider administrators in your organization grant permissions on the storage locations to the generated entity. This option allows users to avoid supplying credentials when creating stages or when loading or unloading data.

Read more at: `Snowflake storage integrations <https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html>`_

.. code:: shell

   AIRFLOW__ASTRO_SDK__SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = "aws_integration"
   AIRFLOW__ASTRO_SDK__SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = "gcp_integration"

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   snowflake_storage_integration_amazon = "aws_integration"
   snowflake_storage_integration_google = "gcp_integration"

Configuring the table autodetect row count
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Following configuration indicates how many file rows should be loaded to infer the table columns types. This defaults to 1000 rows.

.. code:: shell

   AIRFLOW__ASTRO_SDK__LOAD_TABLE_AUTODETECT_ROWS_COUNT = 1000

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   load_table_autodetect_rows_count = 1000

Configuring the RAW SQL maximum response size
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reduce responses sizes returned by aql.run_raw_sql to avoid trashing the Airflow DB if the BaseXCom is used.

.. code:: shell

   AIRFLOW__ASTRO_SDK__RUN_RAW_SQL_RESPONSE_SIZE = 1

or by updating Airflow's configuration

.. code:: shell

   [astro_sdk]
   run_raw_sql_response_size = 1
