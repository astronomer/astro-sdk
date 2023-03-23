Configuration
=============

Configuring the database default schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Universal Transfer Operator uses the global default schema configuration for the ``Table`` metadata, unless users
specify a different specific ``Table`` (metadata) ``schema``.

User can define the default schema in two ways:
1. Globally, for all the databases
2. Individually, for each database

The Universal transfer operator applies the global default schema (``tmp_transfers`` by default) when the user does not
set up a database-specific configuration. Example:
environment variable :

.. code:: ini

   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__SQL_SCHEMA="tmp_transfers"

or by updating Airflow's configuration

.. code:: ini

   [universal_transfer_operator]
   schema = "tmp_transfers"

We can set up the default schema for each database type, such as Snowflake, BigQuery, or Postgres. The database-specific schema overrides the global default schema if both are defined.

.. code:: ini

   [universal_transfer_operator]
   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__POSTGRES_DEFAULT_SCHEMA = "postgres_tmp"
   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__BIGQUERY_DEFAULT_SCHEMA = "bigquery_tmp"
   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__SNOWFLAKE_DEFAULT_SCHEMA = "snowflake_tmp"
   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__REDSHIFT_DEFAULT_SCHEMA = "redshift_tmp"
   AIRFLOW__UNIVERSAL_TRANSFER_OPERATOR__MSSQL_DEFAULT_SCHEMA = "mssql_tmp"


or by updating Airflow's configuration

.. code:: ini

   [universal_transfer_operator]
   postgres_default_schema = "postgres_tmp"
   bigquery_default_schema = "bigquery_tmp"
   snowflake_default_schema = "snowflake_tmp"
   redshift_default_schema = "redshift_tmp"
   mssql_default_schema = "mssql_tmp"
