.. _databricks:

======================
Databricks Integration
======================
Introduction

The Astro Python SDK now allows users to easily access and work with data stored in Databricks through the Delta API.

The Astro Python SDK provides a simple, yet powerful interface for working with Spark and Delta tables in Databricks, allowing users to take advantage of the scalability and performance of Databricks while maintaining the flexibility of Airflow DAG authoring and all the benefits of Airflow scheduling.

Installation
============
To use the Astro Python SDK with Databricks, complete the following steps:

The first step is to install the databricks submodule of the astro-sdk-python pip library. This can be done by running the following command:

.. code-block:: bash

    pip install 'astro-sdk-python[databricks]'

The second step is to create a connection to your Databricks cluster.
This requires creating a `personal access token <https://docs.databricks.com/dev-tools/api/latest/authentication.html>`_ in Databricks and creating a cluster with an http_endpoint. Once you have these, you can create a connection using the following syntax:


.. code-block:: yaml

    - conn_id: databricks_conn
      conn_type: databricks
      host: https://my-cluster.cloud.databricks.com/
      password: my-token
      extra:
        http_path: /sql/1.0/warehouses/foobar

.. note::

    You need to update the ``password`` with your token from Databricks.

You can also set a default databricks cluster to run all jobs against with this environment variable:

.. code-block:: bash

    AIRFLOW__ASTRO_SDK__DATABRICKS_CLUSTER_ID=<my databricks cluster>

Loading Data
============
Once you have installed the astro-sdk package and created a connection to your Databricks cluster, you can begin loading data into your Databricks cluster.

To load data into your Databricks cluster, you can use the same ``aql.load_file()`` function that works for all other databases.
The only thing that is different with delta is that you now can use the ``delta_options`` parameter to specify delta specific parameters(such as ``copy_options`` and ``format_options`` for the ``COPY INTO`` command).
Please note that when loading data into Delta using ``COPY INTO``, you must specify the filetype as Databricks does not automatically infer the data (this is not the case for autoloader).

Currently, only local files can be loaded using the ``aql.load_file()`` function. Support for loading data from S3 and GCS will be added soon.

To use the ``aql.load_file()`` function, you will need to specify the path to the file you want to load, the target Delta table you want to pass the result to.

.. code-block:: python

    aql.load_file(
        input_file=File("data.csv"),
        output_table=Table(conn_id="my_databricks_conn"),
    )

To load data into databrick, you only need to set the ``AIRFLOW__ASTRO_SDK__DATABRICKS_CLUSTER_ID`` env variable
so the Astro SDK knows where to send your load_file job.
Autoloader Support
==================

Autoloader is a powerful data loading feature in Databricks that allows users to efficiently load large amounts of data into delta tables. It has several benefits over traditional methods such as COPY INTO:

* Incremental loading: Autoloader can detect new files in a directory and only load those, rather than all files every time. This makes it more efficient for loading data on a regular basis.
* Schema inference: Autoloader can automatically infer the schema of the data being loaded, making it easier to get up and running quickly with new datasets.
* Scalability: Autoloader is designed to handle very large datasets, making it a more scalable option for data loading.

By default, the Astro SDK uses autoloader to load data into Databricks. However, if you want to use COPY INTO instead, you can set the load_mode option in your load_options object like this:

.. code-block:: python

    from astro.databricks.load_options import DeltaLoadOptions
    from astro.constants import DatabricksLoadMode

    delta_options = DeltaLoadOptions.get_default_delta_options()
    delta_options.load_mode = DatabricksLoadMode.COPY_INTO
    aql.load_file(
        input_file=File("data.csv"),
        output_table=Table(conn_id="my_databricks_conn"),
        load_options=delta_options,
    )


COPY INTO Options
=================
If you have extra options you would like to add, you can user the ``load_options`` parameter to pass ``copy_into_parameters`` into the ``COPY INTO`` command.

Please note that we by default set ``header`` and ``inferSchema`` to true, so if you pass in your own commands you will need to set those values explicitly.

.. code-block:: python

    from astro.databricks.load_options import DeltaLoadOptions

    aql.load_file(
        input_file=File("data.csv"),
        output_table=Table(conn_id="my_databricks_conn"),
        databricks_options=DeltaLoadOptions(copy_into_format_options={"header": "true"}),
    )

We also offer a ``astro.databricks.load_options.default_delta_options`` for those who do not want to manually set options.

Loading files from S3
=====================

There are two options for loading data to s3:

The first option is to pass in an s3 conn_id to the aql.load_file function, as shown in the example below:

.. code-block:: python

    file = File("s3://tmp9/databricks-test/", conn_id="default_aws", filetype=FileType.CSV)
    aql.load_file(
        input_file=file,
        output_table=Table(conn_id="my_databricks_conn"),
    )

The second option is to pre-load your s3 secrets into the databricks cluster before setting up.
Instructions for this can be found `here <https://docs.databricks.com/external-data/amazon-s3.html>`_. This approach has the benefit of not passing any sensitive information to databricks,
but at the expense of the ability to load arbitrary datasets into your databricks cluster.

If you want to go with this option, set the environment variable ``AIRLFOW__ASTRO_SDK__LOAD_STORAGE_CONFIGS_TO_DATABRICKS`` to False.
This will ensure that the Astro SDK does not attempt to load any information to databricks.
You can also set this value on a per-job basis using the ``astro.databricks.DeltaLoadOptions`` class.

Loading files from GCS
======================

GCS support works very similar to how S3 support is mentioned above. Users who want to manage their databricks loading manually
can follow `This guide <https://docs.gcp.databricks.com/external-data/gcs.html>`_ and set ``AIRLFOW__ASTRO_SDK__LOAD_STORAGE_CONFIGS_TO_DATABRICKS`` to False.
For those who want Airflow to handle access management, simply offer a gcs_conn in their file and all necessary credentials
will be loaded to databricks using the secrets API.

.. code-block:: python

    file = File("gs://tmp9/databricks-test/", conn_id="gcp_conn", filetype=FileType.CSV)
    aql.load_file(
        input_file=file,
        output_table=Table(conn_id="my_databricks_conn"),
    )


NOTE:
-----
In order to use the GCS -> Databricks automatic connection, we require one of these to be true:
1. You set ``key_path`` to your auth file in the ``extras`` section of your GCS connection
2. You set ``keyfile_dict`` to a dictionary of credentials in the ``extras`` section of your GCS connection
3. You set the environment variable ``GOOGLE_APPLICATION_CREDENTIALS``

Querying Data
=============
Once you have loaded your data into Databricks, you can use the ``aql.transform()`` functions to create queries against the Delta tables. We currently do not support arbitrary Spark Python, but users can pass resulting Delta tables into local Pandas DataFrames (though please be careful of how large of a table you are passing).

For example, you can use the ``aql.transform()`` function decorator to create a query that selects all users over the age of 30 and returns the results as a Pandas DataFrame:

.. code-block:: python

    @aql.transform()
    def get_eligible_users(user_table):
        return "SELECT * FROM {{user_table}} WHERE age > 30"


    with dag:
        user_table = aql.load_file(
            input_file=File("data.csv"),
            output_table=Table(conn_id="my_databricks_conn"),
            databricks_options={
                "copy_into_options": {"format_options": {"header": "true"}}
            },
        )
        results = get_eligible_users(user_table)

Parameterized Queries
=====================

The aql.transform() function in the Astro Python SDK allows users to create parameterized queries that can be executed with different values for the parameters. This is useful for reusing queries and for preventing SQL injection attacks.

To create a parameterized query, you can use double brackets ({{ and }}) to enclose the parameter names in the query string. The aql.transform() function will replace the parameter names with the corresponding values when the query is executed.

For example, you can create a parameterized query to select all users over a specified age like this:

.. code-block:: python

    @aql.transform()
    def my_query(table: Table, age: int):
        return "SELECT * FROM {{ table }} WHERE age > {{ age }}"

The aql.transform() function will replace {{ table }} with users and {{ age }} with 30, and then run the resulting query against the Delta table.
