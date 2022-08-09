======================================
load_file operator
======================================

When to use the ``load_file`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two use cases of the ``load_file`` operator.

#. When we want a (group of) file(s) to be loaded into a database table
#. When we want a (group of) file(s) to be loaded to a Pandas dataframe

Case 1: Load into a database table
    you need to pass the ``output_table`` param to the load_file operator to convert it to a table and the operator returns an instance of the table object passed in ``output_table``. The output table will be created if it doesn't exist and will be replaced if it does. We can change this behavior with the `if_exists` parameter.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_1]
       :end-before: [END load_file_example_1]

Case 2: Load into pandas dataframe
    If you don't pass the ``output_table`` to the load_file operator it converts the file into a pandas dataframe and returns the reference to dataframe.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_2]
       :end-before: [END load_file_example_2]

.. _custom_schema:

Parameters to use when loading a file to the database table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. **if_exists** - This parameter comes in handy when the table you trying to create already exists. In such a case, we have two options, either replace or append. Which can we accomplish by passing ``if_exists='append'`` or ``if_exists='replace'``.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_4]
       :end-before: [END load_file_example_4]

    Note - When we are using ``if_exists='replace'`` we are dropping the existing table and then creating a new table. Here we are not reusing the schema.

#. **output_table** - We can specify the output table to be created by passing in this parameter, which is expected to be an instance of ``astro.sql.table.Table``. Users can specify the schema of tables by passing in the ``columns`` parameter of ``astro.sql.table.Table`` object, which is expected to be a list of the instance of ``sqlalchemy.Column``. If the user doesn't specify the schema, the schema is inferred using pandas.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_5]
       :end-before: [END load_file_example_5]

#. **columns_names_capitalization** - Only applies when we want to create table in the ``Snowflake`` database with :ref:`table_schema` - auto schema detect and with ``if_exists=replace``. Default is to convert all the columns to lowercase. Users can change behavior by this parameter, valid values are ``lower`` and ``upper``, if the user gives ``original`` we convert cols to lowercase.


#. **ndjson_normalize_sep** - This parameter is useful when the input file type is NDJSON. Since NDJSON file can be multidimensional, we normalize the data to two-dimensional data, so that it is suitable to be loaded into a table and this parameter is used as a delimiter for combining columns names if required.
    example:
        input JSON:

        .. code:: python

           {"a": {"b": "c"}}

        output table:

        .. list-table::
           :widths: auto

           * - a_b
           * - c

    Note - columns a and b are merged to form one col a_b and `_` is used as a delimiter.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_3]
       :end-before: [END load_file_example_3]

.. _table_schema:

Inferring Table Schema
~~~~~~~~~~~~~~~~~~~~~~

There are two ways to get the schema of the table to be created, listed by priority

#. **User specified schema** - Users can specify the schema of the table to be created in the Table object like the ``output_table`` section in :ref:`custom_schema`

#. **Auto schema detection** - if the user doesn't specify the schema in the table object then by using the top 1000 rows of the table we infer the schema of the table. The default value is 1000, which can be changed by creating an environment variable

    .. code:: shell

       Shell
       AIRFLOW__ASTRO_SDK_LOAD_TABLE_AUTODETECT_ROWS_COUNT

    or within airflow config

    .. code:: shell

       [astro_sdk]
       load_table_autodetect_rows_count = 1000

    Note - this only applies to :ref:`filetype` JSON, NDJSON and CSV, PARQUET have type information and we don't need to infer it.


Parameters to use when loading a file to pandas dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. ``columns_names_capitalization`` Control col names case of the dataframe generated from the file. The default value is ``original``.
        *  **original** - Remains the same as the input file
        *  **upper** - Convert to uppercase
        *  **lower** - Convert to lowercase

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_6]
       :end-before: [END load_file_example_6]



Parameters for native transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please refer :ref:`load_file_working` for detail on Native Path.

#. **use_native_support** - native transfer support is available for some FileSource and Databases, if it is available the default is to use this path. To leverage these paths certain settings/changes need to be done on destination databases. If for some reason users don't want to use these paths they can turn off this behavior by passing ``use_native_support=False``.
        This feature is enabled by default, to disable it refer to the below code.

        .. literalinclude:: ../../../../example_dags/example_load_file.py
           :language: python
           :start-after: [START load_file_example_7]
           :end-before: [END load_file_example_7]

        To check if the native transfer will be used for data transfer for a combination of file location and database, refer section :ref:`supported_native_path`

        **Case when you would like to turn off native transfer:**

            * There are some limitations and/or additional services that need to be enabled on the database to use native transfer.

                example - https://cloud.google.com/bigquery-transfer/docs/s3-transfer

            * There may be some additional costs associated due to the services used to perform the native transfer.

            * Native transfers are overkill in cases when we want to transfer the smaller file. It may take lesser time with the default approach.

#. **native_support_kwargs** - Since we support multiple databases they may require some parameters to process a file or control error rate etc, those parameters can be passed in ``native_support_kwargs``. These parameters are passed to the destination database.

        Check for valid parameters based on **file location** and **database** combination in section :ref:`supported_native_path`

        .. literalinclude:: ../../../../example_dags/example_load_file.py
           :language: python
           :start-after: [START load_file_example_8]
           :end-before: [END load_file_example_8]


#. **enable_native_fallback** -  When ``use_native_support`` is True, we try to use the native transfer, and if this fails we try to use the default path to load data, giving the user a warning. If you want to change this behavior pass ``enable_native_fallback=False``.

        .. literalinclude:: ../../../../example_dags/example_load_file.py
           :language: python
           :start-after: [START load_file_example_9]
           :end-before: [END load_file_example_9]

.. _supported_native_path:

Supported native transfers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: auto

   * - File Location
     - Database
     - native_support_kwargs params
   * - S3
     - Bigquery
     - https://cloud.google.com/bigquery-transfer/docs/s3-transfer#bq
   * - GCS
     - Bigquery
     - https://cloud.google.com/bigquery-transfer/docs/cloud-storage-transfer#bq
   * - S3
     - Snowflake
     - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
   * - GCS
     - Snowflake
     - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html


Patterns in File path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Load file can also resolve patterns in file path, there are three types of patterns supported by load file based on the :ref:`file_location`

#. **Local** - On local we support glob pattern - https://docs.python.org/3/library/glob.html
#. **S3** - prefix in file path - https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
#. **GCS** - prefix and wildcard in file path - https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_10]
       :end-before: [END load_file_example_10]


Inferring File Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two ways we infer :ref:`filetype` in the following order:

#. **File object** - If the user has passed the ``filetype`` param while declaring the ``astro.files.File`` object, we use that as file type. Valid values are listed :ref:`filetype`

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_10]
       :end-before: [END load_file_example_10]

    Note - This param becomes mandatory when the file path don't have extension.

#. **From file extensions** - When we create ``astro.files.File`` object and passed a fully qualified path like below, file extensions are used to infer file types. Here the file type is CSV.

    .. literalinclude:: ../../../../example_dags/example_load_file.py
       :language: python
       :start-after: [START load_file_example_4]
       :end-before: [END load_file_example_4]

Note - 1st way take priority over 2nd way.

Loading data from HTTP API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Users can also load data from HTTP API

.. literalinclude:: ../../../../example_dags/example_google_bigquery_gcs_load_and_save.py
   :language: python
   :start-after: [START load_file_http_example]
   :end-before: [END load_file_http_example]
