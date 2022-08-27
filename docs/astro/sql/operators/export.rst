.. _export_file:

====================
export_file operator
====================

.. _export_file_operator:

When to use the ``export_file`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``export_file`` operator allows you to write SQL tables to CSV or parquet files and store them locally, on S3, or on GCS. The ``export_file`` function can export data from :ref:`supported_databases` or a Pandas dataframe.

There are two main uses for the ``export_file`` operator.

Case 1: Export data from a table.

    .. literalinclude:: ../../../../example_dags/example_google_bigquery_gcs_load_and_save.py
       :language: python
       :start-after: [START export_example_1]
       :end-before: [END export_example_1]

Case 2: Export data from a Pandas dataframe.

    .. literalinclude:: ../../../../example_dags/example_google_bigquery_gcs_load_and_save.py
       :language: python
       :start-after: [START export_example_2]
       :end-before: [END export_example_2]
