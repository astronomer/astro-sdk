***************
Google Bigquery
***************

Transfer to Google Bigquery as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Google Bigquery from following sources:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. :ref:`non_native`
    Following is an example of a non-native transfer between GCS to BigQuery using non-native transfer:

    .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_bigquery]
       :end-before: [END transfer_non_native_gs_to_bigquery]

Examples
########
1. AWS S3 to Google Bigquery transfers
    - :ref:`non_native`
        Following is an example of a non-native transfer between AWS S3 to BigQuery using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_bigquery]
               :end-before: [END transfer_non_native_s3_to_bigquery]

2. GCS to Google Bigquery transfers
    - :ref:`non_native`
        Following is an example of a non-native transfer between GCS to BigQuery using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_bigquery]
               :end-before: [END transfer_non_native_gs_to_bigquery]

Transfer from Google Bigquery as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Google BigQuery to the following destination dataset:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. Transfer using non-native approach
    Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Transfer using third-party platform

Examples
########

1. Bigquery to Snowflake transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Bigquery to Sqlite transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between Bigquery to Sqlite using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_sqlite]
               :end-before: [END transfer_non_native_bigquery_to_sqlite]
