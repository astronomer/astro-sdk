********************
Google Cloud storage
********************

Transfer to Google Cloud storage as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Google cloud storage as destination as from following sources dataset:

#. :ref:`table`

    .. literalinclude:: ../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. Transfer using non-native approach
    Following is an example of non-native transfers between AWS S3 and Google cloud storage using non-native transfers:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_s3_to_gs]
       :end-before: [END transfer_non_native_s3_to_gs]


Examples
########
1. S3 to GCS transfers
    - Non-native
        Following is an example of non-native transfers between AWS S3 to Google cloud storage using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_gs]
               :end-before: [END transfer_non_native_s3_to_gs]


Transfer from Google Cloud storage as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Google cloud storage the following destination dataset:

#. :ref:`table`

    .. literalinclude:: ../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. Transfer using non-native approach
    Following is an example of non-native transfers between Google cloud storage and AWS S3 using non-native transfers:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_s3]
       :end-before: [END transfer_non_native_gs_to_s3]

2. Transfer using third-party platform

Examples
########
1. GCS to AWS S3 transfers
    - Non-native
        Following is an example of non-native transfers between Google cloud storage to AWS S3 using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_s3]
               :end-before: [END transfer_non_native_gs_to_s3]

2. GCS to Sqlite transfers
    - Non-native
        Following is an example of non-native transfers between Google cloud storage to Sqlite using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_sqlite]
               :end-before: [END transfer_non_native_gs_to_sqlite]

3. GCS to Snowflake transfers
    - Non-native
        Following is an example of non-native transfers between Google cloud storage to Snowflake using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_snowflake]
               :end-before: [END transfer_non_native_gs_to_snowflake]

4. GCS to Google Bigquery transfers
    - Non-native
        Following is an example of non-native transfers between Google cloud storage to Google Bigquery using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_bigquery]
               :end-before: [END transfer_non_native_gs_to_bigquery]
