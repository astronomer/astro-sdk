.. _universal_transfer_operator:

========================================================================================================
:py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>`
========================================================================================================

When to use the ``universal_transfer_operator`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Universal Transfer Operator allows data transfers between any supported source :ref:`dataset` and destination :ref:`dataset`. It offers a consistent agnostic interface, simplifying the users' experience, so they do not need to use specific providers or operators for transfers.

This ensures a consistent set of :py:mod:`Data Providers <universal_transfer_operator.data_providers>` that can read from and write to :ref:`dataset`. The Universal Transfer
Operator can use the respective :py:mod:`Data Providers <universal_transfer_operator.data_providers>` to transfer between as a source and a destination. It also takes advantage of any existing fast and
direct high-speed endpoints, such as Snowflake’s built-in ``COPY INTO`` command to load S3 files efficiently into the Snowflake.

Universal transfer operator also supports the transfers using third-party platforms like Fivetran.

.. to edit figure below refer - https://drive.google.com/file/d/1Ih0SRnMvgKTQHLJaW9k21jutjEiyacRz/view?usp=sharing
.. figure:: /images/approach.png

There are three modes to transfer data using of the ``universal_transfer_operator``.

1. :ref:`non_native`
2. :ref:`native`
3. :ref:`third_party`

More details on how transfer works can be found at :ref:`transfer_working`.

Case 1: Transfer using non-native approach
    Following is an example of non-native transfers between Google cloud storage and Sqlite:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_sqlite]
       :end-before: [END transfer_non_native_gs_to_sqlite]

Case 2: Transfer using native approach

Case 3: Transfer using third-party platform
    Here is an example of how to use Fivetran for transfers:

    .. literalinclude:: ../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_with_setup]
       :end-before: [END fivetran_transfer_with_setup]
