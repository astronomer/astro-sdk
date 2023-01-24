===================================================================================
:py:mod:`check_column operator <astro.sql.operators.data_validations.check_column>`
===================================================================================

.. _check_column:

When to use the ``check_column`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``check_column`` operator allows you add checks on columns of tables and dataframes. This operator is a wrapper around Airflow's `SQLColumnCheckOperator <https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#check-sql-table-columns>`_ to allow seamless integrations with SDK supported dataset like ``Astro tables`` and an extension to support ``Pandas dataframe``.

Supported Checks
~~~~~~~~~~~~~~~~
Supported checks are also explained `here <https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#check-sql-table-columns>`_.

.. literalinclude:: ../../../../../example_dags/example_check_column.py
   :language: python
   :start-after: [START data_validation__check_column]
   :end-before: [END data_validation__check_column]
