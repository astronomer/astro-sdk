===================================================================================
:py:mod:`check_table operator <astro.sql.operators.data_validations.check_table>`
===================================================================================

.. _check_table:

When to use the ``check_table`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``check_table`` operator allows you add checks on table level.
For example

* Count the number of row
* Sum of a column
* Checks involving multiple columns

This operator is a wrapper around Airflow's `SQLTableCheckOperator <https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#check-sql-table-values>`_ to allow seamless integrations with SDK supported dataset like ``Astro tables``.

Supported Checks
~~~~~~~~~~~~~~~~
Supported checks are also explained `here <https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#check-sql-table-values>`_.

.. literalinclude:: ../../../../../example_dags/example_check_table.py
   :language: python
   :start-after: [START data_validation__check_table]
   :end-before: [END data_validation__check_table]
