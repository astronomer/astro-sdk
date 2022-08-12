.. _merge_operator:

======================================
merge operator
======================================

When to use the ``merge`` operator
----------------------------------

Unlike the :ref:`append_operator`, which expects data to be unique and conflict free, the Merge operator allows users to add data with conflict resolution techniques like ``ignore`` and ``update``.

Prerequisite
------------

Merge operator internally runs different SQL queries based on the databases and some databases only allow you to run these queries if there are constraints on columns specified in parameter ``target_conflict_columns``. Below is the list of databases and their constraint requirement.

.. list-table::
   :widths: auto
   :header-rows: 1

   * - Database
     - Constraint required
   * - Bigquery
     - No
   * - Postgres
     - Yes
   * - Snowflake
     - Yes
   * - SQLite
     - Yes


Schema
------

When tables have the same schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the tables have the same schema, we have to specify the complete ``list`` or ``tuple`` of columns in ``columns`` parameters because AstroSDK also has to handle :ref:`merge_conflicts` and it cannot assume which columns can raise conflicts, unlike :ref:`append_operator`. Users can also select a subset of columns to be part of the merge.

.. literalinclude:: ../../../../example_dags/example_merge.py
   :language: python
   :start-after: [START merge_col_list_example]
   :end-before: [END merge_col_list_example]

When tables have different schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If tables have different schema user is expected to pass a ``dict`` mapped from ``source_table`` columns to ``target_table`` columns.

.. literalinclude:: ../../../../example_dags/example_merge.py
   :language: python
   :start-after: [START merge_col_dict_example]
   :end-before: [END merge_col_dict_example]

.. _merge_conflicts:

Conflicts
---------
Conflicts arise due to ``target_table`` having constraints on columns. For example, we can have duplicate primary keys in ``source_table`` and ``target_table``. Now there are three strategies to deal with this situation.

#. **ignore**

    Assumption - There is a unique constraint on table col A and user has specified ``if_conflicts='ignore'`` and ``target_conflict_columns=['A']``.

    .. list-table:: Target
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 1
         - 2
       * - 3
         - 4

    .. list-table:: Source
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 8
         - 2
       * - 3
         - 9


    .. list-table:: post-merge Target table
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 1
         - 2
       * - 3
         - 4
       * - 8
         - 2

#. **update**

    Assumption - There is a unique constraint on table col A and user has specified ``if_conflicts='update'`` and ``target_conflict_columns=['A']``.

    .. list-table:: Target
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 1
         - 2
       * - 3
         - 4

    .. list-table:: Source
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 8
         - 2
       * - 3
         - 9


    .. list-table:: post-merge Target table
       :widths: auto
       :header-rows: 1

       * - A
         - B
       * - 1
         - 2
       * - 3
         - 9
       * - 8
         - 2

#. **exception**
    This option will simply raise an exception if there are any conflicts at the time of merging.
