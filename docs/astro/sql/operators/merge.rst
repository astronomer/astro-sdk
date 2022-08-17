.. _merge_operator:

===============
merge operator
===============

When to use the ``merge`` operator
----------------------------------

Unlike the :ref:`append_operator`, which expects data to be unique and conflict free, the ``merge`` operator allows you to to add data to an existing table with conflict resolution techniques like ``ignore`` and ``update``.

Prerequisites
------------

The ``merge`` operator runs different SQL queries behind the scenes based on the database used. Some databases only allow you to run these queries if there are constraints on the columns specified in the parameter ``target_conflict_columns``. Below is the list of supported databases and their constraint requirements.

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

You can create and add constraints on a table by providing them in the ``columns`` parameter of :ref:`load_file`. Refer to the :ref:`custom_schema` section for details.

.. literalinclude:: ../../../../example_dags/example_merge.py
   :language: python
   :start-after: [START merge_load_file_with_primary_key_example]
   :end-before: [END merge_load_file_with_primary_key_example]

When tables have the same schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the source and target tables have the same schema, you should specify the complete ``list`` or ``tuple`` of columns to the ``columns`` parameter. This is required so the ``merge`` function can determine which columns might have :ref:`merge_conflicts`. You can also select a subset of columns to be part of the merge.

.. literalinclude:: ../../../../example_dags/example_merge.py
   :language: python
   :start-after: [START merge_col_list_example]
   :end-before: [END merge_col_list_example]

When tables have different schemas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the source and target tables have different schemas, you should pass a ``dict`` object mapping the ``source_table`` columns to the ``target_table`` columns.

.. literalinclude:: ../../../../example_dags/example_merge.py
   :language: python
   :start-after: [START merge_col_dict_example]
   :end-before: [END merge_col_dict_example]

.. _merge_conflicts:

Conflicts
---------
Conflicts arise due to the ``target_table`` having constraints on certain columns. For example, your ``source_table`` and ``target_table`` may have duplicate primary keys. There are three strategies for resolving merge conflicts.

#. **ignore**

    This method will not update the target table with the source table data if there is a conflict. 

    To use this method, you should specify ``if_conflicts='ignore'`` and provide the column(s) with constraints to ``target_conflict_columns``. For this example, ``target_conflict_columns=['A']``.

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

    This method will update the target table with the source table data if there is a conflict.

    To use this method, you should specify ``if_conflicts='update'`` and provide the column(s) with constraints to ``target_conflict_columns``. For this example, ``target_conflict_columns=['A']``.

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
    This method will raise an exception if there are any conflicts at the time of merging.

    To use this method, you should specify ``if_conflicts='exception'``.
