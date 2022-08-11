======================================
merge operator
======================================

When to use the ``merge`` operator
----------------------------------

Merge operator is used when you expects some conflicts while merging two tables due to underline constraints. If you don't expect any conflicts you can use :ref:`append_operator`. There are two common cases when merging tables.

When tables have same schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If the tables have same schema, we have to specify the complete ``list`` or ``tuple`` of columns in ``columns`` parameters because AstroSDK also have to handle :ref:`merge_conflicts` and it cannot assume which columns can raise conflicts, unlike :ref:`append_operator`. User can also select subset of columns to be part of merge.



When table have different schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If tables have different schema user is expected to pass a ``dict`` mapped from ``source_table`` columns to ``target_table`` columns.



.. _merge_conflicts:
Conflicts
~~~~~~~~~
Conflicts arises due to ``target_table`` having constraints on columns. For example we can have duplicate primary key in ``source_table`` and ``target_table``. Now there are three strategy to deal with this situation.

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


    .. list-table:: post merge Target table
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


    .. list-table:: post merge Target table
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
    This option will simply raise an exception if there is any conflicts at the time of merging.
