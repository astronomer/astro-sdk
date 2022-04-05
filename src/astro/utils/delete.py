from typing import Union

import pandas as pd
from airflow.hooks.base import BaseHook

from astro.sql.table import Table, TempTable, create_unique_table_name
from astro.utils.database import get_sqlalchemy_engine, run_sql
from astro.utils.load import load_dataframe_into_sql_table


def delete_dataframe_rows_from_table(
    pandas_dataframe: pd.DataFrame,
    target_table: Union[Table, TempTable],
    hook: BaseHook,
):
    """
    Deletes all SQL table records which match the dataframe rows.

    :param pandas_dataframe: Dataframe containing values to be delete from table
    :param target_table: Target table which will have rows deleted
    :param hook: Airflow hook related to the desired Database
    :type pandas_dataframe: pandas.Dataframe
    :type target_table: astro.table.Table
    :type hook: Airflow Hook to the target database
    """
    # First we create a temporary table using the dataframe values
    tmp_table = TempTable(
        conn_id=target_table.conn_id,
        database=target_table.database,
        warehouse=target_table.warehouse,
        role=target_table.role,
    )
    tmp_table_name = create_unique_table_name()
    named_table = tmp_table.to_table(tmp_table_name)
    load_dataframe_into_sql_table(pandas_dataframe, named_table, hook)

    # Then we remove the (dataframe) temporary table values from the target table
    engine = get_sqlalchemy_engine(hook)
    run_sql(
        engine,
        f"DELETE FROM {target_table.qualified_name()} WHERE Id IN (SELECT Id FROM {named_table.qualified_name()})",
    )

    # Finally, we delete the temporary table which had the dataframe values
    run_sql(engine, f"DROP TABLE {named_table.qualified_name()}")
