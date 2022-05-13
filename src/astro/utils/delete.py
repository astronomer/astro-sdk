import pandas as pd
from airflow.hooks.base import BaseHook

from astro.databases import create_database
from astro.sql.tables import Metadata, Table
from astro.utils.database import get_sqlalchemy_engine, run_sql
from astro.utils.load import load_dataframe_into_sql_table


def delete_dataframe_rows_from_table(
    pandas_dataframe: pd.DataFrame,
    target_table: Table,
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
    tmp_table = Table(
        conn_id=target_table.conn_id,
        metadata=Metadata(
            database=getattr(target_table.metadata, "database", None),
            warehouse=getattr(target_table.metadata, "warehouse", None),
            role=getattr(target_table.metadata, "role", None),
        ),
    )
    # tmp_table_name = create_unique_table_name()
    # named_table = tmp_table.to_table(tmp_table_name)
    load_dataframe_into_sql_table(pandas_dataframe, tmp_table, hook)

    # Then we remove the (dataframe) temporary table values from the target table
    engine = get_sqlalchemy_engine(hook)
    db = create_database(target_table.conn_id)
    run_sql(
        engine,
        f"DELETE FROM {db.get_table_qualified_name(target_table)} "
        f"WHERE Id IN (SELECT Id FROM {db.get_table_qualified_name(tmp_table)})",
    )

    # Finally, we delete the temporary table which had the dataframe values
    run_sql(engine, f"DROP TABLE {db.get_table_qualified_name(tmp_table)}")
