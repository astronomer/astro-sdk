import sqlalchemy
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


# TODO: This function should be removed after the refactor as this is handled in the Database
def create_sqlalchemy_engine_with_sqlite(hook: SqliteHook) -> sqlalchemy.engine.Engine:
    # Airflow uses sqlite3 library and not SqlAlchemy for SqliteHook
    # and it only uses the hostname directly.
    airflow_conn = hook.get_connection(getattr(hook, hook.conn_name_attr))
    engine = sqlalchemy.create_engine(f"sqlite:///{airflow_conn.host}")
    return engine
