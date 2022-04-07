from abc import ABCMeta

from astronew.table import Table


class BaseDB(metaclass=ABCMeta):

    # Connection types
    conn_types = []
    max_table_size = 63

    def __init__(self, table: Table):
        self.table = table
        self.table_name = self.table.table_name
        self.conn_id = self.table.conn_id
        self.database = self.table.database
        self.schema = self.table.schema
        self.warehouse = self.table.warehouse

    @property
    def get_hook(self):
        return ...

    def get_connection(self):
        return ...

    def get_sqlalchemy_engine(self):
        pass

    def load_file(self, file):
        pass

    def save_file(self, file):
        pass

    def load_pandas_dataframe(self, df, chunksize, if_exists):
        pass

    def get_pandas_dataframe(self):
        pass

    def run_sql(self, sql):
        pass

    def generate_table_name(self, table_name):
        pass

    def generate_temp_table_name(self, table_name):
        pass

    @property
    def qualified_name(self):
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def create_table(self):
        pass

    def drop_table(self):
        pass

    def schema_exists(self):
        pass

    def identifier_args(self):
        """For Merge"""
        return (self.schema, self.table_name) if self.schema else (self.table_name,)
