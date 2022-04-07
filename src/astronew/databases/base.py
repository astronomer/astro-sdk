from abc import ABCMeta


class BaseDB(metaclass=ABCMeta):

    # Connection types
    conn_types = []

    def __init__(self):
        ...

    def get_hook(self):
        pass

    def load_file(self, file):
        pass

    def save_file(self, file):
        pass

    def get_pandas_dataframe(self):
        pass

    def generate_table_name(self, table_name):
        pass

    def generate_temp_table_name(self, table_name):
        pass

    @property
    def qualified_name(self):
        return f"{self.schema}.{self.table}"

    def create_table(self):
        pass

    def drop_table(self):
        pass
