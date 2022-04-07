from .base import BaseDB


class SQLite(BaseDB):

    # Connection types
    conn_types = ["sqlite"]

    @property
    def qualified_name(self):
        return self.table_name
