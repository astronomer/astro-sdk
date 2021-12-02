class Table:
    def __init__(
        self, table_name="", conn_id=None, database=None, schema=None, warehouse=None
    ):
        self.table_name = table_name
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
