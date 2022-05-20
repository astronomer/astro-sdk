from astro.sql.table import Metadata, Table
from astro.utils.table_handler import TableHandler


def test__set_variables_from_first_table_with_same_db_tables_in_op_args():
    """Test _set_variables_from_first_table() when the tables passed are with same tables"""

    def dummy_function(param_1: str, param_2: str):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.python_callable = dummy_function
    handler.op_kwargs = {"param_1": "dummy_value", "param_2": "dummy_value"}
    handler.parameters = {}
    handler.op_args = (
        Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
        Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
    )
    handler._set_variables_from_first_table()
    assert handler.conn_id == "conn_1"
    assert handler.database == "database_1"
    assert handler.schema == "scheme_1"


def test__set_variables_from_first_table_with_different_db_tables_in_op_args():
    """Test _set_variables_from_first_table() when the tables passed are with same tables"""

    def dummy_function(param_1: str, param_2: str):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.python_callable = dummy_function
    handler.op_kwargs = {"param_1": "dummy_value", "param_2": "dummy_value"}
    handler.parameters = {}
    handler.op_args = (
        Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
        Table(
            conn_id="conn_2",
            metadata=Metadata(
                database="database_2",
                schema="scheme_2",
            ),
        ),
    )

    handler._set_variables_from_first_table()
    assert not hasattr(handler, "conn_id")
    assert not hasattr(handler, "database")
    assert not hasattr(handler, "schema")
    assert not hasattr(handler, "warehouse")
    assert not hasattr(handler, "role")


def test__set_variables_from_first_table_with_same_db_tables_in_python_callable():
    """Test _set_variables_from_first_table() when the tables passed are with same tables in python_callable"""
    table_1 = Table(
        conn_id="conn_1",
        metadata=Metadata(
            database="database_1",
            schema="scheme_1",
        ),
    )
    table_2 = Table(
        conn_id="conn_1",
        metadata=Metadata(
            database="database_1",
            schema="scheme_1",
        ),
    )

    def dummy_function(param_1: Table, param_2: Table):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.python_callable = dummy_function
    handler.op_args = ()
    handler.parameters = {}
    handler.op_kwargs = {
        "param_1": table_1,
        "param_2": table_2,
    }

    handler._set_variables_from_first_table()
    assert handler.conn_id == "conn_1"
    assert handler.database == "database_1"
    assert handler.schema == "scheme_1"


def test__set_variables_from_first_table_with_different_db_tables_in_python_callable():
    """Test _set_variables_from_first_table() when the tables passed are with same tables in python_callable"""
    table_1 = Table(
        conn_id="conn_1",
        metadata=Metadata(
            database="database_1",
            schema="scheme_1",
        ),
    )
    table_2 = Table(
        conn_id="conn_2",
        metadata=Metadata(
            database="database_2",
            schema="scheme_2",
        ),
    )

    def dummy_function(param_1: Table, param_2: Table):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.python_callable = dummy_function
    handler.op_args = ()
    handler.parameters = {}
    handler.op_kwargs = {
        "param_1": table_1,
        "param_2": table_2,
    }

    handler._set_variables_from_first_table()
    assert not hasattr(handler, "conn_id")
    assert not hasattr(handler, "database")
    assert not hasattr(handler, "schema")
    assert not hasattr(handler, "warehouse")
    assert not hasattr(handler, "role")


def test__set_variables_from_first_table_with_same_db_tables_in_parameters():
    """Test _set_variables_from_first_table() when the tables passed are with same tables in parameters"""

    def dummy_function(param_1: str, param_2: str):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.op_args = ()
    handler.python_callable = dummy_function
    handler.op_kwargs = {"param_1": "dummy_value", "param_2": "dummy_value"}
    handler.parameters = {
        "param_1": Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
        "param_3": Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
    }

    handler._set_variables_from_first_table()

    assert handler.conn_id == "conn_1"
    assert handler.database == "database_1"
    assert handler.schema == "scheme_1"


def test__set_variables_from_first_table_with_different_db_tables_in_parameters():
    """Test _set_variables_from_first_table() when the tables passed are with same tables in parameters"""

    def dummy_function(param_1: str, param_2: str):  # skipcq: PTC-W0049, PY-D0003
        pass

    handler = TableHandler()
    handler.op_args = ()
    handler.python_callable = dummy_function
    handler.op_kwargs = {"param_1": "dummy_value", "param_2": "dummy_value"}
    handler.parameters = {
        "param_1": Table(
            conn_id="conn_1",
            metadata=Metadata(
                database="database_1",
                schema="scheme_1",
            ),
        ),
        "param_3": Table(
            conn_id="conn_2",
            metadata=Metadata(
                database="database_2",
                schema="scheme_2",
            ),
        ),
    }

    handler._set_variables_from_first_table()
    assert not hasattr(handler, "conn_id")
    assert not hasattr(handler, "database")
    assert not hasattr(handler, "schema")
    assert not hasattr(handler, "warehouse")
    assert not hasattr(handler, "role")
