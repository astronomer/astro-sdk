import pathlib

import pytest

from astro.constants import Database
from astro.databricks.load_options import default_delta_options
from astro.files import File

CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_autoloader_load_file_local(database_table_fixture):
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database, table = database_table_fixture
    database.load_file_to_table(
        input_file=File(filepath),
        output_table=table,
        load_options=default_delta_options,
        databricks_job_name="test_local_local",
    )
    assert database.table_exists(table)
    df = database.export_table_to_pandas_dataframe(table)
    assert not df.empty
    assert len(df) == 3
    assert df.columns.to_list() == ["id", "name"]
    database.drop_table(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_autoloader_load_file_s3(database_table_fixture):
    with pytest.raises(ValueError):
        file = File("s3://tmp9/databricks-test/", conn_id="default_aws")
        database, table = database_table_fixture
        database.load_file_to_table(
            input_file=file,
            output_table=table,
            load_options=default_delta_options,
        )
