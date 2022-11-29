import pytest

from astro.constants import Database
from astro.files import File


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
    file = File("s3://tmp9/databricks-test/", conn_id="default_aws")
    database, table = database_table_fixture
    database.load_file_to_table(input_file=file, output_table=table)
    assert database.table_exists(table)
    database.drop_table(table)
