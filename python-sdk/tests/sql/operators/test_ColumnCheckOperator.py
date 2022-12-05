import pathlib

from astro import sql as aql
from astro.files import File

CWD = pathlib.Path(__file__).parent


def test_column_check_operator(sample_dag):
    aql.ColumnCheckOperator(
        dataset=File(path=str(CWD) + "/../../data/homes2.csv"),
        column_mapping={
            "sell": {
                "null_check": {
                    "equal_to": 1,
                }
            }
        },
    ).execute({})
