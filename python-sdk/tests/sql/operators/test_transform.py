import tempfile
from unittest import mock

from astro import sql as aql
from tests.sql.operators import utils as test_utils


class MockReturn:
    _scalar = []

    def scalar(self):
        return self._scalar


@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_transform_calls_with_query_tag(run_sql, sample_dag):
    from astro.query_modifier import QueryModifier

    run_sql.execute.return_value = MockReturn()

    with sample_dag:

        @aql.transform(
            conn_id="sqlite_default",
            query_modifier=QueryModifier(pre_queries=["ALTER team_1", "ALTER team_2"]),
        )
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    enriched_query = run_sql.method_calls[1].args[0].text
    assert enriched_query.startswith("ALTER team_1;ALTER team_2;CREATE TABLE IF NOT EXISTS ")
    assert enriched_query.endswith("AS SELECT 1+1")


@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_transform_file_calls_with_query_tag(run_sql, sample_dag):
    from astro.query_modifier import QueryModifier

    run_sql.execute.return_value = MockReturn()

    with tempfile.NamedTemporaryFile(suffix=".sql") as tmp_file:
        tmp_file.write(b"SELECT 1+1")
        tmp_file.flush()

        with sample_dag:
            aql.transform_file(
                file_path=tmp_file.name,
                conn_id="sqlite_default",
                query_modifier=QueryModifier(pre_queries=["ALTER team_1", "ALTER team_2"]),
            )
        test_utils.run_dag(sample_dag)

        enriched_query = run_sql.method_calls[1].args[0].text
        assert enriched_query.startswith("ALTER team_1;ALTER team_2;CREATE TABLE IF NOT EXISTS ")
        assert enriched_query.endswith("AS SELECT 1+1")
