from typing import Dict, List, Set

from airflow.hooks.base import BaseHook
from sqlalchemy import MetaData, case, func, or_, select
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table
from astro.utils.schema_util import (
    get_error_string_for_multiple_dbs,
    get_table_name,
    tables_from_same_db,
)


class OutlierCheck:
    def __init__(
        self,
        name: str,
        columns_map: map,
        accepted_std_div: int,
        threshold: float,
    ) -> None:
        self.name = name
        self.columns_map = columns_map
        self.accepted_std_div = accepted_std_div
        self.threshold = threshold


class ChecksHandler:
    def __init__(self, checks: List[OutlierCheck], conn_type: str):
        self.checks = checks
        self.conn_type = conn_type

    def prepare_main_stats_sql(self, main_table: Table, main_table_sqla):
        select_expressions = []
        for check in self.checks:
            for key in check.columns_map:
                select_expressions.extend(
                    [
                        func.stddev(main_table_sqla.c.get(key)).label(
                            f"{check.name}_{key}_stddev"
                        ),
                        func.avg(main_table_sqla.c.get(key)).label(
                            f"{check.name}_{key}_avg"
                        ),
                    ]
                )
        return select(select_expressions).alias("main_stats_sql")

    def prepare_column_sql(self, check, main_stats, compare_table):
        column_sql = []
        for column in check.columns_map:
            main_table_col = column
            compare_table_col = compare_table.c.get(column)
            main_stats_check_avg = main_stats.c.get(
                f"{check.name}_{main_table_col}_avg"
            )
            main_stats_check_stddev = main_stats.c.get(
                f"{check.name}_{main_table_col}_stddev"
            )
            accepted_std_div = check.accepted_std_div

            statement = compare_table_col > (
                main_stats_check_avg + (main_stats_check_stddev * accepted_std_div)
            )
            column_sql.append(statement)
            statement = compare_table_col < (
                main_stats_check_avg - (main_stats_check_stddev * accepted_std_div)
            )
            column_sql.append(statement)

        return or_(*column_sql)

    def prepare_cases_sql(self, main_stats, compare_table_sqla):
        cases = []
        for check in self.checks:

            cases.append(
                case(
                    [
                        (
                            self.prepare_column_sql(
                                check, main_stats, compare_table_sqla
                            ),
                            1,
                        ),
                    ],
                    else_=0,
                ).label(check.name)
            )
        return cases

    def prepare_checks_sql(self, temp_table):
        aggregations = []
        for check in self.checks:
            aggregations.append(func.sum(temp_table.c.get(check.name)))
        return aggregations

    def prepare_comparison_sql(
        self, main_table: Table, compare_table: Table, engine, metadata_obj
    ):
        main_table_sqla = SqlaTable(
            get_table_name(main_table), metadata_obj, autoload_with=engine
        )
        compare_table_sqla = SqlaTable(
            get_table_name(compare_table), metadata_obj, autoload_with=engine
        )

        main_table_stats_sql = self.prepare_main_stats_sql(main_table, main_table_sqla)
        cases_sql = self.prepare_cases_sql(main_table_stats_sql, compare_table_sqla)
        cases_sql.append(compare_table_sqla)
        temp_table = select(cases_sql).alias("temp_table")
        checks_sql = self.prepare_checks_sql(temp_table)

        checks_sql.insert(0, func.count().label("total"))
        comparison_sql = select(checks_sql).select_from(temp_table)

        return comparison_sql

    def check_results(self, row, check, index):
        return row[index + 1]

    def evaluate_results(self, rows, max_rows_returned, conn_type):
        total_rows = rows[0][0]
        failed_rows_count = {
            check.name: self.check_results(rows[0], check, index)
            for index, check in enumerate(self.checks)
        }
        failed_checks = set()
        for check in self.checks:
            if failed_rows_count[check.name] / total_rows > check.threshold:
                failed_checks.add(check.name)
        return failed_checks

    def prepare_failed_checks_results(
        self,
        conn_type,
        main_table: Table,
        compare_table: Table,
        failed_checks: Set[str],
        max_rows_returned,
        engine,
        metadata_obj,
    ):
        main_table_sqla = SqlaTable(
            get_table_name(main_table), metadata_obj, autoload_with=engine
        )
        compare_table_sqla = SqlaTable(
            get_table_name(compare_table), metadata_obj, autoload_with=engine
        )

        main_stats = self.prepare_main_stats_sql(main_table, main_table_sqla)

        failed_checks_sql: List[tuple] = []
        for check in self.checks:
            if check.name in failed_checks:
                statement = (
                    select([compare_table_sqla])
                    .select_from(main_stats)
                    .select_from(compare_table_sqla)
                    .where(
                        self.prepare_column_sql(check, main_stats, compare_table_sqla)
                    )
                    .limit(max_rows_returned)
                )

                failed_checks_sql.append((check.name, statement, {}))
        return failed_checks_sql


class AgnosticStatsCheck(SqlDecoratedOperator):
    template_fields = ("table",)

    def __init__(
        self,
        checks: List[OutlierCheck],
        main_table: Table,
        compare_table: Table,
        max_rows_returned: int,
        **kwargs,
    ):
        """
        Operator to run Statistical checks on tables.

        :param main_table: main table
        :type main_table: table object
        :param compare_table: table to be compared
        :type compare_table: table object
        :param checks: check class object, which represent boolean expression
        :type checks: Check
        :param max_rows_returned: number of row returned if the check fails.
        :type max_rows_returned: int
        """

        self.main_table = main_table
        self.compare_table = compare_table
        self.max_rows_returned = max_rows_returned
        self.conn_id = main_table.conn_id
        self.checks = checks
        self.database = main_table.database

        task_id = main_table.table_name + "_" + "stats_check"

        if not tables_from_same_db([main_table, compare_table]):
            raise ValueError(
                get_error_string_for_multiple_dbs([main_table, compare_table])
            )

        def null_function():
            pass

        def handler_func(results):
            return results.fetchall()

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=main_table.conn_id,
            database=main_table.database,
            schema=main_table.schema,
            warehouse=main_table.warehouse,
            task_id=task_id,
            op_args=(),
            handler=handler_func,
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn_type = BaseHook.get_connection(self.conn_id).conn_type  # type: ignore
        checkHandler = ChecksHandler(self.checks, conn_type)
        metadata = MetaData()
        self.sql = checkHandler.prepare_comparison_sql(
            main_table=self.main_table,
            compare_table=self.compare_table,
            engine=self.get_sql_alchemy_engine(),
            metadata_obj=metadata,
        )

        results = super().execute(context)
        failed_checks = checkHandler.evaluate_results(
            results, self.max_rows_returned, conn_type
        )
        if len(failed_checks) > 0:
            failed_check_sql = checkHandler.prepare_failed_checks_results(
                conn_type=conn_type,
                main_table=self.main_table,
                compare_table=self.compare_table,
                failed_checks=failed_checks,
                max_rows_returned=self.max_rows_returned,
                engine=self.get_sql_alchemy_engine(),
                metadata_obj=metadata,
            )
            failed_values = {}
            for check_query in failed_check_sql:
                check, self.sql, self.parameters = check_query
                failed_values[check] = super().execute(context)

            raise ValueError("Stats Check Failed. %s", failed_values)


def stats_check(
    main_table: Table,
    compare_table: Table,
    checks: List[OutlierCheck] = [],
    max_rows_returned: int = 100,
):
    """
    :param main_table: main table
    :type main_table: table object
    :param compare_table: table to be compared
    :type compare_table: table object
    :param checks: check class object, which represent boolean expression
    :type checks: Check
    :param max_rows_returned: number of row returned if the check fails.
    :type max_rows_returned: int
    """

    return AgnosticStatsCheck(
        main_table=main_table,
        compare_table=compare_table,
        checks=checks,
        max_rows_returned=max_rows_returned,
    )
