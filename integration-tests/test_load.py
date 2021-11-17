import timeit

from astronomer_sql_decorator import sql as aql

start = timeit.default_timer()
f = aql.load_file(
    "./integration-tests/test_data_s.csv",
    output_conn_id="snowflake_conn",
    output_table_name="TEST_SCALE_100K",
)

f.operator.execute(None)
end = timeit.default_timer()

print(f"runtime {end-start}")
