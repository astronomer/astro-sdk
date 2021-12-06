import timeit

from astro import sql as aql

start = timeit.default_timer()
f = aql.load_file(
    "./integration-tests/test_data_s.csv",
    output_table_name="TEST_SCALE_100K",
)

f.operator.execute({"run_id": "foo"})
end = timeit.default_timer()

print(f"runtime {end-start}")
