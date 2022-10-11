import os

publish_benchmarks = os.getenv("ASTRO_PUBLISH_BENCHMARK_DATA", False)
publish_benchmarks_db_conn_id = os.getenv("ASTRO_PUBLISH_BENCHMARK_DB_CONN_ID", "bigquery")
publish_benchmarks_schema = os.getenv("ASTRO_PUBLISH_BENCHMARK_SCHEMA", "benchmark")
publish_benchmarks_table = os.getenv("ASTRO_PUBLISH_BENCHMARK_TABLE", "load_files_to_databaseV3")
publish_benchmarks_table_grouping_col = os.getenv("ASTRO_PUBLISH_BENCHMARK_GROUPING_COL", "revision")
