# Benchmark Results

## Dataset Used
The following datasets are used for running performance benchmarking on Astro SDK `load_file` feature.

All of them are based on real external datasets. For reproducibility of the benchmark tests, they were copied to:
`gs://astro-sdk/benchmark/`

Within `gs://astro-sdk/benchmark/`, there are two paths:
* `original`: files with original size
* `trimmed`: files trimmed to meet the desired sizes (for example, 10 GB)

|            | size   | format  | rows      | columns | path to trimmed file(s)                     | description                 |
|------------|--------|---------|-----------|---------|---------------------------------------------|-----------------------------|
| ten_kb     | 10 KB  | parquet | 160       | 8       | covid_overview/covid_overview_10kb.parquet  | UK Covid overview sample    |
| hundred_kb | 100 KB | csv     | 748       | 9       | tate_britain/artist_data_100kb.csv          | Tate Gallery artist sample  |
| ten_mb     | 10 MB  | csv     | 600,000   | 3       | imdb/title_ratings_10mb.csv                 | IMDB title ratings sample   |
| hundred_mb | 100 MB | csv     | 139,519   | 199     | github/github_timeline_100mb.csv            | Github timeline sample      |
| one_gb     | 1 GB   | ndjson  | 940,000   | 17 (*)  | stackoverflow/stackoverflow_posts_1g.ndjson | Stack Overflow posts sample |
| five_gb    | 5 GB   | ndjson  | 7,530,243 | 7 (*)   | pypi/*                                      | PyPI downloads sample       |
| ten_gb     | 10 GB  | ndjson  | 1,263,685 | 9 (*)   | github/github-archive/*                     | Github timeline sample      |
(*) Nested JSON, this number represents just the root-level properties


## Performance evaluation of loading datasets from GCS with Astro Python SDK 0.9.2 into BigQuery
The configuration used for this benchmarking can be found here [config.json](config.json)


| dataset_size | duration (seconds) | memory_full_info.rss | memory_full_info.vms | memory_full_info.shared | memory_full_info.text | memory_full_info.lib | memory_full_info.data | memory_full_info.dirty | memory_full_info.uss | memory_full_info.pss | memory_full_info.swap | cpu_time.user      | cpu_time.system | cpu_time.children_user | cpu_time.children_system | cpu_time.iowait | disk_usage | io_counters.0.read_count | io_counters.0.write_count | io_counters.0.read_bytes | io_counters.0.write_bytes | io_counters.0.read_chars | io_counters.0.write_chars | dag_id                         | execution_date                   | revision | chunk_size |
|--------------|--------------------| -------------------- | -------------------- | ----------------------- | --------------------- | -------------------- | --------------------- | ---------------------- | -------------------- | -------------------- | --------------------- | ------------------ | --------------- | ---------------------- | ------------------------ | --------------- | ---------- | ------------------------ | ------------------------- | ------------------------ | ------------------------- | ------------------------ | ------------------------- | ------------------------------ | -------------------------------- | -------- | ---------- |
| ten_kb       | 7.584598064422607  | 37265408             | 404275200            | 15585280                | 0                     | 0                    | 62201856              | 0                      | 26099712             | 29671424             | 0                     | 0.5700000000000003 | 0.06            | 0                      | 0                        | 0.02            | 0          | 1285                     | 332                       | 0                        | 585728                    | 3681666                  | 537932                    | load_file_ten_kb_into_bigquery | 2022-06-16 14:07:20.442378+00:00 | ad76dc5  | 1000000    |
| hundred_kb   | 9.883461952209473  | 21893120             | 14598144             | 12308480                | 0                     | 0                    | 10182656              | 0                      | 10768384             | 16963584             | 0                     | 0.5399999999999996 | 0.050000000000000044 | 0                      | 0                        | 0.02            | 4096       | 1383                     | 338                       | 0                        | 630784                    | 3776367                  | 620579                    | load_file_hundred_kb_into_bigquery | 2022-06-16 14:07:20.435489+00:00 | ad76dc5  | 1000000    |
| ten_mb       | 11.796650886535645 | 34787328             | 79327232             | 11268096                | 0                     | 0                    | 74911744              | 0                      | 31330304             | 35921920             | 0                     | 1.2200000000000002 | 0.28            | 0                      | 0                        | 0.05            | 4096       | 5200                     | 647                       | 0                        | 5279744                   | 18775545                 | 9984362                   | load_file_ten_mb_into_bigquery | 2022-06-16 14:07:20.394071+00:00 | ad76dc5  | 1000000    |
| one_gb       | 140.586843252182 | 27975680             | 25366528             | 10825728                | 0                     | 0                    | 20951040              | 0                      | 17833984             | 28928000             | 0                     | 16.99         | 1.8200000000000003 | 0                      | 0                        | 0.02            | 4096       | 282054                   | 4408                      | 0                        | 1036288                   | 1053828202               | 6955028                   | load_file_one_gb_into_bigquery | 2022-06-16 14:07:20.411368+00:00 | ad76dc5  | 1000000    |
| five_gb      | 783.5595722198486 | 50921472             | 44675072             | 12242944                | 0                     | 0                    | 40259584              | 0                      | 42237952             | 61539328             | 0                     | 85.71000000000001 | 9.06            | 0                      | 0                        | 0.02            | 8192       | 1449504                  | 23610                     | 0                        | 3309568                   | 5413302990               | 34677372                  | load_file_five_gb_into_bigquery | 2022-06-16 14:07:20.427967+00:00 | ad76dc5  | 1000000    |
| ten_gb       | 1549.6354126930237 | 37027840             | 38432768             | 11091968                | 0                     | 0                    | 34017280              | 0                      | 84156416             | 75588608             | 0                     | 162.06        | 17.680000000000003 | 0                      | 0                        | 0.03            | 8192       | 2883845                  | 45012                     | 0                        | 9596928                   | 10831004708              | 76636592                  | load_file_ten_gb_into_bigquery | 2022-06-16 14:07:20.355620+00:00 | ad76dc5  | 1000000    |
