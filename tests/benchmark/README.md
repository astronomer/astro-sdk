# Benchmarking

This section contains tooling, which helps evaluate how long **astro** tasks take and how much computation resources they consume, given one or multiple datasets.


## Requirements

### Datasets

If **astro** supports a dataset file format, you can use it to benchmark.

We selected a subset of datasets as a starting point. These datasets can be available:
* locally
* GCP GCS
* AWS S3

To download the sample datasets locally, run:

```
GCS_BUCKET=<YOUR_GCS_BUCKEt> ./download_datasets.sh
```

The GCS bucket `<YOUR_GCS_BUCKEt>` is only required to download the `github_timeline`, and you should have write permissions in this bucket.

Summary of the sample datasets:


|         | size   | rows      | columns | file                     |                            |
|---------|--------|-----------|---------|--------------------------|----------------------------|
| few_kb  | 45 KB  | 732       | 8       | /tmp/covid_overview.csv  | UK Covid overview sample   |
| many_kb | 472 KB | 3532      | 9       | /tmp/artist_data.csv     | Tate Gallery artist sample |
| few_mb  | 20 MB  | 1,213,991 | 3       | /tmp/title_ratings.csv   | IMDB title ratings sample  |
| many_mb | 279 MB | 385,817   | 199     | /tmp/github_timeline.csv | Github timeline sample     |


### Configuration

Edit the file `config.json` based on the datasets and databases you would like to benchmark.

There is a complete example at `./config-sample.json`.

Example of content:
```
{
  "databases": [
    {
      "name": "postgres",
      "params": {
        "database": "postgres",
        "conn_id": "postgres_conn",
        "schema": "postgres"
      }
    }
  ],
  "datasets": [
    {
        "name": "few_kb",
        "path":"/tmp/covid_overview.csv"
    },
    {
        "name": "many_mb",
        "path": "/tmp/github_timeline.csv"
    }
  ]
}
```

The `datasets` section should match the datasets of choice. It has two mandatory properties:
* `name`: the label used to refer to this dataset
* `path`: the local, GCS, or S3 path to the dataset

The `databases` section must match existing connections or connections declared in the file `test-connections.yml`, available at the `astro` root directory. It must contain:
* `name`: the label used to refer to this database
* `params`: a dictionary containing the configuration to the database. The properties `database` and `conn_id` are required.


## Execute the benchmark

The following command will run one DAG for each combination of database x dataset (in the case of the example, four combinations):
```
./run.sh

```

If you're interested in running each test case more times, this is an example of how to run five times each combination:
```
./run.sh 5
```

It is also possible to specify the chunk size, if the `load_file` or `export_file` operators are being used, by setting an integer value in the environment variable `ASTRO_CHUNKSIZE`.

The command outputs something similar to:
```
Benchmark test started
- Input configuration: /home/test-user/Code/astro/tests/benchmark/config.json
- Output: /tmp/results-2022-02-10T14:36:09.ndjson
```

The benchmark results are stored in `/tmp`, and the name is generated based on when the analysis started. In this case, the name is:

This is an example of the content of `/tmp/results-2022-02-10T14:36:09.ndjson`:
```
{"duration": "1.66s", "rss": "125.37MB", "vms": "990.95MB", "shared": "41.73MB", "dag_id": "load_file_few_kb_into_postgres", "execution_date": "2022-02-12 00:33:56.463244+00:00", "revision": "e1fb164"}
{"duration": "2.28s", "rss": "138.56MB", "vms": "1.0GB", "shared": "41.55MB", "dag_id": "load_file_many_kb_into_postgres", "execution_date": "2022-02-12 00:33:59.808866+00:00", "revision": "e1fb164"}
{"duration": "1.2min", "rss": "157.17MB", "vms": "1.02GB", "shared": "41.9MB", "dag_id": "load_file_few_mb_into_postgres", "execution_date": "2022-02-12 00:34:03.794995+00:00", "revision": "e1fb164"}
```

### Publishing Benchmark results

To publish the results to bigquery table, we need to create an environment variable `ASTRO_PUBLISH_BENCHMARK_DATA` and set it to `True`. Once this is done the benchmarking data can be seen in bigquery table `astronomer-dag-authoring.Benchmark.load_files_to_database`.

## Analyze the results

After running the benchmark, it is possible to run an analysis script using:

```
./analyse.py --results-filepath=/tmp/some-results.ndjson
```

If there are multiple runs, it calculates the mean/average per (database/dataset) combination.

It exports the results in markdown tables, using stdout.

This is an example running the benchmark for the astro `78805a` revision and chunk size size `1,000,000`:
* Ubuntu 20.04.3 LTS
* CPU: 8-Core 11th Gen Intel Core i7-11800H
* Memory: 64 GiB

### Database: bigquery

| database   | dataset   | total_time   | memory_rss   | memory_shared   | memory_pss   | cpu_time_user   | cpu_time_system   |
|:-----------|:----------|:-------------|:-------------|:----------------|:-------------|:----------------|:------------------|
| bigquery   | few_kb    | 11.28s       | 120.73MB     | 40.91MB         | 118.02MB     | 3.5s            | 1.63s             |
| bigquery   | many_kb   | 18.57s       | 122.39MB     | 40.68MB         | 119.74MB     | 3.55s           | 1.58s             |
| bigquery   | few_mb    | 42.99min     | 140.99MB     | 40.94MB         | 138.21MB     | 20.07s          | 2.27s             |

### Database: postgres

| database   | dataset   | total_time   | memory_rss   | memory_shared   | memory_pss   | cpu_time_user   | cpu_time_system   |
|:-----------|:----------|:-------------|:-------------|:----------------|:-------------|:----------------|:------------------|
| postgres   | few_kb    | 1.73s        | 125.62MB     | 41.89MB         | 122.95MB     | 1.95s           | 1.4s              |
| postgres   | many_kb   | 2.24s        | 138.5MB      | 41.78MB         | 135.95MB     | 2.4s            | 1.38s             |
| postgres   | few_mb    | 1.23min      | 156.85MB     | 41.96MB         | 154.15MB     | 1.09min         | 1.74s             |
| postgres   | many_mb   | 23.94min     | 1.23GB       | 41.95MB         | 1.23GB       | 22.15min        | 31.5s             |


### Database: snowflake

| database   | dataset   | total_time   | memory_rss   | memory_shared   | memory_pss   | cpu_time_user   | cpu_time_system   |
|:-----------|:----------|:-------------|:-------------|:----------------|:-------------|:----------------|:------------------|
| snowflake  | few_kb    | 14.28s       | 136.06MB     | 50.39MB         | 132.96MB     | 3.12s           | 1.48s             |
| snowflake  | many_kb   | 12.42s       | 138.83MB     | 50.22MB         | 135.85MB     | 3.17s           | 1.48s             |
|
| snowflake  | few_mb    | 7.21min      | 264.67MB     | 50.2MB          | 261.61MB     | 25.95s          | 3.27s             |
