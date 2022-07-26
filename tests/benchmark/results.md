# Benchmark Results

## Dataset
Details about the dataset used can be found at [dataset.md](datasets.md)

## Performance evaluation of loading datasets from GCS with Astro Python SDK 0.9.2 into BigQuery
The configuration used for this benchmarking can be found here [config.json](config.json)

### Database: bigquery
The benchmark ran with chunk size size 1,000,000 and following VM details:
For Machine types: e2-medium
- VM Image: Debian GNU/Linux 11 (bullseye)
- CPU:2 vCPU
- Memory: 4 GB memory

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   | memory_pss   | memory_shared   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|:-------------|:----------------|
| bigquery   | five_gb    | 13.06min     | 50.92MB      | 1.43min         | 9.06s             | 61.54MB      | 12.24MB         |
| bigquery   | hundred_kb | 9.88s        | 21.89MB      | 540.0ms         | 50.0ms            | 16.96MB      | 12.31MB         |
| bigquery   | one_gb     | 2.34min      | 27.98MB      | 16.99s          | 1.82s             | 28.93MB      | 10.83MB         |
| bigquery   | ten_gb     | 25.83min     | 37.03MB      | 2.7min          | 17.68s            | 75.59MB      | 11.09MB         |
| bigquery   | ten_kb     | 7.58s        | 37.27MB      | 570.0ms         | 60.0ms            | 29.67MB      | 15.59MB         |
| bigquery   | ten_mb     | 11.8s        | 34.79MB      | 1.22s           | 280.0ms           | 35.92MB      | 11.27MB         |

For Machine types: n2-standard-4
- VM Image: Debian GNU/Linux 11 (bullseye)
- CPU:4 vCPUs
- Memory: 16 GB memory

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   | memory_pss   | memory_shared   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|:-------------|:----------------|
| bigquery   | five_gb    | 14.17min     | 52.93MB      | 1.41min         | 6.94s             | 64.24MB      | 11.52MB         |
| bigquery   | hundred_kb | 8.68s        | 20.54MB      | 3.63s           | 250.0ms           | 13.8MB       | 10.03MB         |
| bigquery   | one_gb     | 2.43min      | 26.75MB      | 15.04s          | 1.5s              | 27.28MB      | 11.55MB         |
| bigquery   | ten_gb     | 29.22min     | 43.85MB      | 2.68min         | 13.29s            | 82.42MB      | 11.23MB         |
| bigquery   | ten_kb     | 9.57s        | 30.13MB      | 3.69s           | 220.0ms           | 24.97MB      | 15.76MB         |
| bigquery   | ten_mb     | 34.96s       | 34.5MB       | 3.9s            | 410.0ms           | 35.58MB      | 11.55MB         |


#### Baseline using `bq load`

|Dataset                                    |Size |Duration(h-m-s)|
|-------------------------------------------|-----|---------------|
|covid_overview/covid_overview_10kb.csv     |10 KB|0:00:02        |
|tate_britain/artist_data_100kb.csv         |100KB|0:00:02        |
|imdb/title_ratings_10mb.csv                |10MB |0:00:05        |
|stackoverflow/stackoverflow_posts_1g.ndjson|1GB  |0:00:50        |
|trimmed/pypi/*                             |5GB  |0:00:41        |
|github/github-archive/*                    |10GB |0:01:09        |


#### Baseline using `GCSToBigQueryOperator` using [benchmark_gcs_to_bigquery.py](dags/benchmark_gcs_to_big_query.py)

|Dataset                                    |Size | Duration(seconds)  |
|-------------------------------------------|-----|--------------------|
|covid_overview/covid_overview_10kb.csv     |10 KB| 5.129522           |
|tate_britain/artist_data_100kb.csv         |100KB| 3.319834           |
|imdb/title_ratings_10mb.csv                |10MB | 5.558414           |
|stackoverflow/stackoverflow_posts_1g.ndjson|1GB  | 85.409014          |
|trimmed/pypi/*                             |5GB  | 48.973093          |

#### Results post optimization to load files from GCS to Bigquery using BigqueryHook

|Dataset                                    |Size | Duration(seconds)  |
|-------------------------------------------|-----|--------------------|
|covid_overview/covid_overview_10kb.csv     |10 KB| 13.57           |
|tate_britain/artist_data_100kb.csv         |100KB| 16.10          |
|imdb/title_ratings_10mb.csv                |10MB | 19.40         |
|stackoverflow/stackoverflow_posts_1g.ndjson|1GB  | 30.26          |
|trimmed/pypi/*                             |5GB  | 59.90          |


## Performance evaluation of loading datasets from GCS with Astro Python SDK 0.11.0 into Snowflake

### Without native support

Astro Python SDK 0.11.0 only support loading to Snowflake using Pandas, without any further optimizations.

The benchmark was run as a Kubernetes job in GKE:

* Version: `astro-sdk-python` 0.11.0 (`36a3042`)
* Machine type: `n2-standard-4`
  * vCPU: 4
  * Memory: 16 GB RAM
* Container resource limit:
  * Memory: 10 Gi

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|
| snowflake  | ten_kb     | 4.75s        | 59.3MB       | 1.45s           | 100.0ms           |
| snowflake  | hundred_kb | 4.7s         | 54.96MB      | 1.42s           | 90.0ms            |
| snowflake  | ten_mb     | 10.4s        | 65.4MB       | 2.27s           | 240.0ms           |
| snowflake  | one_gb     | 4.96min      | 57.3MB       | 14.21s          | 1.16s             |
| snowflake  | five_gb    | 24.46min     | 97.85MB      | 1.43min         | 5.94s             |
| snowflake  | ten_gb     | 50.85min     | 104.53MB     | 2.7min          | 12.11s            |

### With native support

The benchmark was run as a Kubernetes job in GKE:

* Version: `astro-sdk-python` 1.0.0a1 (`bc58830`)
* Machine type: `n2-standard-4`
  * vCPU: 4
  * Memory: 16 GB RAM
* Container resource limit:
  * Memory: 10 Gi

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|
| snowflake  | ten_kb     | 9.1s         | 56.45MB      | 2.56s           | 110.0ms           |
| snowflake  | hundred_kb | 9.19s        | 45.4MB       | 2.55s           | 120.0ms           |
| snowflake  | ten_mb     | 10.9s        | 47.51MB      | 2.58s           | 160.0ms           |
| snowflake  | one_gb     | 1.07min      | 47.94MB      | 8.7s            | 5.67s             |
| snowflake  | five_gb    | 5.49min      | 53.69MB      | 18.76s          | 1.6s              |

### Database: postgres

| database                | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:------------------------|:-----------|:-------------|:-------------|:----------------|:------------------|
| postgres_conn_benchmark | few_kb     | 494.42ms     | 36.06MB      | 570.0ms         | 50.0ms            |
| postgres_conn_benchmark | ten_kb     | 689.05ms     | 42.96MB      | 540.0ms         | 40.0ms            |
| postgres_conn_benchmark | hundred_kb | 580.7ms      | 36.43MB      | 570.0ms         | 50.0ms            |
| postgres_conn_benchmark | ten_mb     | 44.67s       | 1.38GB       | 31.03s          | 4.03s             |
| postgres_conn_benchmark | one_gb     | 5.56min      | 62.5MB       | 14.07s          | 1.14s             |
| postgres_conn_benchmark | five_gb    | 24.44min     | 78.15MB      | 1.34min         | 5.73s             |
| postgres_conn_benchmark | ten_gb     | 45.64min     | 61.71MB      | 2.37min         | 11.48s            |
