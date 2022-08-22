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
| snowflake  | ten_gb     | 7.9min       | 53.68MB      | 34.41s          | 2.66s             |

## Performance evaluation of loading datasets from GCS with Astro Python SDK 0.11.0 into Postgres in K8s

### Without native support
The benchmark was run as a Kubernetes job in GKE:

* Version: `astro-sdk-python` 1.0.0a1 (`bc58830`)
* Machine type: `n2-standard-4`
  * vCPU: 4
  * Memory: 16 GB RAM
* Container resource limit:
  * Memory: 10 Gi

| database                | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:------------------------|:-----------|:-------------|:-------------|:----------------|:------------------|
| postgres_conn_benchmark | few_kb     | 494.42ms     | 36.06MB      | 570.0ms         | 50.0ms            |
| postgres_conn_benchmark | ten_kb     | 689.05ms     | 42.96MB      | 540.0ms         | 40.0ms            |
| postgres_conn_benchmark | hundred_kb | 580.7ms      | 36.43MB      | 570.0ms         | 50.0ms            |
| postgres_conn_benchmark | ten_mb     | 44.67s       | 1.38GB       | 31.03s          | 4.03s             |
| postgres_conn_benchmark | one_gb     | 5.56min      | 62.5MB       | 14.07s          | 1.14s             |
| postgres_conn_benchmark | five_gb    | 24.44min     | 78.15MB      | 1.34min         | 5.73s             |
| postgres_conn_benchmark | ten_gb     | 45.64min     | 61.71MB      | 2.37min         | 11.48s            |

### With native support
Two primary optimizations that have led to significant speed up

The first optimization is to not directly place a smart_open into the read function of pandas. There have been multiple reports of pandas and smart_open working very slow when used together. Instead, we read the smart_open file into an IO buffer (either StringIO or BytesIO). This also has the benefit that we do not need to write to disc if there is enough memory to hold the object.

The second optimization is postgres specific. The pandas.to_sql function uses the HIGHLY suboptimal INSERT statement for postgres. This is 10X slower than the COPY command. By saving objects as CSV buffers, we can use COPY to load the data into postgres as a stream instead of one value at a time.

Here are the new numbers with the optimizations:

The benchmark was run as a Kubernetes job in GKE:

* Version: `astro-sdk-python` 1.0.0a1 (`bc58830`)
* Machine type: `n2-standard-4`
  * vCPU: 4
  * Memory: 16 GB RAM
* Container resource limit:
  * Memory: 10 Gi

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|
| postgres   | ten_kb     | 432.93ms     | 43.19MB      | 540.0ms         | 60.0ms            |
| postgres   | hundred_kb | 417.2ms      | 31.34MB      | 560.0ms         | 40.0ms            |
| postgres   | ten_mb     | 2.21s        | 79.91MB      | 1.76s           | 170.0ms           |
| postgres   | one_gb     | 13.9s        | 35.43MB      | 7.97s           | 5.76s             |
| postgres   | five_gb    | 50.23s       | 43.27MB      | 26.64s          | 18.7s             |
| postgres   | ten_gb     | 3.11min      | 43.29MB      | 1.36min         | 1.48min           |


### Database S3 to Bigquery using default path
Note - These results are generated manually, there is a issue added for the same [#574](https://github.com/astronomer/astro-sdk/issues/574)

|  database | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:----------|:-----------|:-------------|:-------------|:----------------|:------------------|
| bigquery  | hundred_kb | 18.64s       | 54.51MB      | 20.45ms         | 5.73ms            |
| bigquery  | one_gb     | 1.18hrs      | 6.56GB       | 8.4s            | 1.59s             |
| bigquery  | ten_kb     | 19.02s       | 53.84MB      | 17.9ms          | 5.2ms             |
| bigquery  | ten_mb     | 47.02s       | 201.8MB      | 32.79ms         | 11.09ms           |

### Database S3 to Bigquery using native path
Note - These results are generated manually, there is a issue added for the same [#574](https://github.com/astronomer/astro-sdk/issues/574)

| database   | dataset    | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:-----------|:-------------|:-------------|:----------------|:------------------|
| bigquery   | hundred_kb | 2.92min      | 56.57MB      | 41.94ms         | 35.83ms           |
| bigquery   | one_gb     | 4.25min      | 66.32MB      | 54.88ms         | 48.01ms           |
| bigquery   | ten_kb     | 2.9min       | 55.51MB      | 40.74ms         | 34.94ms           |
| bigquery   | ten_mb     | 4.17min      | 57.6MB       | 46.64ms         | 46.83ms           |

### Local to Bigquery using native path

| database   | dataset   | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:----------|:-------------|:-------------|:----------------|:------------------|
| bigquery   | one_gb    | 13.18min     | 231.33MB     | 10.5min         | 1.15min           |
| bigquery   | ten_kb    | 12.46s       | 62.74MB      | 8.28s           | 730.0ms           |
| bigquery   | ten_mb    | 14.36s       | 39.78MB      | 8.2s            | 780.0ms           |

### Local to Bigquery using default path

| database   | dataset   | total_time   | memory_rss   | cpu_time_user   | cpu_time_system   |
|:-----------|:----------|:-------------|:-------------|:----------------|:------------------|
| bigquery   | one_gb    | 12.23min     | 231.33MB     | 10.5min         | 1.15min           |
| bigquery   | ten_kb    | 17.06s       | 61.65MB      | 7.16s           | 1.01s             |
| bigquery   | ten_mb    | 15.22s       | 66.08MB      | 7.35s           | 940.0ms           |
