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

#### Baseline using `GCSToBigQueryOperator` using [benchmark_gcs_to_bigquery.py](tests/benchmark/dags/benchmark_gcs_to_big_query.py)
|Dataset                                    |Size | Duration(seconds)  |
|-------------------------------------------|-----|--------------------|
|covid_overview/covid_overview_10kb.csv     |10 KB| 5.129522           |
|tate_britain/artist_data_100kb.csv         |100KB| 3.319834           |
|imdb/title_ratings_10mb.csv                |10MB | 5.558414           |
|stackoverflow/stackoverflow_posts_1g.ndjson|1GB  | 85.409014          |
|trimmed/pypi/*                             |5GB  | 48.973093          |
