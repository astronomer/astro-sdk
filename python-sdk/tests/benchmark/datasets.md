# Datasets

## Overview

The following datasets are used for running performance benchmarking on Astro SDK `load_file` feature for all databases
except for Redshift. For Redshift, files with consistent schema for various sizes have been created using the
[Python Faker library](https://faker.readthedocs.io/en/master/) by generating fake data.

All of them (except the ones for Redshift) are based on real external datasets. For reproducibility of the benchmark
tests, they were copied to: `gs://astro-sdk/benchmark/`.
For Redshift, the generated fake data files have been copied over to `s3://astro-sdk-test/benchmark/fake_data/` as the
Redshift `COPY` command supports loading data only from S3 bucket files.

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
| ten_gb     | 10 GB  | ndjson  | 9400000 | 17 (*)  | benchmark/trimmed/stackoverflow/output_file.ndjson                  | Github timeline sample      |
(*) Nested JSON, this number represents just the root-level properties

Within `s3://astro-sdk-test/benchmark/fake_data/`, following are the files with fake data:

|            | size   | format  | rows        | columns | path to fake data file(s) |
|------------|--------|---------|-------------|---------|---------------------------|
| ten_kb     | 10 KB  | csv     | 378         | 7       | 10_kb.csv                 |
| hundred_kb | 100 KB | csv     | 3853        | 7       | 100_kb.csv                |
| one_mb     | 1 MB   | csv     | 38539       | 7       | 1_mb.csv                  |
| ten_mb     | 10 MB  | csv     | 385,217     | 7       | 10_mb.csv                 |
| hundred_mb | 100 MB | csv     | 3,850,536   | 7       | 100_mb.csv                |
| one_gb     | 1 GB   | csv     | 38,499,508  | 7       | 1_gb.csv                  |
| five_gb    | 5 GB   | csv     | 192,508,057 | 7       | 5_gb.csv                  |
| ten_gb     | 10 GB  | csv     | 385,007,364 | 7       | 10_gb.csv                 |


## Origin of the synthetic datasets

To make the benchmarking results uniform, we have generated synthetic dataset for various size for all the file types:

1. CSV files

- GCS path

|            | size   | format | GCS path to csv file(s)                                       |
|------------|--------|--------|---------------------------------------------------------------|
| ten_kb     | 10 KB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/ten_kb.csv     |
| hundred_kb | 100 KB | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/hundred_kb.csv |
| ten_mb     | 10 MB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/ten_mb.csv     |
| hundred_mb | 100 MB | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/hundred_mb.csv |
| one_gb     | 1 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/one_gb.csv     |
| two_gb     | 2 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/two_gb.csv     |
| five_gb    | 5 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/five_gb.csv    |
| ten_gb     | 10 GB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/csv/ten_gb.csv     |

2. Parquet files

- GCS path

|            | size   | format     | GCS path to parquet file(s)                                           |
|------------|--------|------------|-----------------------------------------------------------------------|
| ten_kb     | 10 KB  | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/ten_kb.parquet     |
| hundred_kb | 100 KB | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/hundred_kb.parquet |
| ten_mb     | 10 MB  | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/ten_mb.parquet     |
| hundred_mb | 100 MB | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/hundred_mb.parquet |
| one_gb     | 1 GB   | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/one_gb.parquet     |
| two_gb     | 2 GB   | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/two_gb.parquet     |
| five_gb    | 5 GB   | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/five_gb.parquet    |
| ten_gb     | 10 GB  | parquet    | gs://astro-sdk/benchmark/synthetic-dataset/parquet/ten_gb.parquet     |

3. NDJSON files

- GCS path

|            | size   | format | GCS path to ndjson file(s)                                          |
|------------|--------|--------|---------------------------------------------------------------------|
| ten_kb     | 10 KB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/ten_kb.ndjson     |
| hundred_kb | 100 KB | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/hundred_kb.csv    |
| ten_mb     | 10 MB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/ten_mb.ndjson     |
| hundred_mb | 100 MB | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/hundred_mb.ndjson |
| one_gb     | 1 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/one_gb.ndjson     |
| two_gb     | 2 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/two_gb.ndjson     |
| five_gb    | 5 GB   | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/five_gb.ndjson    |
| ten_gb     | 10 GB  | csv    | gs://astro-sdk/benchmark/synthetic-dataset/ndjson/ten_gb.ndjson     |

## Origin of the real datasets

### UK COVID overview
<!-- markdown-link-check-disable -->

Subset of the data available in the government website
* Source: https://coronavirus.data.gov.uk/api/v2/data?areaType=overview&metric=covidOccupiedMVBeds&metric=cumCasesByPublishDate&metric=newOnsDeathsByRegistrationDate&metric=hospitalCases&format=csv
* Download date: 8 February 2022
* Original size: 45 KB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.parquet`
* Processing details: [download_datasets.sh](download_datasets.sh)
<!-- markdown-link-check-enable -->

```commandline
$ sed -n '1,160 p' covid_overview.csv > covid_overview_160rows.csv
$python -c 'import pandas; df = pandas.read_csv("covid_overview_160rows.csv").to_parquet("covid_overview_10kb.parquet")'
```


### Tate Britain
Subset of artists of pieces exposed at the Tate Britain museum
* Source:  https://raw.githubusercontent.com/tategallery/collection/master/artist_data.csv ]
* Download date: 1 February 2022
* Original size: 45 KB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/tate/*`
* Processing details: [download_datasets.sh](download_datasets.sh)

### IMDB
Subset of the Internet Movies Database:
* Source: https://datasets.imdbws.com/title.ratings.tsv.gz
* Download date: 11 February 2022
* Original size: 20 MB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/imdb/*`
* Processing details: [download_datasets.sh](download_datasets.sh)

### Github timeline
Subset of the Github git repository records
* Source: https://datasets.imdbws.com/title.ratings.tsv.gz
* Download date: 11 February 2022
* Original size: 20 MB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/github/github_timeline_100mb.csv`
* Processing details: [download_datasets.sh](download_datasets.sh)

### Stack Overflow
Subset of the archives of Stack Overflow:
* Source: https://archive.org/download/stackexchange (superuser.com.7z)
* Download date: 9 June 2022
* Original (file) size: 1.1 GB
* License: CC BY-SA 3.0
* More information: https://www.kaggle.com/datasets/stackoverflow/stackoverflow
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/stackoverflow/stackoverflow_posts.parquet`
* Processing details:
```commandline
$ sudo apt install --assume-yes p7zip-full
$ 7z x superuser.com.7z
$ pip install yq
$ cat Posts.xml | xq . > Posts.json
$ jq -c '.posts.row | .[] ' Posts.json > stackoverflow_posts.ndjson
$ sed -n '1,940000 p' stackoverflow_posts.ndjson > stackoverflow_posts_1g.ndjson
```


### PyPI

Subset of records from the Python Package Index
* Source: [BigQuery](https://console.cloud.google.com/bigquery?project=astronomer-dag-authoring&page=project&ws=!1m5!1m4!4m3!1sthe-psf!2spypi!3sdownloads20210328) - `the-psf.pypi.downloads20210328`
* Download date: 9 June 2022
* Original (table) size: 13.92 GB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/pypi/*`
* Processing details:
```
bq extract \
    --destination_format JSON \
    the-psf.pypi.downloads20210328 \
    gs://astro-sdk/benchmark/original/pypi/*
```

### Github archive

Subset from the Github archive:
* Source: [BigQuery](https://console.cloud.google.com/bigquery?project=astronomer-dag-authoring&page=project&ws=!1m5!1m4!4m3!1sgithubarchive!2sday!3s20220601) - `githubarchive.day.20220601`
* Download date: 9 June 2022
* Original (table) size: 29.86 GB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/github/github-archives/`
* Processing details:
```
bq extract \
    --destination_format JSON \
    githubarchive.day.20220601 \
    gs://astro-sdk/benchmark/datasets/github/github-archive-20220601-*.ndjson
```
