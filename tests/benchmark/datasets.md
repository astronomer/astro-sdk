# Datasets

## Overview

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


## Origin of these datasets

### UK COVID overview
Subset of the data available in the government website
* Source: https://coronavirus.data.gov.uk/api/v2/data?areaType=overview&metric=covidOccupiedMVBeds&metric=cumCasesByPublishDate&metric=newOnsDeathsByRegistrationDate&metric=hospitalCases&format=csv
* Download date: 8 February 2022
* Original size: 45 KB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.parquet`
* Processing details: [download_datasets.sh](tests/benchmark/download_datasets.sh)

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
* Processing details: [download_datasets.sh](tests/benchmark/download_datasets.sh)

### IMDB
Subset of the Internet Movies Database:
* Source: https://datasets.imdbws.com/title.ratings.tsv.gz
* Download date: 11 February 2022
* Original size: 20 MB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/imdb/*`
* Processing details: [download_datasets.sh](tests/benchmark/download_datasets.sh)

### Github timeline
Subset of the Github git repository records
* Source: https://datasets.imdbws.com/title.ratings.tsv.gz
* Download date: 11 February 2022
* Original size: 20 MB
* Dataset URI: `gs://astro-sdk/benchmark/trimmed/github/github_timeline_100mb.csv`
* Processing details: [download_datasets.sh](tests/benchmark/download_datasets.sh)

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
