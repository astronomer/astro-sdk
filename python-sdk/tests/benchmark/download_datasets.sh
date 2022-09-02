  GNU nano 5.4                                                                                       download_datasets.sh
#!/usr/bin/env bash

#set -x
#set -v
set -e

ten_kb=/tmp/covid_overview_10kb.parquet
gcs_ten_kb=gs://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.parquet
echo $'\nDownloading the 10 kb covid_overview dataset to' $covid_overview...
gsutil cp $gcs_ten_kb $ten_kb

hundred_kb=/tmp/artist_data_100kb.csv
gcs_hundred_kb=gs://astro-sdk/benchmark/trimmed/tate_britain/artist_data_100kb.csv
echo $'\nDownloading the 100 kb artist_data dataset to' $hundred_kb...
gsutil cp $gcs_hundred_kb $hundred_kb

ten_mb=/tmp/title_ratings_10mb.csv
gcs_ten_mb=gs://astro-sdk/benchmark/trimmed/imdb/title_ratings_10mb.csv
echo $'\nDownloading the 10 mb imdb dataset to' $ten_mb...
gsutil cp $gcs_ten_mb $ten_mb

one_gb=/tmp/stackoverflow_posts_1g.ndjson
gcs_one_gb=gs://astro-sdk/benchmark/trimmed/stackoverflow/stackoverflow_posts_1g.ndjson
echo $'\nDownloading the 1 Gb stackoverflow dataset to' $one_gb...
gsutil cp $gcs_one_gb $one_gb

five_gb=/tmp/pypi/
gcs_five_gb=gs://astro-sdk/benchmark/trimmed/pypi/
mkdir $five_gb
echo $'\nDownloading the 5 Gb pypi dataset to' $five_gb...
gsutil -m cp -r $gcs_five_gb $five_gb

ten_gb=/tmp/github-archive/
gcs_ten_gb=gs://astro-sdk/benchmark/trimmed/github/github-archive/
mkdir $ten_gb
echo $'\nDownloading the 10 Gb github archive dataset to' $ten_gb...
gsutil -m cp -r $gcs_ten_gb $ten_gb
