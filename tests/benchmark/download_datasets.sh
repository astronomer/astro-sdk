#!/usr/bin/env bash

#set -x
#set -v
set -e

tate_artist_path=/tmp/artist_data.csv
imdb_title_ratings_path=/tmp/title_ratings.csv
github_timeline_path=/tmp/github_timeline.csv
gcs_github_timeline_dir=gs://$GCS_BUCKET/github_timeline
covid_overview_path=/tmp/covid_overview.csv

echo $'\nDownloading the Tate Gallery artist dataset to' $tate_artist_path...
curl https://raw.githubusercontent.com/tategallery/collection/master/artist_data.csv --output  $tate_artist_path

echo $'\nDownloading and extracting the IMDB title.ratings dataset to' $imdb_title_ratings_path...
curl https://datasets.imdbws.com/title.ratings.tsv.gz --output /tmp/title_ratings.tsv.gz
gzip -d /tmp/title_ratings.tsv.gz -f
tr '\t' ',' < /tmp/title_ratings.tsv > $imdb_title_ratings_path
rm /tmp/title_ratings.tsv


echo $'\nDownloading the UK COVID overview dataset to' $covid_overview_path...
curl 'https://coronavirus.data.gov.uk/api/v2/data?areaType=overview&metric=covidOccupiedMVBeds&metric=cumCasesByPublishDate&metric=newOnsDeathsByRegistrationDate&metric=hospitalCases&format=csv' --output /tmp/covid_overview.csv

# The following dataset assume the user has:
# 1. a Google Cloud Platform account
# 2. the GCP SDK

echo $'\nDownloading the Github timeline dataset to' $github_timeline_path...
if [ ! -n "$(gsutil ls $gcs_github_timeline_dir)" ]; then
  bq extract \
    --destination_format CSV \
    bigquery-public-data:samples.github_timeline \
    $gcs_github_timeline_dir/github_timeline_*.csv
fi
gsutil cp $gcs_github_timeline_dir/github_timeline_000000000007.csv /tmp/github_timeline.csv
