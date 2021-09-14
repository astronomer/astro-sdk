#!/bin/bash
set -e

apt-get update && apt-get -y install git

cd
git clone https://github.com/jamestjw/pagila.git
cd pagila

echo 'Importing data into db $POSTGRES_DB'
psql --username $POSTGRES_USER -d $POSTGRES_DB < pagila-schema.sql
echo 'Importing schema finished successfully'
psql --username $POSTGRES_USER -d $POSTGRES_DB < pagila-insert-data.sql
echo 'Importing insert-data finished successfully'

echo 'Data import finished successfully'
