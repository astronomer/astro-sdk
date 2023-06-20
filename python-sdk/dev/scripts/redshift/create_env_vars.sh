#!/bin/bash
echo $1
echo $2

output="$(python ./dev/scripts/redshift/create_redshift_cluster.py --action=restore --snapshot_id=$1 --cluster_id=$2)"

echo "===========================output=============================="
echo $output

host="$(echo "$output" | cut -d' ' -f1)"
cluster_id="$(echo "$output" | cut -d' ' -f2)"

echo "===========================host=============================="
echo $host

echo "===========================cluster_id=============================="
echo $cluster_id

export REDSHIFT_HOST=$host
