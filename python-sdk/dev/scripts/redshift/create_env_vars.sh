#!/bin/bash
output="$(python ./create_redshift_cluster.py --actions=create)"

echo "===========================output=============================="
echo $output

host="$(echo "$output" | cut -d' ' -f1)"
cluster_id="$(echo "$output" | cut -d' ' -f2)"

echo "===========================host=============================="
echo $host

echo "===========================cluster_id=============================="
echo $cluster_id

export REDSHIFT_HOST=$host
export REDSHIFT_CLUSTER_ID=$cluster_id