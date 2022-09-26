#!/usr/bin/env bash

set -x  # print the shell command before being executed
set -v  # run bash in verbose mode (echos each command before running it)
set -e  # stop the execution instantly if a command returns a non-zero status


repeat=${1:-3}  # how many times we want to repeat each DAG run (default: 3)

benchmark_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
config_path="${benchmark_dir}/config.json"
runner_path="${benchmark_dir}/run.py"
astro_dir="${benchmark_dir}/../../src"
airflow_home="${AIRFLOW_HOME:-$benchmark_dir}"
airflow_db=$airflow_home/*.db

if test -f "${benchmark_dir}/test-connections.yaml"; then
  connections_file="${benchmark_dir}/test-connections.yaml"
else
  connections_file="${benchmark_dir}/../../test-connections.yaml"
fi

git_revision="${GIT_HASH:=`git rev-parse --short HEAD`}"
echo $git_revision

results_file=/tmp/results-`date -u +%FT%T`.ndjson

chunk_sizes_array=( 1000000 )

export AIRFLOW__CORE__LOGGING_LEVEL=ERROR
export AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=False
export AIRFLOW__CORE__UNIT_TEST_MODE=True
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
export AIRFLOW__ASTRO_SDK__SQL_SCHEMA=postgres
export AIRFLOW_HOME=$airflow_home
export PYTHONWARNINGS="ignore"
export PYTHONPATH=$astro_dir:$PYTHONPATH


parse_yaml_to_valid_airflow_config() {
  python3 -c "import yaml; import json; data = yaml.safe_load(open('$connections_file'))['connections']; print(json.dumps({item['conn_id']:item for item in data}))"
}

get_abs_filename() {
  # $1 : relative filename
  filename=$1
  parentdir=$(dirname "${filename}")

  if [ -d "${filename}" ]; then
      echo "$(cd "${filename}" && pwd)"
  elif [ -d "${parentdir}" ]; then
    echo "$(cd "${parentdir}" && pwd)/$(basename "${filename}")"
  fi
}

if [ ! -n "$(ls -A $airflow_db 2>/dev/null)" ]; then
  echo "Initialising Airflow"
  airflow db init;
  (parse_yaml_to_valid_airflow_config) > /tmp/connections.json;
  airflow connections import /tmp/connections.json;
  rm /tmp/connections.json;
  echo
fi

echo Benchmark test started:
echo - Input configuration: $(get_abs_filename $config_path)
echo - Output: $(get_abs_filename $results_file)


  for i in {1..$(($2))}; do
    jq -r '.databases[] | [.name] | @tsv' $config_path | while IFS=$'\t' read -r database; do
      jq -r '.datasets[] | [.name] | @tsv' $config_path | while IFS=$'\t' read -r dataset; do
        for chunk_size in "${chunk_sizes_array[@]}"; do
          echo "$i $dataset $database $chunk_size"
          set +e  # allow us to see the content of $results_file regardless of the run being successful or not
          ASTRO_CHUNKSIZE=$chunk_size python3 -W ignore $runner_path --dataset="$dataset" --database="$database" --revision $git_revision --chunk-size=$chunk_size 1>> $results_file
          cat $results_file
          set -e  # do not allow errors from here onwards
          if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
        echo "$GOOGLE_APPLICATION_CREDENTIALS is not defined"
      else
        echo "$GOOGLE_APPLICATION_CREDENTIALS is defined"
            gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}
      fi

#          gsutil cp $results_file gs://${GCP_BUCKET}/benchmark/results/
          if command -v peekprof &> /dev/null; then
             # https://github.com/exapsy/peekprof
             peekprof -html "/tmp/$dataset-$database-$chunk_size.html" -refresh 1000ms -pid $! > /tmp/$dataset-$database-$chunk_size.csv
          fi
        done
      done
    done
  done

echo Benchmark test completed!
