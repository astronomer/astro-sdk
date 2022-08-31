# Benchmarking

This section contains tooling, which helps evaluate how long **astro** tasks take and how much computation resources they consume, given one or multiple datasets.

For current benchmark results, please, access (benchmark results)[./results.md].


## How to run

## Requirements

### Datasets

If **astro** supports a dataset file format, you can use it to benchmark.

We selected a subset of datasets as a starting point. These datasets can be available:
* locally
* GCP GCS
* AWS S3

Read more about them on the [datasets.md](./datasets.md) document.


### Configuration

Edit the file `config.json` based on the datasets and databases you would like to benchmark.

There is a complete example at `./config-sample.json`.

Example of content:
```
{
  "databases": [
    {
      "name": "postgres",
      "params": {
        "conn_id": "postgres",
        "metadata": {
          "database": "astronomer-dag-authoring",
          "schema": "postgres"
        }
      }
    }
  ],
  "datasets": [
    {
      "conn_id": "postgres",
      "file_type": "parquet",
      "name": "ten_kb",
      "path": "/tmp/covid_overview_10kb.parquet",
      "rows": 385817,
      "size": "10 KB"
    },
    {
      "conn_id": "postgres",
      "file_type": "csv",
      "name": "hundred_kb",
      "path": "tmp/artist_data_100kb.csv",
      "rows": 385817,
      "size": "100 KB"
    },
    {
      "conn_id": "postgres",
      "file_type": "csv",
      "name": "ten_mb",
      "path": "tmp/title_ratings_10mb.csv",
      "rows": 385817,
      "size": "9.9M"
    }
  ]
}
```

The `datasets` section should match the datasets of choice. It has two mandatory properties:
* `name`: the label used to refer to this dataset
* `path`: the local, GCS, or S3 path to the dataset
* `file_type`: the type of file or extension of file

The `databases` section must match existing connections or connections declared in the file `test-connections.yml`, available at the `astro` root directory. It must contain:
* `name`: the label used to refer to this database
* `params`: a dictionary containing the configuration to the database. The properties `database` and `conn_id` are required.

## Resource configuration for the benchmark
For all the benchmarking, it recommended to choose following configuration of worker node. The current benchmarking results
are run on following three types of worker node configuration on cloud:
1. `Large` worker node
    - 4 vCPUs, 16 GB RAM
    - For example, in GCP n2-standard-4 machines or on AWS EC2 t2.xlarge machines etc
2. `Medium` worker node
    - 2 vCPU, 8 GB RAM
    - For example, in GCP n1-standard-2 machines or on AWS EC2 t2.large machines etc.
3. `Small` worker node
    - 1 vCPU, 4 GB
    - For example, in GCP n1-standard-1 machines or on AWS EC2 t2.medium machines etc.

## Execute the benchmark

There are three possible ways of running the benchmark.

### Locally, outside of a container

The following command will run one DAG for each combination of database x dataset (in the case of the example, four combinations):
```
./run.sh

```

If you're interested in running each test case more times, this is an example of how to run five times each combination:
```
./run.sh 5
```

It is also possible to specify the chunk size, if the `load_file` or `export_file` operators are being used, by setting an integer value in the environment variable `ASTRO_CHUNKSIZE`.

The command outputs something similar to:
```
Benchmark test started
- Input configuration: /home/test-user/Code/astro/tests/benchmark/config.json
- Output: /tmp/results-2022-02-10T14:36:09.ndjson
```

The benchmark results are stored in `/tmp`, and the object name is generated based on when the analysis started. In this case, the name is: ` /tmp/results-2022-02-10T14:36:09.ndjson`

This is an example of the content of `/tmp/results-2022-02-10T14:36:09.ndjson`:
```
{"duration": "1.66s", "rss": "125.37MB", "vms": "990.95MB", "shared": "41.73MB", "dag_id": "load_file_few_kb_into_postgres", "execution_date": "2022-02-12 00:33:56.463244+00:00", "revision": "e1fb164"}
{"duration": "2.28s", "rss": "138.56MB", "vms": "1.0GB", "shared": "41.55MB", "dag_id": "load_file_many_kb_into_postgres", "execution_date": "2022-02-12 00:33:59.808866+00:00", "revision": "e1fb164"}
{"duration": "1.2min", "rss": "157.17MB", "vms": "1.02GB", "shared": "41.9MB", "dag_id": "load_file_few_mb_into_postgres", "execution_date": "2022-02-12 00:34:03.794995+00:00", "revision": "e1fb164"}
```

### Locally, using a container

Build a docker container and attempt to run it, using the `config.json` file.

Requirements:
* A GCP account
* Environment variable `GOOGLE_APPLICATION_CREDENTIALS`, which should reference a local file with a [GCP service account credentails](https://cloud.google.com/docs/authentication/production).
* Datasets available in a GCS bucket

The environment variable is necessary so the container can access data in GCS and export the results to BigQuery.

```
make local
```

The result will be made available in GCS and Bigquery

### Remotely, using a container and a Kubernetes cluster


Set the environment variable `GOOGLE_APPLICATION_CREDENTIALS`, which should reference a local file with a [GCP service account credentails](https://cloud.google.com/docs/authentication/production).

Requirements:
* A GCP account
* Google SDK installed, including both `gcloud` and `kubectl` commands.
* Terraform (version specified in the document [versions.tf](./infrastructure/terraform/versions.tf)
* Environment variable `GOOGLE_APPLICATION_CREDENTIALS`, which should reference a local file with a [GCP service account credentails](https://cloud.google.com/docs/authentication/production).

If the Kubernetes does not exist yet, create it and run all the commands using:
```
make
```

This will create a GKE (Google managed Kubernetes cluster) instance named `astro-sdk` and will create a job in the `benchmark` namespace.

To only rebuild the container and store it in the Google Container Registry, run:
```
make container
```

This command creates a container within the Google project (retrieved from the `$GCP_PROJECT` environment variable) and, by default, it names the image `benchmark`.

If the Kubernetes cluster is already set and the container image already was updated, the job can be triggered using:
```
make run_job
```

The command:
```
teardown_gke
```

Can be used to destroy all.

### Publishing Benchmark results

To publish the results to bigquery table, we need to create an environment variable `ASTRO_PUBLISH_BENCHMARK_DATA` and set it to `True`. Once this is done the benchmarking data can be seen in bigquery table `astronomer-dag-authoring.Benchmark.load_files_to_database`.

## Analyze the results

[analyse.py](analyse.py) takes local file path or GCS file path for ndjson file.

After running the benchmark, it is possible to run an analysis script using:

Sample Local file path for ndjson file:
```
./analyse.py --results-filepath=/tmp/some-results.ndjson
```

Sample GCS file path for ndjson file:
```
./analyse.py --results-filepath=gs://bucket/result/some-results.ndjson
```

If there are multiple runs, it calculates the mean/average per (database/dataset) combination.

It exports the results in markdown tables, using stdout.

This is an example running the benchmark for the astro-sdk-python `78805a` revision (before the 0.7.0 release) and
chunk size size `1,000,000`:
* Ubuntu 20.04.3 LTS
* CPU: 8-Core 11th Gen Intel Core i7-11800H
* Memory: 64 GiB

The latest results can be found at (results.md)[./results.md].
