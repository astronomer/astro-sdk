import json

# util script to generate configuration file
# This is currently bit hardcode like path ideally we should use AWS/GCP API
# But since we are not making call to GCP/AWS we don't worry about credential if in case want to try
# Maybe in future we will improve it to use GCP/AWS API


def generate_datasets():
    locations = ["gs", "s3"]
    path = "astro-sdk/benchmark/synthetic-dataset"
    file_types = ["csv", "parquet", "ndjson"]
    file_sizes = {
        "ten_kb": "10 KB",
        "hundred_kb": "100 KB",
        "ten_mb": "10 MB",
        "hundred_mb": "100 MB",
        "one_gb": "1 GB",
        # "two_gb": "2 GB",
        # "five_gb": "5 BG",
        # "five_gb/": "5 GB",
        # "ten_gb/": "10 GB",
    }

    conn_maps = {"gs": "bigquery_conn_benchmark", "s3": "s3_conn_benchmark"}

    datasets = []

    for location in locations:
        for file_type in file_types:
            for file_size in file_sizes:
                if file_size.endswith("/"):
                    dataset = {
                        "conn_id": conn_maps.get(location),
                        "file_type": file_type,
                        "name": file_size[:-1],
                        "path": f"{location}://{path}/{file_type}/{file_size}",
                        "rows": 1,
                        "size": file_sizes.get(file_size),
                    }
                else:
                    dataset = {
                        "conn_id": conn_maps.get(location),
                        "file_type": file_type,
                        "name": file_size,
                        "path": f"{location}://{path}/{file_type}/{file_size}.{file_type}",
                        "rows": 1,
                        "size": file_sizes.get(file_size),
                    }
                datasets.append(dataset)
    return datasets


def generate_databases():
    databases = ["bigquery", "postgres", "redshift", "snowflake", "sqlite"]

    astro_databases = []
    for database in databases:
        db = {
            "kwargs": {"enable_native_fallback": False},
            "name": database,
            "output_table": {"conn_id": f"{database}_conn_benchmark"},
        }
        if database == "postgres":
            db["output_table"] = {
                "conn_id": f"{database}_conn_benchmark",
                "metadata": {"database": "postgres"},
            }
        astro_databases.append(db)
    return astro_databases


config = {
    "databases": generate_databases(),
    "datasets": generate_datasets(),
}

with open("generated_config.json", "w") as f:
    json_dumps_str = json.dumps(config, indent=4)
    print(json_dumps_str, file=f)
