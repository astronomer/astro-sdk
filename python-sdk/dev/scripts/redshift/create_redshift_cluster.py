from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import random
import string
import sys
import time

import boto3

REDSHIFT_REGION_NAME = os.getenv("REDSHIFT_REGION_NAME", "us-east-2")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "dev")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME", "not-set")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "not-set")
REDSHIFT_NODE_TYPE = os.getenv("REDSHIFT_NODE_TYPE", "ra3.xlplus")

AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "not-set"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "not-set"),
    "region_name": REDSHIFT_REGION_NAME,
}

client = boto3.client("redshift", **AWS_S3_CREDS)


@contextlib.contextmanager
def nostdout():
    log = logging.getLogger(__name__)
    previousloglevel = log.getEffectiveLevel()
    save_stdout = sys.stdout
    sys.stdout = io.BytesIO()
    yield
    sys.stdout = save_stdout
    log.setLevel(previousloglevel)


def generate_cluster_identifier(len: int = 15):
    return "".join(random.choices(string.ascii_lowercase, k=15))


def create_redshift_cluster(cluster_id: str | None = None):
    cluster_id = cluster_id or generate_cluster_identifier()

    client.create_cluster(
        DBName=REDSHIFT_DATABASE,
        ClusterIdentifier=str(cluster_id),
        Port=5439,
        PubliclyAccessible=True,
        MasterUsername=REDSHIFT_USERNAME,
        MasterUserPassword=REDSHIFT_PASSWORD,
        NodeType=REDSHIFT_NODE_TYPE,
        ClusterType="single-node",
        AutomatedSnapshotRetentionPeriod=0
    )
    while True:
        time.sleep(30)
        status, host = describe_cluster_status(cluster_id)
        if status == "available":
            break
    return host, cluster_id


def describe_cluster_status(cluster_identifier: str):
    response = client.describe_clusters(ClusterIdentifier=cluster_identifier)
    return response["Clusters"][0]["ClusterStatus"], response["Clusters"][0].get("Endpoint", {}).get(
        "Address", None
    )


def delete_redshift_cluster(cluster_identifier: str):
    client.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)


if __name__ == "__main__":
    action_create = "create"
    action_delete = "delete"
    parser = argparse.ArgumentParser(description="create/delete redshift cluster")
    parser.add_argument("--action", type=str, help="accepted values are create/delete", required=True)
    parser.add_argument(
        "--cluster_id",
        type=str,
        help=f"redshift cluster id to be use for {action_create}/{action_delete}",
        required=False,
    )
    args = parser.parse_args()

    if args.action == action_create:
        with nostdout():
            host, cluster_id = create_redshift_cluster(args.cluster_id)
        print(f"{host} {cluster_id}")
    elif args.action == action_delete:
        if args.cluster_id is None:
            raise ValueError("Need --cluster_id when deleting")
        delete_redshift_cluster(args.cluster_id)
