from __future__ import annotations

import argparse
import logging
import sys

import requests

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_access_token(api_key_id: str, api_key_secret: str) -> str:
    """
    Gets bearer access token for the Astro Cloud deployment needed for REST API authentication.

    :param api_key_id: API key ID of the Astro Cloud deployment
    :param api_key_secret: API key secret of the Astro Cloud deployment
    """
    request_json = {
        "client_id": api_key_id,
        "client_secret": api_key_secret,
        "audience": "astronomer-ee",
        "grant_type": "client_credentials",
    }
    response = requests.post("https://auth.astronomer.io/oauth/token", json=request_json)
    response_json = response.json()
    return response_json["access_token"]


def trigger_dag_runs(
    *, dag_ids_name: list[str], astro_subdomain: str, deployment_id: str, bearer_token: str
) -> None:
    """
    Triggers the DAG using Airflow REST API.

    :param dag_ids_name: list of dag_id to trigger
    :param astro_subdomain: subdomain of the Astro Cloud  (e.g., https://<subdomain>.astronomer.run/)
    :param deployment_id: ID of the Astro Cloud deployment. Using this, we generate the short deployment ID needed
        for the construction of Airflow endpoint to hit
    :param bearer_token: bearer token to be used for authentication with the Airflow REST API
    """
    short_deployment_id = f"d{deployment_id[-7:]}"
    integration_tests_deployment_url = f"https://{astro_subdomain}.astronomer.run/{short_deployment_id}"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Authorization": f"Bearer {bearer_token}",
    }
    failed_dag_ids: list[str] = []
    for dag_id in dag_ids_name:
        dag_trigger_url = f"{integration_tests_deployment_url}/api/v1/dags/{dag_id}/dagRuns"
        response = requests.post(dag_trigger_url, headers=headers, json={})

        if response.status_code != 200:
            failed_dag_ids.append(dag_id)

        logging.info(f"Response for {dag_id} DAG trigger is %s", response.json())

    if failed_dag_ids:
        for dag_id in failed_dag_ids:
            logging.error("Failed to run DAG %s", {dag_id})
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("astro_subdomain", help="subdomain of the Astro Cloud", type=str)
    parser.add_argument("deployment_id", help="ID of the deployment in Astro Cloud", type=str)
    parser.add_argument("astronomer_key_id", help="Key ID of the Astro Cloud deployment", type=str)
    parser.add_argument("astronomer_key_secret", help="Key secret of the Astro Cloud deployment", type=str)
    parser.add_argument(
        "--dag-ids",
        help=(
            "Comma separated list of dag_ids_name to trigger"
            " e.g. 'example_async_adf_run_pipeline, example_async_batch'"
        ),
        default="example_master_dag",
        # fallback to "example_master_dag" if empty string "" is provided
        type=lambda dag_ids: dag_ids if dag_ids else "example_master_dag",
    )

    args = parser.parse_args()
    token = get_access_token(args.astronomer_key_id.strip(), args.astronomer_key_secret.strip())

    input_dag_ids = args.dag_ids
    dag_ids = [dag_id.strip() for dag_id in input_dag_ids.split(",")]

    trigger_dag_runs(
        dag_ids_name=dag_ids,
        astro_subdomain=args.astro_subdomain,
        deployment_id=args.deployment_id,
        bearer_token=token,
    )
