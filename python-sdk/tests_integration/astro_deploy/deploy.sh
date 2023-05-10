#!/usr/bin/env bash

# Make the script exit with the status if one of the commands fails. Without this, the Airflow task calling this script
# will be marked as 'success' and the DAG will proceed on to the subsequent tasks.
set -e

# This script deploys to an already existing Astro Cloud Airflow deployment.
# It currently does not support creating a new deployment.
#
# Execute the script with below positional params
#         bash deploy.sh <ASTRO_DOCKER_REGISTRY> <ASTRO_ORGANIZATION_ID>  <ASTRO_DEPLOYMENT_ID> <ASTRO_KEY_ID> <ASTRO_KEY_SECRET>
#         - ASTRO_DOCKER_REGISTRY: Docker registry domain. Script will push the docker image here.
#         - ASTRO_ORGANIZATION_ID: Astro cloud deployment organization Id. Get it from UI.
#         - ASTRO_DEPLOYMENT_ID: Astro cloud deployment Id. Get it from UI.
#         - ASTRO_KEY_ID: Astro cloud deployment service account API key Id.
#         - ASTRO_KEY_SECRET: Astro cloud deployment service account API key secret.

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_PATH="${SCRIPT_PATH}/../.."

function echo_help() {
    echo "Usage:"
    echo "ASTRO_DOCKER_REGISTRY:        Docker registry"
    echo "ASTRO_ORGANIZATION_ID         Astro cloud organization Id"
    echo "ASTRO_DEPLOYMENT_ID           Astro cloud Deployment id"
    echo "ASTRO_KEY_ID       Astro cloud service account API key id"
    echo "ASTRO_KEY_SECRET   Astro cloud service account API key secret"
    echo "ASTRO_DEPLOYMENT_ID_SINGLE_WORKER           Astro cloud Deployment id"
    echo "ASTRO_KEY_ID_SINGLE_WORKER       Astro cloud service account for Single Worker API key id"
    echo "ASTRO_KEY_SECRET_SINGLE_WORKER   Astro cloud service account for Single Worker API key secret"
    echo "bash deploy.sh <ASTRO_DOCKER_REGISTRY> <ASTRO_ORGANIZATION_ID>  <ASTRO_DEPLOYMENT_ID> <ASTRO_KEY_ID> <ASTRO_KEY_SECRET> <ASTRO_DEPLOYMENT_ID_SINGLE_WORKER> <ASTRO_KEY_ID_SINGLE_WORKER> <ASTRO_KEY_SECRET_SINGLE_WORKER>"
}

if [ "$1" == "-h" ]; then
  echo_help
  exit
fi

# Delete if source old source files exist
function clean() {
  if [ -d "${SCRIPT_PATH}"/python-sdk ]; then rm -Rf "${SCRIPT_PATH}"/python-sdk; fi
  if [ -d "${SCRIPT_PATH}"/example_dags ]; then rm -Rf "${SCRIPT_PATH}"/example_dags; fi
  if [ -d "${SCRIPT_PATH}"/tests ]; then rm -Rf "${SCRIPT_PATH}"/tests; fi
}

ASTRO_DOCKER_REGISTRY=$1
ASTRO_ORGANIZATION_ID=$2
ASTRO_DEPLOYMENT_ID=$3
ASTRO_KEY_ID=$4
ASTRO_KEY_SECRET=$5
ASTRO_DEPLOYMENT_ID_SINGLE_WORKER=$6
ASTRO_KEY_ID_SINGLE_WORKER=$7
ASTRO_KEY_SECRET_SINGLE_WORKER=$8
MASTER_DAG_DOCKERFILE="Dockerfile"
MASTER_DAG_MUTLI_WORKER_DOCKERFILE="Dockerfile.single_worker"

clean

function deploy(){
    docker_registry_astro=$1
    organization_id=$2
    deployment_id=$3
    key_id=$4
    key_secret=$5
    dockerfile=$6

    # Build image and deploy
    BUILD_NUMBER=$(awk 'BEGIN {srand(); print srand()}')

    # Enforce registry name to be in lowercase
    docker_registry_astro=$(echo $docker_registry_astro | tr '[:upper:]' '[:lower:]')
    organization_id=$(echo $organization_id | tr '[:upper:]' '[:lower:]')
    deployment_id=$(echo $deployment_id | tr '[:upper:]' '[:lower:]')
    IMAGE_NAME=${docker_registry_astro}/${organization_id}/${deployment_id}:ci-${BUILD_NUMBER}
    IMAGE_NAME="images.astronomer.cloud/cknaqyipv05731evsry6cj4n0/cle2pjpom100468k3jxmmkey8w:ci-${BUILD_NUMBER}"
    docker build --platform=linux/amd64 -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/${dockerfile} "${SCRIPT_PATH}"
    echo "====================1====================="
    docker login "${docker_registry_astro}" -u "${key_id}" -p "${key_secret}"
    echo "====================2====================="
    docker push "${IMAGE_NAME}"
    echo "====================3====================="

    TOKEN=$( curl --location --request POST "https://auth.astronomer.io/oauth/token" \
        --header "content-type: application/json" \
        --data-raw "{
            \"client_id\": \"$key_id\",
            \"client_secret\": \"$key_secret\",
            \"audience\": \"astronomer-ee\",
            \"grant_type\": \"client_credentials\"}" | jq -r '.access_token' )

    # Create the Image
    echo "get image id"
    IMAGE=$( curl --location --request POST "https://api.astronomer.io/hub/v1" \
        --header "Authorization: Bearer $TOKEN" \
        --header "Content-Type: application/json" \
        --data-raw "{
            \"query\" : \"mutation imageCreate(\n    \$input: ImageCreateInput!\n) {\n    imageCreate (\n    input: \$input\n) {\n    id\n    tag\n    repository\n    digest\n    env\n    labels\n    deploymentId\n  }\n}\",
            \"variables\" : {
                \"input\" : {
                    \"deploymentId\" : \"$deployment_id\",
                    \"tag\" : \"ci-$BUILD_NUMBER\"
                    }
                }
            }" | jq -r '.data.imageCreate.id')
    # Deploy the Image
    echo "deploy image"
    curl --location --request POST "https://api.astronomer.io/hub/v1" \
            --header "Authorization: Bearer $TOKEN" \
            --header "Content-Type: application/json" \
            --data-raw "{
                \"query\" : \"mutation imageDeploy(\n    \$input: ImageDeployInput!\n  ) {\n    imageDeploy(\n      input: \$input\n    ) {\n      id\n      deploymentId\n      digest\n      env\n      labels\n      name\n      tag\n      repository\n    }\n}\",
                \"variables\" : {
                    \"input\" : {
                        \"id\" : \"$IMAGE\",
                        \"tag\" : \"ci-$BUILD_NUMBER\",
                        \"repository\" : \"images.astronomer.cloud/$organization_id/$deployment_id\"
                        }
                    }
            }"

}

# Copy source files
mkdir "${SCRIPT_PATH}"/python-sdk
mkdir "${SCRIPT_PATH}"/tests
cp -r "${PROJECT_PATH}"/src "${SCRIPT_PATH}"/python-sdk
cp -r "${PROJECT_PATH}"/pyproject.toml "${SCRIPT_PATH}"/python-sdk
cp -r "${PROJECT_PATH}"/README.md "${SCRIPT_PATH}"/python-sdk
cp -r "${PROJECT_PATH}"/example_dags "${SCRIPT_PATH}"/example_dags
cp -r "${PROJECT_PATH}"/tests/data "${SCRIPT_PATH}"/tests/data


deploy $ASTRO_DOCKER_REGISTRY $ASTRO_ORGANIZATION_ID $ASTRO_DEPLOYMENT_ID $ASTRO_KEY_ID $ASTRO_KEY_SECRET $MASTER_DAG_DOCKERFILE

deploy $ASTRO_DOCKER_REGISTRY $ASTRO_ORGANIZATION_ID $ASTRO_DEPLOYMENT_ID_SINGLE_WORKER $ASTRO_KEY_ID_SINGLE_WORKER $ASTRO_KEY_SECRET_SINGLE_WORKER $MASTER_DAG_MUTLI_WORKER_DOCKERFILE

clean
