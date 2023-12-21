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
#         - TOKEN: Astro workspace token.

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_PATH="${SCRIPT_PATH}/../.."

function echo_help() {
    echo "Usage:"
    echo "ASTRO_DOCKER_REGISTRY:        Docker registry"
    echo "ASTRO_ORGANIZATION_ID         Astro cloud organization Id"
    echo "ASTRO_DEPLOYMENT_ID           Astro cloud Deployment id"
    echo "TOKEN     Astro workspace token"
    echo "bash deploy.sh <ASTRO_DOCKER_REGISTRY> <ASTRO_ORGANIZATION_ID>  <ASTRO_DEPLOYMENT_ID> <TOKEN> "
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
TOKEN=$4
MASTER_DAG_DOCKERFILE="Dockerfile"


clean

function deploy(){
    docker_registry_astro=$1
    organization_id=$2
    deployment_id=$3
    TOKEN=$4
    dockerfile=$5

    # Build image and deploy
    BUILD_NUMBER=$(awk 'BEGIN {srand(); print srand()}')
    # Enforce registry name to be in lowercase
    docker_registry_astro=$(echo $docker_registry_astro | tr '[:upper:]' '[:lower:]')
    organization_id=$(echo $organization_id | tr '[:upper:]' '[:lower:]')
    deployment_id=$(echo $deployment_id | tr '[:upper:]' '[:lower:]')
    IMAGE_NAME=${docker_registry_astro}/${organization_id}/${deployment_id}:ci-${BUILD_NUMBER}
    docker build --platform=linux/amd64 -t "${IMAGE_NAME}" -f "${SCRIPT_PATH}"/${dockerfile} "${SCRIPT_PATH}"
    docker login "${docker_registry_astro}" -u "cli" -p "$TOKEN"
    docker push "${IMAGE_NAME}"

    # Create the Image
      echo "get image id"
      IMAGE=$( curl --location --request POST "https://api.astronomer-stage.io/hub/graphql" \
        --header "Authorization: Bearer "${TOKEN}"" \
        --header "Content-Type: application/json" \
        --data-raw "{
            \"query\" : \"mutation CreateImage(\n    \$input: CreateImageInput!\n) {\n    createImage (\n    input: \$input\n) {\n    id\n    tag\n    repository\n    digest\n    env\n    labels\n    deploymentId\n  }\n}\",
            \"variables\" : {
                \"input\" : {
                    \"deploymentId\" : \"$deployment_id\",
                    \"tag\" : \"ci-$BUILD_NUMBER\"
                    }
                }
            }" | jq -r '.data.createImage.id')
    # Deploy the Image
    echo "deploy image"
    curl --location --request POST "https://api.astronomer-stage.io/hub/graphql" \
          --header "Authorization: Bearer $TOKEN" \
          --header "Content-Type: application/json" \
          --data-raw "{
              \"query\" : \"mutation DeployImage(\n    \$input: DeployImageInput!\n  ) {\n    deployImage(\n      input: \$input\n    ) {\n      id\n      deploymentId\n      digest\n      env\n      labels\n      name\n      tag\n      repository\n    }\n}\",
              \"variables\" : {
                  \"input\" : {
                      \"deploymentId\" : \"$deployment_id\",
                      \"imageId\" : \"$IMAGE\",
                      \"tag\" : \"ci-$BUILD_NUMBER\",
                      \"repository\" : \"images.astronomer-stage.cloud/$organization_id/$deployment_id\",
                      \"dagDeployEnabled\":false
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


deploy $ASTRO_DOCKER_REGISTRY $ASTRO_ORGANIZATION_ID $ASTRO_DEPLOYMENT_ID $TOKEN $MASTER_DAG_DOCKERFILE

clean
