FROM debian:bullseye AS build
MAINTAINER Astronomer DAG Authoring Team <airflow_dag_authors@astronomer.io>

ARG GIT_HASH=""
LABEL git_hash=${GIT_HASH}

# Set default environment variables
# These will be overridden by K8s if declared eg. by using ConfigMaps.
#ENV SDK_DEPENDENCIES=/tmp/
ENV AIRFLOW_HOME=/opt/app/
ENV PYTHONPATH=/opt/app/
ENV ASTRO_PUBLISH_BENCHMARK_DATA=True
ENV GCP_BUCKET=dag-authoring
ENV AIRFLOW__ASTRO_SDK__SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE=gcs_int_python_sdk
ENV AIRFLOW__ASTRO_SDK__LOAD_TABLE_AUTODETECT_ROWS_COUNT=1000
ENV GIT_HASH ${GIT_HASH}

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get install -y wget && \
    apt install -y jq && \
    apt install -y vim && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install google-cloud-sdk -y && \
    apt-get install google-cloud-sdk-gke-gcloud-auth-plugin -y && \
    apt install -y curl && \
    apt install -y libpq-dev python3-dev

# Install Astro SDK dependencies
COPY . .
# We need to update pip as only newer versions of pip allow installing in edit mode for pyproject.toml projects. For more info: https://peps.python.org/pep-0660/
RUN pip install --upgrade pip
RUN pip install -e .[all]


FROM build AS benchmark
ENV AIRFLOW_HOME=/opt/app/
ENV PYTHONPATH=/opt/app/
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True


RUN mkdir -p $AIRFLOW_HOME
WORKDIR $AIRFLOW_HOME

COPY . .
COPY tests/benchmark/dags $AIRFLOW_HOME/dags
COPY tests/benchmark/run.sh $AIRFLOW_HOME/
COPY tests/benchmark/run.py $AIRFLOW_HOME/
COPY tests/benchmark/config-weekly.json $AIRFLOW_HOME/config-weekly.json
COPY tests/benchmark/config-ci.json $AIRFLOW_HOME/config-ci.json

WORKDIR ./tests/benchmark/

CMD ./run.sh
