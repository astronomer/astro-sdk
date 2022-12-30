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
ENV USE_GKE_GCLOUD_AUTH_PLUGIN=True

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt install -y jq
RUN apt install -y vim

# Install the Google SDK
RUN apt install -y curl
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-327.0.0-linux-x86_64.tar.gz \
    -O /tmp/google-cloud-sdk.tar.gz | bash

RUN mkdir -p /usr/local/gcloud \
    && tar -C /usr/local/gcloud -xvzf /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh -q

ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN gcloud components install gke-gcloud-auth-plugin

RUN gcloud components update

RUN apt install -y libpq-dev python3-dev

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
