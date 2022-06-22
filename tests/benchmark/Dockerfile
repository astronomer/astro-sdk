FROM debian:bullseye
MAINTAINER Astronomer DAG Authoring Team <airflow_dag_authors@astronomer.io>

ARG GIT_HASH=""
LABEL git_hash=${GIT_HASH}

# Set default environment variables
# These will be overridden by K8s if declared eg. by using ConfigMaps.
ENV SDK_DEPENDENCIES=/tmp/
ENV AIRFLOW_HOME=/opt/app/
ENV PYTHONPATH=/opt/app/
ENV GIT_REVISION=${GIT_HASH}
ENV ASTRO_PUBLISH_BENCHMARK_DATA=True
ENV GCP_BUCKET=dag-authoring

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt install -y jq

# Install the Google SDK
RUN apt install -y curl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

# Copy relevant files that are not in the current folder
COPY astro-sdk/pyproject.toml $SDK_DEPENDENCIES
COPY astro-sdk/README.md $SDK_DEPENDENCIES
COPY astro-sdk/src $SDK_DEPENDENCIES
COPY astro-sdk/test-connections.yaml $AIRFLOW_HOME

# Install Astro SDK dependencies
WORKDIR $SDK_DEPENDENCIES
RUN pip install .[all]
RUN pip uninstall astro-sdk-python -y
RUN rm -rf $SDK_DEPENDENCIES/*

RUN echo $GOOGLE_APPLICATION_CREDENTIALS

# Create the Airflow home directory and use it as the working dir
# Assert the Astro SDK is available in the PYTHONPATH
RUN mkdir -p $AIRFLOW_HOME
WORKDIR $AIRFLOW_HOME
COPY astro-sdk/src/astro $AIRFLOW_HOME/astro
COPY run.sh $AIRFLOW_HOME/
COPY run.py $AIRFLOW_HOME/
COPY config-docker.json $AIRFLOW_HOME/config.json
COPY dags $AIRFLOW_HOME/dags
RUN rm -rf astro-sdk

CMD ./run.sh
