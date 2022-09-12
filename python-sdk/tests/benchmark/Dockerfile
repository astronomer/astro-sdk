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
ENV GIT_HASH ${GIT_HASH}

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt install -y jq
RUN apt install -y vim

# Install the Google SDK
RUN apt install -y curl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

RUN apt install -y libpq-dev python3-dev

# Install Astro SDK dependencies

COPY . .
COPY python-sdk .
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


COPY python-sdk/tests/benchmark/dags $AIRFLOW_HOME/dags
COPY python-sdk/tests/benchmark/run.sh $AIRFLOW_HOME/
COPY python-sdk/tests/benchmark/run.py $AIRFLOW_HOME/
COPY python-sdk/tests/benchmark/config.json $AIRFLOW_HOME/config.json

# Debian Bullseye is shipped with Python 3.9
# Upgrade built-in pip
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install -y git
RUN apt install -y jq

# Install the Google SDK
RUN apt install -y curl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

WORKDIR ./python-sdk/tests/benchmark/

CMD ./run.sh
