FROM quay.io/astronomer/astro-runtime:10.0.0-base
USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        freetds-dev \
        libssl-dev \
        libkrb5-dev \
        libmariadb-dev \
        pkg-config libxml2-dev libxmlsec1-dev libxmlsec1-openssl

ENV SETUPTOOLS_USE_DISTUTILS=stdlib

COPY python-sdk /tmp/python-sdk
RUN pip install /tmp/python-sdk[all]
RUN pip install "apache-airflow-providers-slack>=8.0.0"
RUN pip install astronomer-starship-provider

RUN mkdir -p ${AIRFLOW_HOME}/dags
RUN mkdir -p ${AIRFLOW_HOME}/tests
RUN mkdir -p ${AIRFLOW_HOME}/include

COPY example_dags/ ${AIRFLOW_HOME}/dags/
COPY master_dag.py/ ${AIRFLOW_HOME}/dags/
COPY example_snowflake_cleanup.py/ ${AIRFLOW_HOME}/dags/
COPY astronomer_migration_dag.py/ ${AIRFLOW_HOME}/dags/
COPY tests/ ${AIRFLOW_HOME}/tests/

RUN ls ${AIRFLOW_HOME}/dags/
COPY service-account-key.json  $AIRFLOW_HOME/include/astro_sdk_gcp_cred.json
