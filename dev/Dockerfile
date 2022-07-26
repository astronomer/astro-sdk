FROM quay.io/astronomer/astro-runtime:5.0.6-base

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules

RUN mkdir ${AIRFLOW_HOME}/astro_sdk
COPY setup.cfg ${AIRFLOW_HOME}/astro_sdk/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astro_sdk/pyproject.toml

RUN python -m pip install -e "${AIRFLOW_HOME}/astro_sdk"
USER astro