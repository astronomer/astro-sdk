FROM quay.io/astronomer/astro-runtime:10.4.0-python-3.9-base


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
        pkg-config

ENV SETUPTOOLS_USE_DISTUTILS=stdlib

COPY ../pyproject.toml  ${AIRFLOW_HOME}/astro_sdk/
# The following file are needed because version they are referenced from pyproject.toml
COPY ../README.md  ${AIRFLOW_HOME}/astro_sdk/
COPY ../src/astro/__init__.py  ${AIRFLOW_HOME}/astro_sdk/src/astro/__init__.py

RUN pip install --upgrade --force-reinstall  -e "${AIRFLOW_HOME}/astro_sdk[all,tests,doc]"
USER astro
