FROM quay.io/astronomer/astro-runtime:7.1.0-base

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules
ENV SETUPTOOLS_USE_DISTUTILS=stdlib

COPY ../pyproject.toml  ${AIRFLOW_HOME}/universal_transfer_operator/
# The following file are needed because version they are referenced from pyproject.toml
COPY ../README.md  ${AIRFLOW_HOME}/universal_transfer_operator/
COPY ../src/universal_transfer_operator/__init__.py  ${AIRFLOW_HOME}/universal_transfer_operator/src/universal_transfer_operator/__init__.py

RUN pip install -e "${AIRFLOW_HOME}/universal_transfer_operator[all]"
USER astro
