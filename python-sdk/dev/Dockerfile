FROM quay.io/astronomer/astro-runtime:6.0.0-base

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules
ENV SETUPTOOLS_USE_DISTUTILS=stdlib

COPY ../pyproject.toml  ${AIRFLOW_HOME}/astro_sdk/
# The following file are needed because version they are referenced from pyproject.toml
COPY ../README.md  ${AIRFLOW_HOME}/astro_sdk/
COPY ../src/astro/__init__.py  ${AIRFLOW_HOME}/astro_sdk/src/astro/__init__.py

RUN pip install -e "${AIRFLOW_HOME}/astro_sdk[all,tests,doc]"
USER astro
