FROM quay.io/astronomer/astro-runtime:5.0.6-base

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules
ENV SETUPTOOLS_USE_DISTUTILS=stdlib

COPY . ${AIRFLOW_HOME}/astro_sdk/

RUN pip install -e "${AIRFLOW_HOME}/astro_sdk/"
USER astro