FROM quay.io/astronomer/astro-runtime:7.1.0-base

COPY python-sdk /tmp/python-sdk
RUN pip install /tmp/python-sdk[all]
RUN pip install apache-airflow[slack]

RUN mkdir -p ${AIRFLOW_HOME}/dags

COPY example_dags/ ${AIRFLOW_HOME}/dags/
COPY master_dag.py/ ${AIRFLOW_HOME}/dags/

RUN ls ${AIRFLOW_HOME}/dags/
