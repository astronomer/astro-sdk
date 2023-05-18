from datetime import datetime
from textwrap import dedent

import os

from airflow.decorators import dag, task
from airflow.models import Connection

from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session
import yaml


connections_yaml = f"{os.environ['AIRFLOW_HOME']}/include/connections_template.yaml"


@task
def replace_env_var_in_yaml():
    with open(connections_yaml, "r") as file:
        yaml_content = yaml.safe_load(file)

        # Recursively replace environment variables in the YAML content
    connections_details = recursive_replace_env_vars(yaml_content)

    # Save the modified YAML content back to the file
    with open(connections_yaml, "w") as file:
        yaml.dump(connections_details, file, default_flow_style=False)


def recursive_replace_env_vars(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                # Check if the string value is an environment variable
                if value.startswith("$") and value[1:] in os.environ:
                    data[key] = os.environ[value[1:]]
            else:
                # Recursively replace environment variables in nested data structures
                data[key] = recursive_replace_env_vars(value)
    elif isinstance(data, list):
        # Recursively replace environment variables in lists
        data = [recursive_replace_env_vars(item) for item in data]

    return data


@task
def create_connections():
    with open(connections_yaml) as fp:
        yaml_with_env = os.path.expandvars(fp.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts["connections"]:
            connections.append(Connection(**i))
    with create_session() as session:
        create_default_connections(session)
        for conn in connections:
            last_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
            if last_conn is not None:
                session.delete(last_conn)
                session.flush()
                print(
                    f"Overriding existing conn_id {conn.conn_id} "
                    f"with connection specified in test_connections.yaml"
                )
            session.add(conn)


@dag(
    dag_id="create_sdk_required_connections",
    start_date=datetime(1970, 1, 1),
    schedule_interval=None,
    tags=["setup"],
    doc_md=dedent(
        f"""
        This DAG creates connections required by astro sdk dags connection 
        This Dag takes as input connections_template.yaml(connections information with some unresolved Env vars),
        Resolve the variables in file and create connections in airflow, we can add new connections in connections_template.yaml
        to push them to airflow

        Run this before you run any dags that depend on that connection
        """
    ),
)
def setup():
    replace_env_var_in_yaml()
    create_connections()


the_dag = setup()
