import os

import yaml
from airflow.models import Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session



def fix_keys(conn_dict):
    new_dict = {}
    for k, v in conn_dict.items():
        if k == "conn_id" or k == "conn_type":
            new_dict[k] = v
        else:
            new_dict[k.replace("conn_", "")] = v
    return new_dict

def create_database_connections():
    with open(os.path.dirname(__file__) + "/test.yaml") as fp:
        yaml_with_env = os.path.expandvars(fp.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts['airflow']["connections"]:
            i = fix_keys(i)
            connections.append(Connection(**i))
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        create_default_connections(session)
        for conn in connections:
            session.add(conn)
