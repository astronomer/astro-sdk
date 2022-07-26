## Development Environment

- Put the DAGs you want to run in the `dags` directory
- If you want to add Connections, create a `connections.yaml` file in the `dev` directory.
  See the [Connections Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) for more information.

  Example:
  ```yaml
  druid_broker_default:
    conn_type: druid
    extra: '{"endpoint": "druid/v2/sql"}'
    host: druid-broker
    login: null
    password: null
    port: 8082
    schema: null
  airflow_db:
    conn_type: mysql
    extra: null
    host: mysql
    login: root
    password: plainpassword
    port: null
    schema: airflow
  ```

- Run `docker-compose up` to start all the services
- If you want to add additional dependencies, add them to `setup.cfg` and re-run `docker-compose up`

## Useful Commands

- **Stop** all the services
    ```shell
    docker-compose down
    ```
- **Cleanup** the environment
    ```shell
    docker-compose down --volumes --remove-orphans
    ```
