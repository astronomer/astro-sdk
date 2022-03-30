**NOTE: Please read the [python quickstart guide](QUICK_START_PYTHON.md) before reading this section. We will assume
you have followed the set-up steps in subsequent steps**

## Creating your first SQL file-based DAG

```python
import os
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql

DAG_ID = "test_astro_dag"

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id=DAG_ID,
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
)


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    models = aql.render(dir_path + "/models")
```

### Understanding astro.render

Before you make your first SQL task, let’s take a look at what the `astro.render` function is doing.


```python
dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    models = aql.render(dir_path + "/models")
```


What is happening here is that `aql.render` allows a user to pass in a directory and then parses that directory as a whole (we’ll get into more detail on that later). When we pass in the argument `dir_path + “/models”`, we are telling astro to look for a file that is “current directory + “/models”. In reality, this directory could live anywhere on your machine! We figured that a local path would be the easiest for project management.


### Now the fun part

So now that you understand a bit more about how astro finds directories for SQL file processing, let’s create the `models` directory to match the code in our DAG.

Once you’ve created the models directory let’s create a file called `test_astro.sql`.


```Your project
├── dags
│   ├── astro_dag.py
│   ├── models
│   │   ├── test_astro.sql
```


For this file, we can put in a simple query that hits the pagila database


test_astro.sql
```
SELECT * FROM actor
```


Now let’s head over to our Airflow instance and we will see that there is now a dag with a single task tied to the `test_astro.sql` file.





![Single task not run](img/single_task.png)


And we’re up and running! Let’s unpause this DAG, hit the play button, and…



![task failing](img/failed_run.png)


Oh.

Well it looks like there is a bit more work before we can start writing SQL DAGs.

Looking at the logs, we see this error:





![error from no conn_id](img/no_conn_error_msg.png)


So the reason our SQL isn’t running is because we never actually told it where to run. Let’s fix that.

The first step to connecting our SQL to a postgres instance is to create a connection. Assuming you ran the docker command we provided, your postgres should live at localhost:5433. Here is an example of what that could look like in the UI



![](img/creating_conn.png)


Or you can copy this command-line command into your terminal, and it will set up the connection on your local airflow.


```
airflow connections add 'postgres_conn_2' \
    --conn-type 'postgresql' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-host 'localhost' \
    --conn-port '5433'
```


Now that you’ve created your postgres connection, we need a way to tell airflow that you want to use that connection (and database) for this query. To do this, we take advantage of a common webpractice of creating a [frontmatter](https://jekyllrb.com/docs/front-matter/). Frontmatters allow us to store all metadata for your SQL in a place where it won’t make your SQL hard to read. In this first example we are setting the conn_id, and database, but there are other options that we will show later on.

With the frontmatter set your `astro_test.sql` file should now look like this:


```
---
conn_id: postgres_conn
database: pagila
---
SELECT * FROM actor;
```


We give airflow a few seconds to parse the file, run it again, and…




![](img/single_task_passing.png)


Success!

So now we have a single passing SQL query running in Airflow, let’s make the second task!


## Adding the Second Task

To add a second task, let’s make a second SQL file named `test_inheritance.sql` in the `models` directory. Your project should now look like this:


```
Your project
├── dags
│   ├── astro_dag.py
│   ├── models
│   │   ├── test_astro.sql
│   │   ├── test_inheritance.sql
```


To start out, we’ll just put a simple SQL statement into this file

test_inheritance.sql


```
SELECT * FROM test_astro;
```


When we go back to Airflow, we will see the second task created, but these tasks don’t have any dependencies, so they would both act in parallel (and `test_inheritance` would fail as there’s no connection id or table named `test_astro`).
![](img/two_task_parallel.png)



To set the proper inheritance, we set a jinja template ``{{}}`` around `test_astro` to alert airflow that this is actually referring
to a task instead of a table in the database.




```
SELECT * FROM {{test_astro}};
```



This results in the following:

![](img/two_task_passing_inherit.png)


And it works!

But how is that possible if you didn’t set the `conn_id` or `database` variables? Well that’s because we were able to automatically inherit the context of the parent tasks. This inheritance prevents a lot of unnecessary repetitions and should make changing databases much easier if you decide to do so.


## Tying SQL Results to Dataframes

Finally let’s tie the results of our SQL models into a dataframe so we can complete our processing in a pythonic context.

To add this step, copy the bolded lines into your DAG file:


```
import os
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.dataframe import dataframe as adf
import pandas as pd

DAG_ID = "my_astro_dag"

default_args = {
   "retries": 1,
   "retry_delay": 0,
}

dag = DAG(
   dag_id=DAG_ID,
   start_date=datetime(2019, 1, 1),
   max_active_runs=3,
   schedule_interval=None,
   default_args=default_args,
)


@adf
def my_dataframe_func(df: pd.DataFrame):
   print(df.to_string)


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
   models = aql.render(dir_path + "/models")
   my_dataframe_func(df=models["test_inheritance"])
```


The first thing you’ll notice here is that we can treat the results of our model rendering as a python dictionary. You can access each model we have defined via that dictionary. You may also notice that you can pass a SQL table into a python function and have that table surface as a dataframe with no extra configuration. That is because we handle all of that complexity for you!

Even in cases where there are multiple tables from multiple SQL sources, Astro can parse and load those tables into local dataframes for any number of machine learning or data processing use-cases.
