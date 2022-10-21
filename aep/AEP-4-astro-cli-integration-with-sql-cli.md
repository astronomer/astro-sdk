# AEP-4 Astro CLI and SQL CLI integration

Authors:

* Tatiana Al-Chueyr (tatiana.alchueyr@astronomer.io)
* Pankaj Koti (pankaj.koti@astronomer.io)


## Status

| <!-- -->       |  <!-- -->                                                 |
| -------------- | --------------------------------------------------------- |
| **State**      | Accepted                                                  |
| **Discussion** | <https://github.com/astronomer/astro-sdk/issues/828>      |
| **Created**    | 2022-09-29                                                |


## Motivation

A requirement for the SQL CLI 0.1 delivery is for it to be available from within the Astro CLI. This document discusses the approaches for doing this integration.


## Scope

There are three main aspects of the integration:
1. Packaging
2. Interface
3. Add-ons


## Assumptions and Limitations

### 1. Packaging

Astronomer uses the Go programming language to develop the Astro CLI, whereas SQL CLI is developed using Python.

There are a few options for calling Python code from Go:
a) Docker-container
b) System shared library (.dll / .lib.so)
c) (Native) operating system package (Brew recipe / .dmg  / .deb / .rpm)

We'll analyse each of these options.


#### a) Docker-container

Currently, the Astro CLI uses this approach for multiple commands, including [pytest](https://docs.astronomer.io/astro/cli/astro-dev-pytest). The idea is that we create a Python library, make it available in PyPI and have a Docker container to install it.

By implementing this approach, we would run commands (forwarding parameters passed to Astro CLI) from within Docker. This pattern is vastly used within the Astro CLI project itself and Astronomer developers are familiar with maintaining it.

**Pros**:
* **Reliability**: Docker isolates the environment from the operating system. This gives us control of the Python version and any other system dependencies the SQL CLI may need.
* **Maintenance**: the SQL CLI team only needs to worry about maintaining the Python library and docker image. It’s easy from a maintenance perspective.
* **Decoupling**: from a packaging perspective, this approach makes the Astro CLI and the SQL CLI decoupled and independent.

**Cons**
* **System-dependencies**: it relies on the user having Docker pre-installed
* **Execution speed**: this approach is slower from a user’s perspective (pulling docker image)
* **Troubleshooting**: in the past, Astro CLI developers reported having challenges debugging things when they had issues due to the layer in-between the user and the system.
Memory-consumption: there is an overhead with memory consumption (by Docker)


#### b) System shared-library

It is possible to use [cgo](https://pkg.go.dev/cmd/cgo)  to interact with Python, from Go, as a shared library. There are a few blog posts which discuss this approach ([example 1](https://www.ardanlabs.com/blog/2020/09/using-python-memory.html) and [example 2](https://poweruser.blog/embedding-python-in-go-338c0399f3d5).

**Pros**
* **Memory-consumption**: uses the least memory, supports sharing memory between Python and Go
* **Execution speed**: probably the fastest from a user perspective
* **Troubleshooting**: removes the Docker layer in between. Still, it introduces a thinner wrapping layer.

**Cons**
* **System dependencies**: it relies on the user having Python pre-installed
* **Reliability**: Relies on system dependencies, which may vary from user to user
* **Maintenance**: the SQL CLI team must maintain a Python library and the Go interface to communicate with it. For each operating system (Linux, Mac, Windows), we’d need a dedicated, dynamic library.
* **Coupling**: from a packaging perspective, this approach makes the Astro CLI and the SQL CLI very coupled. We would have to change how we build the Astro CLI, if we wanted to share a memory, we would have to handle garbage collection on the Go code, and it seems we’d have to write more Go boilerplate code to interface with Python.


#### c) (Native) OS package

This strategy involves building an operating system package, which may be native (e.g. .deb for Ubuntu and Debian, or .dmg for MacOS) or not (such as a brew recipe for MacOS).

As of now, there are lots of tools which make this process easy ([Homebrew docs](https://docs.brew.sh/Homebrew-and-Python) and [Debian](https://pi3g.com/2019/04/19/packaging-python-projects-for-debian-raspbian-with-dh-virtualenv/), as examples).

In this case, we’d be installing a SQL CLI client in the end-user operating system, and the Astro CLI would invoke this binary.

**Pros**
* **Reliability**: it doesn’t need to rely on anything pre-installed; the package can detail all the dependencies and resolve them
* **System-dependencies**: it doesn’t need to rely on anything pre-installed; the package can detail all the dependencies and resolve them.
* **Memory-consumption**: less memory overhead than Docker
* **Execution speed**: faster than docker, probably slower than the dynamic library
* **Troubleshooting**: no additional layers in between - simplest to troubleshoot
* **Decoupling**: from a packaging perspective, this approach makes the Astro CLI and the SQL CLI very decoupled.

**Cons**
* **Maintenance**: the SQL CLI team would need to maintain a Python library and, for each operating system support, a dedicated package. There is an overhead with the initial development, but once the first package is shipped, it is easy and cheap to maintain. We need to ensure the Astro CLI can install the correct binary version/operating system.


#### Overview

|                     | Docker | OS Shared Library | OS Package |
|---------------------|--------|-------------------|------------|
| Reliability         | ★★★★★  | ★★                | ★★★★★      |
| System-dependencies | ★★     | ★★                | ★★★★★      |
| Memory-consumption  | ★★     | ★★★★★             | ★★★★       |
| Troubleshooting     | ★★     | ★★★★★             | ★★★★       |
| Decoupling          | ★★★    | ★★★               | ★★★★★      |
| Maintenance         | ★★★★★  | ★★                | ★★★        |


#### Recommendation

From a maintenance and implementation perspective, it is easier and quicker to deliver the SQL CLI using approach 1(a), by implementing a Python library and using Docker to integrate with the Astro CLI.

From a user's perspective, approach 1(c) gives them a better experience - allowing them to not worry about system dependencies and having a lightweight way of installing and running commands related to SQL  workflows.

Given that time-to-market is important, for release 0.1 of the SQL CLI, we will adopt approach 1(a).  After that, we can discuss if we want to implement 1(c), which could improve the overall user experience.


### 2. Interface

After several sessions with the Astro CLI team, we were able to identify three ways of aligning the interface of the SQL CLI and Astro CLI:

a) New sub-command
b) One new context
c) Two new contexts
d) Feature flag

Below there are more details about each of them and a recommendation.


#### a) New sub-command

All the SQL CLI commands would be available from within a sub-command of the Astro CLI (the name is to be defined - could be sql , workflow, or something else).

This approach is the least aligned with the product expectations because it gives a non-unified experience between the SQL and the existing Astro CLI interface.

Users would follow these steps:

```bash
    $ astro flow init
    $ astro flow validate
    $ astro flow run # pre-generate the DAG from the SQL dir & run locally
    $ astro flow deploy # do we want pre-generate the DAG from the SQL dir
```

**Pros**
* **SQL-interface consistency**: All SQL-related commands are grouped, so the end-user only needs to worry about one sub-command
* **Integration**: Easy to implement - the coupling with the Astro CLI can be as lightweight as we want (some arguments can be hard-coded in the Astro CLI, others may just be forwarded). This would allow us to have additional features without the need to change the Astro CLI code.
* **Context-aware**: works in conjunction with any context, including the existing software and cloud contexts
* **Isolation**: We’d only include in the project structure what is needed for the SQL CLI, without adding python-specific files
* **Maintenance**: easy from a DAG authoring team, since most of the code will be in Python and we will have full control of the SQL CLI itself. The implementation could be similar to pytest:
  * Initially we would have our own Dockerfile  - in future be part of Runtime
  * Container: https://github.com/astronomer/astro-cli/blob/3c18bac4e378ebce3949af3cac2a0874009bc74b/airflow/container.go#L27
  * Docker: https://github.com/astronomer/astro-cli/blob/3c18bac4e378ebce3949af3cac2a0874009bc74b/airflow/docker.go#L349-L459

**Cons**
* **Astro-interface consistency**: we would have a new subset of commands which are not the same as existing Astro CLI users are used to
* **Re-work**: we wouldn’t be reusing parts of the Astro CLI which we could benefit from (including, but not limited to, the deployment to Astro-cloud)

#### b) One new context

The Astro-CLI allows developers to create dedicated contexts to override the behaviour of existing commands. Currently, this feature allows the same command to do different things, including deploying to different installations of Astronomer. Existing contexts: cloud (used by most Astronomer customers) and software (used by JPMC, for example).

By adopting this approach, we’d add a new context (e.g. flow), which would change the behaviour of the existing commands.

Users would follow these steps:

```bash
    $ astro context switch sql.astronomer.io
    $ astro dev init  # overridden
    $ astro flow validate
    $ astro flow run
    $ astro deploy  # overridden
```

**Pros**
* **Astro-interface consistencyi**: users who are already used to the Astro CLI would be able to reuse some of the existing commands, including astro dev init and astro deploy.
* **Re-work**: we can reuse current features of the Astro CLI, such as deployment, without the need for any rework
* **Integration**: already done before, simple to integrate with
* **Isolation**: We could only include in the project structure what is needed for the SQL CLI, without adding python-specific files. However, this may be confusing to users used to the command line in the Python context.
* **Maintenance**: Medium. The following parts of code would have to be maintained:
    * A folder (similar to software and cloud) with all the commands, linking to either cloud or software https://github.com/astronomer/astro-cli/tree/main/cmd/software)
    * This would include tests written in Go for these

**Cons**
* **SQL-interface consistency**: The SQL-related commands are not grouped. The user needs to use different subcommands for different features
* **Context-aware**: Since it is a new context and the Astro CLI only supports one context at a time, we would need to decide between either supporting the cloud or the software context, not allowing a subset of Astronomer customers to use it.

#### c) Two new contexts

Very similar to (b), except we’d have a dedicated SQL context for each of the other contexts.

Users would follow these steps:

```bash
    $ astro context switch flow-cloud.astronomer.io
    $ astro dev init  # overridden
    $ astro flow validate
    $ astro flow run
    $ astro deploy  # overridden,
```

Whereas users used the software context would use:

```bash
$ astro context switch flow-software.astronomer.io
```

**Pros**

* **Astro-interface consistency**: users who are already used to the Astro CLI would be able to reuse some of the existing commands, including astro dev init and astro deploy.
* **Re-work**: we can reuse current features of the Astro CLI, such as deployment, without the need for any rework
* **Integration**: already done before, simple to integrate with
* **Isolation**: We could only include in the project structure what is needed for the SQL CLI, without adding python-specific files. However, this may be confusing to users used to the command line in the Python context.
* **Context-aware**: Would allow both software and cloud users to use the SQL feature

**Cons**
* **SQL-interface consistency**: The SQL-related commands are not grouped. The user must use different subcommands for different features
* **Maintenance**: double the work, feels like a hack

#### d) Feature flag
We would define a feature flag which, if enabled, would make the Astro CLI invoke - internally - SQL CLI commands.

Users would follow these steps:
```bash
    $ export ASTRO_ENABLE_SQL=1
    $ astro dev init  # enriched
    $ astro flow validate
    $ astro flow run
    $ astro deploy  # enriched
```

**Pros**

* **Astro-interface consistency**: users who are already used to the Astro CLI would be able to reuse some of the existing commands, including astro dev init and astro deploy.
* **Re-work**: we can reuse current features of the Astro CLI, such as deployment, without the need for any rework
* **Integration**: already done before
* **Context-aware**: Supports all contexts

**Cons**
* **Isolation**: high coupling between the Astro CLI and SQL CLI. The SQL CLI could break Astro CLI users' experience.
* **SQL-interface consistency**: The SQL-related commands are not grouped. The user must use different subcommands for different features
* **Maintenance**: it is more coupled to the Astro CLI, but the maintenance would be smaller than the context, but we would still have duplication, For instance, we would have to add if to all the available contexts: https://github.com/astronomer/astro-cli/blob/3c18bac4e378ebce3949af3cac2a0874009bc74b/cmd/cloud/deploy.go#L110 and respective tests
* **Prone to error**: if user opens a new terminal or forgets setting the environment variable

#### e) Command flag

We would define a flag which, if enabled, would make the Astro CLI invoke - internally - SQL CLI commands.

Users would follow these steps:

```bash
    $ astro dev init --flow # enriched
    $ astro flow validate
    $ astro flow run
    $ astro deploy --flow # enriched, this could be confusing (if the flag is not passed, would we still be deploying the DAGs which were pre-generated before?)
```

**Pros**
* **Astro-interface consistency**: users who are already used to the Astro CLI would be able to reuse some of the existing commands, including astro dev init and astro deploy.
* **Re-work**: we can reuse current features of the Astro CLI, such as deployment, without the need for any rework
* **Integration**: already done before
* **Context-aware**: Supports all contexts

**Cons**
* **Isolation**: high coupling between the Astro CLI and SQL CLI. The SQL CLI could break Astro CLI users' experience.
* **SQL-interface consistency**: The SQL-related commands are not grouped. The user must use different subcommands for different features
* **Maintenance**: it is more coupled to the Astro CLI, but the maintenance would be smaller than the context, but we would still have duplication, For instance, we would have to add if to all the available contexts: https://github.com/astronomer/astro-cli/blob/3c18bac4e378ebce3949af3cac2a0874009bc74b/cmd/cloud/deploy.go#L110 and respective tests
* **Prone to error**: if the user forgets adding the flag to deploy, for instance, they won’t deploy SQL DAGs


#### Overview

|                             | Sub-command | One context | Two contexts | Feature flag/ command flag |
|-----------------------------|-------------|-------------|--------------|----------------------------|
| Astro-interface consistency | ★★          | ★★★★★       | ★★★★★        | ★★★★★                      |
| SQL-interface consistency   | ★★★★★       | ★★          | ★★           | ★★                         |
| Integration                 | ★★★★★       | ★★★★★       | ★★★★★        | ★★★★★                      |
| Isolation                   | ★★★★★       | ★★          | ★★           | ★★                         |
| Re-work                     | ★★          | ★★★★★       | ★★★★★        | ★★★★★                      |
| Context-aware               | ★★★★★       | ★★          | ★★★          | ★★★★★                      |
| Maintenance                 | ★★★★★       | ★★★         | ★★           | ★★★                        |


#### Recommendation

Our recommendation is for us to start by implementing the sub-command approach 2(a). This will make the development more decoupled, which is useful, particularly during the alpha/beta development. This approach will give a more unified experience to SQL users, with the downside of being less connected to the existing Astro CLI experience.

We will revisit this for Beta. The feature flag/command flag approaches 2(d) and 2(e) may prove to be more suitable for a unified Astro CLI user experience.


### 3. Add-ons

The Astro CLI already creates several:
* Directory
* Files

It also supports building docker containers using those files and directories and deploying them to the Astronomer cloud.

The current directory structure created when we use the astro dev init command is:

```bash
.
├── airflow_settings.yaml
├── dags
│   ├── example_dag_advanced.py
│   └── example_dag_basic.py
├── Dockerfile
├── include
├── packages.txt
├── plugins
├── README.md
├── requirements.txt
└── tests
    └── dags
        └── test_dag_integrity.py
```

Most of these things are not meaningful from a SQL workflow author - since they are very Python-focused.

The files and directories that matter from a SQL perspective are:
* Environment-specific configuration files which contain only properties that the SQL analysts care about (e.g. connections)
* Directory which will contain the SQL files
* Sample SQL statements
* Output directory where the materialised Python DAGs will be saved to, which could even be a “hidden” directory and set as the Airflow home directory, so SQL analysis don’t have to worry about it.

To keep or not the files from the Python project is a decision related to (2). So this section focuses on proposing the minimal files we need for delivering the SQL SDK 0.1.


### Recommendation

This is a recommendation of what the SQL CLI should be creating:

```
sql-test/
    config/
        default/
            configuration.yml
        dev/
            configuration.yml
    workflows/
        workflow_example1/
            example_1a.sql
            example_1b.sql
    .dags/
```

The initial configuration file could look like:

```
connections:
  - conn_id: postgres_conn
    conn_type: postgres
    host: localhost
    schema:
    login: postgres
    password: postgres
    port: 5433
    extra:
```

And it should support referencing from environment variables, example:

```
connections:
  - conn_id: postgres_conn
    conn_type: postgres
    host: localhost
    schema:
    login: postgres
    password: ${POSTGRES_CONN_PASSWORD}
    port: 5433
    extra:
```
