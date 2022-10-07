# Example DAGs

This directory contains a collection of all example dags.

The directory structure is as follows:

It goes from highest to lowest supported airflow version. The directory name has to be an airflow version. E.g. 2.4 -> 2.3 -> 2.2.5

For example, when an airflow version 2.3.1 is installed, it will run all example dags in 2.3 recursively (which includes the dags from 2.2.5).
