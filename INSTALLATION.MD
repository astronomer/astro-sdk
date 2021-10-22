# Installing the SQL Decorators

## Add the pip URL to your requirements.txt

First add the `git` package to `packages.txt`. This will allow pip to install packages from github.

In your requirements.txt add the following line (with values filled in):

```shell script
git+https://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@github.com/astronomer/sql-decorator.git@{RELEASE_VERSION}
```

Once you complete these steps, simply run `astro dev start,` and you should have a running Airflow
with the SQL decorator!