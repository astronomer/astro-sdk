# Installing the SQL Decorators

## Create a Personal Access Token on GitHub

As an unpublished project, releases for this project will only be available via our github
page. However, since this is a private project, we will need to create Personal Access Token s.t.
pip can install pretending to be you. Please use this [guide](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to create a Personal Access Token.

## Add the pip URL to your requirements.txt

First add the `git` package to `packages.txt. This will allow pip to install packages from github.

In your requirements.txt add the following line (with values filled in):

```shell script
git+https://{PERSONAL_ACCESS_TOKEN}@github.com/astronomer/sql-decorator.git@{RELEASE_VERSION}
```

Once you complete these steps, simply run `astro dev start,` and you should have a running Airflow
with the SQL decorator!