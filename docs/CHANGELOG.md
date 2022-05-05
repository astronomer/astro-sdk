# Changelog

## 0.8.3

Bug fix:
* Do not attempt to create a schema if it already exists [#329](https://github.com/astro-projects/astro/issues/329).


## 0.8.2

Bug fix:
* Support dataframes from different databases in dataframe operator [#325](https://github.com/astro-projects/astro/pull/325)

Enhancements:
* Add integration testcase for `SqlDecoratedOperator` to test execution of Raw SQL [#316](https://github.com/astro-projects/astro/pull/316)


## 0.8.1

Bug fix:
* Snowflake transform without `input_table` [#319](https://github.com/astro-projects/astro/issues/319)


## 0.8.0

Feature:

*`load_file` support for nested NDJSON files [#257](https://github.com/astro-projects/astro/issues/257)

Breaking change:
* `aql.dataframe` switches the capitalization to lowercase by default. This behaviour can be changed by using `identifiers_as_lower` [#154](https://github.com/astro-projects/astro/issues/154)

Documentation:
* Fix commands in README.md [#242](https://github.com/astro-projects/astro/issues/242)
* Add scripts to auto-generate Sphinx documentation

Enhancements:
* Improve type hints coverage
* Improve Amazon S3 example DAG, so it does not rely on pre-populated data [#293](https://github.com/astro-projects/astro/issues/293)
* Add example DAG to load/export from BigQuery [#265](https://github.com/astro-projects/astro/issues/265)
* Fix usages of mutable default args [#267](https://github.com/astro-projects/astro/issues/267)
* Enable DeepSource validation [#299](https://github.com/astro-projects/astro/issues/299)
* Improve code quality and coverage

Bug fixes:
* Support `gcpbigquery` connections [#294](https://github.com/astro-projects/astro/issues/294)
* Support `params` argument in `aql.render` to override SQL Jinja template values [#254](https://github.com/astro-projects/astro/issues/254)
* Fix `aql.dataframe` when table arg is absent [#259](https://github.com/astro-projects/astro/issues/259)

Others:
* Refactor integration tests, so they can run across all supported databases [#229](https://github.com/astro-projects/astro/issues/229), [#234](https://github.com/astro-projects/astro/issues/234), [#235](https://github.com/astro-projects/astro/issues/235), [#236](https://github.com/astro-projects/astro/issues/236), [#206](https://github.com/astro-projects/astro/issues/206), [#217](https://github.com/astro-projects/astro/issues/217)


## 0.7.0

Feature:
* `load_file` to a Pandas dataframe, without SQL database dependencies [#77](https://github.com/astro-projects/astro/issues/77)

Documentation:
* Simplify README [#101](https://github.com/astro-projects/astro/issues/101)
* Add Release Guidelines [#160](https://github.com/astro-projects/astro/pull/160)
* Add Code of Conduct [#101](https://github.com/astro-projects/astro/pull/101)
* Add Contribution Guidelines [#101](https://github.com/astro-projects/astro/pull/101)

Enhancements:
* Add SQLite example [#149](https://github.com/astro-projects/astro/issues/149)
* Allow customization of `task_id` when using `dataframe` [#126](https://github.com/astro-projects/astro/issues/126)
* Use standard AWS environment variables, as opposed to `AIRFLOW__ASTRO__CONN_AWS_DEFAULT` [#175](https://github.com/astro-projects/astro/issues/175)

Bug fixes:
* Fix `merge` `XComArg` support [#183](https://github.com/astro-projects/astro/issues/183)
* Fixes to `load_file`:
   * `file_conn_id` support [#137](https://github.com/astro-projects/astro/issues/137)
   * `sqlite_default` connection support [#158](https://github.com/astro-projects/astro/issues/158)
* Fixes to `render`:
   * `conn_id` are optional in SQL files [#117](https://github.com/astro-projects/astro/issues/117)
   * `database` and `schema` are optional in SQL files [#124](https://github.com/astro-projects/astro/issues/124)
* Fix `transform`, so it works with SQLite [#159](https://github.com/astro-projects/astro/issues/159)

Others:
* Remove `transform_file` [#162](https://github.com/astro-projects/astro/pull/162)
* Improve integration tests coverage [#174](https://github.com/astro-projects/astro/pull/174)

## 0.6.0

Features:
* Support SQLite [#86](https://github.com/astro-projects/astro/issues/86)
* Support users who can't create schemas [#121](https://github.com/astro-projects/astro/issues/121)
* Ability to install optional dependencies (amazon, google, snowflake) [#82](https://github.com/astro-projects/astro/issues/82)

Enhancements:
* Change `render` so it creates a DAG as opposed to a TaskGroup [#143](https://github.com/astro-projects/astro/issues/143)
* Allow users to specify a custom version of `snowflake_sqlalchemy` [#127](https://github.com/astro-projects/astro/issues/127)

Bug fixes:
* Fix tasks created with `dataframe` so they inherit connection id [#134](https://github.com/astro-projects/astro/issues/134)
* Fix snowflake URI issues [#102](https://github.com/astro-projects/astro/issues/102)

Others:
* Run example DAGs as part of the CI [#114](https://github.com/astro-projects/astro/issues/114)
* Benchmark tooling to validate performance of `load_file` [#105](https://github.com/astro-projects/astro/issues/105)
