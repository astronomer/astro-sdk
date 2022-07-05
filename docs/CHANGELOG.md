# Changelog

## 0.11.0

Feature:
* Added Cleanup operator to clean temporary tables [#187](https://github.com/astronomer/astro-sdk/issues/187) [#436](https://github.com/astronomer/astro-sdk/issues/436)

Internals:
* Added a Pull Request template [#205](https://github.com/astronomer/astro-sdk/issues/205)
* Added sphinx documentation for readthedocs [#276](https://github.com/astronomer/astro-sdk/issues/276) [#472](https://github.com/astronomer/astro-sdk/issues/472)

Enhancement:
* Fail LoadFile operator when input_file does not exist [#467](https://github.com/astronomer/astro-sdk/issues/467)
* Create scripts to launch benchmark testing to Google cloud [#432](https://github.com/astronomer/astro-sdk/pull/496)
* Bump Google Provider for google extra [#294](https://github.com/astronomer/astro-sdk/pull/294)


## 0.10.0

Feature:
* Allow list and tuples as columns names in Append & Merge Operators [#343](https://github.com/astronomer/astro-sdk/issues/343), [#435](https://github.com/astronomer/astro-sdk/issues/335)

Breaking Change:
* `aql.merge` interface changed. Argument `merge_table` changed to `target_table`, `target_columns` and `merge_column` combined to `column` argument, `merge_keys` is changed to `target_conflict_columns`, `conflict_strategy` is changed to `if_conflicts`. More details can be found at [422](https://github.com/astronomer/astro-sdk/pull/422), [#466](https://github.com/astronomer/astro-sdk/issues/466)

Enhancement:
* Document (new) load_file benchmark datasets [#449](https://github.com/astronomer/astro-sdk/pull/449)
* Made improvement to benchmark scripts and configurations [#458](https://github.com/astronomer/astro-sdk/pull/458), [#434](https://github.com/astronomer/astro-sdk/issues/434), [#461](https://github.com/astronomer/astro-sdk/pull/461), [#460](https://github.com/astronomer/astro-sdk/issues/460), [#437](https://github.com/astronomer/astro-sdk/issues/437), [#462](https://github.com/astronomer/astro-sdk/issues/462)
* Performance evaluation for loading datasets with Astro Python SDK 0.9.2 into BigQuery [#437](https://github.com/astronomer/astro-sdk/issues/437)

## 0.9.2

Bug fix:
* Change export_file to return File object [#454](https://github.com/astronomer/astro-sdk/issues/454).

## 0.9.1

Bug fix:
* Table unable to have Airflow templated names [#413](https://github.com/astronomer/astro-sdk/issues/413)


## 0.9.0

Enhancements:
* Introduction of the user-facing `Table`, `Metadata` and `File` classes

Breaking changes:
* The operator `save_file` became `export_file`
* The tasks `load_file`, `export_file` (previously `save_file`) and `run_raw_sql` should be used with use `Table`, `Metadata` and `File` instances
* The decorators `dataframe`, `run_raw_sql` and `transform` should be used with `Table` and `Metadata` instances
* The operators `aggregate_check`, `boolean_check`, `render` and `stats_check` were temporarily removed
* The class `TempTable` was removed. It is possible to declare temporary tables by using `Table(temp=True)`. All the temporary tables names are prefixed with `_tmp_`. If the user decides to name a `Table`, it is no longer temporary, unless the user enforces it to be.
* The only mandatory property of a `Table` instance is `conn_id`. If no metadata is given, the library will try to extract schema and other information from the connection object. If it is missing, it will default to the `AIRFLOW__ASTRO__SQL_SCHEMA` environment variable.

Internals:
* Major refactor introducing `Database`, `File`, `FileType` and `FileLocation` concepts.

## 0.8.4

Enhancements:
* Add support for Airflow 2.3 [#367](https://github.com/astronomer/astro-sdk/pull/367).

Breaking change:
* We have renamed the artifacts we released to `astro-sdk-python` from `astro-projects`.
`0.8.4` is the last version for which we have published both `astro-sdk-python` and `astro-projects`.

## 0.8.3

Bug fix:
* Do not attempt to create a schema if it already exists [#329](https://github.com/astronomer/astro-sdk/issues/329).

## 0.8.2

Bug fix:
* Support dataframes from different databases in dataframe operator [#325](https://github.com/astronomer/astro-sdk/pull/325)

Enhancements:
* Add integration testcase for `SqlDecoratedOperator` to test execution of Raw SQL [#316](https://github.com/astronomer/astro-sdk/pull/316)

## 0.8.1

Bug fix:
* Snowflake transform without `input_table` [#319](https://github.com/astronomer/astro-sdk/issues/319)


## 0.8.0

Feature:

*`load_file` support for nested NDJSON files [#257](https://github.com/astronomer/astro-sdk/issues/257)

Breaking change:
* `aql.dataframe` switches the capitalization to lowercase by default. This behaviour can be changed by using `identifiers_as_lower` [#154](https://github.com/astronomer/astro-sdk/issues/154)

Documentation:
* Fix commands in README.md [#242](https://github.com/astronomer/astro-sdk/issues/242)
* Add scripts to auto-generate Sphinx documentation

Enhancements:
* Improve type hints coverage
* Improve Amazon S3 example DAG, so it does not rely on pre-populated data [#293](https://github.com/astronomer/astro-sdk/issues/293)
* Add example DAG to load/export from BigQuery [#265](https://github.com/astronomer/astro-sdk/issues/265)
* Fix usages of mutable default args [#267](https://github.com/astronomer/astro-sdk/issues/267)
* Enable DeepSource validation [#299](https://github.com/astronomer/astro-sdk/issues/299)
* Improve code quality and coverage

Bug fixes:
* Support `gcpbigquery` connections [#294](https://github.com/astronomer/astro-sdk/issues/294)
* Support `params` argument in `aql.render` to override SQL Jinja template values [#254](https://github.com/astronomer/astro-sdk/issues/254)
* Fix `aql.dataframe` when table arg is absent [#259](https://github.com/astronomer/astro-sdk/issues/259)

Others:
* Refactor integration tests, so they can run across all supported databases [#229](https://github.com/astronomer/astro-sdk/issues/229), [#234](https://github.com/astronomer/astro-sdk/issues/234), [#235](https://github.com/astronomer/astro-sdk/issues/235), [#236](https://github.com/astronomer/astro-sdk/issues/236), [#206](https://github.com/astronomer/astro-sdk/issues/206), [#217](https://github.com/astronomer/astro-sdk/issues/217)


## 0.7.0

Feature:
* `load_file` to a Pandas dataframe, without SQL database dependencies [#77](https://github.com/astronomer/astro-sdk/issues/77)

Documentation:
* Simplify README [#101](https://github.com/astronomer/astro-sdk/issues/101)
* Add Release Guidelines [#160](https://github.com/astronomer/astro-sdk/pull/160)
* Add Code of Conduct [#101](https://github.com/astronomer/astro-sdk/pull/101)
* Add Contribution Guidelines [#101](https://github.com/astronomer/astro-sdk/pull/101)

Enhancements:
* Add SQLite example [#149](https://github.com/astronomer/astro-sdk/issues/149)
* Allow customization of `task_id` when using `dataframe` [#126](https://github.com/astronomer/astro-sdk/issues/126)
* Use standard AWS environment variables, as opposed to `AIRFLOW__ASTRO__CONN_AWS_DEFAULT` [#175](https://github.com/astronomer/astro-sdk/issues/175)

Bug fixes:
* Fix `merge` `XComArg` support [#183](https://github.com/astronomer/astro-sdk/issues/183)
* Fixes to `load_file`:
   * `file_conn_id` support [#137](https://github.com/astronomer/astro-sdk/issues/137)
   * `sqlite_default` connection support [#158](https://github.com/astronomer/astro-sdk/issues/158)
* Fixes to `render`:
   * `conn_id` are optional in SQL files [#117](https://github.com/astronomer/astro-sdk/issues/117)
   * `database` and `schema` are optional in SQL files [#124](https://github.com/astronomer/astro-sdk/issues/124)
* Fix `transform`, so it works with SQLite [#159](https://github.com/astronomer/astro-sdk/issues/159)

Others:
* Remove `transform_file` [#162](https://github.com/astronomer/astro-sdk/pull/162)
* Improve integration tests coverage [#174](https://github.com/astronomer/astro-sdk/pull/174)

## 0.6.0

Features:
* Support SQLite [#86](https://github.com/astronomer/astro-sdk/issues/86)
* Support users who can't create schemas [#121](https://github.com/astronomer/astro-sdk/issues/121)
* Ability to install optional dependencies (amazon, google, snowflake) [#82](https://github.com/astronomer/astro-sdk/issues/82)

Enhancements:
* Change `render` so it creates a DAG as opposed to a TaskGroup [#143](https://github.com/astronomer/astro-sdk/issues/143)
* Allow users to specify a custom version of `snowflake_sqlalchemy` [#127](https://github.com/astronomer/astro-sdk/issues/127)

Bug fixes:
* Fix tasks created with `dataframe` so they inherit connection id [#134](https://github.com/astronomer/astro-sdk/issues/134)
* Fix snowflake URI issues [#102](https://github.com/astronomer/astro-sdk/issues/102)

Others:
* Run example DAGs as part of the CI [#114](https://github.com/astronomer/astro-sdk/issues/114)
* Benchmark tooling to validate performance of `load_file` [#105](https://github.com/astronomer/astro-sdk/issues/105)
