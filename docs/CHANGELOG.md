# Changelog
## 1.0.1

### Bug fixes
* Added a check to create table only when `if_exists` is `replace` in `aql.load_file` for snowflake. [#729](https://github.com/astronomer/astro-sdk/issues/729)
* Fix the file type for NDJSON file in Data transfer job in AWS S3 to Google BigQuery. [#724](https://github.com/astronomer/astro-sdk/issues/724)
* Create a new version of imdb.csv with lowercase column names and update the examples to use it, so this change is backwards-compatible. [#721](https://github.com/astronomer/astro-sdk/issues/721), [#727](https://github.com/astronomer/astro-sdk/pull/727)
* Skip folders while processing paths in load_file operator when file patterns is passed. [#733](https://github.com/astronomer/astro-sdk/issues/733)

### Enhancements
* Updated the performance of `aql.load_file` for files for below:
  * From GCS to Snowflake up to 95%. [#712](https://github.com/astronomer/astro-sdk/issues/712)
  * From AWS S3 to Snowflake up to 77%. [#707](https://github.com/astronomer/astro-sdk/issues/707)

* Restructured the documentation in the project.toml, quickstart, readthedocs and README.md [#698](https://github.com/astronomer/astro-sdk/issues/698), [#704](https://github.com/astronomer/astro-sdk/issues/704), [#706](https://github.com/astronomer/astro-sdk/issues/706)
* Make astro-sdk-python compatible with major version of Google Providers. [#703](https://github.com/astronomer/astro-sdk/issues/703)

### Misc
* Consolidate the documentation requirements for sphinx. [#699](https://github.com/astronomer/astro-sdk/issues/699)
* Add CI/CD triggers on release branches with dependency on tests. [#672](https://github.com/astronomer/astro-sdk/issues/672)


## 1.0.0

### Features
* Improved the performance of `aql.load_file` by supporting database-specific (native) load methods.
  This is now the default behaviour. Previously, the Astro SDK Python would always use Pandas to load files to
  SQL databases which passed the data to worker node which slowed the performance.
  [#557](https://github.com/astronomer/astro-sdk/issues/557),
  [#481](https://github.com/astronomer/astro-sdk/issues/481)

  Introduced new arguments to `aql.load_file`:
    - `use_native_support` for data transfer if available on the destination (defaults to `use_native_support=True`)
    - `native_support_kwargs` is a keyword argument to be used by method involved in native support flow.
    - `enable_native_fallback` can be used to fall back to default transfer(defaults to `enable_native_fallback=True`).

  Now, there are three modes:
    - `Native`: Default, uses [Bigquery Load Job](https://cloud.google.com/bigquery/docs/loading-data) in the
       case of BigQuery and Snowflake [COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html)
       using [external stage](https://docs.snowflake.com/en/sql-reference/sql/create-stage.html) in the case of Snowflake.
    - `Pandas`: This is how datasets were previously loaded. To enable this mode, use the argument
      `use_native_support=False` in `aql.load_file`.
    - `Hybrid`: This attempts to use the native strategy to load a file to the database and if native strategy(i)
       fails , fallback to Pandas (ii) with relevant log warnings. [#557](https://github.com/astronomer/astro-sdk/issues/557)
* Allow users to specify the table schema (column types) in which a file is being loaded by using `table.columns`.
  If this table attribute is not set, the Astro SDK still tries to infer the schema by using Pandas
  (which is previous behaviour).[#532](https://github.com/astronomer/astro-sdk/issues/532)
* Add [Example DAG](../example_dags/example_bigquery_dynamic_map_task.py) for Dynamic Map Task with Astro-SDK.
  [#377](https://github.com/astronomer/astro-sdk/issues/377),[airflow-2.3.0](https://airflow.apache.org/blog/airflow-2.3.0/)

### Breaking Change
* The `aql.dataframe` argument `identifiers_as_lower` (which was `boolean`, with default set to `False`)
  was replaced by the argument `columns_names_capitalization` (`string` within possible values
  `["upper", "lower", "original"]`, default is `lower`).[#564](https://github.com/astronomer/astro-sdk/issues/564)
* The `aql.load_file` before would change the capitalization of all column titles to be uppercase, by default,
  now it makes them lowercase, by default. The old behaviour can be achieved by using the argument
  `columns_names_capitalization="upper"`. [#564](https://github.com/astronomer/astro-sdk/issues/564)
* `aql.load_file` attempts to load files to BigQuery and Snowflake by using native methods, which may have
  pre-requirements to work. To disable this mode, use the argument `use_native_support=False` in `aql.load_file`.
  [#557](https://github.com/astronomer/astro-sdk/issues/557), [#481](https://github.com/astronomer/astro-sdk/issues/481)
* `aql.dataframe` will raise an exception if the default Airflow XCom backend is being used.
  To solve this, either use an [external XCom backend, such as S3 or GCS](https://www.astronomer.io/guides/custom-xcom-backends)
  or set the configuration `AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True`. [#444](https://github.com/astronomer/astro-sdk/issues/444)
* Change the declaration for the default Astro SDK temporary schema from using `AIRFLOW__ASTRO__SQL_SCHEMA`
  to `AIRFLOW__ASTRO_SDK__SQL_SCHEMA` [#503](https://github.com/astronomer/astro-sdk/issues/503)
* Renamed `aql.truncate` to `aql.drop_table` [#554](https://github.com/astronomer/astro-sdk/issues/554)

### Bug fixes
* Fix missing airflow's task terminal states to `CleanupOperator` [#525](https://github.com/astronomer/astro-sdk/issues/525)
* Allow chaining `aql.drop_table` (previously `truncate`) tasks using the Task Flow API syntax. [#554](https://github.com/astronomer/astro-sdk/issues/554), [#515](https://github.com/astronomer/astro-sdk/issues/515)

### Enhancements
* Improved the performance of `aql.load_file` for files for below:
  * From AWS S3 to Google BigQuery up to 94%. [#429](https://github.com/astronomer/astro-sdk/issues/429), [#568](https://github.com/astronomer/astro-sdk/pull/568)
  * From Google Cloud Storage to Google BigQuery up to 93%. [#429](https://github.com/astronomer/astro-sdk/issues/429), [#562](https://github.com/astronomer/astro-sdk/issues/562)
  * From AWS S3/Google Cloud Storage to Snowflake up to 76%. [#430](https://github.com/astronomer/astro-sdk/issues/430), [#544](https://github.com/astronomer/astro-sdk/pull/544)
  * From GCS to Postgres in K8s up to 93%. [#428](https://github.com/astronomer/astro-sdk/issues/428), [#531](https://github.com/astronomer/astro-sdk/pull/531)
* Get configurations via Airflow Configuration manager. [#503](https://github.com/astronomer/astro-sdk/issues/503)
* Change catching `ValueError` and `AttributeError` to `DatabaseCustomError` [#595](https://github.com/astronomer/astro-sdk/pull/595)
* Unpin pandas upperbound dependency [#620](https://github.com/astronomer/astro-sdk/pull/620)
* Remove markupsafe from dependencies [#623](https://github.com/astronomer/astro-sdk/pull/623)
* Added `extend_existing` to Sqla Table object [#626](https://github.com/astronomer/astro-sdk/pull/626)
* Move config to store DF in XCom to settings file [#537](https://github.com/astronomer/astro-sdk/pull/537)
* Make the operator names consistent [#634](https://github.com/astronomer/astro-sdk/pull/634)
* Use `exc_info` for exception logging [#643](https://github.com/astronomer/astro-sdk/pull/643)
* Update query for getting bigquery table schema  [#661](https://github.com/astronomer/astro-sdk/pull/661)
* Use lazy evaluated Type Annotations from PEP 563 [#650](https://github.com/astronomer/astro-sdk/pull/650)
* Provide Google Cloud Credentials env var for bigquery [#679](https://github.com/astronomer/astro-sdk/pull/679)
* Handle breaking changes for Snowflake provide version 3.2.0 and 3.1.0 [#686](https://github.com/astronomer/astro-sdk/pull/686)

### Misc
* Allow running tests on PRs from forks + label [#179](https://github.com/astronomer/astro-sdk/issues/179)
* Standardize language in docs files [#678](https://github.com/astronomer/astro-sdk/pull/678)

## 0.11.0

Feature:
* Added Cleanup operator to clean temporary tables [#187](https://github.com/astronomer/astro-sdk/issues/187) [#436](https://github.com/astronomer/astro-sdk/issues/436)

Internals:
* Added a Pull Request template [#205](https://github.com/astronomer/astro-sdk/issues/205)
* Added sphinx documentation for readthedocs [#276](https://github.com/astronomer/astro-sdk/issues/276) [#472](https://github.com/astronomer/astro-sdk/issues/472)

Enhancement:
* Fail LoadFileOperator operator when input_file does not exist [#467](https://github.com/astronomer/astro-sdk/issues/467)
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
