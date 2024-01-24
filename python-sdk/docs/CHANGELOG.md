# Changelog

## 1.8.0

### Misc
- Replace `openlineage-airflow` with Apache Airflow OSS provider `apache-airflow-providers-openlineage` [#2103](https://github.com/astronomer/astro-sdk/pull/2103)
- Bump up minimum version of `apache-airflow` to 2.7 [#2103](https://github.com/astronomer/astro-sdk/pull/2103)
- Bump up minimum version of `Python` to 3.8 [#2103](https://github.com/astronomer/astro-sdk/pull/2103)

### Bug Fixes
- Limit `pandas` version to `<2.2.0` due to an open issue for the release `pandas==2.2.0` https://github.com/pandas-dev/pandas/issues/57053 [#2105] (https://github.com/astronomer/astro-sdk/pull/2105)


## 1.7.0

### Feature
- Allow users to disable schema check and creation on `load_file` [#1922](https://github.com/astronomer/astro-sdk/pull/1922)
- Allow users to disable schema check and creation on `transform` [#1925](https://github.com/astronomer/astro-sdk/pull/1925)
- Add support for Excel files [#1978](https://github.com/astronomer/astro-sdk/pull/1978)
- Support loading metadata columns from stage into table for Snowflake [#2023](https://github.com/astronomer/astro-sdk/pull/2023)

### Bug Fixes
- Add `openlineage_dataset_uri` in databricks [#1919](https://github.com/astronomer/astro-sdk/pull/1919)
- Fix QueryModifier issue on Snowflake [#1962](https://github.com/astronomer/astro-sdk/pull/1962)
- Fix AstroCustomXcomBackend circular import issue [#1943](https://github.com/astronomer/astro-sdk/pull/1943)

### Misc
- Add an example DAG for using dynamic task with dataframe [#1912](https://github.com/astronomer/astro-sdk/pull/1912)
- Improve `example_load_file` DAG tasks names [#1958](https://github.com/astronomer/astro-sdk/pull/1958)
- Limit `databricks-sql-connector<2.9.0` [#2013](https://github.com/astronomer/astro-sdk/pull/2013)

### Docs
- Add docs about using dtype [#1903](https://github.com/astronomer/astro-sdk/pull/1903)
- Make cleanup operator summary docs smaller [#2017](https://github.com/astronomer/astro-sdk/pull/2017)


## 1.6.2

### Bug Fixes

- Fix Snowflake QueryModifier issue [#1962](https://github.com/astronomer/astro-sdk/pull/1962)
- Add support for Pandas 2, Airflow 2.6.3 and Python 3.11 [#1989](https://github.com/astronomer/astro-sdk/pull/1989)
- Update the WASB connection [#1994](https://github.com/astronomer/astro-sdk/pull/1994)


## 1.6.1

### Bug Fixes

- Fix AstroCustomXcomBackend circular import issue. [#1943](https://github.com/astronomer/astro-sdk/pull/1943)


## 1.6.0

### Feature
- Add MySQL support [#1801](https://github.com/astronomer/astro-sdk/pull/1801)
- Add support to load from Azure blob storage into Databricks [#1561](https://github.com/astronomer/astro-sdk/pull/1561)
- Add argument  `skip_on_failure` to `CleanupOperator` [#1837](https://github.com/astronomer/astro-sdk/pull/1837) by @scottleechua
- Add `query_modifier` to `raw_sql`, `transform` and `transform_file`, which allow users to define SQL statements to be run before the main query statement [#1898](https://github.com/astronomer/astro-sdk/pull/1898).
  Example of how to use this feature can be used to add Snowflake query tags to a SQL statement:
  ```python
  from astro.query_modifier import QueryModifier


  @aql.run_raw_sql(
      results_format="pandas_dataframe",
      conn_id="sqlite_default",
      query_modifier=QueryModifier(pre_queries=["ALTER team_1", "ALTER team_2"]),
  )
  def dummy_method():
      return "SELECT 1+1"
  ```
- Upgrade astro-runtime to 7.4.2 [#1878](https://github.com/astronomer/astro-sdk/pull/1878)

### Bug fix:
- Raise exception in case larger dataframes than expected are passed to `aql.dataframe` [#1839](https://github.com/astronomer/astro-sdk/pull/1839)
- Revert breaking change introduced in 1.5.0, re-allowing `aql.transform` to receive `sql filepath [#1879](https://github.com/astronomer/astro-sdk/pull/1879)

### Docs
- Update open lineage documentation [#1881](https://github.com/astronomer/astro-sdk/pull/1881)

### Misc
- Support Apache Airflow 2.6 [#1899](https://github.com/astronomer/astro-sdk/pull/1899), with internal serialization changes
- Add basic `tiltifle` for local dev [#1819](https://github.com/astronomer/astro-sdk/pull/1819)


## 1.5.4
### Bug Fixes
- Fix AstroCustomXcomBackend circular import issue. [#1943](https://github.com/astronomer/astro-sdk/pull/1943)


## 1.5.3

### Bug fix:

- Support using SQL operators (`run_raw_sql`, `transform`, `dataframe`) to convert a Pandas dataframe into a table when using a DuckDB in-memory
database. [#1848](https://github.com/astronomer/astro-sdk/pull/1848).
- Fix code coverage issues [#1815](https://github.com/astronomer/astro-sdk/pull/1815)
- Upgrade astro-runtime to 7.4.1 [#1858](https://github.com/astronomer/astro-sdk/pull/1858)


## 1.5.2

### Improvements
- Restore pandas load option classes - `PandasCsvLoadOptions`, `PandasJsonLoadOptions`, `PandasNdjsonLoadOptions` and `PandasParquetLoadOptions` [#1795](https://github.com/astronomer/astro-sdk/pull/1795)


## 1.5.1

### Improvements
- Add Openlineage facets for Microsoft SQL server. [#1752](https://github.com/astronomer/astro-sdk/pull/1752)

### Bug fixes
- Use `use_native_support` param in load_file operator for table creation. [#1756](https://github.com/astronomer/astro-sdk/pull/1756)
- Resolved `pandas-gbq` dependency issue. [#1768](https://github.com/astronomer/astro-sdk/pull/1768)
- Fix Minio support for Snowflake. [#1767](https://github.com/astronomer/astro-sdk/pull/1767)
- Add handler param in database.run_sql() method [1773](https://github.com/astronomer/astro-sdk/pull/1773)


## 1.5.0

### Feature:
- Add support for Microsoft SQL server. [#1538](https://github.com/astronomer/astro-sdk/pull/1538)
- Add support for DuckDB. [#1695](https://github.com/astronomer/astro-sdk/pull/1695)
- Add `result_format` and `fail_on_empty` params to `run_raw_sql` operator [#1584](https://github.com/astronomer/astro-sdk/pull/1584)
- Add support `validation_mode` as part of the `COPY INTO` command for snowflake. [#1689](https://github.com/astronomer/astro-sdk/pull/1689)
- Add support for native transfers for Azure Blob Storage to Snowflake in `LoadFileOperator`. [#1675](https://github.com/astronomer/astro-sdk/pull/1675)

### Improvements
- Use cache to reduce redundant database calls [#1488](https://github.com/astronomer/astro-sdk/pull/1488)
- Remove default `copy_options` as part of `SnowflakeLoadOptions`. All `copy_options` are now supported as part of `SnowflakeLoadOptions` as per [documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions). [#1689](https://github.com/astronomer/astro-sdk/pull/1689)
- Remove `load_options` from `File` object. [#1721](https://github.com/astronomer/astro-sdk/pull/1721)
- Render SQL code with parameters in BaseSQLDecoratedOperator. [#897](https://github.com/astronomer/astro-sdk/pull/897)

### Bug fixes
- Fix handling of multiple dataframes in the `run_raw_sql` operator. [#1700](https://github.com/astronomer/astro-sdk/pull/1700)

### Docs
- Add documentation around Microsoft SQL support with example DAG. [#1538](https://github.com/astronomer/astro-sdk/pull/1538)
- Add documentation around DuckDB support with example DAG. [#1695](https://github.com/astronomer/astro-sdk/pull/1695)
- Add documentation for `validation_mode` as part of the `COPY INTO` command for snowflake. [#1689](https://github.com/astronomer/astro-sdk/pull/1689)
- Add documentation and example DAGs for snowflake `SnowflakeLoadOptions` for various available options around `copy_options` and `file_options`. [#1689](https://github.com/astronomer/astro-sdk/pull/1689)
- Fix the documentation to run the quickstart example described in the Python SDK README. [#1716](https://github.com/astronomer/astro-sdk/pull/1716)

### Misc
- Add cleanup DAG to clean snowflake tables created as part of CI when the runners fail as part of GitHub actions. [#1663](https://github.com/astronomer/astro-sdk/issues/1663)
- Run example DAGs on astro-cloud and collect the results. [#1499](https://github.com/astronomer/astro-sdk/pull/1499)

### Breaking Change
- Consolidated `PandasCsvLoadOptions`, `PandasJsonLoadOptions`, `PandasNdjsonLoadOptions` and `PandasParquetLoadOptions` to single `PandasLoadOptions`. [#1722](https://github.com/astronomer/astro-sdk/pull/1722)


## 1.4.1

### Feature:
- Implement `check_table` Operator to validate data quality at table level [#1239](https://github.com/astronomer/astro-sdk/pull/1239)
- Add `check_column` Operator to validate data quality for columns in a table/dataframe [#1239](https://github.com/astronomer/astro-sdk/pull/1239)

## Bug fixes
- Support "s3" conn type for S3Location [#1647](https://github.com/astronomer/astro-sdk/pull/1647)

### Docs
- Add the documentation and example DAG for Azure blob storage [#1598](https://github.com/astronomer/astro-sdk/pull/1598)
- Fix dead link in documentation [#1596](https://github.com/astronomer/astro-sdk/pull/1596)
- Update README with newly supported location and database [#1596](https://github.com/astronomer/astro-sdk/pull/1579)
- Update configuration reference for XCom [#1646](https://github.com/astronomer/astro-sdk/pull/1646)
- Add step to generate constraints in Python SDK release process [#1474](https://github.com/astronomer/astro-sdk/issues/1474)
- Add document to showcase the use of `check_table` and `check_column` operators [#1631](https://github.com/astronomer/astro-sdk/pull/1631)

### Misc
- Install `google-cloud-sdk-gke-gcloud-auth-plugin` in benchmark CI job [#1557](https://github.com/astronomer/astro-sdk/issues/1557)
- Pin `sphinx-autoapi==2.0.0` version for docs build [#1609](https://github.com/astronomer/astro-sdk/pull/1609)


## 1.4.0

### Feature:

- Support SFTP as file location [docs](https://astro-sdk-python.readthedocs.io/en/1.4/astro/sql/operators/load_file.html#loading-data-from-sftp) [#1481](https://github.com/astronomer/astro-sdk/pull/1481)
- Support FTP as file location [docs](https://astro-sdk-python.readthedocs.io/en/1.4/astro/sql/operators/load_file.html#loading-data-from-ftp) [#1482](https://github.com/astronomer/astro-sdk/pull/1482)
- Add support for Azure Blob Storage (*only non-native implementation*) [#1275](https://github.com/astronomer/astro-sdk/pull/1275), [#1542](https://github.com/astronomer/astro-sdk/pull/1542)
- Add databricks delta table support [docs](https://astro-sdk-python.readthedocs.io/en/1.4/guides/databricks.html) [#1352](https://github.com/astronomer/astro-sdk/pull/1352), [#1397](https://github.com/astronomer/astro-sdk/pull/1397), [#1452](https://github.com/astronomer/astro-sdk/pull/1452), [#1476](https://github.com/astronomer/astro-sdk/pull/1476), [#1480](https://github.com/astronomer/astro-sdk/pull/1480), [#1555](https://github.com/astronomer/astro-sdk/pull/1555)
- Add [sourceCode](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#job-facets) facet to  `aql.dataframe()` and `aql.transform()` as part of OpenLineage integration [#1537](https://github.com/astronomer/astro-sdk/pull/1537)
- Enhance `LoadFileOperator` so that users can send pandas attributes through `PandasLoadOptions` [docs](https://astro-sdk-python.readthedocs.io/en/1.4/astro/sql/operators/load_file.html#loadoptions) [#1466](https://github.com/astronomer/astro-sdk/pull/1466)
- Enhance `LoadFileOperator` so that users can send Snowflake specific load attributes through  `SnowflakeLoadOptions` [docs](https://astro-sdk-python.readthedocs.io/en/1.4/astro/sql/operators/load_file.html#loadoptions) [#1516](https://github.com/astronomer/astro-sdk/pull/1516)
- Expose `get_file_list_func` to users so that it returns iterable File list from given destination file storage [#1380](https://github.com/astronomer/astro-sdk/pull/1380)

### Improvements
- Deprecate `export_table_to_file` in favor of `export_to_file` (*`ExportTableToFileOperator` and `export_table_to_file` operator would be removed in astro-python-sdk 1.5.0*) [#1503](https://github.com/astronomer/astro-sdk/pull/1503)

### Bug fixes
- `LoadFileOperator` operator checks for `conn_type` and `conn_id` provided to `File` [#1471](https://github.com/astronomer/astro-sdk/issues/1471)
- Generate constraints on releases and pushes (not PRs) [#1472](https://github.com/astronomer/astro-sdk/pull/1472)

### Docs
- Change `export_file` to `export_table_to_file`  in the documentation [#1477](https://github.com/astronomer/astro-sdk/pull/1477)
- Enhance documentation to describe the new Xcom requirements from Astro SDK 1.3.3 and airflow 2.5 [#1483](https://github.com/astronomer/astro-sdk/pull/1483)
- Add documentation around `LoadOptions` with example DAGs [#1567](https://github.com/astronomer/astro-sdk/pull/1567)

### Misc
- Refactor snowflake merge function for easier maintenance [#1493](https://github.com/astronomer/astro-sdk/pull/1493)


## 1.3.3

### Bug fixes
- Disable Custom serialization for Back-compat [#1453](https://github.com/astronomer/astro-sdk/pull/1453)
- Use different approach to get location for Bigquery tables [#1449](https://github.com/astronomer/astro-sdk/pull/1449)

## 1.3.2

### Bug fixes
- Fix the `run_raw_sql()` operator as handler return `None` causing the serialization logic to fail. [#1431](https://github.com/astronomer/astro-sdk/pull/1431)

### Misc
- Update the deprecation warning for `export_file()` operator. [#1411](https://github.com/astronomer/astro-sdk/issues/1411)


## 1.3.1

### Feature:
- Dataframe operator would now allow a user to either `append` to a table or `replace` a table with `if_exists` parameter. [#1379](https://github.com/astronomer/astro-sdk/issues/1379)

### Bug fixes
- Fix the `aql.cleanup()` operator as failing as the attribute `output` was implemented in 2.4.0 [#1359](https://github.com/astronomer/astro-sdk/issues/1359)
- Fix the backward compatibility with `apache-airflow-providers-snowflake==4.0.2`. [#1351](https://github.com/astronomer/astro-sdk/issues/1351)
- LoadFile operator returns a dataframe if not using XCom backend.[#1348](https://github.com/astronomer/astro-sdk/pull/1348),[#1337](https://github.com/astronomer/astro-sdk/issues/1337)
- Fix the functionality to create region specific temporary schemas when they don't exist in same region. [#1369](https://github.com/astronomer/astro-sdk/issues/1369)

### Docs
- Cross-link to API reference page from Operators page.[#1383](https://github.com/astronomer/astro-sdk/issues/1383)

### Misc
- Improve the integration tests to count the number of rows impacted for database operations. [#1273](https://github.com/astronomer/astro-sdk/issues/1273)
- Run python-sdk tests with airflow 2.5.0 and fix the CI failures. [#1232](https://github.com/astronomer/astro-sdk/issues/1232), [#1351](https://github.com/astronomer/astro-sdk/issues/1351),[#1317](https://github.com/astronomer/astro-sdk/pull/1317), [#1337](https://github.com/astronomer/astro-sdk/issues/1337)
- Deprecate `export_file` before renaming to `export_table_to_file`. [#1411](https://github.com/astronomer/astro-sdk/issues/1411)

## 1.3.0

### Feature:
- Remove the need to use a custom Xcom backend for storing dataframes when Xcom pickling is disabled. [#1334](https://github.com/astronomer/astro-sdk/pull/1334), [#1331](https://github.com/astronomer/astro-sdk/pull/1331),[#1319](https://github.com/astronomer/astro-sdk/pull/1319)
- Add support to Google Drive to be used as `FileLocation` . Example to load file from Google Drive to Snowflake [#1044](https://github.com/astronomer/astro-sdk/issues/1044)

  ```python
  aql.load_file(
      input_file=File(
          path="gdrive://sample-google-drive/sample.csv", conn_id="gdrive_conn"
      ),
      output_table=Table(
          conn_id=SNOWFLAKE_CONN_ID,
          metadata=Metadata(
              database=os.environ["SNOWFLAKE_DATABASE"],
              schema=os.environ["SNOWFLAKE_SCHEMA"],
          ),
      ),
  )
  ```

### Improvements
- Use `DefaultExtractor` from OpenLineage. Users need not set environment variable `OPENLINEAGE_EXTRACTORS` to use OpenLineage. [#1223](https://github.com/astronomer/astro-sdk/issues/1223), [#1292](https://github.com/astronomer/astro-sdk/issues/1292)
- Generate constraints file for multiple Python and Airflow version that display the set of "installable" constraints for a particular Python (3.7, 3.8, 3.9) and Airflow version (2.2.5, 2.3.4, 2.4.2) [#1226](https://github.com/astronomer/astro-sdk/issues/1226)
- Improve the logs in case native transfers fallbacks to Pandas as well as fallback indication in `LoadFileOperator`. [#1263](https://github.com/astronomer/astro-sdk/issues/1263)

### Bug fixes
- Temporary tables should be cleaned up, even with mapped tasks via `aql.cleanup()` [#963](https://github.com/astronomer/astro-sdk/issues/963)
- Update the name and namespace as per Open Lineage new conventions introduced [here](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md#local-file-system). [#1281](https://github.com/astronomer/astro-sdk/issues/1281)
- Delete the Snowflake stage when `LoadFileOperator` fails. [#1262](https://github.com/astronomer/astro-sdk/issues/1262)

### Docs
- Update the documentation for Google Drive support. [#1044](https://github.com/astronomer/astro-sdk/issues/1044)
- Update the documentation to remove the environment variable `OPENLINEAGE_EXTRACTORS` to use OpenLineage. [#1292](https://github.com/astronomer/astro-sdk/issues/1292)

### Misc
- Fix the GCS path in `aql.export_file` in the example DAGs. [#1339](https://github.com/astronomer/astro-sdk/issues/1339)

## 1.2.3

### Bug fixes
* When `if_exists` is set to `replace` in Dataframe operator, replace the table rather than append. This change fixes a regression on the Dataframe operator which caused it to append content to an output table instead of replacing.  [#1260](https://github.com/astronomer/astro-sdk/issues/1260)
* Pass the table metadata `database` value to the underlying airflow `PostgresHook` instead of `schema` as schema is renamed to database in airflow as per this [PR](https://github.com/apache/airflow/pull/26744). [#1276](https://github.com/astronomer/astro-sdk/pull/1276)

### Docs
* Include description on pickling and usage of custom Xcom backend in README.md [#1203](https://github.com/astronomer/astro-sdk/issues/1203)

### Misc
* Investigate and fix tests that are filling up Snowflake database with tmp tables as part of our CI execution. [#738](https://github.com/astronomer/astro-sdk/issues/738)

## 1.2.2

### Bug fixes
* Make `openlineage` an optional dependency [#1252](https://github.com/astronomer/astro-sdk/pull/1252)
* Update snowflake-sqlalchemy version [#1228](https://github.com/astronomer/astro-sdk/pull/1228)
* Raise error if dataframe is empty [#1238](https://github.com/astronomer/astro-sdk/pull/1238)
* Raise error db mismatch of operation [#1233](https://github.com/astronomer/astro-sdk/pull/1233)
* Pass `task_id` to be used for parent class on `LoadFileOperator` init [#1259](https://github.com/astronomer/astro-sdk/pull/1259)

## 1.2.1

### Feature:
* Add support for Minio [#750](https://github.com/astronomer/astro-sdk/issues/750)
* Open Lineage support - Add Extractor for `ExportFileOperator`, `DataframeOperator` [#903](https://github.com/astronomer/astro-sdk/issues/903), [#1183](https://github.com/astronomer/astro-sdk/issues/1183)

### Bug fixes
* Add check for missing conn_id on transform operator. [#1152](https://github.com/astronomer/astro-sdk/issues/1152)
* Raise error when `copy into` query fails in snowflake. [#890](https://github.com/astronomer/astro-sdk/issues/890)
* Transform op - database/schema is not picked from table's metadata. [#1034](https://github.com/astronomer/astro-sdk/issues/1034)

### Improvement:
* Change the namespace for Open Lineage [#1179](https://github.com/astronomer/astro-sdk/issues/1179)
* Add `LOAD_FILE_ENABLE_NATIVE_FALLBACK` config to globally disable native
fallback [#1089](https://github.com/astronomer/astro-sdk/issues/1089)
* Add `OPENLINEAGE_EMIT_TEMP_TABLE_EVENT` config to emit events for tmp table in Open Lineage. [#1121](https://github.com/astronomer/astro-sdk/issues/1121)
* Fix issue with fetching table row count for snowflake [#1145](https://github.com/astronomer/astro-sdk/issues/1145)
* Generate unique Open Lineage namespace for Sqlite based operations [#1141](https://github.com/astronomer/astro-sdk/issues/1141)

### Docs
* Include section in docs to cover file pattern for native path of GCS to Bigquery . [#800](https://github.com/astronomer/astro-sdk/issues/800)
* Add guide for Open Lineage integration with Astro Python SDK [#1116](https://github.com/astronomer/astro-sdk/issues/1116)

### Misc
* Pin SQLAlchemy version to >=1.3.18,<1.4.42 [#1185](https://github.com/astronomer/astro-sdk/pull/1185)


## 1.2.0

### Feature:
* Remove dependency on `AIRFLOW__CORE__ENABLE_XCOM_PICKLING`. Users can set new environment variables, namely `AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID` and `AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL` and use a custom XCOM backend namely, `AstroCustomXcomBackend` which enables the XCOM data to be saved to an S3 or GCS location. [#795](https://github.com/astronomer/astro-sdk/issues/795), [#997](https://github.com/astronomer/astro-sdk/pull/997)
* Added OpenLineage support for `LoadFileOperator` , `AppendOperator` , `TransformOperator` and `MergeOperator` [#898](https://github.com/astronomer/astro-sdk/issues/898), [#899](https://github.com/astronomer/astro-sdk/issues/899), [#902](https://github.com/astronomer/astro-sdk/issues/902), [#901](https://github.com/astronomer/astro-sdk/issues/901) and [#900](https://github.com/astronomer/astro-sdk/issues/900)
* Add `TransformFileOperator` that
   - parses a SQL file with templating
   - applies all needed parameters
   - runs the SQL to return a table object to keep the `aql.transform_file` function, the function can return `TransformFileOperator().output` in a similar fashion to the merge operator. [#892](https://github.com/astronomer/astro-sdk/issues/892)
* Add the implementation for row count for `BaseTable`. [#1073](https://github.com/astronomer/astro-sdk/issues/1073)

### Improvement:
* Improved handling of snowflake identifiers for smooth experience with `dataframe` and `run_raw_sql` and `load_file` operators. [#917](https://github.com/astronomer/astro-sdk/issues/917), [#1098](https://github.com/astronomer/astro-sdk/issues/1098)
* Fix `transform_file` to not depend on `transform` decorator [#1004](https://github.com/astronomer/astro-sdk/pull/1004)
* Set the CI to run and publish benchmark reports once a week [#443](https://github.com/astronomer/astro-sdk/issues/443)
* Fix cyclic dependency and improve import time. Reduces the import time for `astro/databases/__init__.py` from 23.254 seconds to 0.062 seconds [#1013](https://github.com/astronomer/astro-sdk/pull/1013)

### Docs
* Create GETTING_STARTED.md [#1036](https://github.com/astronomer/astro-sdk/pull/1036)
* Document the Open Lineage facets published by Astro Python SDK. [#1086](https://github.com/astronomer/astro-sdk/issues/1086)
* Documentation changes to specify permissions needed for running BigQuery jobs. [#896](https://github.com/astronomer/astro-sdk/issues/896)
* Document the details on custom XCOM. [#1100](https://github.com/astronomer/astro-sdk/issues/1100)
* Document the benchmarking process. [#1017](https://github.com/astronomer/astro-sdk/issues/1017)
* Include a detailed description on the default Dataset concept in Astro Python SDK. [#1092](https://github.com/astronomer/astro-sdk/pull/1092)

### Misc
- NFS volume mount in Kubernetes to test benchmarking from local to databases. [#883](https://github.com/astronomer/astro-sdk/issues/883)

## 1.1.1

### Improvements
* Add filetype when resolving path in case of loading into dataframe [#881](https://github.com/astronomer/astro-sdk/issues/881)

### Bug fixes
* Fix postgres performance regression (example from one_gb file - 5.56min to 1.84min) [#876](https://github.com/astronomer/astro-sdk/issues/876)


## 1.1.0

### Features
* Add native autodetect schema feature [#780](https://github.com/astronomer/astro-sdk/pull/780)
* Allow users to disable auto addition of inlets/outlets via airflow.cfg [#858](https://github.com/astronomer/astro-sdk/pull/858)
* Add support for Redshift [#639](https://github.com/astronomer/astro-sdk/pull/639), [#753](https://github.com/astronomer/astro-sdk/pull/753), [#700](https://github.com/astronomer/astro-sdk/pull/700)
* Support for Datasets introduced in Airflow 2.4 [#786](https://github.com/astronomer/astro-sdk/pull/786), [#808](https://github.com/astronomer/astro-sdk/pull/808), [#862](https://github.com/astronomer/astro-sdk/pull/862), [#871](https://github.com/astronomer/astro-sdk/pull/871)

    - `inlets` and `outlets` will be automatically set for all the operators.
    - Users can now schedule DAGs on `File` and `Table` objects. Example:

      ```python
      input_file = File(
          path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
      )
      imdb_movies_table = Table(name="imdb_movies", conn_id="sqlite_default")
      top_animations_table = Table(name="top_animation", conn_id="sqlite_default")
      START_DATE = datetime(2022, 9, 1)


      @aql.transform()
      def get_top_five_animations(input_table: Table):
          return """
              SELECT title, rating
              FROM {{input_table}}
              WHERE genre1='Animation'
              ORDER BY rating desc
              LIMIT 5;
          """


      with DAG(
          dag_id="example_dataset_producer",
          schedule=None,
          start_date=START_DATE,
          catchup=False,
      ) as load_dag:
          imdb_movies = aql.load_file(
              input_file=input_file,
              task_id="load_csv",
              output_table=imdb_movies_table,
          )

      with DAG(
          dag_id="example_dataset_consumer",
          schedule=[imdb_movies_table],
          start_date=START_DATE,
          catchup=False,
      ) as transform_dag:
          top_five_animations = get_top_five_animations(
              input_table=imdb_movies_table,
              output_table=top_animations_table,
          )
      ```
* Dynamic Task Templates: Tasks that can be used with Dynamic Task Mapping (Airflow 2.3+)
  * Get list of files from a Bucket - `get_file_list` [#596](https://github.com/astronomer/astro-sdk/pull/596)
  * Get list of values from a DB - `get_value_list` [#673](https://github.com/astronomer/astro-sdk/pull/673), [#867](https://github.com/astronomer/astro-sdk/pull/867)

* Create upstream_tasks parameter for dependencies independent of data transfers [#585](https://github.com/astronomer/astro-sdk/pull/585)


### Improvements
* Avoid loading whole file into memory with load_operator for schema detection [#805](https://github.com/astronomer/astro-sdk/pull/805)
* Directly pass the file to native library when native support is enabled [#802](https://github.com/astronomer/astro-sdk/pull/802)
* Create file type for patterns for schema auto-detection [#872](https://github.com/astronomer/astro-sdk/pull/872)

### Bug fixes
* Add compat module for typing execute `context` in operators [#770](https://github.com/astronomer/astro-sdk/pull/770)
* Fix sql injection issues [#807](https://github.com/astronomer/astro-sdk/pull/807)
* Add response_size to run_raw_sql and warn about db thrashing [#815](https://github.com/astronomer/astro-sdk/pull/815)

### Docs
* Update quick start example [#819](https://github.com/astronomer/astro-sdk/pull/819)
* Add links to docs from README [#832](https://github.com/astronomer/astro-sdk/pull/832)
* Fix Astro CLI doc link [#842](https://github.com/astronomer/astro-sdk/pull/842)
* Add configuration details from settings.py [#861](https://github.com/astronomer/astro-sdk/pull/861)
* Add section explaining table metadata [#774](https://github.com/astronomer/astro-sdk/pull/774)
* Fix docstring for run_raw_sql [#817](https://github.com/astronomer/astro-sdk/pull/817)
* Add missing docs for Table class [#788](https://github.com/astronomer/astro-sdk/pull/788)
* Add the readme.md example dag to example dags folder [#681](https://github.com/astronomer/astro-sdk/pull/681)
* Add reason for enabling XCOM pickling [#747](https://github.com/astronomer/astro-sdk/pull/747)

## 1.0.2

### Bug fixes
* Skip folders while processing paths in load_file operator when file pattern is passed. [#733](https://github.com/astronomer/astro-sdk/issues/733)

### Misc
* Limit Google Protobuf for compatibility with bigquery client. [#742](https://github.com/astronomer/astro-sdk/issues/742)


## 1.0.1

### Bug fixes
* Added a check to create table only when `if_exists` is `replace` in `aql.load_file` for snowflake. [#729](https://github.com/astronomer/astro-sdk/issues/729)
* Fix the file type for NDJSON file in Data transfer job in AWS S3 to Google BigQuery. [#724](https://github.com/astronomer/astro-sdk/issues/724)
* Create a new version of imdb.csv with lowercase column names and update the examples to use it, so this change is backwards-compatible. [#721](https://github.com/astronomer/astro-sdk/issues/721), [#727](https://github.com/astronomer/astro-sdk/pull/727)
* Skip folders while processing paths in load_file operator when file patterns is passed. [#733](https://github.com/astronomer/astro-sdk/issues/733)

### Docs
* Updated the Benchmark docs for GCS to Snowflake and S3 to Snowflake of `aql.load_file` [#712](https://github.com/astronomer/astro-sdk/issues/712)[#707](https://github.com/astronomer/astro-sdk/issues/707)

* Restructured the documentation in the `project.toml`, quickstart, readthedocs and README.md [#698](https://github.com/astronomer/astro-sdk/issues/698), [#704](https://github.com/astronomer/astro-sdk/issues/704), [#706](https://github.com/astronomer/astro-sdk/issues/706)
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
  To solve this, either use an [external XCom backend, such as S3 or GCS](https://docs.astronomer.io/learn/xcom-backend-tutorial)
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
