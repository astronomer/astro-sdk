# Changelog

## 0.6.0

Features:
* Support SQLite (#86)
* Support users who can't create schemas (#121)
* Ability to install optional dependencies (amazon, google, snowflake) (#82)

Enhancements:
* Change `render` so it creates a DAG as opposed to a TaskGroup (#143)
* Allow users to specify a custom version of `snowflake_sqlalchemy` (#127)

Bug fixes:
* Fix tasks created with `dataframe` so they inherit connection id (#134)
* Fix snowflake URI issues (#102)

Others:
* Run example DAGs as part of the cI (#114)
* Benchmark tooling to validate performance of load_file (#105)