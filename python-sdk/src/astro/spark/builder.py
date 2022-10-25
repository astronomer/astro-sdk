import pyspark.sql
from delta import *


def build_spark_session(app_name, configurations, extra_packages) -> pyspark.sql.SparkSession:
    builder = pyspark.sql.SparkSession.builder.appName(app_name)
    for conf_name, conf_value in configurations.items():
        getattr(builder, "config",)(conf_name, conf_value)
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    return spark
