from astro.spark.builder import build_spark_session
from astro.files import File
from astro.files.locations import FileLocation


def load_to_spark_df(file: File):
    configs = {}
    extra_packages = []
    if file.location.location_type == FileLocation.S3:
        configs.update(file.location.spark_config())
        extra_packages.extend(file.location.spark_packages())

    spark = build_spark_session("foo", configs, extra_packages)
    df = spark.read.format("csv").load(file.path, header=True, infer_schema=True)

    return df