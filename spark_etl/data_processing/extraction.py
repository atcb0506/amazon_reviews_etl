from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


def dataframe_create(spark_session: SparkSession,
                     path: str,
                     file_format: str,
                     schema: StructType = None,
                     **kwarg) -> DataFrame:

    """

    :param spark_session: spark session needed for dataframe creation
    :param path: file path
    :param file_format: format of the files
    :param schema:
    :param kwarg:
    :return:
    """

    df = spark_session \
        .read \
        .load(path=path, format=file_format, schema=schema, **kwarg)

    return df
