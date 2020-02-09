from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def session_create(session_loglevel: str) -> SparkSession:

    """

    :param session_loglevel: set log level of the spark session
    :return: SparkSession
    """

    spark_config = SparkConf()

    spark_session = SparkSession\
        .builder\
        .config(conf=spark_config)\
        .getOrCreate()

    spark_session.sparkContext.setLogLevel(session_loglevel)

    return spark_session
