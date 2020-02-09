import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from typing import List


def session_create(master: str,
                   app_name: str) -> SparkSession:

    """

    :param master: Sets the Spark master URL to connect to
    :param app_name: Sets a name for the application, which will be shown in the Spark web UI
    :return: SparkSession
    """

    spark = SparkSession \
        .builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()

    return spark


def print_log(log_level: str,
              msg: str) -> None:

    """

    :param log_level: DEBUG, INFO, WARNING, ERROR
    :param msg: log message
    :return:
    """

    print('-' * 100)
    print(f'[{log_level.upper()}] | {datetime.datetime.utcnow()} | {msg}')
