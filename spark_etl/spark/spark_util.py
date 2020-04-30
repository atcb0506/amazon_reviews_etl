from pyspark.sql import SparkSession
from typing import Dict, Any


def session_create(config: Dict[str ,Any]) -> SparkSession:

    """

    :param config: spark session config
    :return: SparkSession
    """

    # init config
    session_loglevel = config['session_loglevel']

    spark_session = SparkSession\
        .builder\
        .getOrCreate()

    spark_session.sparkContext.setLogLevel(session_loglevel)

    return spark_session
