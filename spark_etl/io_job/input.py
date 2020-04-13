from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Dict, Any


def dataframe_create(spark_session: SparkSession,
                     config: Dict[str, Any]) -> DataFrame:

    """

    :param spark_session: spark session needed for dataframe creation
    :param config: etl config
    :return:
    """

    # init config
    path = config["input_data_path"]
    file_format = config['file_format']
    sep = config['sep']
    header = config['header']

    df = spark_session \
        .read \
        .load(path=path,
              format=file_format,
              schema=None,
              file_format=file_format,
              sep=sep,
              header=header)

    return df
