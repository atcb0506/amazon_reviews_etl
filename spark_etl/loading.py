from pyspark.sql import DataFrame


def to_csv(dataframe: DataFrame,
           output_path: str) -> None:

    """

    :param dataframe: input dataframe
    :param output_path: output path of data
    :return:
    """

    dataframe.write.csv(output_path)
