from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from typing import List, Dict, Union, Tuple, Any


def dim_product(dataframe: DataFrame,
                key: Union[str, List[str]],
                attributes: List[str]
                ) -> DataFrame:

    """

    :param dataframe: input dataframe
    :param key: key of the mapping
    :param attributes: the attributes to be lookup by key
    :return: resultant dataframe
    """

    # init variables
    ls_column = list()

    if isinstance(key, list):
        ls_column = ls_column + key
    else:
        ls_column.append(key)

    ls_column = ls_column + attributes

    # create list of key with only one attribute details, for 1-1 mapping
    df_uni = dataframe\
        .withColumn('concat_attr', sf.concat(*attributes))\
        .groupBy(key)\
        .agg(sf.countDistinct('concat_attr').alias('cnt_uni_attr'))\
        .filter('cnt_uni_attr = 1')

    # select only key-attri pair with only one attribute details
    df_result = dataframe\
        .join(other=df_uni, on=key, how='inner')\
        .groupBy(ls_column)\
        .count()\
        .select(ls_column)

    return df_result


def fct_review(dataframe: DataFrame,
               key: Union[str, List[str]],
               ls_filter: List[str],
               agg_mapping: Dict[str, Tuple[str, Any]],
               ) -> DataFrame:

    """

    :param dataframe: input dataframe
    :param key: unique key for aggregation
    :param ls_filter: where-condition
    :param agg_mapping: aggregation
    :return: resultant dataframe
    """

    # create list of aggregation expression
    ls_agg = [v[1](dataframe[v[0]]).alias(k) for k, v in agg_mapping.items()]

    # filtering
    df_filtered = dataframe
    for f in ls_filter:
        df_filtered = df_filtered\
            .filter(f)

    # aggregation
    df_result = df_filtered\
        .groupBy(key)\
        .agg(*ls_agg)

    return df_result
