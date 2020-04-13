from pyspark.sql import functions as sf

from config.app_util import init_log, print_log, sh_argparser
from config.config import JobConfig
from data_processing.spark_util import session_create
from data_processing.extraction import dataframe_create
from data_processing.transformation import dim_product, fct_review
from data_processing.loading import df_to_db


def main(mode: str) -> None:

    """

    :param mode: test or full run
    :return:
    """

    # init log level
    init_log(baselevel='INFO')

    # get config
    print_log(log_level='INFO', msg=f'Get config')
    dict_config = JobConfig()

    # create spark session
    spark = session_create(**dict_config.SPARK)
    print_log(log_level='INFO', msg=f'Spark session created: {spark}')

    # create spark dataframe
    print_log(log_level='INFO', msg='Creating spark dataframe...')
    df = dataframe_create(spark_session=spark,
                          path=dict_config.INPUT['input_data_path'][mode],
                          file_format='csv',
                          sep='\t',
                          header=True)
    print_log(log_level='INFO', msg=f'Spark dataframe is created')

    # clean out "\"
    df = df.withColumn('product_category', sf.regexp_replace('product_category', '\\\\', ''))
    print_log(log_level='INFO', msg=f'cleaned \\ for product_category')
    df = df.withColumn('product_title', sf.regexp_replace('product_title', '\\\\', ''))
    print_log(log_level='INFO', msg=f'cleaned \\ for product_title')

    # create dim_product
    print_log(log_level='INFO', msg='Create dim_product')
    df_dim_product = dim_product(dataframe=df,
                                 key='product_id',
                                 attributes=['product_category', 'product_title'])

    # save the df_dim_product
    print_log(log_level='INFO', msg='Save the df_dim_product')
    df_to_db(dataframe=df_dim_product,
             db_table='dim_product',
             **dict_config.OUTPUT)

    # create df_fct_review
    print_log(log_level='INFO', msg='Create df_fct_review')
    df_fct_review = fct_review(dataframe=df,
                               key=['customer_id', 'product_id'],
                               ls_filter=['verified_purchase = \'Y\''],
                               agg_mapping={'total_votes_sum': ('total_votes', sf.sum),
                                            'helpful_votes_sum': ('helpful_votes', sf.sum),
                                            'star_rating_sum': ('star_rating', sf.sum),
                                            'star_rating_mean': ('star_rating', sf.mean),
                                            'ttl_review_count': ('review_id', sf.count)})

    # save the df_fct_review
    print_log(log_level='INFO', msg='Save the df_fct_review')
    df_to_db(dataframe=df_fct_review,
             db_table='fct_review',
             **dict_config.OUTPUT)

    # Done
    print_log(log_level='INFO', msg='Done')


if __name__ == '__main__':

    main(**sh_argparser())
