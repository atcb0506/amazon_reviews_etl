from pyspark.sql import functions as sf

from spark_etl.util import print_log
from spark_etl.util import session_create
from spark_etl.extraction import dataframe_create
from spark_etl.transformation import dim_product, fct_review
from spark_etl.loading import to_csv

if __name__ == '__main__':

    # create spark session
    spark = session_create(master='local[*]',
                           app_name='my_first_sparkapp')

    print_log(log_level='INFO', msg=f'Spark session created: {spark}')

    # create spark dataframe
    print_log(log_level='INFO', msg='Creating spark dataframe...')
    df = dataframe_create(spark_session=spark,
                          path='data/tsv/amazon_reviews_multilingual_DE_v1_00.tsv.gz',
                          file_format='csv',
                          sep='\t',
                          header=True)

    print_log(log_level='INFO', msg=f'Spark dataframe is created')

    # create dim_product
    print_log(log_level='INFO', msg='Create dim_product')
    df_dim_product = dim_product(dataframe=df,
                                 key='product_id',
                                 attributes=['product_category', 'product_title'])

    # save the df_dim_product
    print_log(log_level='INFO', msg='Save the df_dim_product')
    to_csv(dataframe=df_dim_product,
           output_path='output_data/dim_product')

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

    # show the df_fct_review
    print_log(log_level='INFO', msg='Save the df_fct_review')
    to_csv(dataframe=df_fct_review,
           output_path='output_data/fct_review')
