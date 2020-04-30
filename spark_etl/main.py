from config.app_config import init_log, print_log, sh_argparser
from config.io_config import IOConfig
from config.job_config import PipelineConfig
from config.spark_config import SparkConfig
from spark.spark_util import session_create
from io_job.input import dataframe_create
from etl_class.resultset import Resultset
from etl_class.cleansing import Cleansing


def main(mode: str) -> None:

    """

    :param mode: test or full run
    :return:
    """

    # init log level
    init_log(baselevel='INFO')

    # get config
    print_log(log_level='INFO', msg=f'Get config')
    io_config = IOConfig(mode=mode)
    spark_config = SparkConfig()
    pipeline_config = PipelineConfig()

    # create spark session
    spark_session = session_create(config=spark_config.SPARK)
    print_log(log_level='INFO', msg=f'Spark session created: {spark_session}')

    # create spark dataframe
    print_log(log_level='INFO', msg='Creating spark dataframe...')
    df = dataframe_create(spark_session=spark_session,
                          config=io_config.DF_CONFIG)
    print_log(log_level='INFO', msg=f'Spark dataframe is created')

    # global cleansing pipeline
    df_cleansed = df
    for dict_job in pipeline_config.CLEANSING_PIPELINE:
        print_log(log_level='INFO', msg=dict_job['job'])
        cleansing_obj = Cleansing(df=df_cleansed,
                                  meta_job_config=dict_job['job_config'])
        cleansing_obj.transformation()
        df_cleansed = cleansing_obj.get_df()

    # resultset pipeline
    for dict_job in pipeline_config.RESULTSET_PIPELINE:
        print_log(log_level='INFO', msg=dict_job['job'])
        resultset_obj = Resultset(df=df_cleansed,
                                  meta_job_config=dict_job['job_config'],
                                  output_config=io_config.OUTPUT_CONFIG)
        resultset_obj.transformation()
        resultset_obj.load()

    # done
    print_log(log_level='INFO', msg='Done')


if __name__ == '__main__':

    main(**sh_argparser())
