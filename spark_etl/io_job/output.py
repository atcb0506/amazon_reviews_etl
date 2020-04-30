import psycopg2
from pyspark.sql import DataFrame
from config.app_config import print_log
from aws.secrets import get_secret
from aws.s3 import get_s3_key


def df_to_csv(dataframe: DataFrame,
              output_path: str) -> None:

    """

    :param dataframe: input dataframe
    :param output_path: output path of data
    :return:
    """

    dataframe \
        .coalesce(1)\
        .write\
        .csv(path=output_path,
             mode='overwrite',
             sep='\t',
             header=False)


def csv_to_db(s3_bucket: str,
              s3_key: str,
              s3_region: str,
              dbname: str,
              host: str,
              port: str,
              user: str,
              password: str,
              db_table: str,
              db_schema: str,
              ) -> None:

    """

    :param s3_bucket: from which s3_bucket
    :param s3_key: from which s3_key
    :param s3_region: from which s3_region
    :param dbname: to which dbname
    :param host: to which host
    :param port: to which
    :param user:
    :param password:
    :param db_table: target table in db
    :param db_schema:
    :return:
    """

    conn = None
    cur = None

    try:

        # create redshift connection
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password
        )
        cur = conn.cursor()
        print_log(log_level='INFO', msg='Connected to the redshift database')

        # truncate table
        sql = f'truncate table {db_schema}.{db_table}'
        print_log(log_level='INFO', msg=f'EXECUTE: {sql}')
        cur.execute(sql)
        print_log(log_level='INFO', msg='Truncate completed')

        sql = f'select aws_s3.table_import_from_s3(' \
              f'\'{db_schema}.{db_table}\',' \
              f'\'\',' \
              f'\'\',' \
              f'aws_commons.create_s3_uri(' \
              f'\'{s3_bucket}\',' \
              f'\'{s3_key}\',' \
              f'\'{s3_region}\')' \
              f');'

        print_log(log_level='INFO', msg=f'EXECUTE: {sql}')
        cur.execute(sql)
        print_log(log_level='INFO', msg='Copy completed')

    except psycopg2.DatabaseError as db_error:
        conn.rollback()
        raise print_log(log_level='ERROR', msg=f'DB ERROR: {db_error}')

    finally:
        if cur is not None:
            cur.close()
            print_log(log_level='INFO', msg='Database cursor closed')

            # close connection
        if conn is not None:
            conn.commit()
            conn.close()
            print_log(log_level='INFO', msg='Database connection closed')


def df_to_db(dataframe: DataFrame,
             db_table: str,
             **kwarg) -> None:
    """

    :param dataframe: dataframe to be loaded
    :param db_table: table to be imported
    :param kwarg: paras in the config
    :return:
    """

    # get the db secret from secrets manager
    db_secret = get_secret(secret_name=kwarg['secret_name'],
                           secret_region=kwarg['secret_region'])
    kwarg.update(db_secret)

    # export dataframe to csv
    df_to_csv(dataframe=dataframe,
              output_path=f'{kwarg["output_data_path"]}/{db_table}')

    # get the key from s3
    s3_key = get_s3_key(s3_bucket=kwarg['s3_bucket'],
                        s3_prefix=db_table,
                        s3_region=kwarg['s3_region'])
    kwarg.update({'s3_key': s3_key})

    # import csv to db
    csv_to_db(db_table=db_table, **kwarg)


