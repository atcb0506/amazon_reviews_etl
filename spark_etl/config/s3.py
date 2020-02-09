import boto3
from config.app_util import print_log


def get_s3_key(s3_bucket: str,
               s3_prefix: str,
               s3_region: str) -> str:

    """

    :param s3_bucket:
    :param s3_prefix:
    :param s3_region:
    :return:
    """

    session = boto3.Session(region_name=s3_region)
    s3 = session.client(service_name='s3')

    results = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    ls_key = list()
    for item in results['Contents']:
        if item['Key'] == f'{s3_prefix}/_SUCCESS':
            continue
        ls_key.append(item['Key'])

    if len(ls_key) > 1:
        print_log(log_level='ERROR', msg='More than 1 file in the s3 bucket/prefix')
        raise Exception('More than 1 file in the s3 bucket/prefix')

    return ls_key[0]
