import boto3
import base64
from botocore.exceptions import ClientError
from json import loads
from typing import Dict, Union
from config.app_config import print_log


def get_secret(secret_name: str,
               secret_region: str) -> Dict[str, Union[str, int]]:

    """

    :param secret_name: the secret name from secrets manager
    :param secret_region: the region name for secrets manager
    :return: secrets
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(
        region_name=secret_region
    )
    client = session.client(
        service_name='secretsmanager'
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise print_log(log_level='ERROR', msg=e.response["Error"]["Code"])

    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = loads(get_secret_value_response['SecretString'])
            print_log(log_level='INFO', msg='Got secret in string')
        else:
            secret = loads(base64.b64decode(get_secret_value_response['SecretBinary']))
            print_log(log_level='INFO', msg='Got secret in binary')

    result = {'host': secret['host'],
              'dbname': secret['dbname'],
              'user': secret['user'],
              'port': secret['port'],
              'password': secret['password']}

    return result
