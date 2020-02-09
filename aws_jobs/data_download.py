import boto3
import os


def s3_download(profile_name: str,
                bucket: str,
                prefix: str = '',
                local_path: str = None) -> None:

    """
    to download data from s3
    :param profile_name:
    :param bucket:
    :param prefix:
    :param local_path:
    :return:
    """

    session = boto3.Session(profile_name=profile_name)
    s3 = session.client(service_name='s3')

    # init
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
        'Prefix': prefix,
    }

    # loop the list of objects from s3 until there is no next_token is found
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})  # append the next_token to be ContinuationToken
        results = s3.list_objects_v2(**kwargs)
        contents = results.get('Contents')

        # for each content, store the keys and directories separately
        for content in contents:
            k = content.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)

        next_token = results.get('NextContinuationToken')

    for d in dirs:

        dest_pathname = os.path.join(local_path, d)

        if not os.path.exists(os.path.dirname(dest_pathname)):
            print(f'[INFO] Created dir: {os.path.dirname(dest_pathname)}')
            os.makedirs(os.path.dirname(dest_pathname))

    for k in keys:

        dest_pathname = os.path.join(local_path, k)

        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
            print(f'[INFO] data_download.py: Created dir: {os.path.dirname(dest_pathname)}')

        print(f'[INFO] data_download.py: Downloaded file: {dest_pathname}')
        s3.download_file(Bucket=bucket, Key=k, Filename=dest_pathname)


if __name__ == '__main__':

    # download amazon review data from https://s3.console.aws.amazon.com/s3/buckets/amazon-reviews-pds/?region=us-east-1
    s3_download(profile_name='willis',
                bucket='amazon-reviews-pds',
                prefix='tsv',
                local_path='data')


