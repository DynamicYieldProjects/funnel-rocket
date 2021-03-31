import os
import boto3
from frocket.common.config import config, ConfigDict
from frocket.common.helpers.utils import timestamped_uuid, memoize

SKIP_S3_TESTS = os.environ.get('SKIP_S3_TESTS', "False").lower() == 'true'
if not SKIP_S3_TESTS:
    MOCK_S3_URL = os.environ.get('MOCK_S3_URL', 'http://127.0.0.1:9000')
    MOCK_S3_USER = os.environ.get('MOCK_S3_USER', 'testonly')
    MOCK_S3_SECRET = os.environ.get('MOCK_S3_SERCET', 'testonly')


@memoize
def _init_mock_s3_config():
    if SKIP_S3_TESTS:
        print(f"Skipping mock S3 config")
    config['s3.aws.endpoint.url'] = MOCK_S3_URL
    config['s3.aws.access.key.id'] = MOCK_S3_USER
    config['s3.aws.secret.access.key'] = MOCK_S3_SECRET


def mock_s3_env_variables():
    _init_mock_s3_config()
    return {
        ConfigDict.to_env_variable(key): config.get(key)
        for key in ['s3.aws.endpoint.url', 's3.aws.access.key.id', 's3.aws.secret.access.key']
    }


def new_mock_s3_bucket():
    if SKIP_S3_TESTS:
        return None
    _init_mock_s3_config()

    bucket_name = timestamped_uuid('testbucket-')
    s3 = boto3.resource('s3', **config.aws_client_settings(service='s3'))
    bucket = s3.Bucket(bucket_name)
    bucket.create()
    print(f"Bucket '{bucket_name}' created")
    return bucket
