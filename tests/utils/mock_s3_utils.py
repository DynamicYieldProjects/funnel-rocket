import os
import boto3
from frocket.common.config import config
from frocket.common.helpers.utils import timestamped_uuid, memoize

SKIP_MOCK_S3_TESTS = os.environ.get('SKIP_LOCAL_S3_TESTS', False)
if not SKIP_MOCK_S3_TESTS:
    MOCK_S3_URL = os.environ.get('MOCK_S3_URL', 'http://127.0.0.1:9000')
    MOCK_S3_USER = os.environ.get('MOCK_S3_USER', 'testonly')
    MOCK_S3_SECRET = os.environ.get('MOCK_S3_SERCET', 'testonly')


@memoize
def _init_mock_s3_config():
    if SKIP_MOCK_S3_TESTS:
        print(f"Skipping mock S3 config")
    config['aws.endpoint.url'] = MOCK_S3_URL
    config['aws.access.key.id'] = MOCK_S3_USER
    config['aws.secret.access.key'] = MOCK_S3_SECRET


def new_mock_s3_bucket():
    if SKIP_MOCK_S3_TESTS:
        return None
    _init_mock_s3_config()

    bucket_name = timestamped_uuid('testbucket-')
    s3 = boto3.resource('s3', **config.aws_client_settings(service='s3'))
    bucket = s3.Bucket(bucket_name)
    bucket.create()
    print(f"Bucket '{bucket_name}' created")
    return bucket
