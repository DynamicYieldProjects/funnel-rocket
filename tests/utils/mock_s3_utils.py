#  Copyright 2021 The Funnel Rocket Maintainers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import boto3
from frocket.common.config import config, ConfigDict
from frocket.common.helpers.utils import timestamped_uuid, memoize

SKIP_S3_TESTS = os.environ.get('SKIP_S3_TESTS', "False").lower() == 'true'


@memoize
def _init_mock_s3_config():
    if SKIP_S3_TESTS:
        print(f"Skipping mock S3 config")
    config['s3.aws.endpoint.url'] = \
        os.environ.get('MOCK_S3_URL', config.get('s3.aws.endpoint.url', 'http://localhost:9000'))
    config['s3.aws.access.key.id'] = \
        os.environ.get('MOCK_S3_USER', config.get('s3.aws.access.key.id', 'testonly'))
    config['s3.aws.secret.access.key'] = \
        os.environ.get('MOCK_S3_SERCET', config.get('s3.aws.secret.access.key', 'testonly'))


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
