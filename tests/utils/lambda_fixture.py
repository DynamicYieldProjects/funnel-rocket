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

import pytest
from frocket.common.config import config


@pytest.fixture(scope="session", autouse=True)
def init_mock_lambda_settings():
    config['lambda.aws.endpoint.url'] = config.get('lambda.aws.endpoint.url', 'http://localhost:9001')
    config['lambda.aws.region'] = config.get('lambda.aws.region', 'us-east-1')
    config['lambda.aws.no.signature'] = 'true'
    config['invoker.lambda.legacy.async'] = 'false'
