import pytest
from frocket.common.config import config


@pytest.fixture(scope="session", autouse=True)
def init_mock_lambda_settings():
    config['lambda.aws.endpoint.url'] = config.get('lambda.aws.endpoint.url', 'http://localhost:9001')
    config['lambda.aws.region'] = config.get('lambda.aws.region', 'us-east-1')
    config['lambda.aws.no.signature'] = 'true'
    config['invoker.lambda.legacy.async'] = 'false'
